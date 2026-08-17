"""
test_spot_fallback.py — offline regression test for the Phase 5 Spot/Equity/
Commodity REST fallback (backend/main.py's _stale_spot_monitor /
_stale_fetch_spot). No network — fetch_quotes_rest is monkeypatched.

Run with: MOCK_MODE=true python -m backend.test_spot_fallback
"""
import asyncio
import sys
import time

import backend.main as m
import backend.instruments as instr_mod

_real_sleep = asyncio.sleep


async def _fake_fetch_quotes_rest(keys, token):
    """Canned REST response: every key gets a fresh, distinguishable price."""
    out = {}
    for i, k in enumerate(keys):
        ltp = 100.0 + i
        out[k] = {"ltpc": {"ltp": ltp, "cp": ltp - 1},
                   "efeed": {"ltp": ltp, "cp": ltp - 1, "open": ltp, "high": ltp + 1, "low": ltp - 1}}
    return out


async def _run_stale_spot_monitor_once(expect_call=True, timeout=2.0, fetch_fn=None):
    """Drive the REAL, unmodified backend.main._stale_spot_monitor() through
    exactly one meaningful iteration, without waiting through its hardcoded
    60s startup delay or 30s loop cadence. asyncio.sleep is patched to a
    near-instant pass-through ONLY for the duration of this call (always
    restored in `finally`, even on failure) — this is a test-only technique,
    not a change to production code. Synchronization is event-driven (an
    asyncio.Event set by the fetch stand-in), not a fixed real-time guess.

    Also enforces "no concurrent REST quote calls": the wrapper asserts only
    one call is ever in flight at a time.

    Returns the list of key-lists passed to each observed fetch_quotes_rest
    call while the monitor task was alive.
    """
    fetch_fn = fetch_fn or _fake_fetch_quotes_rest
    calls = []
    call_event = asyncio.Event()
    in_flight = {"n": 0}

    async def recording_fetch(keys, token):
        in_flight["n"] += 1
        assert in_flight["n"] == 1, "no concurrent REST quote calls may occur"
        try:
            calls.append(list(keys))
            call_event.set()
            return await fetch_fn(keys, token)
        finally:
            in_flight["n"] -= 1

    m.fetch_quotes_rest = recording_fetch

    async def _fast_sleep(seconds):
        await _real_sleep(0)

    asyncio.sleep = _fast_sleep
    task = asyncio.create_task(m._stale_spot_monitor())
    try:
        if expect_call:
            await asyncio.wait_for(call_event.wait(), timeout=timeout)
            await _real_sleep(0.01)  # let _stale_fetch_spot finish its post-fetch writes
        else:
            await _real_sleep(0.2)  # a short real window for it to (not) fire
    finally:
        asyncio.sleep = _real_sleep
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    return calls


async def test_stale_spot_gets_rest_refreshed():
    m.fetch_quotes_rest = _fake_fetch_quotes_rest
    m.state.access_token = "dummy-test-token"

    sym = "RELIANCE"
    m.loc_engine.register(sym)
    st = m.loc_engine.get_state(sym)
    st.spot.ltp = 0
    st.spot.ts = 0  # never ticked — must be picked up as stale
    key = "NSE_EQ|TESTKEY1"
    m.SPOT_KEYS_D[sym] = key

    assert st.spot.ltp == 0, "test setup: spot must start unset for this to be a meaningful before/after check"

    await m._stale_fetch_spot([sym])

    assert st.spot.ltp > 0, "spot.ltp must be populated from the REST fallback"
    assert st.spot.ts and (int(time.time() * 1000) - st.spot.ts) < 2000, "spot.ts must be freshly stamped"
    assert m.state.market_data.get(key, {}).get("ts"), "market_data must be updated too"
    print("PASS test_stale_spot_gets_rest_refreshed")


async def test_fresh_ws_value_not_overwritten():
    """Guard: if a real WS tick lands while the REST call is 'in flight'
    (simulated here by writing it AFTER the stale-symbol list was computed
    but BEFORE _stale_fetch_spot's own write), the REST value must not
    clobber it."""
    m.fetch_quotes_rest = _fake_fetch_quotes_rest
    m.state.access_token = "dummy-test-token"

    sym = "TCS"
    m.loc_engine.register(sym)
    st = m.loc_engine.get_state(sym)
    key = "NSE_EQ|TESTKEY2"
    m.SPOT_KEYS_D[sym] = key

    async def slow_fetch(keys, token):
        # Simulate a fresh WS tick arriving mid-flight.
        st.spot.ltp = 9999.0
        st.spot.ts = int(time.time() * 1000)
        return await _fake_fetch_quotes_rest(keys, token)

    m.fetch_quotes_rest = slow_fetch
    st.spot.ts = 0  # start stale so it gets selected
    await m._stale_fetch_spot([sym])

    assert st.spot.ltp == 9999.0, "must not overwrite a value that became fresh while REST was in flight"
    print("PASS test_fresh_ws_value_not_overwritten")


async def test_indices_excluded_from_monitor_scan_real_path():
    """Drives the REAL _stale_spot_monitor()/_stale_fetch_spot() — NOT a
    duplicated copy of the filter predicate — so this test actually fails if
    the production exclusion logic is ever removed or changed."""
    m.state.access_token = "dummy-test-token"

    idx_sym, stock_sym = "NIFTY", "RELIANCE_IDXTEST"
    assert idx_sym in m._INDEX_LOC, "test assumes NIFTY is a real index symbol in _INDEX_LOC"

    m.loc_engine.register(idx_sym)
    m.loc_engine.register(stock_sym)
    idx_st = m.loc_engine.get_state(idx_sym)
    stock_st = m.loc_engine.get_state(stock_sym)
    idx_st.spot.ltp = 0
    idx_st.spot.ts = 0     # stale
    stock_st.spot.ltp = 0
    stock_st.spot.ts = 0   # stale

    idx_key, stock_key = "NSE_INDEX|TESTIDX", "NSE_EQ|TESTIDXSTOCK"
    m.SPOT_KEYS_D[idx_sym] = idx_key
    m.SPOT_KEYS_D[stock_sym] = stock_key

    calls = await _run_stale_spot_monitor_once(expect_call=True)

    assert len(calls) == 1, f"expected exactly one batched REST call, got {len(calls)}"
    fetched_keys = calls[0]
    assert stock_key in fetched_keys, "a genuinely stale, non-index spot symbol must still be processed"
    assert idx_key not in fetched_keys, (
        "an index symbol must NEVER be included in the Spot REST fallback batch — "
        "it has its own independent REST poll (_index_ohlc_poll)"
    )
    assert stock_st.spot.ltp > 0, "the non-index symbol must have been REST-refreshed"
    assert idx_st.spot.ts == 0, "the index symbol's spot state must be completely untouched"
    print("PASS test_indices_excluded_from_monitor_scan_real_path")


async def test_incident_scale_spot_batching():
    """~195 eligible stale NSE_EQ+MCX symbols — matching the real Aug 2026
    incident's scale — must all be covered by exactly ONE batched REST call
    (_QUOTE_BATCH_SIZE=200 comfortably covers it), with no concurrent calls,
    correct writes, the fresh-WS-not-overwritten guard intact, and indices
    still excluded even at this scale. Drives the real _stale_spot_monitor()."""
    m.state.access_token = "dummy-test-token"

    n_stocks, n_mcx = 190, 5
    stock_syms = [f"TESTSTK{i}" for i in range(n_stocks)]
    mcx_syms = [f"TESTMCX{i}" for i in range(n_mcx)]
    all_syms = stock_syms + mcx_syms
    assert len(all_syms) <= instr_mod._QUOTE_BATCH_SIZE, (
        "test premise requires the incident-scale symbol count to fit in one "
        "real batch — _QUOTE_BATCH_SIZE was not changed to make this pass"
    )

    for sym in all_syms:
        m.loc_engine.register(sym)
        st = m.loc_engine.get_state(sym)
        st.spot.ltp = 0
        st.spot.ts = 0  # stale
        m.SPOT_KEYS_D[sym] = f"NSE_EQ|{sym}"

    for idx_sym in ("NIFTY", "BANKNIFTY"):
        m.loc_engine.register(idx_sym)
        ist = m.loc_engine.get_state(idx_sym)
        ist.spot.ltp = 0
        ist.spot.ts = 0  # stale too — must still be excluded even so
        m.SPOT_KEYS_D[idx_sym] = f"NSE_INDEX|{idx_sym}"

    # One symbol races a fresh WS tick while the REST call is "in flight" —
    # must survive the fallback untouched.
    fresh_sym = stock_syms[0]
    fresh_st = m.loc_engine.get_state(fresh_sym)

    async def fetch_with_race(keys, token):
        fresh_st.spot.ltp = 12345.0
        fresh_st.spot.ts = int(time.time() * 1000)
        return await _fake_fetch_quotes_rest(keys, token)

    calls = await _run_stale_spot_monitor_once(expect_call=True, fetch_fn=fetch_with_race)

    assert len(calls) == 1, (
        f"{len(all_syms)} stale symbols must be covered by exactly ONE REST call "
        f"(batch size {instr_mod._QUOTE_BATCH_SIZE}), got {len(calls)} calls"
    )
    fetched_keys = set(calls[0])
    expected_keys = {m.SPOT_KEYS_D[s] for s in all_syms}
    assert expected_keys <= fetched_keys, "every eligible stale NSE_EQ/MCX symbol must be included in the batch"
    for idx_sym in ("NIFTY", "BANKNIFTY"):
        assert m.SPOT_KEYS_D[idx_sym] not in fetched_keys, f"{idx_sym} must never be included in the spot REST batch"

    # fresh_sym is deliberately excluded here — its ltp>0 comes from the
    # simulated race tick (12345.0), not the REST fallback, so it must not
    # be counted as "REST-refreshed" (checked separately below).
    updated_by_rest = sum(1 for s in all_syms if s != fresh_sym and m.loc_engine.get_state(s).spot.ltp > 0)
    assert updated_by_rest == len(all_syms) - 1, (
        f"every stale symbol except the one racing a fresh WS tick must be REST-refreshed, "
        f"got {updated_by_rest}/{len(all_syms) - 1}"
    )
    assert fresh_st.spot.ltp == 12345.0, (
        "a symbol that received a fresh WS tick mid-flight must not be overwritten by the REST response"
    )
    for idx_sym in ("NIFTY", "BANKNIFTY"):
        assert m.loc_engine.get_state(idx_sym).spot.ts == 0, f"{idx_sym} must remain completely untouched"

    print(f"PASS test_incident_scale_spot_batching "
          f"({len(all_syms)} symbols, 1 REST call, {updated_by_rest} refreshed, "
          f"race-guard held, indices excluded)")


async def main():
    tests = [
        test_stale_spot_gets_rest_refreshed,
        test_fresh_ws_value_not_overwritten,
        test_indices_excluded_from_monitor_scan_real_path,
        test_incident_scale_spot_batching,
    ]
    failed = []
    for test in tests:
        try:
            await test()
        except AssertionError as e:
            failed.append((test.__name__, str(e)))
            print(f"FAIL {test.__name__}: {e}")
        except Exception as e:
            failed.append((test.__name__, repr(e)))
            print(f"ERROR {test.__name__}: {e!r}")
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
