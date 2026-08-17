"""
test_ticker_lifecycle.py — offline regression tests for backend/history2/ticker.py's
reconnect lifecycle (Phases 1-3 of the Aug 2026 Zerodha WS outage fix).

No network, no real Zerodha connection, no pytest dependency — plain asserts,
a fake KiteTicker double, and asyncio.run(). Run with:

    python -m backend.test_ticker_lifecycle

Safety: os._exit is monkeypatched to a no-op recorder for the ENTIRE run,
before any test executes, so a bug in the escalation path can never kill
this test process.
"""
import asyncio
import os
import sys
import time

# ---- Safety net: never allow a real process exit from this test run ----
_os_exit_calls = []
os._exit = lambda code=0: _os_exit_calls.append(code)  # noqa: E731

import backend.history2.ticker as ticker_mod  # noqa: E402
from backend.history2.ticker import Ticker2   # noqa: E402
from backend.history2.state import state2     # noqa: E402


class FakeKiteTicker:
    """Test double for kiteconnect.KiteTicker. Class-level `behavior` scripts
    what happens when .connect() is called on the NEXT instance created:
      - "connect_ok":     on_connect fires immediately (synchronous, like a
                           same-thread simulation of a fast successful handshake)
      - "connect_silent": connect() does nothing at all — reproduces the
                           exact production symptom (51 attempts, zero callbacks)
      - "connect_async":  connect() returns immediately WITHOUT firing
                           on_connect — matching the real KiteTicker(threaded=True),
                           which hands off to a background thread and returns
                           before the handshake finishes. The test must call
                           fire_on_connect() explicitly to complete it, so the
                           subscribe()-before-on_connect race can be driven
                           deterministically instead of guessed at with timing.
    """
    MODE_LTP = "ltp_mode"
    MODE_QUOTE = "quote_mode"
    MODE_FULL = "full_mode"

    behavior = "connect_ok"
    instances = []

    def __init__(self, api_key, access_token, reconnect=True,
                 reconnect_max_tries=300, reconnect_max_delay=30):
        self.api_key = api_key
        self.access_token = access_token
        self._connected = False
        self.subscribed = set()
        self.mode_calls = []
        self.closed = False
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None
        self.on_reconnect = None
        self.on_noreconnect = None
        FakeKiteTicker.instances.append(self)

    def connect(self, threaded=True):
        if FakeKiteTicker.behavior == "connect_ok":
            self._connected = True
            if self.on_connect:
                self.on_connect(self, {})
        # "connect_silent": intentionally do nothing — no callback ever fires,
        # matching the 51 real, observed production attempts.
        # "connect_async": also does nothing here — see fire_on_connect().

    def fire_on_connect(self):
        """Test helper for behavior='connect_async': manually completes the
        handshake that a real threaded connect() would finish asynchronously,
        on its own thread, sometime after connect() itself already returned."""
        self._connected = True
        if self.on_connect:
            self.on_connect(self, {})

    def is_connected(self):
        return self._connected

    def close(self):
        self.closed = True
        self._connected = False

    def subscribe(self, tokens):
        self.subscribed |= set(tokens)

    def unsubscribe(self, tokens):
        self.subscribed -= set(tokens)

    def set_mode(self, mode, tokens):
        self.mode_calls.append((mode, tuple(tokens)))

    def simulate_tick(self, token, price):
        if self.on_ticks:
            self.on_ticks(self, [{"instrument_token": token, "last_price": price}])


def _reset():
    ticker_mod.KiteTicker = FakeKiteTicker
    FakeKiteTicker.instances = []
    FakeKiteTicker.behavior = "connect_ok"
    state2.access_token = "dummy-test-token"
    ticker_mod._within_market_hours = lambda *a, **kw: True
    os.environ.pop("ZERODHA_WS_AUTO_RESTART", None)
    _os_exit_calls.clear()


_ORIG_WATCHDOG_INTERVAL_S = ticker_mod.WATCHDOG_INTERVAL_S
_ORIG_STALE_TICK_THRESHOLD_S = ticker_mod.STALE_TICK_THRESHOLD_S


def _patch_constants(grace=0.05, threshold=3, cooldown=0.3,
                      watchdog_interval=None, stale_threshold=None):
    ticker_mod.RECONNECT_VERIFY_GRACE_S = grace
    ticker_mod.RECONNECT_FAILURE_ESCALATION_THRESHOLD = threshold
    ticker_mod.RESTART_COOLDOWN_S = cooldown
    # Only touched by tests that specifically need the watchdog LOOP itself
    # to run within test time (default: real production values, so every
    # other test's background watchdog task stays effectively dormant).
    ticker_mod.WATCHDOG_INTERVAL_S = (watchdog_interval if watchdog_interval is not None
                                       else _ORIG_WATCHDOG_INTERVAL_S)
    ticker_mod.STALE_TICK_THRESHOLD_S = (stale_threshold if stale_threshold is not None
                                          else _ORIG_STALE_TICK_THRESHOLD_S)


async def _noop_tick(token, price, ts):
    pass


async def _noop_status(status):
    pass


async def _drain(seconds=0.2):
    """Let any pending _verify_reconnect task finish before the next test
    starts — otherwise a task left over from a test that didn't itself wait
    for its own reconnect verification can fire mid-way through a LATER
    test and print a confusing (but harmless — it's bound to the earlier
    test's own Ticker2 instance) log line. Purely test-output hygiene, not
    a correctness issue: each Ticker2 instance's own state is never shared
    across tests."""
    await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# Test 1 — normal startup: connects, tokens subscribe, MODE_FULL restored,
# live ticks arrive.
# ---------------------------------------------------------------------------
async def test_normal_startup():
    _reset()
    _patch_constants()
    t = Ticker2()
    received = []

    async def on_tick(token, price, ts):
        received.append((token, price))

    t.start(on_tick, _noop_status)
    assert t.kws is not None and t.kws.is_connected(), "should connect on first start()"
    assert t._connect_generation == 1

    t.subscribe([111, 222], "full")
    assert t.kws.mode_calls[-1] == (FakeKiteTicker.MODE_FULL, (111, 222)), \
        "MODE_FULL must be restored for subscribed tokens"

    t.kws.simulate_tick(111, 100.5)
    await asyncio.sleep(0.01)
    assert received == [(111, 100.5)], "listener must receive live ticks"
    assert t.consecutive_reconnect_failures == 0
    await _drain()
    print("PASS test_normal_startup")


# ---------------------------------------------------------------------------
# Test 2 — daily token refresh: the exact Bug A regression test. A healthy
# existing kws must be reused (no duplicate socket); a dead one must be torn
# down and replaced.
# ---------------------------------------------------------------------------
async def test_token_refresh_reconnects_when_dead():
    _reset()
    _patch_constants()
    t = Ticker2()
    t.start(_noop_tick, _noop_status)
    first_kws = t.kws
    assert first_kws.is_connected()

    # Case A: healthy existing connection — start() must NOT reconnect.
    state2.access_token = "fresh-token-but-socket-still-fine"
    t.start(_noop_tick, _noop_status)
    assert t.kws is first_kws, "must not create a duplicate connection when existing one is healthy"
    assert len(FakeKiteTicker.instances) == 1

    # Case B: the exact production bug — old kws silently died (is_connected()
    # False) but the object reference is still non-None. Old code returned
    # here forever; new code must tear down and reconnect.
    first_kws._connected = False
    t.start(_noop_tick, _noop_status)
    assert t.kws is not first_kws, "a dead existing ticker must be replaced, not reused"
    assert first_kws.closed is True, "the dead ticker must be torn down (closed)"
    assert t.kws.is_connected()
    assert len(FakeKiteTicker.instances) == 2, "exactly one new connection, no duplicates"
    await _drain()
    print("PASS test_token_refresh_reconnects_when_dead")


# ---------------------------------------------------------------------------
# Test — asynchronous connect/subscribe race. The real KiteTicker(threaded=True)
# returns from connect() immediately, while on_connect fires later from its
# own background thread — unlike "connect_ok"'s synchronous simulation. This
# exercises the real, UNMODIFIED subscribe() queueing path
# (ticker.py's own existing comments describe exactly this race).
# ---------------------------------------------------------------------------
async def test_async_connect_subscribe_race():
    _reset()
    _patch_constants()
    FakeKiteTicker.behavior = "connect_async"
    t = Ticker2()
    t.start(_noop_tick, _noop_status)
    kws = t.kws
    assert kws is not None
    assert not kws.is_connected(), "connect_async must not report connected until fire_on_connect()"

    # subscribe() called BEFORE the (simulated) async handshake completes.
    t.subscribe([111, 222], "full")
    assert t.subscribed_tokens == {111, 222}, "intent must be recorded even though the socket isn't open yet"
    assert kws.subscribed == set(), "must NOT have been sent over the wire yet — socket isn't open"

    # The handshake now actually completes.
    kws.fire_on_connect()
    assert kws.is_connected()
    assert kws.subscribed == {111, 222}, "on_connect's resubscribe must restore the queued tokens"
    # set(subscribed_tokens) iteration order isn't guaranteed, so compare the
    # token set rather than an exact tuple.
    last_mode, last_tokens = kws.mode_calls[-1] if kws.mode_calls else (None, ())
    assert last_mode == FakeKiteTicker.MODE_FULL and set(last_tokens) == {111, 222}, \
        "correct subscription mode must be restored for all queued tokens"
    assert len(FakeKiteTicker.instances) == 1, "no duplicate ticker/subscription created by the race"

    await _drain()
    print("PASS test_async_connect_subscribe_race")


# ---------------------------------------------------------------------------
# Test 3 — disconnect -> watchdog -> reconnect -> on_connect -> resubscription
# -> fresh tick -> recovery.
# ---------------------------------------------------------------------------
async def test_watchdog_reconnect_recovers():
    _reset()
    _patch_constants(grace=0.05)
    t = Ticker2()
    t.start(_noop_tick, _noop_status)
    t.subscribed_tokens = {111}
    t.token_modes = {111: "full"}

    # Simulate silent death exactly as the watchdog would detect it.
    t.kws._connected = False
    t.last_tick_at = time.time() - 9999
    t._force_reconnect()

    assert t.kws.is_connected(), "force_reconnect must actually reconnect"
    assert t.kws.subscribed == {111}, "on_connect must resubscribe all prior tokens"
    assert t.kws.mode_calls and t.kws.mode_calls[-1][0] == FakeKiteTicker.MODE_FULL, \
        "on_connect must restore the correct subscription mode"

    t.kws.simulate_tick(111, 99.9)
    await asyncio.sleep(0.1)  # let a stray _verify_reconnect no-op past
    assert t.consecutive_reconnect_failures == 0
    assert t.last_reconnect_verified_at > 0
    print("PASS test_watchdog_reconnect_recovers")


# ---------------------------------------------------------------------------
# Test — is_connected() "lying" (the documented pre-existing caveat: the
# KiteTicker's own connected flag can stay True after an unclean disconnect).
# This is a SYNTHETIC simulation of that documented behavior, not a proof of
# real Zerodha/Twisted internals — it exists to confirm the watchdog's
# last_tick_at-based staleness check is still an effective backstop even when
# is_connected() itself would fool start()'s own reuse-vs-reconnect check.
# ---------------------------------------------------------------------------
async def test_is_connected_lies_watchdog_backstop():
    _reset()
    # Speed up the watchdog loop itself (not just verification) so this test
    # doesn't need to wait through the real 60s/150s production cadence.
    # stale_threshold is kept well above the total wait below (0.3s) so only
    # the ORIGINAL (999s-old) staleness can ever trigger a reconnect here —
    # once on_connect resets last_tick_at, no in-test idle gap can reach it,
    # keeping the test deterministic (exactly one reconnect, not a race).
    _patch_constants(grace=0.05, watchdog_interval=0.05, stale_threshold=1.0)
    try:
        t = Ticker2()
        t.start(_noop_tick, _noop_status)
        # The watchdog loop no-ops entirely when there are no subscribed
        # tokens (see _watchdog_loop's own "if not self.subscribed_tokens:
        # continue" guard) — a real deployment always has tokens subscribed
        # by the time this matters, so set some here too.
        t.subscribed_tokens = {111}
        t.token_modes = {111: "full"}
        dead_kws = t.kws
        assert dead_kws.is_connected() is True  # the "lie": the flag never flips False

        t.last_tick_at = time.time() - 999  # no real tick has ever actually arrived
        await asyncio.sleep(0.3)  # several watchdog cycles at the patched 0.05s interval

        assert t.kws is not dead_kws, (
            "the watchdog must still force a reconnect via last_tick_at staleness "
            "even though is_connected() never reported the lie — this is the backstop, "
            "not a claim that is_connected() itself was fixed"
        )
        # Confirm recovery can still complete normally afterward.
        t.kws.simulate_tick(111, 50.0)
        await asyncio.sleep(0.1)
        assert t.consecutive_reconnect_failures == 0
        print("PASS test_is_connected_lies_watchdog_backstop "
              "(models the documented is_connected() caveat — does not prove "
              "real Zerodha/Twisted internals)")
    finally:
        # These two are watchdog-cadence constants shared with production
        # defaults — always restore, even on assertion failure, so no other
        # test's background watchdog task is affected.
        ticker_mod.WATCHDOG_INTERVAL_S = _ORIG_WATCHDOG_INTERVAL_S
        ticker_mod.STALE_TICK_THRESHOLD_S = _ORIG_STALE_TICK_THRESHOLD_S


# ---------------------------------------------------------------------------
# Test 4 — failed reconnect: the exact production symptom (connect() called,
# nothing ever fires). Must detect failure, increment counter, escalate at
# threshold, and NOT restart when ZERODHA_WS_AUTO_RESTART is unset.
# ---------------------------------------------------------------------------
async def test_failed_reconnect_escalates_without_restart():
    _reset()
    _patch_constants(grace=0.05, threshold=3)
    FakeKiteTicker.behavior = "connect_silent"
    t = Ticker2()
    t.start(_noop_tick, _noop_status)  # first connect also "silent" here — fine, same code path

    for _ in range(3):
        t._force_reconnect()
        await asyncio.sleep(0.12)  # > grace, let _verify_reconnect judge failure

    assert t.consecutive_reconnect_failures >= 3, "failures must accumulate across attempts"
    assert _os_exit_calls == [], "must NOT restart when ZERODHA_WS_AUTO_RESTART is unset/false"
    print("PASS test_failed_reconnect_escalates_without_restart")


# ---------------------------------------------------------------------------
# Test — superseded generation must not affect the CURRENT generation's
# failure state. Asserts the generation guard directly (end state), rather
# than relying on log output.
# ---------------------------------------------------------------------------
async def test_superseded_generation_does_not_count_as_failure():
    _reset()
    _patch_constants(grace=0.05)
    FakeKiteTicker.behavior = "connect_silent"  # gen=1: connect() does nothing
    t = Ticker2()
    t.start(_noop_tick, _noop_status)  # generation 1, verify(1) scheduled for T+0.05
    assert t._connect_generation == 1

    # Before gen=1's verify task fires, a NEW reconnect supersedes it — and
    # THIS one succeeds with a real tick, so the only thing that could still
    # wrongly report a failure is generation 1's now-superseded verify task.
    FakeKiteTicker.behavior = "connect_ok"
    await asyncio.sleep(0.01)  # well before gen=1's 0.05s grace deadline
    t._force_reconnect()  # -> generation 2, connects successfully
    assert t._connect_generation == 2
    t.kws.simulate_tick(111, 42.0)  # gen=2 gets a real tick immediately
    assert t.consecutive_reconnect_failures == 0

    # Let time pass well beyond gen=1's original grace deadline so its verify
    # task actually executes its late, superseded check.
    await asyncio.sleep(0.08)

    assert t._connect_generation == 2, "no further reconnect should have happened"
    assert t.consecutive_reconnect_failures == 0, (
        "generation 1's late verify task fired AFTER being superseded by "
        "generation 2 and must be a no-op — the generation guard "
        "(generation != self._connect_generation) must have caught it"
    )
    print("PASS test_superseded_generation_does_not_count_as_failure")


# ---------------------------------------------------------------------------
# Optional test — a reconnect that succeeds slightly LATE (after the T+45s
# verification grace already marked a transient failure) must self-correct
# the moment the real tick arrives. Documents the accepted limitation that
# verification is a single check, not a redesign of it.
# ---------------------------------------------------------------------------
async def test_slow_reconnect_self_corrects_after_late_tick():
    _reset()
    _patch_constants(grace=0.05)
    FakeKiteTicker.behavior = "connect_silent"  # no tick at connect time
    t = Ticker2()
    t.start(_noop_tick, _noop_status)
    await asyncio.sleep(0.08)  # past the 0.05s grace — verify marks a transient failure
    assert t.consecutive_reconnect_failures == 1

    # The "late" first tick finally arrives.
    t.kws.simulate_tick(111, 77.0)
    assert t.consecutive_reconnect_failures == 0, \
        "a late-but-real tick must reset the failure counter immediately"
    print("PASS test_slow_reconnect_self_corrects_after_late_tick")


# ---------------------------------------------------------------------------
# Test 4b — with auto-restart explicitly enabled: escalation triggers exactly
# one (mocked) process exit, and a cooldown prevents a restart loop.
# ---------------------------------------------------------------------------
async def test_escalation_restart_when_enabled_respects_cooldown():
    _reset()
    _patch_constants(grace=0.05, threshold=2, cooldown=0.3)
    os.environ["ZERODHA_WS_AUTO_RESTART"] = "true"
    FakeKiteTicker.behavior = "connect_silent"
    t = Ticker2()
    t.start(_noop_tick, _noop_status)

    for _ in range(2):
        t._force_reconnect()
        await asyncio.sleep(0.12)
    assert len(_os_exit_calls) == 1, "must escalate to a restart exactly once at the threshold"

    # Keep failing immediately — cooldown must block a second restart.
    for _ in range(2):
        t._force_reconnect()
        await asyncio.sleep(0.12)
    assert len(_os_exit_calls) == 1, "cooldown must prevent a restart loop"
    os.environ.pop("ZERODHA_WS_AUTO_RESTART", None)
    print("PASS test_escalation_restart_when_enabled_respects_cooldown")


# ---------------------------------------------------------------------------
# Test — _reconnect_started_at / _first_tick_at must stay bounded across many
# reconnects over a long-running process (the memory-growth fix). Never
# prunes the current generation.
# ---------------------------------------------------------------------------
async def test_generation_dicts_stay_bounded():
    _reset()
    _patch_constants(grace=0.05)
    t = Ticker2()
    t.start(_noop_tick, _noop_status)  # generation 1

    n_extra = ticker_mod.MAX_TRACKED_GENERATIONS + 10
    for _ in range(n_extra):
        t.kws._connected = False
        t.last_tick_at = time.time() - 9999
        t._force_reconnect()
        t.kws.simulate_tick(111, 1.0)  # resolve each generation immediately

    total_generations = 1 + n_extra
    assert t._connect_generation == total_generations

    assert len(t._reconnect_started_at) <= ticker_mod.MAX_TRACKED_GENERATIONS, (
        f"_reconnect_started_at must stay bounded, got {len(t._reconnect_started_at)} "
        f"entries after {total_generations} generations"
    )
    assert len(t._first_tick_at) <= ticker_mod.MAX_TRACKED_GENERATIONS, (
        f"_first_tick_at must stay bounded, got {len(t._first_tick_at)} entries "
        f"after {total_generations} generations"
    )
    # The current generation's own bookkeeping must never be pruned.
    assert t._connect_generation in t._reconnect_started_at
    assert t._connect_generation in t._first_tick_at
    # And verification/failure semantics must still work normally afterward.
    assert t.consecutive_reconnect_failures == 0

    await _drain()
    print(f"PASS test_generation_dicts_stay_bounded "
          f"({total_generations} generations -> "
          f"{len(t._reconnect_started_at)}/{len(t._first_tick_at)} tracked entries)")


# ---------------------------------------------------------------------------
# Test 10 — repeated recovery: the disconnect/reconnect cycle must succeed
# consistently, not just once, with no duplicate/leaked ticker instances.
# ---------------------------------------------------------------------------
async def test_repeated_recovery_cycles():
    _reset()
    _patch_constants(grace=0.05)
    t = Ticker2()
    t.start(_noop_tick, _noop_status)

    for cycle in range(5):
        t.kws._connected = False
        t.last_tick_at = time.time() - 9999
        t._force_reconnect()
        assert t.kws.is_connected(), f"cycle {cycle}: must reconnect"
        t.kws.simulate_tick(111, 100 + cycle)
        await asyncio.sleep(0.02)
        assert t.consecutive_reconnect_failures == 0, f"cycle {cycle}: must verify as recovered"

    assert len(FakeKiteTicker.instances) == 6, "1 initial + 5 reconnects, no duplicate connections"
    await _drain()
    print("PASS test_repeated_recovery_cycles (5/5 recoveries)")


# ---------------------------------------------------------------------------
# Test — health() state machine sanity (used by /api/status, Phase 4)
# ---------------------------------------------------------------------------
async def test_health_states():
    _reset()
    _patch_constants(threshold=2)
    t = Ticker2()
    assert t.health()["state"] == "disconnected"

    t.start(_noop_tick, _noop_status)
    assert t.health()["state"] == "connected"
    assert t.health()["connected"] is True

    FakeKiteTicker.behavior = "connect_silent"
    t.kws._connected = False
    t._force_reconnect()
    h = t.health()
    assert h["connected"] is False
    assert h["state"] == "reconnecting"

    t.consecutive_reconnect_failures = 2
    h = t.health()
    assert h["state"] == "recovery_failed"
    await _drain()
    print("PASS test_health_states")


async def main():
    tests = [
        test_normal_startup,
        test_token_refresh_reconnects_when_dead,
        test_async_connect_subscribe_race,
        test_watchdog_reconnect_recovers,
        test_is_connected_lies_watchdog_backstop,
        test_failed_reconnect_escalates_without_restart,
        test_superseded_generation_does_not_count_as_failure,
        test_slow_reconnect_self_corrects_after_late_tick,
        test_escalation_restart_when_enabled_respects_cooldown,
        test_generation_dicts_stay_bounded,
        test_repeated_recovery_cycles,
        test_health_states,
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
    if failed:
        print("Failures:")
        for name, msg in failed:
            print(f"  - {name}: {msg}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
