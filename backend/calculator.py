"""
calculator.py — Per-(symbol, expiry) calculator pipeline, isolated from
the live LOC engine and the main feed loop.

Why a separate module:
  • The live LOC table stays pinned to each symbol's default expiry. None
    of this file touches loc_engine state, the main rollover path, or any
    existing /api route besides /api/calculator.
  • The MCX option/spot month-pairing rule (below) shouldn't leak into
    the always-on default-expiry pipeline that the LOC table depends on.

MCX option/spot month-pairing rule:
  CRUDEOIL options expire 14-MAY but the May futures contract trades
  until 18-MAY. The pair "May options + May futures" is what should be
  shown together on the calculator. Once the user picks June options,
  the calculator switches to "June options + June futures" — even if
  May futures are still alive. This module:
    1. Resolves the futures key whose month matches the selected option
       expiry.
    2. Subscribes that key on the upstream Upstox WS feed so live ticks
       flow into state.market_data automatically (idempotent).
    3. Reads spot LTP/OHLC from state.market_data; falls back to the
       chain's underlying_spot_price or a one-shot REST quote until WS
       ticks arrive.

For NSE indices (NIFTY/SENSEX/...) and F&O stocks, the spot key is the
same across expiries, so this resolution returns the symbol's default
spot key unchanged — the existing live data path stays in effect.
"""
import asyncio, time

from . import instruments as _instr_mod
from .instruments import (
    fetch_option_chain, fetch_quotes_rest, get_itm2_strikes, STRIKE_STEPS,
)
from .instrument_keys import NSE_EQ_KEYS
from .loc_engine import calc_loc_25

_M = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
MCX_SYMBOLS = {"CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER"}

# (sym, expiry) -> (timestamp, chain) — 60 s TTL
_chain_cache: dict = {}
# (sym, expiry) -> asyncio.Event for in-flight dedup
_chain_inflight: dict = {}


def resolve_spot_key(sym: str, expiry: str, default_spot_keys: dict) -> str:
    """Return the instrument_key whose price = 'spot' for this (sym, expiry).

    MCX → futures contract of the same calendar month as the options
    expiry (CRUDEOIL options 2026-05-14 → CRUDEOIL26MAYFUT). Falls back
    to the default-month spot key if the master doesn't have a matching
    contract (e.g. far-future expiry with no listed futures yet).

    NSE_INDEX / NSE_EQ → unchanged. The index value or the equity ISIN
    does not depend on which weekly/monthly options expiry the user is
    inspecting.
    """
    sym = sym.upper()
    default_key = default_spot_keys.get(sym, "") or NSE_EQ_KEYS.get(sym, "")
    if sym not in MCX_SYMBOLS or not expiry or len(expiry) < 7:
        return default_key
    try:
        yr = int(expiry[:4]); mo = int(expiry[5:7])
    except Exception:
        return default_key
    target_sym = f"{sym}{str(yr)[2:]}{_M[mo - 1]}FUT"
    return _instr_mod._mcx_sym_to_key.get(target_sym, default_key)


async def ensure_spot_subscribed(spot_key, upstox_ws, sub_binary,
                                  subscribed_set):
    """Subscribe spot_key on the upstream WS feed if not already done.
    Idempotent — repeat calls for the same key are no-ops. Errors are
    swallowed so a transient WS hiccup doesn't 502 the calculator endpoint
    (REST fallback inside _resolve_spot_ohlc still works)."""
    if not spot_key or not upstox_ws or spot_key in subscribed_set:
        return False
    try:
        await sub_binary(upstox_ws, [spot_key], "full")
        subscribed_set.add(spot_key)
        print(f"[Calc] WS subscribed spot key {spot_key}")
        return True
    except Exception as e:
        print(f"[Calc] WS subscribe {spot_key}: {e}")
        return False


async def _fetch_chain_cached(sym: str, expiry: str, access_token: str) -> dict:
    """Cached chain fetch with inflight dedup and progressive 429 backoff.
    Same shape/behavior as main.py's prior _fetch_chain_for_calculator —
    moved here so the calculator pipeline is self-contained.

    - 60 s cache: with the frontend polling every 5 s, only every ~12th
      poll hits Upstox.
    - Inflight dedup: concurrent callers wait on the same event.
    - 5-attempt backoff (~35 s window): rides out moderate 429 spells.
    """
    key = (sym, expiry)
    cached = _chain_cache.get(key)
    if cached and (time.time() - cached[0]) < 60:
        return cached[1]
    inflight = _chain_inflight.get(key)
    if inflight is not None:
        try:
            await asyncio.wait_for(inflight.wait(), timeout=40)
        except asyncio.TimeoutError:
            pass
        cached = _chain_cache.get(key)
        if cached and (time.time() - cached[0]) < 60:
            return cached[1]
        return {}
    event = asyncio.Event()
    _chain_inflight[key] = event
    try:
        backoffs = [0, 2, 5, 10, 18]
        chain = {}
        for delay in backoffs:
            if delay:
                await asyncio.sleep(delay)
            try:
                chain = await fetch_option_chain(sym, expiry, access_token)
            except Exception as e:
                print(f"[CalcChain] {sym}/{expiry} fetch error: {e}")
                chain = {}
            if chain:
                break
        if chain:
            _chain_cache[key] = (time.time(), chain)
        return chain or {}
    finally:
        event.set()
        _chain_inflight.pop(key, None)


async def _resolve_spot_ohlc(spot_key: str, market_data: dict,
                              prev_close: dict, access_token: str,
                              chain_fallback_spot: float = 0.0) -> dict:
    """Get spot LTP+OHLC for spot_key.

    Preference order:
      1. state.market_data[spot_key] — populated by the live WS feed once
         ensure_spot_subscribed() has run and a tick has arrived.
      2. chain_fallback_spot — Upstox returns underlying_spot_price on
         the option/chain response for indices/stocks. Used only when no
         WS tick is in yet (won't have OHLC; fills LTP only).
      3. /v2/market-quote/quotes REST one-shot — last-resort lookup so
         the very first calculator response after a fresh subscription
         still carries spot data, before WS ticks land.
    """
    md = market_data.get(spot_key) or {}
    ef = md.get("efeed", {}) or {}
    ltpc = md.get("ltpc", {}) or {}
    ltp = float(ef.get("ltp") or ltpc.get("ltp") or 0)
    if ltp:
        return {
            "ltp":   ltp,
            "open":  float(ef.get("open") or ltp),
            "high":  float(ef.get("high") or ltp),
            "low":   float(ef.get("low")  or ltp),
            "close": float(ef.get("cp") or ltpc.get("cp")
                            or prev_close.get(spot_key) or ltp),
        }
    if access_token and spot_key:
        try:
            data = await fetch_quotes_rest([spot_key], access_token)
        except Exception as e:
            print(f"[Calc] REST spot fetch {spot_key}: {e}")
            data = {}
        v = data.get(spot_key) or {}
        ef2 = v.get("efeed", {}) or {}
        ltpc2 = v.get("ltpc", {}) or {}
        rest_ltp = float(ef2.get("ltp") or ltpc2.get("ltp") or 0)
        if rest_ltp:
            return {
                "ltp":   rest_ltp,
                "open":  float(ef2.get("open") or rest_ltp),
                "high":  float(ef2.get("high") or rest_ltp),
                "low":   float(ef2.get("low")  or rest_ltp),
                "close": float(ef2.get("cp") or ltpc2.get("cp") or 0)
                          or rest_ltp,
            }
    if chain_fallback_spot:
        s = chain_fallback_spot
        return {
            "ltp": s, "open": s, "high": s, "low": s,
            "close": prev_close.get(spot_key) or s,
        }
    return {}


async def compute_calc_result(*, sym: str, expiry: str, default_spot_keys: dict,
                               market_data: dict, prev_close: dict,
                               access_token: str, upstox_ws, sub_binary,
                               subscribed_set: set):
    """End-to-end /api/calculator computation for the given (sym, expiry).

    Returns the same response shape the existing endpoint emits, plus
    spot_key/spot_ltp/spot_open/spot_close so the frontend can render
    the matched-month spot data. Returns None when no chain is available
    (caller should respond 502)."""
    sym = sym.upper()
    chain = await _fetch_chain_cached(sym, expiry, access_token)
    if not chain:
        return None

    # For MCX, prefer the underlying futures key the chain function
    # actually resolved (carries `_option_key` per row). This is the
    # contract on which these options are *written*, which is critical
    # for bi-monthly commodities — GOLD's Aug 31 and Sep 25 options are
    # both written on Oct futures, so both expiries should display Oct
    # spot. The month-name guess in resolve_spot_key() correctly handles
    # CRUDEOIL/NATURALGAS but fails for GOLD/SILVER/COPPER where no
    # same-month futures exists.
    spot_key = ""
    chain_spot = 0.0
    for row in chain.values():
        if not spot_key:
            spot_key = row.get("_option_key", "") or ""
        sp = row.get("_spot", 0)
        if sp and not chain_spot:
            chain_spot = float(sp)
        if spot_key and chain_spot:
            break
    if not spot_key:
        spot_key = resolve_spot_key(sym, expiry, default_spot_keys)

    await ensure_spot_subscribed(spot_key, upstox_ws, sub_binary, subscribed_set)

    spot = await _resolve_spot_ohlc(spot_key, market_data, prev_close,
                                     access_token, chain_fallback_spot=chain_spot)
    if not spot or not spot.get("ltp"):
        return None

    spot_ltp = spot["ltp"]
    ce_strike, pe_strike = get_itm2_strikes(spot_ltp, sym)
    step = STRIKE_STEPS.get(sym, 50)

    def _pick(target):
        if target in chain:
            return target, chain[target]
        if not chain:
            return target, {}
        nearest = min(chain.keys(), key=lambda k: abs(k - target))
        if abs(nearest - target) <= step * 4:
            return nearest, chain[nearest]
        return target, {}

    ce_actual, ce_row = _pick(ce_strike)
    pe_actual, pe_row = _pick(pe_strike)
    ce = (ce_row or {}).get("CE", {}) if ce_row else {}
    pe = (pe_row or {}).get("PE", {}) if pe_row else {}

    ce_ltp   = float(ce.get("ltp")   or 0)
    ce_close = float(ce.get("close") or 0)
    ce_high  = float(ce.get("high")  or 0)
    ce_low   = float(ce.get("low")   or 0)
    pe_ltp   = float(pe.get("ltp")   or 0)
    pe_close = float(pe.get("close") or 0)
    pe_high  = float(pe.get("high")  or 0)
    pe_low   = float(pe.get("low")   or 0)

    res = calc_loc_25(
        spot_ltp, spot["close"], spot["high"], spot["low"], spot["open"],
        ce_ltp, ce_close, ce_high, ce_low,
        pe_ltp, pe_close, pe_high, pe_low,
    )
    res.update({
        "symbol":     sym,
        "expiry":     expiry,
        "spot_key":   spot_key,
        "spot_ltp":   round(spot_ltp,    2),
        "spot_open":  round(spot["open"], 2),
        "spot_high":  round(spot["high"], 2),
        "spot_low":   round(spot["low"],  2),
        "spot_close": round(spot["close"],2),
        "ce_strike":  ce_actual,
        "pe_strike":  pe_actual,
        "ce_ltp":     round(ce_ltp,   2),
        "pe_ltp":     round(pe_ltp,   2),
        "ce_close":   round(ce_close, 2),
        "pe_close":   round(pe_close, 2),
        "ce_high":    round(ce_high,  2),
        "ce_low":     round(ce_low,   2),
        "pe_high":    round(pe_high,  2),
        "pe_low":     round(pe_low,   2),
        "ce_iv":      round(float(ce.get("iv") or 0), 2),
        "pe_iv":      round(float(pe.get("iv") or 0), 2),
        "source":     "calculator",
    })
    return res
