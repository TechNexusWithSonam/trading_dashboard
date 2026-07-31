"""Autonomous Zerodha History 2 engine — the equivalent of Upstox's always-on
start_feed()/periodic_refresh() pair in backend/main.py, but for History 2.

Runs entirely independent of any connected browser client: once Zerodha is
authenticated, it computes ATM/ITM-2 CE+PE strikes per watchlist symbol
from live spot price (via the shared broker-independent backend/strike_calc.py
— the same math Upstox uses), resolves their instrument tokens, subscribes
them on the shared ticker2, and registers them with history.py so
minute-bucketed rows get recorded continuously. This is what makes History 2
"just work" without anyone opening the page — matching how Upstox's History
already behaves (backend/main.py's on_startup() -> start_feed()).

Only ITM-2 CE/PE is tracked here (not ITM-1) — a deliberate product decision
to keep History 2's own display and Zerodha token footprint limited to what
it actually shows; strike_calc.get_itm_strikes still supports any level and
Upstox's History page separately tracks both ITM-1 and ITM-2 via its own
(unrelated) loc_engine.py code path.

Isolated from the manual per-page Symbol/Expiry/Strike flow in routes.py /
History2.jsx, which is untouched and keeps working exactly as before for
live/manual viewing (its tokens are added to state2.broadcast_tokens so the
browser still gets live ticks for them; this engine's tokens are recording-
only and are not broadcast, to avoid flooding every connected client with
~200 symbols' worth of ticks nobody asked to watch).
"""
import asyncio

from ..instrument_keys import NSE_EQ_KEYS
from ..strike_calc import get_atm_strike, get_itm_strikes
from . import instruments as instr
from . import zerodha_client as zc
from .history import register_context
from .logger import log_h2, log_h2_error
from .state import state2
from .ticker import ticker2

# Mirrors backend/main.py's _INDEX_LOC / _MCX_LOC composition of LOC_SYMBOLS.
# Small literal duplication (kept in sync manually) rather than importing
# from main.py, which would create a circular import (main.py imports this
# package's router). Full watchlist = indices + MCX + all F&O stocks, matching
# Upstox's LOC_SYMBOLS exactly, per product decision to give History 2 full
# background parity with Upstox's History.
_INDEX_LOC = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
_MCX_LOC = ["CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER"]
WATCHLIST = _INDEX_LOC + _MCX_LOC + [s for s in NSE_EQ_KEYS if s not in _INDEX_LOC + _MCX_LOC]

AUTH_POLL_INTERVAL_S = 5
REFRESH_INTERVAL_S = 60       # ATM-recheck cadence, matches Upstox's periodic_refresh
LTP_BATCH_SIZE = 200          # Kite ltp()/quote() supports large batches; stay conservative
LTP_BATCH_PAUSE_S = 0.35      # stay well under Kite's REST rate limit across batches


class _SymbolTracking:
    """Per-watchlist-symbol bookkeeping the engine needs across refresh
    cycles — not stored in state2 because it's purely this engine's internal
    working state, not something any route/frontend reads directly (recorded
    strikes/tokens ARE exposed, via state2.symbol_meta / symbol_context)."""
    __slots__ = ("expiry", "spot_token", "spot_key", "last_atm")

    def __init__(self):
        self.expiry = None
        self.spot_token = None
        self.spot_key = None   # "EXCHANGE:TRADINGSYMBOL", for batched kite.ltp()
        self.last_atm = None


_tracking: dict[str, _SymbolTracking] = {}


async def start():
    """Fire-and-forget background task (started once from main.py's
    on_startup(), supervised so a crash restarts it). Waits for Zerodha auth
    — either a fresh manual OAuth login or a restored session cache — then
    runs the initial resolve+subscribe pass and an ongoing ATM-recheck loop,
    forever."""
    while not state2.access_token:
        await asyncio.sleep(AUTH_POLL_INTERVAL_S)

    log_h2(f"History2 autonomous engine starting — watchlist={len(WATCHLIST)} symbols")
    # Lazy import: routes.py imports this module to launch start() as a task,
    # so importing routes.py back at module load time would be circular.
    # Deferred until here (after both modules have finished importing) is safe.
    from .routes import _on_tick, _on_status
    if not ticker2.kws:
        try:
            ticker2.start(_on_tick, _on_status)
        except Exception as e:
            log_h2_error(f"History2 engine: failed to start ticker: {e}")
            return

    await _resolve_all_and_subscribe()

    while True:
        await asyncio.sleep(REFRESH_INTERVAL_S)
        try:
            await _recheck_atm_shifts()
        except Exception as e:
            log_h2_error(f"History2 engine: recheck error (continuing): {e}")


async def _resolve_all_and_subscribe():
    """Initial pass: resolve every watchlist symbol's spot instrument, fetch
    all spot LTPs in one batched REST call, then compute+resolve ITM-2
    strikes per symbol and subscribe everything on the ticker."""
    spot_by_symbol: dict[str, dict] = {}
    for sym in WATCHLIST:
        spot = instr.resolve_spot(sym)
        if not spot or not spot.get("instrumentToken"):
            log_h2_error(f"History2 engine: could not resolve spot instrument for {sym} — skipped")
            continue
        expiries = instr.list_expiries(sym)
        if not expiries:
            log_h2_error(f"History2 engine: no expiries found for {sym} — skipped")
            continue
        spot_by_symbol[sym] = spot
        tracking = _tracking.setdefault(sym, _SymbolTracking())
        tracking.spot_token = spot["instrumentToken"]
        tracking.spot_key = f"{spot['exchange']}:{spot['tradingsymbol']}"
        tracking.expiry = expiries[0]

    ltp_map = await _batched_ltp([t.spot_key for t in _tracking.values() if t.spot_key])

    all_tokens: list[int] = []
    resolved_count = 0
    for sym, tracking in _tracking.items():
        quote = ltp_map.get(tracking.spot_key)
        spot_ltp = (quote or {}).get("last_price")
        if not spot_ltp:
            log_h2_error(f"History2 engine: no spot LTP for {sym} ({tracking.spot_key}) — skipped this cycle")
            continue
        tokens = _resolve_and_register_strikes(sym, tracking, spot_ltp)
        if tokens:
            all_tokens.append(tracking.spot_token)
            all_tokens.extend(tokens)
            resolved_count += 1

    if all_tokens:
        ticker2.subscribe(list(dict.fromkeys(all_tokens)), "ltp")
    log_h2(f"History2 engine: resolved {resolved_count}/{len(WATCHLIST)} symbols, "
           f"subscribed {len(all_tokens)} tokens (recording-only, not broadcast)")


async def _batched_ltp(keys: list[str]) -> dict:
    kite = zc.get_kite()
    out: dict = {}
    keys = list(dict.fromkeys(keys))
    for i in range(0, len(keys), LTP_BATCH_SIZE):
        batch = keys[i:i + LTP_BATCH_SIZE]
        try:
            out.update(kite.ltp(batch))
        except Exception as e:
            log_h2_error(f"History2 engine: batched LTP fetch failed for chunk starting at {i}: {e}")
        await asyncio.sleep(LTP_BATCH_PAUSE_S)
    return out


def _resolve_and_register_strikes(sym: str, tracking: '_SymbolTracking', spot_ltp: float) -> list[int]:
    """Compute ITM-2 CE+PE strikes from spot_ltp, resolve their Zerodha
    instrument tokens, store the strikes in state2.symbol_meta and the tokens
    in state2.symbol_context (merged, not replacing the manual-subscribe
    roles), and return the resolved option tokens to subscribe."""
    atm = get_atm_strike(spot_ltp, sym)
    tracking.last_atm = atm
    # ITM-1 intentionally NOT resolved/subscribed for History 2 — Upstox's
    # History page still shows it, but History 2 only tracks ITM-2 CE/PE
    # (product decision: keep History 2's Zerodha token footprint to what's
    # actually displayed). get_itm_strikes/instr.resolve_option are untouched
    # shared functions — this only changes what this engine calls them for.
    itm2_ce_strike, itm2_pe_strike = get_itm_strikes(spot_ltp, sym, 2)

    itm2_ce = instr.resolve_option(sym, tracking.expiry, itm2_ce_strike, "CE")
    itm2_pe = instr.resolve_option(sym, tracking.expiry, itm2_pe_strike, "PE")

    if not (itm2_ce and itm2_pe):
        log_h2_error(f"History2 engine: {sym} — could not resolve ITM2 option "
                      f"contracts for expiry {tracking.expiry} (spot={spot_ltp}, atm={atm}); skipped this cycle")
        return []

    state2.symbol_meta[sym] = {
        "itm2_ce_strike": itm2_ce_strike, "itm2_pe_strike": itm2_pe_strike,
        "last_atm": atm, "expiry": tracking.expiry,
    }
    register_context(
        sym,
        spot_token=tracking.spot_token,
        itm2_ce_token=itm2_ce["instrumentToken"], itm2_pe_token=itm2_pe["instrumentToken"],
    )
    log_h2(f"History2 engine: {sym} spot={spot_ltp} atm={atm} expiry={tracking.expiry} | "
           f"ITM2 CE={itm2_ce_strike}(tok {itm2_ce['instrumentToken']}) PE={itm2_pe_strike}(tok {itm2_pe['instrumentToken']})")
    return [itm2_ce["instrumentToken"], itm2_pe["instrumentToken"]]


async def _recheck_atm_shifts():
    """Periodic (60s) pass: for each tracked symbol, read its latest spot
    price from state2.live_ticks (already flowing — spot tokens are
    subscribed) and recompute ATM. On a genuine shift, resolve new ITM-2
    tokens, subscribe them, and unsubscribe the superseded ones — this
    explicitly ports the ce_pe_ltp_freeze_fix lesson from the Upstox side
    (unbounded subscription growth from never unsubscribing superseded
    strikes) so the same class of bug isn't reintroduced here."""
    new_tokens: list[int] = []
    stale_tokens: list[int] = []
    for sym, tracking in _tracking.items():
        if not tracking.spot_token:
            continue
        tick = state2.live_ticks.get(tracking.spot_token)
        if not tick or not tick.get("lastPrice"):
            continue
        spot_ltp = tick["lastPrice"]
        new_atm = get_atm_strike(spot_ltp, sym)
        if new_atm == tracking.last_atm:
            continue
        prev_ctx = dict(state2.symbol_context.get(sym, {}))
        prev_itm_tokens = [prev_ctx.get(r) for r in ("itm2_ce", "itm2_pe")]
        tokens = _resolve_and_register_strikes(sym, tracking, spot_ltp)
        if not tokens:
            continue
        new_tokens.extend(tokens)
        stale_tokens.extend(t for t in prev_itm_tokens if t and t not in tokens)

    if new_tokens:
        ticker2.subscribe(list(dict.fromkeys(new_tokens)), "ltp")
    if stale_tokens:
        ticker2.unsubscribe(list(dict.fromkeys(stale_tokens)))
        log_h2(f"History2 engine: ATM shift(s) — subscribed {len(new_tokens)} new, "
               f"unsubscribed {len(stale_tokens)} superseded option tokens")
