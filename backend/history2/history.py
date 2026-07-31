"""Minute-bucketed history recording for History 2 — mirrors the pattern in
main.py's _record_loc_hist / hist.insert(0, ...) for the existing LOC
history, but keyed to Zerodha spot/CE/PE instrument tokens instead. Fully
isolated: separate state (state2.history), separate 200-row cap, no shared
code with the existing loc_history path.
"""
from .logger import log_h2
from .state import state2

MAX_ROWS = 200


def register_context(symbol: str, spot_token: int | None = None, ce_token: int | None = None,
                      pe_token: int | None = None, **extra_roles):
    """Tell the tick router which tokens make up a symbol's roles, so
    on_tick() can attribute incoming ticks to a symbol's history row.

    Merges into any existing context for this symbol instead of replacing it
    — the manual frontend subscribe (spot/ce/pe roles) and the autonomous
    background engine (itm1_ce/itm1_pe/itm2_ce/itm2_pe roles, see engine.py)
    both call this independently for the same symbol and must not clobber
    each other's tokens. Pass extra roles as kwargs, e.g.
    register_context("NIFTY", itm1_ce_token=123, itm2_pe_token=456) — the
    trailing "_token" suffix is stripped to get the role name.
    """
    symbol = symbol.upper()
    roles = {"spot": spot_token, "ce": ce_token, "pe": pe_token}
    for k, v in extra_roles.items():
        role = k[:-len("_token")] if k.endswith("_token") else k
        roles[role] = v
    roles = {k: v for k, v in roles.items() if v is not None}
    state2.symbol_context.setdefault(symbol, {}).update(roles)
    state2.live_by_symbol.setdefault(symbol, {})
    log_h2(f"Tracking context for {symbol}: {roles}")


def _roles_for_token(token: int) -> list[tuple[str, str]]:
    out = []
    for sym, ctx in state2.symbol_context.items():
        for role, tok in ctx.items():
            if tok == token:
                out.append((sym, role))
    return out


def on_tick(token: int, price: float, ts_ms: int):
    for symbol, role in _roles_for_token(token):
        live = state2.live_by_symbol.setdefault(symbol, {})
        live["spot_ltp" if role == "spot" else f"{role}_ltp"] = price
        live["ts"] = ts_ms
        _maybe_record(symbol, ts_ms)


def _maybe_record(symbol: str, ts_ms: int):
    minute_bucket = int(ts_ms // 60000)
    if state2.last_minute.get(symbol) == minute_bucket:
        return
    state2.last_minute[symbol] = minute_bucket
    live = state2.live_by_symbol.get(symbol, {})
    meta = state2.symbol_meta.get(symbol, {})
    row = {
        "ts": ts_ms,
        "spot_ltp": live.get("spot_ltp"),
        # Legacy manual-strike pair (frontend Strike dropdown) — untouched.
        "ce_ltp": live.get("ce_ltp"),
        "pe_ltp": live.get("pe_ltp"),
        # Autonomous engine's 1st/2nd ITM pairs — additive.
        "itm1_ce_strike": meta.get("itm1_ce_strike"),
        "itm1_pe_strike": meta.get("itm1_pe_strike"),
        "itm1_ce_ltp": live.get("itm1_ce_ltp"),
        "itm1_pe_ltp": live.get("itm1_pe_ltp"),
        "itm2_ce_strike": meta.get("itm2_ce_strike"),
        "itm2_pe_strike": meta.get("itm2_pe_strike"),
        "itm2_ce_ltp": live.get("itm2_ce_ltp"),
        "itm2_pe_ltp": live.get("itm2_pe_ltp"),
    }
    hist = state2.history.setdefault(symbol, [])
    hist.insert(0, row)
    if len(hist) > MAX_ROWS:
        hist.pop()
    log_h2(f"New minute history row created for {symbol}")


def get_history(symbol: str) -> list[dict]:
    return state2.history.get(symbol.upper(), [])
