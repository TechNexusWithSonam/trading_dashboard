"""Minute-bucketed history recording for History 2 — mirrors the pattern in
main.py's _record_loc_hist / hist.insert(0, ...) for the existing LOC
history, but keyed to Zerodha spot/CE/PE instrument tokens instead. Fully
isolated: separate state (state2.history), separate 200-row cap, no shared
code with the existing loc_history path.
"""
from .logger import log_h2
from .state import state2

MAX_ROWS = 200


def register_context(symbol: str, spot_token: int | None, ce_token: int | None, pe_token: int | None):
    """Tell the tick router which tokens make up a symbol's spot/CE/PE, so
    on_tick() can attribute incoming ticks to a symbol's history row."""
    symbol = symbol.upper()
    state2.symbol_context[symbol] = {"spot": spot_token, "ce": ce_token, "pe": pe_token}
    state2.live_by_symbol.setdefault(symbol, {})
    log_h2(f"Tracking context for {symbol}: spot={spot_token} ce={ce_token} pe={pe_token}")


def _roles_for_token(token: int) -> list[tuple[str, str]]:
    out = []
    for sym, ctx in state2.symbol_context.items():
        for role in ("spot", "ce", "pe"):
            if ctx.get(role) == token:
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
    row = {
        "ts": ts_ms,
        "spot_ltp": live.get("spot_ltp"),
        "ce_ltp": live.get("ce_ltp"),
        "pe_ltp": live.get("pe_ltp"),
    }
    hist = state2.history.setdefault(symbol, [])
    hist.insert(0, row)
    if len(hist) > MAX_ROWS:
        hist.pop()
    log_h2(f"New minute history row created for {symbol}")


def get_history(symbol: str) -> list[dict]:
    return state2.history.get(symbol.upper(), [])
