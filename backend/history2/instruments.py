"""Zerodha instrument-master resolution for History 2: underlying spot,
CE/PE option tokens by strike/expiry. Isolated from the existing Upstox
instrument mapping in backend/instruments.py and backend/instrument_keys.py —
different broker, different token space, must not be conflated.
"""
import time

from .logger import log_h2, log_h2_error
from .state import state2
from .zerodha_client import get_kite

# Zerodha republishes the instrument dump once a day (pre-market); reloading
# twice a day is more than enough and avoids re-downloading on every request.
CACHE_TTL = 12 * 60 * 60

SPOT_ALIASES = {
    "NIFTY": ("NSE", "NIFTY 50"),
    "BANKNIFTY": ("NSE", "NIFTY BANK"),
    "FINNIFTY": ("NSE", "NIFTY FIN SERVICE"),
    "MIDCPNIFTY": ("NSE", "NIFTY MID SELECT"),
    "SENSEX": ("BSE", "SENSEX"),
    "BANKEX": ("BSE", "BANKEX"),
}


def _load(exchange: str) -> list[dict]:
    now = time.time()
    loaded_at = state2.instruments_loaded_at.get(exchange, 0)
    if exchange in state2.instruments_cache and (now - loaded_at) < CACHE_TTL:
        return state2.instruments_cache[exchange]
    kite = get_kite()
    log_h2(f"Loading instrument master for {exchange}")
    rows = kite.instruments(exchange)
    state2.instruments_cache[exchange] = rows
    state2.instruments_loaded_at[exchange] = now
    log_h2(f"Instrument master loaded for {exchange} ({len(rows)} rows)")
    return rows


def _slim(row: dict) -> dict:
    return {
        "exchange": row.get("exchange"),
        "tradingsymbol": row.get("tradingsymbol"),
        "instrumentToken": row.get("instrument_token"),
        "name": row.get("name"),
        "expiry": str(row.get("expiry")) if row.get("expiry") else None,
        "strike": row.get("strike") or None,
        "instrumentType": row.get("instrument_type"),
    }


def search(query: str, limit: int = 20) -> list[dict]:
    q = query.strip().upper()
    if not q:
        return []
    out = []
    for exchange in ("NSE", "NFO", "BSE"):
        try:
            rows = _load(exchange)
        except Exception as e:
            log_h2_error(f"Instrument search failed for {exchange}: {e}")
            continue
        for r in rows:
            if q in (r.get("tradingsymbol") or "").upper() or q in (r.get("name") or "").upper():
                out.append(_slim(r))
                if len(out) >= limit:
                    return out
    return out


def resolve_spot(underlying: str) -> dict | None:
    underlying = underlying.upper()
    alias = SPOT_ALIASES.get(underlying)
    if not alias:
        log_h2_error(f"Spot instrument not found for {underlying} (no alias mapping)")
        return None
    exchange, name = alias
    try:
        rows = _load(exchange)
    except Exception as e:
        log_h2_error(f"Spot instrument resolution failed for {underlying}: {e}")
        return None
    for r in rows:
        if r.get("tradingsymbol") == name:
            log_h2(f"Spot instrument resolved: {underlying} -> token {r['instrument_token']}")
            return _slim(r)
    log_h2_error(f"Spot instrument not found for {underlying} (segment={exchange})")
    return None


def resolve_option(underlying: str, expiry: str, strike: float, side: str) -> dict | None:
    """side: 'CE' or 'PE'. expiry: 'YYYY-MM-DD'."""
    underlying = underlying.upper()
    side = side.upper()
    try:
        rows = _load("NFO")
    except Exception as e:
        log_h2_error(f"{side} instrument resolution failed for {underlying}: {e}")
        return None
    for r in rows:
        if (r.get("name") == underlying
                and str(r.get("expiry")) == expiry
                and float(r.get("strike") or 0) == float(strike)
                and r.get("instrument_type") == side):
            log_h2(f"{side} instrument resolved: {underlying} {expiry} {strike} -> token {r['instrument_token']}")
            return _slim(r)
    log_h2_error(f"{side} instrument not found for {underlying} {expiry} {strike}")
    return None


def resolve_chain(underlying: str, expiry: str) -> list[dict]:
    """All strikes for underlying/expiry with CE + PE tokens paired up."""
    underlying = underlying.upper()
    try:
        rows = _load("NFO")
    except Exception as e:
        log_h2_error(f"Option chain resolution failed for {underlying}: {e}")
        return []
    by_strike: dict[float, dict] = {}
    for r in rows:
        if r.get("name") != underlying or str(r.get("expiry")) != expiry:
            continue
        itype = r.get("instrument_type")
        if itype not in ("CE", "PE"):
            continue
        strike = float(r.get("strike") or 0)
        entry = by_strike.setdefault(strike, {"strike": strike})
        entry[itype.lower()] = _slim(r)
    return [by_strike[k] for k in sorted(by_strike)]


def list_expiries(underlying: str) -> list[str]:
    underlying = underlying.upper()
    try:
        rows = _load("NFO")
    except Exception as e:
        log_h2_error(f"Expiry list failed for {underlying}: {e}")
        return []
    expiries = {str(r.get("expiry")) for r in rows if r.get("name") == underlying and r.get("expiry")}
    return sorted(expiries)
