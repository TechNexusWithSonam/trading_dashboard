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

# MCX commodities have no cash "spot" instrument the way an index does —
# options are written on the futures contract, so the nearest-expiry future
# stands in as the spot proxy. Options/futures for these live on the MCX
# exchange, not NFO. Requires the Zerodha account to have the commodity
# segment enabled — if it isn't, the instrument dump for MCX will simply
# come back without usable contracts and resolution will fail with a clear
# "not found" rather than a wrong price.
MCX_UNDERLYINGS = {"CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER"}


def _option_exchange(underlying: str) -> str:
    return "MCX" if underlying.upper() in MCX_UNDERLYINGS else "NFO"


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


def _nearest_mcx_future(underlying: str) -> dict | None:
    try:
        rows = _load("MCX")
    except Exception as e:
        log_h2_error(f"MCX instrument load failed for {underlying}: {e}")
        return None
    futs = [r for r in rows if r.get("name") == underlying and r.get("instrument_type") == "FUT" and r.get("expiry")]
    if not futs:
        return None
    futs.sort(key=lambda r: r["expiry"])
    return futs[0]


def search(query: str, limit: int = 20) -> list[dict]:
    q = query.strip().upper()
    if not q:
        return []
    out = []
    for exchange in ("NSE", "NFO", "BSE", "MCX"):
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
    if alias:
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

    if underlying in MCX_UNDERLYINGS:
        fut = _nearest_mcx_future(underlying)
        if fut:
            log_h2(f"Spot instrument resolved (nearest MCX future, no cash spot exists for commodities): "
                   f"{underlying} -> token {fut['instrument_token']}")
            return _slim(fut)
        log_h2_error(f"No MCX future found for {underlying} — confirm the Zerodha account has "
                     f"the commodity segment enabled")
        return None

    # F&O stock — its NSE equity tradingsymbol matches the derivatives underlying name exactly.
    try:
        rows = _load("NSE")
    except Exception as e:
        log_h2_error(f"Spot instrument resolution failed for {underlying}: {e}")
        return None
    for r in rows:
        if r.get("tradingsymbol") == underlying and r.get("instrument_type") == "EQ":
            log_h2(f"Spot instrument resolved (equity): {underlying} -> token {r['instrument_token']}")
            return _slim(r)
    log_h2_error(f"Spot instrument not found for {underlying} (checked NSE equity segment)")
    return None


def resolve_option(underlying: str, expiry: str, strike: float, side: str) -> dict | None:
    """side: 'CE' or 'PE'. expiry: 'YYYY-MM-DD'."""
    underlying = underlying.upper()
    side = side.upper()
    exchange = _option_exchange(underlying)
    try:
        rows = _load(exchange)
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
    log_h2_error(f"{side} instrument not found for {underlying} {expiry} {strike} (exchange={exchange})")
    return None


def resolve_chain(underlying: str, expiry: str) -> list[dict]:
    """All strikes for underlying/expiry with CE + PE tokens paired up."""
    underlying = underlying.upper()
    exchange = _option_exchange(underlying)
    try:
        rows = _load(exchange)
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
    exchange = _option_exchange(underlying)
    try:
        rows = _load(exchange)
    except Exception as e:
        log_h2_error(f"Expiry list failed for {underlying}: {e}")
        return []
    expiries = {str(r.get("expiry")) for r in rows if r.get("name") == underlying and r.get("expiry")}
    return sorted(expiries)
