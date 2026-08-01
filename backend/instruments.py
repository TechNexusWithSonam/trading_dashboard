"""
instruments.py — Zerodha Kite Connect-backed market data / instrument
resolution for the main LOC engine (replaces the former Upstox integration).

Public function names, signatures, and return shapes are unchanged from the
Upstox version so main.py, loc_engine.py, and calculator.py need no edits
beyond this module's internals — see the Upstox->Zerodha migration plan.

Internal key format: "{PREFIX}|{zerodha_instrument_token}" — keeps Upstox's
exact pipe convention (NSE_INDEX|, NSE_EQ|, NSE_FO|, MCX_FO|) so every
`.startswith("MCX")` / `.startswith("NSE_EQ|")` check elsewhere in the
codebase keeps working; only the numeric suffix's source changed (Zerodha
instrument_token instead of Upstox's numeric key/ISIN).

Every kiteconnect call is synchronous (the SDK wraps `requests`) and is
ALWAYS wrapped in `asyncio.to_thread` — a blocking call here freezes the
whole single-threaded event loop, including the live tick feed. This is the
exact bug that froze the Upstox feed when History 2's Zerodha rollout first
shipped (see backend/history2/engine.py and main.py's on_startup, which had
disabled the autonomous engine for this reason) — do not reintroduce it here.

Reuses the shared Zerodha session (backend/history2/zerodha_client.get_kite())
and its instrument-dump cache (backend/history2/state.state2.instruments_cache)
so both this module and History 2's own (independent) instrument resolution
share one login and one set of downloaded dumps instead of two.
"""
import asyncio
import time
from datetime import date, timedelta

from .strike_calc import STRIKE_STEPS, get_itm_strikes, get_itm1_strikes, get_itm2_strikes as _get_itm2_strikes
from .history2.zerodha_client import get_kite
from .history2.state import state2

MONTHLY_SYMBOLS = {"GOLD", "SILVER", "COPPER", "CRUDEOIL", "NATURALGAS"}
_M = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
MCX_SYMBOLS = ["CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER"]

# sym -> (internal_prefix, zerodha_exchange, zerodha_tradingsymbol)
_INDEX_ALIASES = {
    "NIFTY":      ("NSE_INDEX", "NSE", "NIFTY 50"),
    "BANKNIFTY":  ("NSE_INDEX", "NSE", "NIFTY BANK"),
    "FINNIFTY":   ("NSE_INDEX", "NSE", "NIFTY FIN SERVICE"),
    "MIDCPNIFTY": ("NSE_INDEX", "NSE", "NIFTY MID SELECT"),
    "SENSEX":     ("BSE_INDEX", "BSE", "SENSEX"),
    "BANKEX":     ("BSE_INDEX", "BSE", "BANKEX"),
}
_BSE_FO_SYMBOLS = {"SENSEX", "BANKEX"}
_EXCHANGE_TO_PREFIX = {"NFO": "NSE_FO", "BFO": "BSE_FO", "MCX": "MCX_FO"}


def _option_exchange(sym: str) -> str:
    if sym in MONTHLY_SYMBOLS:
        return "MCX"
    if sym in _BSE_FO_SYMBOLS:
        return "BFO"
    return "NFO"


def _prefix_for_exchange(exchange: str) -> str:
    return _EXCHANGE_TO_PREFIX.get(exchange, exchange)


# ══════════════════════════════════════════════════════════════════
#  Instrument-dump cache (shared with History 2 via state2)
# ══════════════════════════════════════════════════════════════════
_DUMP_CACHE_TTL = 12 * 60 * 60  # Zerodha republishes dumps once a day pre-market


async def _load_dump(exchange: str) -> list:
    now = time.time()
    loaded_at = state2.instruments_loaded_at.get(exchange, 0)
    cached = state2.instruments_cache.get(exchange)
    if cached is not None and (now - loaded_at) < _DUMP_CACHE_TTL:
        return cached
    kite = get_kite()
    rows = await asyncio.to_thread(kite.instruments, exchange)
    state2.instruments_cache[exchange] = rows
    state2.instruments_loaded_at[exchange] = now
    print(f"[Instruments] Loaded {exchange}: {len(rows)} rows")
    return rows


# ══════════════════════════════════════════════════════════════════
#  Key registries
# ══════════════════════════════════════════════════════════════════
_mcx_sym_to_key: dict = {}        # MCX tradingsymbol -> internal key
_mcx_name_to_numeric: dict = {}   # "MCX_FO|<tradingsym>" -> internal key (parity; identity today)
_mcx_numeric_to_name: dict = {}   # internal key -> "MCX_FO|<tradingsym>"
_nse_eq_sym_to_key: dict = {}     # stock symbol -> internal key (NSE_EQ|<token>)
_index_keys: dict = {}            # index sym -> internal key
_key_to_quote_str: dict = {}      # internal key -> "EXCHANGE:TRADINGSYMBOL" (kite.quote/ltp)
_key_to_token: dict = {}          # internal key -> instrument_token (ticker subscribe / historical_data)
_token_to_key: dict = {}          # instrument_token -> internal key (WS tick routing)

_validated_mcx: dict = {}
_mcx_option_underlying: dict = {}
_MCX_NEXT_MONTH_SEED: frozenset = frozenset()
_MCX_NEXT_MONTH_OPTION_SYMBOLS: set = set(_MCX_NEXT_MONTH_SEED)


def _register(prefix: str, exchange: str, tradingsymbol: str, token: int) -> str:
    key = f"{prefix}|{token}"
    _key_to_quote_str[key] = f"{exchange}:{tradingsymbol}"
    _key_to_token[key] = token
    _token_to_key[token] = key
    return key


def token_to_key(token: int) -> str:
    """WS tick -> internal key. Used by main.py's Zerodha tick adapter."""
    return _token_to_key.get(token, "")


def key_to_token(key: str) -> int:
    """internal key -> Zerodha instrument_token, for ticker subscribe/unsubscribe
    and historical_data(). Used by main.py's _sub_binary/_unsub_binary."""
    return _key_to_token.get(key, 0)


# ══════════════════════════════════════════════════════════════════
#  Spot / MCX futures key resolution
# ══════════════════════════════════════════════════════════════════
def get_spot_keys() -> dict:
    """Sync, no network — returns whatever's already been resolved by
    validate_mcx_keys() (indices + MCX). Empty entries before that has run
    (e.g. the very first call at process startup, before any token exists)
    are expected and get overwritten once startup_init() runs."""
    keys = dict(_index_keys)
    for s in MCX_SYMBOLS:
        keys[s] = _validated_mcx.get(s, "")
    return keys


def mcx_key(sym: str, months_ahead: int = 0) -> str:
    """Resolve sym's futures key `months_ahead` months out from today.
    Returns "" if the instrument dump hasn't been loaded yet or the target
    month's contract isn't listed by Zerodha yet — no synthetic fallback is
    possible here (Zerodha's ticker/quote calls require a real instrument
    that exists in the dump, unlike Upstox which accepted name-based
    placeholder keys for far-future months)."""
    d = date.today() + timedelta(days=30 * months_ahead)
    return mcx_key_for_month(sym, d.year, d.month)


def mcx_key_for_month(sym: str, year: int, month: int) -> str:
    trading_sym = f"{sym.upper()}{str(year)[2:]}{_M[month - 1]}FUT"
    return _mcx_sym_to_key.get(trading_sym, "")


def _mcx_underlying_for_expiry(symbol: str, expiry: str) -> str:
    """Pick the MCX futures underlying (internal key) whose month matches
    the option's expiry month. Same-month convention confirmed for all 5
    MCX commodities."""
    if not expiry or len(expiry) < 7:
        return ""
    try:
        yr = int(expiry[:4])
        mo = int(expiry[5:7])
    except Exception:
        return ""
    return mcx_key_for_month(symbol, yr, mo)


def get_itm2_strikes(spot: float, symbol: str) -> tuple:
    return _get_itm2_strikes(spot, symbol)


# ══════════════════════════════════════════════════════════════════
#  Fallback / pure expiry logic — broker-independent, unchanged
# ══════════════════════════════════════════════════════════════════
def _last_thu(year, month):
    if month == 12:
        nm = date(year + 1, 1, 1)
    else:
        nm = date(year, month + 1, 1)
    last = nm - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - 3) % 7)


def calculate_expiries_fallback(symbol: str, count: int = 8) -> list:
    today = date.today()
    sym = symbol.upper()
    result = []
    if sym in MONTHLY_SYMBOLS:
        for i in range(3):
            m = today.month + i
            y = today.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            lt = _last_thu(y, m)
            if lt >= today:
                result.append(lt.isoformat())
    else:
        d = today
        n = 0
        while n < count:
            days = (3 - d.weekday()) % 7 or 7
            d += timedelta(days=days)
            result.append(d.isoformat())
            n += 1
    return sorted(set(result))


def _is_past_market_close_ist() -> bool:
    """True when IST clock is past 15:35 — NSE options have settled for the day."""
    from datetime import datetime, timezone, timedelta as _td
    now_ist = datetime.now(timezone.utc) + _td(hours=5, minutes=30)
    return now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 35)


def _is_past_mcx_close_ist() -> bool:
    """True when IST clock is past 23:30 — MCX commodity options have settled."""
    from datetime import datetime, timezone, timedelta as _td
    now_ist = datetime.now(timezone.utc) + _td(hours=5, minutes=30)
    return now_ist.hour == 23 and now_ist.minute >= 30


def get_current_and_next_expiry(expiries: list, symbol: str) -> dict:
    today = date.today()
    today_s = today.isoformat()
    future = sorted([e for e in expiries if e >= today_s])
    active = [e for e in future if e >= today_s] or future

    if (symbol.upper() not in MONTHLY_SYMBOLS
            and future and future[0] == today_s
            and len(active) > 0 and active[0] != today_s
            and _is_past_market_close_ist()):
        future = active

    result = {"all": expiries, "default": future[0] if future else None}

    if symbol.upper() in MONTHLY_SYMBOLS:
        ref = active if active else future
        months_order = []
        for e in ref:
            ym = e[:7]
            if ym not in months_order:
                months_order.append(ym)

        def _last_in(ym):
            return [e for e in ref if e.startswith(ym)][-1]

        result["current_month"] = _last_in(months_order[0]) if len(months_order) > 0 else None
        result["next_month"] = _last_in(months_order[1]) if len(months_order) > 1 else None
        result["default"] = result["current_month"]
    else:
        result["current_week"] = future[0] if future else None
        rest = [e for e in active if e != result["current_week"]]
        result["next_week"] = rest[0] if len(rest) > 0 else None
        result["far_week"] = rest[1] if len(rest) > 1 else None
    return result


# ══════════════════════════════════════════════════════════════════
#  Expiry list
# ══════════════════════════════════════════════════════════════════
async def fetch_expiry_list(symbol: str, token: str) -> list:
    """Expiry list for `symbol`, drawn from the cached NFO/BFO/MCX instrument
    dump — one bulk download covers every expiry at once, unlike Upstox's
    per-contract-key REST probing."""
    if not token:
        return calculate_expiries_fallback(symbol)
    sym = symbol.upper()
    exchange = _option_exchange(sym)
    try:
        rows = await _load_dump(exchange)
    except Exception as e:
        print(f"[Expiry] {symbol}: dump load error: {e}")
        return calculate_expiries_fallback(symbol)
    expiries = sorted({
        str(r.get("expiry")) for r in rows
        if r.get("name") == sym and r.get("instrument_type") in ("CE", "PE") and r.get("expiry")
    })
    if expiries:
        print(f"[Expiry] {symbol}: {expiries[:6]} ({len(expiries)} total)")
        return expiries
    return calculate_expiries_fallback(symbol)


# ══════════════════════════════════════════════════════════════════
#  Quotes / OHLC (kite.quote(), batched + throttled)
# ══════════════════════════════════════════════════════════════════
_QUOTE_BATCH_SIZE = 200          # matches History2's LTP_BATCH_SIZE
_QUOTE_MIN_INTERVAL = 0.35       # conservative spacing under Zerodha's rate limit
_quote_chunk_sem = asyncio.Semaphore(1)
_quote_last_call: float = 0.0


async def _quote_throttle():
    global _quote_last_call
    async with _quote_chunk_sem:
        now = time.time()
        wait = _QUOTE_MIN_INTERVAL - (now - _quote_last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        _quote_last_call = time.time()


async def _batched_quote(keys: list) -> dict:
    """kite.quote() for a list of internal keys -> {internal_key: kite_row}."""
    kite = get_kite()
    out = {}
    qstr_to_key = {}
    batch_strs = []
    for k in dict.fromkeys(keys):
        if not k:
            continue
        qs = _key_to_quote_str.get(k, "")
        if not qs:
            continue
        qstr_to_key[qs] = k
        batch_strs.append(qs)
    for i in range(0, len(batch_strs), _QUOTE_BATCH_SIZE):
        chunk = batch_strs[i:i + _QUOTE_BATCH_SIZE]
        if not chunk:
            continue
        await _quote_throttle()
        try:
            resp = await asyncio.to_thread(kite.quote, chunk)
        except Exception as e:
            print(f"[Quote] chunk error: {e}")
            continue
        for qs, v in (resp or {}).items():
            key = qstr_to_key.get(qs)
            if key and v:
                out[key] = v
    return out


def _quote_to_efeed(v: dict) -> dict:
    """kite.quote()/ltp() row -> {"ltp","cp","open","high","low","oi"}.
    Zerodha's ohlc.close IS the previous day's close already (unlike
    Upstox, no net_change derivation needed) — kept as a defensive
    fallback below rather than removed, in case a row omits ohlc."""
    ltp = float(v.get("last_price") or 0)
    ohlc = v.get("ohlc") or {}
    cp = float(ohlc.get("close") or 0)
    net_change = float(v.get("net_change") or 0)
    if not cp and ltp and net_change:
        cp = round(ltp - net_change, 2)
    return {
        "ltp": ltp, "cp": cp,
        "open": float(ohlc.get("open") or ltp),
        "high": float(ohlc.get("high") or ltp),
        "low": float(ohlc.get("low") or ltp),
        "oi": float(v.get("oi") or 0),
    }


async def fetch_quotes_rest(keys: list, token: str) -> dict:
    if not keys or not token:
        return {}
    quotes = await _batched_quote(keys)
    results = {}
    for key, v in quotes.items():
        ef = _quote_to_efeed(v)
        results[key] = {"ltpc": {"ltp": ef["ltp"], "cp": ef["cp"]}, "efeed": ef}
    return results


async def fetch_index_quotes(index_keys: list, token: str) -> dict:
    """Zerodha's kite.quote() handles indices the same as any other
    instrument — no separate endpoint needed (unlike Upstox's v2/v3 split)."""
    return await fetch_quotes_rest(index_keys, token)


async def fetch_option_ohlc_rest(ce_key: str, pe_key: str, token: str) -> dict:
    keys = [k for k in [ce_key, pe_key] if k]
    if not keys or not token:
        return {}
    quotes = await _batched_quote(keys)
    result = {}
    for key, v in quotes.items():
        ef = _quote_to_efeed(v)
        result[key] = {"ltp": ef["ltp"], "close": ef["cp"], "high": ef["high"],
                        "low": ef["low"], "open": ef["open"], "oi": ef["oi"]}
    return result


# ══════════════════════════════════════════════════════════════════
#  Option chain
# ══════════════════════════════════════════════════════════════════
async def fetch_option_chain(symbol: str, expiry: str, token: str) -> dict:
    """Full option chain built from the instrument dump + batched quotes.
    Returns {strike: {"CE":{...},"PE":{...},"_spot":float,"_option_key":str}} —
    identical shape to the old Upstox-backed version."""
    if not token or not expiry:
        return {}
    sym = symbol.upper()
    exchange = _option_exchange(sym)
    try:
        rows = await _load_dump(exchange)
    except Exception as e:
        print(f"[Chain] {symbol}/{expiry}: dump load error: {e}")
        return {}

    strike_rows = [r for r in rows
                   if r.get("name") == sym and str(r.get("expiry")) == expiry
                   and r.get("instrument_type") in ("CE", "PE")]
    if not strike_rows:
        print(f"[Chain] {symbol}/{expiry}: no contracts in {exchange} dump")
        return {}

    prefix = _prefix_for_exchange(exchange)
    strike_map: dict = {}
    for r in strike_rows:
        strike = float(r.get("strike") or 0)
        itype = r.get("instrument_type")
        tok = r.get("instrument_token")
        tsym = r.get("tradingsymbol")
        if not (strike and itype and tok and tsym):
            continue
        key = _register(prefix, exchange, tsym, tok)
        strike_map.setdefault(strike, {})[itype] = key
    if not strike_map:
        return {}

    # Resolve spot: MCX -> matching-month futures; indices/stocks -> the
    # symbol's regular spot key.
    if sym in MONTHLY_SYMBOLS:
        option_key = _mcx_underlying_for_expiry(sym, expiry) or _validated_mcx.get(sym, "")
    else:
        from . import instrument_keys as _ik
        option_key = get_spot_keys().get(sym, "") or _ik.NSE_EQ_KEYS.get(sym, "")

    spot_price = 0.0
    if option_key:
        sq = await fetch_quotes_rest([option_key], token)
        v = sq.get(option_key) or {}
        spot_price = float((v.get("ltpc") or {}).get("ltp") or 0)

    quote_keys = [k for sides in strike_map.values() for k in sides.values()]
    quotes = await fetch_quotes_rest(quote_keys, token)

    chain = {}
    for strike, sides in strike_map.items():
        ce_key = sides.get("CE", "")
        pe_key = sides.get("PE", "")
        ce_ef = (quotes.get(ce_key) or {}).get("efeed", {})
        pe_ef = (quotes.get(pe_key) or {}).get("efeed", {})
        chain[strike] = {
            "CE": {
                "ltp": ce_ef.get("ltp", 0.0), "close": ce_ef.get("cp", 0.0),
                "high": ce_ef.get("high", 0.0), "low": ce_ef.get("low", 0.0),
                "oi": ce_ef.get("oi", 0.0), "iv": 0.0, "key": ce_key,
            },
            "PE": {
                "ltp": pe_ef.get("ltp", 0.0), "close": pe_ef.get("cp", 0.0),
                "high": pe_ef.get("high", 0.0), "low": pe_ef.get("low", 0.0),
                "oi": pe_ef.get("oi", 0.0), "iv": 0.0, "key": pe_key,
            },
            "_spot": spot_price,
            "_option_key": option_key,
        }

    if chain and spot_price:
        atm_s = min(chain.keys(), key=lambda s: abs(s - spot_price))
        print(f"[Chain] {symbol}/{expiry}: {len(chain)} strikes, spot={spot_price}, "
              f"ATM={atm_s}, CE_ltp={chain[atm_s]['CE']['ltp']}")

    # Settlement-day fallback: if this expiry has passed and every CE shows
    # zero LTP, the contract has settled — roll to the next available expiry
    # so LOC doesn't freeze on dead data (mirrors the old MCX-specific check,
    # generalized to any monthly symbol).
    if sym in MONTHLY_SYMBOLS and chain and expiry <= date.today().isoformat():
        if not any(v.get("CE", {}).get("ltp") for v in chain.values()):
            try:
                import sys
                _main = sys.modules.get("backend.main")
                all_exp = (_main and _main.state.expiry_cache.get(sym, {}).get("all")) or []
                next_exp = next((e for e in sorted(all_exp) if e > expiry), None)
                if next_exp:
                    print(f"[Chain] {sym}: {expiry} settled, falling back to {next_exp}")
                    return await fetch_option_chain(sym, next_exp, token)
            except Exception:
                pass

    return chain


# ══════════════════════════════════════════════════════════════════
#  Historical candles
# ══════════════════════════════════════════════════════════════════
_INTERVAL_MAP = {
    ("minutes", 1): "minute", ("minutes", 3): "3minute", ("minutes", 5): "5minute",
    ("minutes", 10): "10minute", ("minutes", 15): "15minute", ("minutes", 30): "30minute",
    ("minutes", 60): "60minute", ("hours", 1): "60minute", ("days", 1): "day",
}


def _zerodha_interval(unit: str, interval: int) -> str:
    return _INTERVAL_MAP.get((unit, interval), "minute")


async def _historical(key: str, unit: str, interval: int, from_dt, to_dt) -> list:
    tok = _key_to_token.get(key, 0)
    if not tok:
        return []
    kite = get_kite()
    zint = _zerodha_interval(unit, interval)
    try:
        candles = await asyncio.to_thread(kite.historical_data, tok, from_dt, to_dt, zint)
    except Exception as e:
        print(f"[Candle] {key}: {e}")
        return []
    result = []
    for c in candles:
        dt = c.get("date")
        ts = int(dt.timestamp() * 1000) if hasattr(dt, "timestamp") else int(time.time() * 1000)
        result.append({
            "t": ts, "o": float(c.get("open") or 0), "h": float(c.get("high") or 0),
            "l": float(c.get("low") or 0), "c": float(c.get("close") or 0),
            "v": int(c.get("volume") or 0),
        })
    return result


async def fetch_intraday_candles(key: str, token: str, unit: str = "minutes", interval: int = 1) -> list:
    """Fetch today's intraday candles."""
    if not token:
        return []
    from datetime import datetime
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return await _historical(key, unit, interval, start, now)


async def fetch_historical_candles(key: str, token: str, unit: str, interval: int,
                                    from_date: str, to_date: str) -> list:
    """Date-ranged historical candles — used by /api/ohlc-hist (replaces the
    inline Upstox v3/historical-candle call that used to live in main.py)."""
    if not token:
        return []
    from datetime import datetime
    try:
        start = datetime.fromisoformat(from_date)
        end = (datetime.now() if to_date == date.today().isoformat()
               else datetime.fromisoformat(to_date))
    except Exception:
        return []
    return await _historical(key, unit, interval, start, end)


# ══════════════════════════════════════════════════════════════════
#  Index / MCX / NSE_EQ key resolution (startup)
# ══════════════════════════════════════════════════════════════════
async def _resolve_indices():
    """Resolve index sym -> internal key from the NSE/BSE dumps. Indices
    rarely if ever change token — this only needs to run once, as a side
    effect of validate_mcx_keys()."""
    dumps: dict = {}
    for sym, (prefix, exch, tsym) in _INDEX_ALIASES.items():
        try:
            rows = dumps.get(exch)
            if rows is None:
                rows = await _load_dump(exch)
                dumps[exch] = rows
            match = next((r for r in rows if r.get("tradingsymbol") == tsym), None)
            if match:
                _index_keys[sym] = _register(prefix, exch, tsym, match["instrument_token"])
            else:
                print(f"[Index] {sym} not found in {exch} dump (tradingsymbol={tsym})")
        except Exception as e:
            print(f"[Index] {sym} resolution error: {e}")


async def validate_mcx_keys(token: str) -> dict:
    """Resolve current MCX futures keys for the 5 commodities. As a side
    effect also resolves index spot keys (_resolve_indices) — together
    these are the two non-static entries get_spot_keys() returns, mirroring
    the old Upstox function's role as the one startup step that populates
    them."""
    global _validated_mcx
    if not token:
        return {}
    try:
        rows = await _load_dump("MCX")
    except Exception as e:
        print(f"[MCX] instrument dump load failed: {e}")
        rows = []

    for r in rows:
        tsym = r.get("tradingsymbol") or ""
        tok = r.get("instrument_token")
        if r.get("instrument_type") == "FUT" and tok and tsym:
            key = _register("MCX_FO", "MCX", tsym, tok)
            _mcx_sym_to_key[tsym] = key
            _mcx_numeric_to_name[key] = f"MCX_FO|{tsym}"
            _mcx_name_to_numeric[f"MCX_FO|{tsym}"] = key

    result = {}
    today = date.today()
    for sym in MCX_SYMBOLS:
        candidates = []
        for tsym, key in _mcx_sym_to_key.items():
            if not (tsym.startswith(sym) and tsym.endswith("FUT")):
                continue
            suffix = tsym[len(sym):]
            if suffix[0:1] == "M" and suffix[1:2].isdigit():
                continue  # mini contract, skip
            candidates.append((tsym, key))

        def _parse_expiry(tsym, _sym=sym):
            s = tsym[len(_sym):]
            try:
                yr = int("20" + s[:2])
                mon = _M.index(s[2:5]) + 1
                return date(yr, mon, 1)
            except Exception:
                return date(2099, 1, 1)

        candidates.sort(key=lambda x: _parse_expiry(x[0]))
        for tsym, key in candidates:
            if _parse_expiry(tsym) >= today.replace(day=1):
                result[sym] = key
                print(f"[MCX] validate: {sym} = {key} ({tsym})")
                break
        if sym not in result:
            print(f"[MCX] {sym}: no live futures contract found in dump")

    _validated_mcx = result

    # Option underlying: which futures month actually carries live option
    # contracts (may differ from the spot futures for bi-monthly commodities).
    today_str = today.isoformat()
    this_month = today_str[:7]
    for sym, spot_key in result.items():
        live = [r for r in rows if r.get("name") == sym
                and r.get("instrument_type") in ("CE", "PE")
                and str(r.get("expiry") or "") >= today_str]
        if live and any(str(r.get("expiry"))[:7] == this_month for r in live):
            _mcx_option_underlying[sym] = spot_key
        elif live:
            nearest_expiry = str(sorted(live, key=lambda r: r.get("expiry"))[0].get("expiry"))
            underlying = _mcx_underlying_for_expiry(sym, nearest_expiry)
            _mcx_option_underlying[sym] = underlying or spot_key
        else:
            _mcx_option_underlying[sym] = spot_key

    detected_next_month = {
        sym for sym, u in _mcx_option_underlying.items()
        if u and result.get(sym) and u != result[sym]
    }
    _MCX_NEXT_MONTH_OPTION_SYMBOLS.clear()
    _MCX_NEXT_MONTH_OPTION_SYMBOLS.update(detected_next_month)
    print(f"[MCX] Next-month option symbols: {_MCX_NEXT_MONTH_OPTION_SYMBOLS}")

    await _resolve_indices()
    return result


async def refresh_mcx_option_underlying(token: str, spot_keys: dict) -> None:
    """Lightweight post-rollover refresh — re-derive _mcx_option_underlying
    for the given (already re-resolved) spot keys without re-running full
    MCX futures resolution."""
    if not token:
        return
    today = date.today()
    today_str = today.isoformat()
    this_month = today_str[:7]
    try:
        rows = await _load_dump("MCX")
    except Exception as e:
        print(f"[MCX Refresh] dump load error: {e}")
        return
    for sym, spot_key in spot_keys.items():
        if not spot_key:
            continue
        live = [r for r in rows if r.get("name") == sym
                and r.get("instrument_type") in ("CE", "PE")
                and str(r.get("expiry") or "") >= today_str]
        if live and any(str(r.get("expiry"))[:7] == this_month for r in live):
            _mcx_option_underlying[sym] = spot_key
            print(f"[MCX Refresh] {sym} underlying -> {spot_key}")
        elif live:
            nearest = str(sorted(live, key=lambda r: r.get("expiry"))[0].get("expiry"))
            underlying = _mcx_underlying_for_expiry(sym, nearest)
            if underlying:
                _mcx_option_underlying[sym] = underlying
                print(f"[MCX Refresh] {sym} underlying -> {underlying} (via {nearest})")


async def refresh_nse_eq_keys() -> None:
    """Resolve backend/instrument_keys.NSE_EQ_KEYS' stock symbols against
    the Zerodha NSE dump. Unlike Upstox (where this only corrected ISIN
    drift), this is the PRIMARY resolution step for Zerodha —
    instrument_keys.py ships only the symbol list, no hardcoded tokens."""
    from . import instrument_keys
    try:
        rows = await _load_dump("NSE")
    except Exception as e:
        print(f"[NSE] dump load error: {e}")
        return
    by_symbol = {r.get("tradingsymbol"): r for r in rows if r.get("instrument_type") == "EQ"}
    updated = 0
    for sym in list(instrument_keys.NSE_EQ_KEYS.keys()):
        row = by_symbol.get(sym)
        if not row:
            print(f"[NSE] {sym} not found in dump — check tradingsymbol")
            continue
        key = _register("NSE_EQ", "NSE", sym, row["instrument_token"])
        _nse_eq_sym_to_key[sym] = key
        if instrument_keys.NSE_EQ_KEYS[sym] != key:
            instrument_keys.NSE_EQ_KEYS[sym] = key
            updated += 1
    instrument_keys.ISIN_TO_SYMBOL = {v: k for k, v in instrument_keys.NSE_EQ_KEYS.items() if v}
    instrument_keys.FO_STOCK_KEYS = list(dict.fromkeys(v for v in instrument_keys.NSE_EQ_KEYS.values() if v))
    print(f"[Init] Resolved {updated} NSE_EQ keys from Zerodha instrument dump")


# ══════════════════════════════════════════════════════════════════
#  Response-key normalization — trivial pass-through now
# ══════════════════════════════════════════════════════════════════
# Upstox's REST/WS responses sometimes came back keyed by a name-based
# string that needed mapping to our canonical numeric key. With Zerodha we
# construct the internal key ourselves (via _register()/token_to_key()), so
# there is nothing to normalize — these are kept only for call-site
# compatibility (main.py's broadcast() calls normalize_response_key(k) on
# every WS feed key).
def normalize_mcx_response_key(key: str) -> str:
    return key


def normalize_nse_eq_response_key(key: str) -> str:
    return key


def normalize_response_key(key: str) -> str:
    return key
