"""
instruments.py v10 — All bugs fixed
Bug fixes:
1. v3 OHLC response: parse live_ohlc/prev_ohlc not val.get("ohlc")
2. Per-item None check in fetch_quotes_rest
3. Separate index quote via /v2/market-quote/quotes (not ohlc)
4. Option chain: pick closest strike to spot_price field in response
"""
import asyncio, time
from datetime import date, timedelta
import httpx

UPSTOX_CONTRACTS  = "https://api.upstox.com/v2/option/contract"
UPSTOX_CHAIN      = "https://api.upstox.com/v2/option/chain"
UPSTOX_QUOTE_V2   = "https://api.upstox.com/v2/market-quote/quotes"
UPSTOX_OHLC_V3    = "https://api.upstox.com/v3/market-quote/ohlc"
UPSTOX_OHLC_V2    = "https://api.upstox.com/v2/market-quote/ohlc"
UPSTOX_INTRADAY   = "https://api.upstox.com/v3/historical-candle/intraday"

STRIKE_STEPS = {
    "NIFTY":50,"BANKNIFTY":100,"FINNIFTY":50,"MIDCPNIFTY":25,
    "SENSEX":100,"BANKEX":100,"CRUDEOIL":50,"NATURALGAS":10,
    "GOLD":100,"SILVER":1000,"COPPER":5,
}
MONTHLY_SYMBOLS = {"GOLD","SILVER","COPPER","CRUDEOIL","NATURALGAS"}

_M = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

def mcx_key(sym: str, months_ahead: int = 0) -> str:
    """Generate MCX key. Uses instrument master if loaded, else name-based fallback."""
    if _mcx_sym_to_key:
        return _resolve_mcx_key(sym, months_ahead)
    d = date.today() + timedelta(days=30 * months_ahead)
    return f"MCX_FO|{sym.upper()}{str(d.year)[2:]}{_M[d.month-1]}FUT"


def mcx_key_for_month(sym: str, year: int, month: int) -> str:
    """Resolve MCX futures instrument_key for an explicit year/month.
    Used by the rollover step once the current-month options have expired.
    Returns a numeric key from the instrument master when available, else a
    name-based fallback (caller should treat that as invalid)."""
    trading_sym = f"{sym.upper()}{str(year)[2:]}{_M[month - 1]}FUT"
    if _mcx_sym_to_key and trading_sym in _mcx_sym_to_key:
        return _mcx_sym_to_key[trading_sym]
    return f"MCX_FO|{trading_sym}"

INDEX_SPOT = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "BANKEX":     "BSE_INDEX|BANKEX",
}

_validated_mcx: dict = {}   # Set by validate_mcx_keys(), used by get_spot_keys()

def get_spot_keys() -> dict:
    keys = dict(INDEX_SPOT)
    for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]:
        keys[s] = _validated_mcx.get(s, mcx_key(s, 0))
    return keys

def get_itm2_strikes(spot: float, symbol: str) -> tuple:
    step = STRIKE_STEPS.get(symbol.upper(), 50)
    atm  = round(round(spot / step) * step, 2)
    return atm - 2*step, atm + 2*step

def _h(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# ── Fallback expiry ──────────────────────────────────────────────
def _last_thu(year, month):
    if month == 12: nm = date(year+1,1,1)
    else:           nm = date(year,month+1,1)
    last = nm - timedelta(days=1)
    return last - timedelta(days=(last.weekday()-3)%7)

def calculate_expiries_fallback(symbol: str, count: int = 8) -> list:
    today = date.today(); sym = symbol.upper(); result = []
    if sym in MONTHLY_SYMBOLS:
        for i in range(3):
            m=today.month+i; y=today.year+(m-1)//12; m=((m-1)%12)+1
            lt=_last_thu(y,m)
            if lt >= today: result.append(lt.isoformat())
    else:
        d=today; n=0
        while n < count:
            days=(3-d.weekday())%7 or 7
            d+=timedelta(days=days)
            result.append(d.isoformat()); n+=1
    return sorted(set(result))

# ── Expiry list ──────────────────────────────────────────────────
async def fetch_expiry_list(symbol: str, token: str) -> list:
    """Extract unique expiry dates from /v2/option/contract response."""
    spot_keys = get_spot_keys()
    spot_key  = spot_keys.get(symbol.upper(), "")
    if not spot_key:
        from .instrument_keys import NSE_EQ_KEYS
        spot_key = NSE_EQ_KEYS.get(symbol.upper(), "")
    if not spot_key: return calculate_expiries_fallback(symbol)

    # For MCX, each futures key only returns its own month's option expiries.
    # Query multiple futures keys to collect all available expiries.
    if spot_key.startswith("MCX"):
        all_expiries = set()
        keys_to_try = []
        # Collect all futures keys for this symbol from instrument master
        sym_upper = symbol.upper()
        for tsym, ikey in _mcx_sym_to_key.items():
            if tsym.startswith(sym_upper) and tsym.endswith("FUT"):
                suffix = tsym[len(sym_upper):]
                if suffix[0:1] == "M" and suffix[1:2].isdigit():
                    continue  # skip mini
                if any(v in tsym for v in ["PETAL", "GUINEA", "TEN", "MIC"]):
                    continue
                keys_to_try.append((tsym, ikey))
        # Also include the option underlying key if set
        opt_key = _mcx_option_underlying.get(sym_upper)
        if opt_key and opt_key not in [k for _, k in keys_to_try]:
            keys_to_try.append(("OPT_UNDERLYING", opt_key))
        # Fallback to spot key
        if not keys_to_try:
            keys_to_try.append(("SPOT", spot_key))

        try:
            async with httpx.AsyncClient(timeout=15) as c:
                for tsym, ikey in keys_to_try[:4]:  # limit to 4 nearest futures
                    r = await c.get(UPSTOX_CONTRACTS,
                                    params={"instrument_key": ikey},
                                    headers=_h(token))
                    if r.status_code == 200:
                        contracts = r.json().get("data", [])
                        if isinstance(contracts, list):
                            for x in contracts:
                                if isinstance(x, dict) and x.get("expiry"):
                                    all_expiries.add(x["expiry"])
                    await asyncio.sleep(0.15)
        except Exception as e:
            print(f"[Expiry] {symbol}: {e}")

        if all_expiries:
            expiries = sorted(all_expiries)
            print(f"[Expiry] {symbol}: {expiries[:6]} ({len(expiries)} total)")
            return expiries
        return calculate_expiries_fallback(symbol)

    # Non-MCX: single query
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(UPSTOX_CONTRACTS, params={"instrument_key": spot_key}, headers=_h(token))
            print(f"[Expiry] {symbol} HTTP {r.status_code}")
            if r.status_code == 200:
                contracts = r.json().get("data", [])
                if isinstance(contracts, list):
                    expiries = sorted(set(
                        x["expiry"] for x in contracts
                        if isinstance(x, dict) and x.get("expiry")
                    ))
                    if expiries:
                        print(f"[Expiry] {symbol}: {expiries[:4]}")
                        return expiries
    except Exception as e:
        print(f"[Expiry] {symbol}: {e}")
    return calculate_expiries_fallback(symbol)

# ── MCX option chain (built from contracts + quotes) ────────────
def _mcx_underlying_for_expiry(symbol: str, expiry: str) -> str:
    """Pick the MCX futures underlying whose month matches the requested option
    expiry. MCX options are tied to a specific month's futures contract, so
    May options live under May futures, June options under June futures, etc.
    Returns "" if the instrument master hasn't been loaded or no match found.
    """
    if not expiry or len(expiry) < 7 or not _mcx_sym_to_key:
        return ""
    try:
        yr = int(expiry[:4]); mo = int(expiry[5:7])
    except Exception:
        return ""
    mon_str = f"{str(yr)[2:]}{_M[mo - 1]}"   # e.g. "26MAY"
    target = f"{symbol.upper()}{mon_str}FUT"
    if target in _mcx_sym_to_key:
        return _mcx_sym_to_key[target]
    return ""


async def _fetch_mcx_option_chain(symbol: str, expiry: str, token: str) -> dict:
    """
    Build MCX option chain from /v2/option/contract + /v2/market-quote/quotes.
    The /v2/option/chain endpoint doesn't return data for MCX, so we build it manually.
    """
    spot_keys = get_spot_keys()
    spot_key  = spot_keys.get(symbol.upper(), "")
    if not spot_key or not expiry: return {}

    # MCX options are listed under the futures contract of the same month.
    # When the requested expiry belongs to a later month than the current
    # spot futures, resolve the underlying to that month's futures key so
    # /v2/option/contract returns the right strikes. For commodities like
    # GOLD where futures trade bi-monthly but options for a given expiry
    # may be listed under a different month's futures, we fall back to
    # iterating every futures key for the symbol until one returns
    # contracts for the requested expiry.
    sym_upper = symbol.upper()
    primary  = _mcx_underlying_for_expiry(symbol, expiry)
    canonical = _mcx_option_underlying.get(sym_upper, spot_key)
    underlyings = []
    for k in (primary, canonical, spot_key):
        if k and k not in underlyings: underlyings.append(k)
    # Then every futures key the instrument master knows for this symbol
    # (skipping mini/micro variants) — covers cases where 2026-09-25 GOLD
    # options are listed under e.g. October futures rather than September.
    for tsym, ikey in _mcx_sym_to_key.items():
        if not (tsym.startswith(sym_upper) and tsym.endswith("FUT")): continue
        suffix = tsym[len(sym_upper):]
        if suffix[0:1] == "M" and suffix[1:2].isdigit(): continue  # mini
        if any(v in tsym for v in ["PETAL", "GUINEA", "TEN", "MIC"]): continue
        if ikey not in underlyings: underlyings.append(ikey)

    # Add name-based fallback keys for the expiry month + next 2 months.
    # NaturalGas June options live under July futures; July options under Aug
    # futures, etc. The next month's futures may not be in the instrument
    # master yet (CSV updated ~45 days ahead), so we generate name-based keys
    # that Upstox accepts directly. This ensures we always try the right month.
    try:
        exp_yr = int(expiry[:4]); exp_mo = int(expiry[5:7])
        for delta in range(0, 3):
            m = (exp_mo - 1 + delta) % 12 + 1
            y = exp_yr + ((exp_mo - 1 + delta) // 12)
            name_key = f"MCX_FO|{sym_upper}{str(y)[2:]}{_M[m - 1]}FUT"
            if name_key not in underlyings:
                underlyings.append(name_key)
    except Exception:
        pass

    option_key = underlyings[0] if underlyings else spot_key
    filtered = []

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            for idx, ikey in enumerate(underlyings[:10]):
                # Step 1: Get contracts for this expiry from each candidate
                r = await c.get(UPSTOX_CONTRACTS,
                                params={"instrument_key": ikey},
                                headers=_h(token))
                if r.status_code != 200:
                    print(f"[MCXChain] {symbol} underlying[{idx}]={ikey} HTTP {r.status_code}")
                    if r.status_code == 429:
                        await asyncio.sleep(0.8)
                    continue
                contracts = r.json().get("data", [])
                all_expiries = sorted(set(ct.get("expiry","") for ct in contracts if isinstance(ct,dict) and ct.get("expiry")))
                filtered = [ct for ct in contracts
                            if isinstance(ct, dict) and ct.get("expiry") == expiry]
                print(f"[MCXChain] {symbol} underlying[{idx}]={ikey}: {len(contracts)} contracts, expiries={all_expiries[:5]}, matched={len(filtered)}")
                if filtered:
                    option_key = ikey
                    break
                await asyncio.sleep(0.15)
            if not filtered:
                print(f"[MCXChain] {symbol}/{expiry} no contracts after trying {len(underlyings[:10])} underlyings")
                return {}

            # Step 2: Group by strike
            strike_map = {}  # strike → {CE: key, PE: key}
            for ct in filtered:
                strike = float(ct.get("strike_price", 0))
                opt_type = ct.get("instrument_type", "")
                ikey = ct.get("instrument_key", "")
                if strike and opt_type in ("CE", "PE") and ikey:
                    strike_map.setdefault(strike, {})[opt_type] = ikey

            if not strike_map:
                print(f"[MCXChain] {symbol}/{expiry} no strikes")
                return {}

            # Build contract-level numeric→tradingsym mapping for quote parsing.
            # _mcx_numeric_to_name only covers futures; option keys need this fallback.
            key_to_tsym = {}
            for ct in filtered:
                ikey = ct.get("instrument_key", "")
                tsym = ct.get("trading_symbol", "")
                if ikey and tsym:
                    key_to_tsym[ikey] = tsym
                    if ikey not in _mcx_numeric_to_name:
                        _mcx_numeric_to_name[ikey] = f"MCX_FO|{tsym}"

            # Step 3: Get spot price from the resolved option underlying.
            # `option_key` is the futures contract that actually carries
            # this expiry's options — for monthly contracts (CRUDEOIL,
            # NATURALGAS) it equals the same-month spot; for bi-monthly
            # contracts (GOLD/SILVER/COPPER) the Aug+Sep options both
            # live on Oct futures, so `option_key` is the correct
            # underlying even though it differs from the global spot_key.
            spot_from_quote = 0.0
            for attempt in range(2):
                r2 = await c.get(UPSTOX_QUOTE_V2,
                                 params={"instrument_key": option_key},
                                 headers=_h(token))
                if r2.status_code == 200:
                    for _, v in (r2.json().get("data", {}) or {}).items():
                        if v:
                            spot_from_quote = float(v.get("last_price", 0))
                            break
                    break
                elif r2.status_code == 429 and attempt == 0:
                    await asyncio.sleep(1.5)
                else:
                    break

            # Step 4: Find ITM-2 strikes and nearby strikes, fetch their quotes
            step = STRIKE_STEPS.get(symbol.upper(), 50)
            if spot_from_quote:
                atm = round(round(spot_from_quote / step) * step, 2)
                ce_target = atm - 2 * step
                pe_target = atm + 2 * step
            else:
                sorted_s = sorted(strike_map.keys())
                ce_target = sorted_s[len(sorted_s) // 2]
                pe_target = ce_target

            # Collect keys for strikes near ATM (within ±10 strikes)
            sorted_strikes = sorted(strike_map.keys())
            atm_idx = min(range(len(sorted_strikes)),
                          key=lambda i: abs(sorted_strikes[i] - (ce_target + pe_target) / 2))
            lo = max(0, atm_idx - 10)
            hi = min(len(sorted_strikes), atm_idx + 11)
            nearby_strikes = sorted_strikes[lo:hi]

            quote_keys = []
            for s in nearby_strikes:
                if "CE" in strike_map[s]: quote_keys.append(strike_map[s]["CE"])
                if "PE" in strike_map[s]: quote_keys.append(strike_map[s]["PE"])

            # Step 5: Fetch quotes in chunks
            # MCX API returns name-based keys (MCX_FO:CRUDEOIL26APR9450CE)
            # even when we request numeric keys (MCX_FO|562412).
            # Use _mcx_numeric_to_name to map between formats.
            quotes = {}
            for i in range(0, len(quote_keys), 25):
                chunk = quote_keys[i:i+25]
                # Retry once on rate limit (429)
                for attempt in range(2):
                    r3 = await c.get(UPSTOX_QUOTE_V2,
                                     params={"instrument_key": ",".join(chunk)},
                                     headers=_h(token))
                    if r3.status_code == 200:
                        resp_data = (r3.json().get("data", {}) or {})
                        # Map response name-based keys to numeric keys
                        for rk, rv in resp_data.items():
                            if rv:
                                quotes[rk.replace(":", "|", 1)] = rv
                        # Map requested numeric keys -> name-based response keys.
                        # Prefer _mcx_numeric_to_name; fall back to key_to_tsym
                        # built from contracts (covers option keys not in master).
                        for req_key in chunk:
                            name_key = _mcx_numeric_to_name.get(req_key, "")
                            if not name_key:
                                tsym = key_to_tsym.get(req_key, "")
                                if tsym:
                                    name_key = f"MCX_FO|{tsym}"
                            if name_key:
                                colon_name = name_key.replace("|", ":", 1)
                                val = resp_data.get(colon_name)
                                if val:
                                    quotes[req_key] = val
                        break
                    elif r3.status_code == 429 and attempt == 0:
                        print(f"[MCXChain] Rate limited on chunk {i//25+1}, retrying in 1.5s...")
                        await asyncio.sleep(1.5)
                    else:
                        print(f"[MCXChain] Quote chunk {i//25+1} HTTP {r3.status_code}")
                        break
                await asyncio.sleep(0.3)

            # Step 6: Build chain dict
            # Note: ohlc.close = today's close (= LTP during trading), NOT previous day's close.
            # Derive previous close from net_change: prev_close = ltp - net_change.
            chain = {}
            for strike in nearby_strikes:
                ce_key = strike_map[strike].get("CE", "")
                pe_key = strike_map[strike].get("PE", "")
                ce_q = quotes.get(ce_key, {})
                pe_q = quotes.get(pe_key, {})
                ce_ohlc = ce_q.get("ohlc", {}) or {}
                pe_ohlc = pe_q.get("ohlc", {}) or {}

                ce_ltp = float(ce_q.get("last_price", 0) or 0)
                ce_net = float(ce_q.get("net_change", 0) or 0)
                ce_prev_close = round(ce_ltp - ce_net, 2) if ce_ltp and ce_net else 0

                pe_ltp = float(pe_q.get("last_price", 0) or 0)
                pe_net = float(pe_q.get("net_change", 0) or 0)
                pe_prev_close = round(pe_ltp - pe_net, 2) if pe_ltp and pe_net else 0

                chain[strike] = {
                    "CE": {
                        "ltp":   ce_ltp,
                        "close": ce_prev_close,
                        "high":  float(ce_ohlc.get("high", 0) or 0),
                        "low":   float(ce_ohlc.get("low", 0) or 0),
                        "oi":    float(ce_q.get("oi", 0) or 0),
                        "iv":    0.0,
                        "key":   ce_key,
                    },
                    "PE": {
                        "ltp":   pe_ltp,
                        "close": pe_prev_close,
                        "high":  float(pe_ohlc.get("high", 0) or 0),
                        "low":   float(pe_ohlc.get("low", 0) or 0),
                        "oi":    float(pe_q.get("oi", 0) or 0),
                        "iv":    0.0,
                        "key":   pe_key,
                    },
                    "_spot": spot_from_quote,
                    # Surface the resolved underlying futures key so
                    # downstream consumers (calculator.py) can pair this
                    # expiry's options with the right spot — necessary
                    # for bi-monthly MCX symbols where same-month
                    # futures don't exist (GOLD Sep options → Oct futures).
                    "_option_key": option_key,
                }

            if chain:
                atm_s = min(chain.keys(), key=lambda s: abs(s - (spot_from_quote or ce_target)))
                atm_ce_ltp = chain[atm_s]['CE']['ltp']
                print(f"[MCXChain] {symbol}/{expiry}: {len(chain)} strikes, "
                      f"spot={spot_from_quote}, ATM={atm_s}, "
                      f"CE_ltp={atm_ce_ltp}")
                if atm_ce_ltp == 0 and quotes:
                    sample = list(quotes.keys())[:4]
                    print(f"[MCXChain] DEBUG {symbol} quotes keys sample: {sample}")
                elif atm_ce_ltp == 0:
                    print(f"[MCXChain] DEBUG {symbol} quotes dict is EMPTY — API returned no data")
            return chain

    except Exception as e:
        print(f"[MCXChain] {symbol}/{expiry}: {e}")
        return {}


# ── Option chain ─────────────────────────────────────────────────
async def fetch_option_chain(symbol: str, expiry: str, token: str) -> dict:
    """
    Fetch full option chain. Returns {strike: {CE:{...}, PE:{...}}}.
    Also returns the spot price from underlying_spot_price field.
    Supports indices (NSE_INDEX), MCX commodities, and FNO stocks (NSE_EQ).
    """
    spot_keys = get_spot_keys()
    spot_key  = spot_keys.get(symbol.upper(), "")
    if not spot_key:
        from .instrument_keys import NSE_EQ_KEYS
        spot_key = NSE_EQ_KEYS.get(symbol.upper(), "")
    if not spot_key or not expiry: return {}

    # MCX uses contract-based chain building (option/chain API doesn't support MCX)
    if spot_key.startswith("MCX"):
        return await _fetch_mcx_option_chain(symbol, expiry, token)

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(UPSTOX_CHAIN,
                            params={"instrument_key": spot_key, "expiry_date": expiry},
                            headers=_h(token))
            print(f"[Chain] {symbol}/{expiry} HTTP {r.status_code}")
            if r.status_code != 200:
                print(f"[Chain] error: {r.text[:200]}"); return {}
            rows = r.json().get("data", [])
            if not rows: print(f"[Chain] {symbol} empty"); return {}

            chain = {}
            spot_from_chain = 0.0  # Upstox returns underlying_spot_price
            for row in rows:
                strike = float(row.get("strike_price", 0))
                if not strike: continue
                # Get spot price from first row
                if not spot_from_chain:
                    spot_from_chain = float(row.get("underlying_spot_price", 0))

                ce = row.get("call_options", {})
                pe = row.get("put_options", {})
                ce_md = ce.get("market_data", {}) or {}
                pe_md = pe.get("market_data", {}) or {}

                def _p(d, *ks):
                    for k in ks:
                        v = d.get(k) if d else None
                        if v is not None:
                            try:
                                fv = float(v)
                                if fv != 0: return fv
                            except: pass
                    return 0.0

                chain[strike] = {
                    "CE": {
                        "ltp":   _p(ce_md, "ltp"),
                        "close": _p(ce_md, "close_price"),
                        "high":  _p(ce_md, "high_price"),
                        "low":   _p(ce_md, "low_price"),
                        "oi":    _p(ce_md, "oi"),
                        "iv":    float((ce.get("option_greeks") or {}).get("iv", 0)),
                        "key":   ce.get("instrument_key", ""),
                    },
                    "PE": {
                        "ltp":   _p(pe_md, "ltp"),
                        "close": _p(pe_md, "close_price"),
                        "high":  _p(pe_md, "high_price"),
                        "low":   _p(pe_md, "low_price"),
                        "oi":    _p(pe_md, "oi"),
                        "iv":    float((pe.get("option_greeks") or {}).get("iv", 0)),
                        "key":   pe.get("instrument_key", ""),
                    },
                    "_spot": spot_from_chain,
                }

            # Find sample near ATM
            if chain and spot_from_chain:
                atm = min(chain.keys(), key=lambda s: abs(s - spot_from_chain))
                print(f"[Chain] {symbol}/{expiry}: {len(chain)} strikes, "
                      f"spot={spot_from_chain}, ATM={atm}, "
                      f"CE_ltp={chain[atm]['CE']['ltp']}, "
                      f"CE_key={chain[atm]['CE']['key'][:25] if chain[atm]['CE']['key'] else 'MISSING'}")
            return chain
    except Exception as e:
        print(f"[Chain] {symbol}/{expiry}: {e}"); return {}

# ── OHLC snapshot for stocks (v3 format) ─────────────────────────
async def fetch_quotes_rest(keys: list, token: str) -> dict:
    """
    Fetch full quotes for stocks/commodities using /v2/market-quote/quotes.
    Uses net_change to derive previous close (cp = ltp - net_change).
    v3 OHLC returns prev_ohlc:null so we use v2 full quotes instead.
    NOTE: Do NOT pass INDEX keys here — indices are handled by fetch_index_quotes.
    """
    if not keys or not token: return {}
    results = {}

    # Filter out index keys — they need different endpoint
    stock_keys = [k for k in keys if not k.startswith("NSE_INDEX") and not k.startswith("BSE_INDEX")]

    for i in range(0, len(stock_keys), 50):
        chunk = stock_keys[i:i+50]
        if not chunk: continue
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(UPSTOX_QUOTE_V2,
                                params={"instrument_key": ",".join(chunk)},
                                headers=_h(token))
                if r.status_code == 200:
                    resp_json = r.json()
                    data = resp_json.get("data") if resp_json else None
                    if data is None:
                        print(f"[Quote-v2] null data response")
                        continue
                    for resp_key, val in data.items():
                        if val is None: continue
                        try:
                            norm = normalize_response_key(resp_key.replace(":", "|", 1))
                            ltp  = float(val.get("last_price") or 0)
                            ohlc = val.get("ohlc") or {}
                            net_change = float(val.get("net_change") or 0)
                            # Derive previous close from net_change
                            cp = round(ltp - net_change, 2) if ltp and net_change else 0
                            o  = float(ohlc.get("open")  or ltp)
                            h  = float(ohlc.get("high")  or ltp)
                            l  = float(ohlc.get("low")   or ltp)
                            if l == 0 and ltp: l = ltp
                            results[norm] = {
                                "ltpc":  {"ltp": ltp, "cp": cp},
                                "efeed": {"ltp": ltp, "cp": cp, "open": o, "high": h, "low": l},
                            }
                        except Exception as ex:
                            print(f"[Quote-v2] item error {resp_key}: {ex}")
                    print(f"[Quote-v2] chunk {i//50+1}: +{len(data)} items, total={len(results)}")
                elif r.status_code == 429:
                    print("[Quote] Rate limited, waiting 2s...")
                    await asyncio.sleep(2); continue
                else:
                    print(f"[Quote-v2] HTTP {r.status_code}")
        except Exception as e:
            print(f"[Quote] chunk error: {e}")
        await asyncio.sleep(0.3)
    return results


async def fetch_index_quotes(index_keys: list, token: str) -> dict:
    """
    Fetch index LTP+OHLC via /v2/market-quote/quotes.
    Indices don't support the /ohlc endpoint.
    Uses net_change to derive previous close (cp = ltp - net_change).
    Note: ohlc.close in v2 equals current LTP, NOT yesterday's close.
    """
    if not index_keys or not token: return {}
    results = {}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(UPSTOX_QUOTE_V2,
                            params={"instrument_key": ",".join(index_keys)},
                            headers=_h(token))
            if r.status_code == 200:
                data = (r.json() or {}).get("data") or {}
                for resp_key, val in data.items():
                    if not val: continue
                    norm = resp_key.replace(":", "|", 1)
                    ohlc = val.get("ohlc") or {}
                    ltp  = float(val.get("last_price") or 0)
                    net_change = float(val.get("net_change") or 0)
                    # Derive previous close from net_change (ohlc.close = current ltp, not prev close)
                    cp = round(ltp - net_change, 2) if ltp else 0
                    o    = float(ohlc.get("open")  or ltp)
                    h    = float(ohlc.get("high")  or ltp)
                    l    = float(ohlc.get("low")   or ltp)
                    results[norm] = {
                        "ltpc":  {"ltp": ltp, "cp": cp},
                        "efeed": {"ltp": ltp, "cp": cp, "open": o, "high": h, "low": l},
                    }
                print(f"[IndexQuote] {len(results)} indices loaded")
    except Exception as e:
        print(f"[IndexQuote] {e}")
    return results


async def fetch_option_ohlc_rest(ce_key: str, pe_key: str, token: str) -> dict:
    """Get full OHLC for CE and PE option keys via /v2/market-quote/quotes.
    Note: ohlc.close = today's close (= LTP), NOT previous close.
    Previous close is derived from net_change: cp = ltp - net_change.
    """
    if not token: return {}
    keys = [k for k in [ce_key, pe_key] if k]
    if not keys: return {}
    result = {}
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            # Retry once on rate limit
            for attempt in range(2):
                r = await c.get(UPSTOX_QUOTE_V2,
                                params={"instrument_key": ",".join(keys)},
                                headers=_h(token))
                if r.status_code == 200:
                    break
                elif r.status_code == 429 and attempt == 0:
                    await asyncio.sleep(1.5)
                else:
                    print(f"[OptOHLC] HTTP {r.status_code}")
                    return result
            if r.status_code != 200:
                return result
            data = (r.json() or {}).get("data") or {}
            # API returns keys in trading-symbol format (e.g. NSE_FO:NIFTY2640722600CE)
            # which differs from numeric instrument_key (NSE_FO|40738).
            # Match using: 1) direct match, 2) MCX numeric→name mapping, 3) CE/PE suffix
            for resp_key, val in data.items():
                if not val: continue
                matched_key = None
                pipe_key = resp_key.replace(":", "|", 1)
                # Strategy 1: direct key match
                for orig_key in keys:
                    if orig_key == pipe_key or orig_key == resp_key:
                        matched_key = orig_key
                        break
                # Strategy 2: MCX numeric→name mapping
                if not matched_key:
                    for orig_key in keys:
                        name_key = _mcx_numeric_to_name.get(orig_key, "")
                        if name_key and name_key.replace("|", ":", 1) == resp_key:
                            matched_key = orig_key
                            break
                # Strategy 3: CE/PE suffix fallback
                if not matched_key:
                    if resp_key.endswith("CE") and ce_key:
                        matched_key = ce_key
                    elif resp_key.endswith("PE") and pe_key:
                        matched_key = pe_key
                if not matched_key:
                    continue
                ohlc = val.get("ohlc") or {}
                ltp = float(val.get("last_price") or 0)
                net_change = float(val.get("net_change") or 0)
                # Derive previous close from net_change (ohlc.close = today's close = LTP)
                prev_close = round(ltp - net_change, 2) if ltp and net_change else 0
                result[matched_key] = {
                    "ltp":   ltp,
                    "close": prev_close,
                    "high":  float(ohlc.get("high")  or 0),
                    "low":   float(ohlc.get("low")   or 0),
                    "open":  float(ohlc.get("open")  or 0),
                    "oi":    float(val.get("oi")      or 0),
                }
    except Exception as e:
        print(f"[OptOHLC] {e}")
    return result


_MCX_INSTRUMENT_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

# Cache: tradingsymbol → instrument_key (e.g. "CRUDEOIL26APRFUT" → "MCX_FO|486502")
_mcx_sym_to_key: dict = {}
# Reverse: name-based key → numeric key (e.g. "MCX_FO|CRUDEOIL26APRFUT" → "MCX_FO|486502")
_mcx_name_to_numeric: dict = {}
# Reverse: numeric key → name-based key for MCX (e.g. "MCX_FO|562412" → "MCX_FO|CRUDEOIL26APR9450CE")
_mcx_numeric_to_name: dict = {}
# NSE_EQ tradingsymbol → instrument_key (e.g. "RELIANCE" → "NSE_EQ|INE002A01018")
_nse_eq_sym_to_key: dict = {}


async def _load_mcx_instrument_master():
    """Download Upstox instrument master and build MCX futures + NSE_EQ lookups."""
    global _mcx_sym_to_key, _nse_eq_sym_to_key
    if _mcx_sym_to_key:
        return  # already loaded
    try:
        import gzip
        async with httpx.AsyncClient(timeout=30, verify=False) as c:
            r = await c.get(_MCX_INSTRUMENT_MASTER_URL)
            if r.status_code != 200:
                print(f"[MCX] Instrument master HTTP {r.status_code}")
                return
            data = gzip.decompress(r.content).decode("utf-8")
            lines = data.split("\n")
            nse_count = 0
            for line in lines[1:]:
                cols = line.split(",")
                if len(cols) < 12:
                    continue
                exchange = cols[11].strip('"')
                inst_key = cols[0].strip('"')
                trading_sym = cols[2].strip('"')
                if exchange == "MCX_FO":
                    _mcx_name_to_numeric[f"MCX_FO|{trading_sym}"] = inst_key
                    _mcx_numeric_to_name[inst_key] = f"MCX_FO|{trading_sym}"
                    if "FUT" in trading_sym:
                        _mcx_sym_to_key[trading_sym] = inst_key
                elif exchange == "NSE_EQ":
                    _nse_eq_sym_to_key[trading_sym] = inst_key
                    nse_count += 1
        print(f"[MCX] Instrument master loaded: {len(_mcx_sym_to_key)} MCX futures, {nse_count} NSE_EQ")
    except Exception as e:
        print(f"[MCX] Instrument master error: {e}")


def refresh_nse_eq_keys():
    """Update NSE_EQ_KEYS and derived maps from the instrument master."""
    if not _nse_eq_sym_to_key:
        return
    from . import instrument_keys
    updated = 0
    for sym in list(instrument_keys.NSE_EQ_KEYS.keys()):
        master_key = _nse_eq_sym_to_key.get(sym)
        if master_key and master_key != instrument_keys.NSE_EQ_KEYS[sym]:
            instrument_keys.NSE_EQ_KEYS[sym] = master_key
            updated += 1
    if updated:
        # Rebuild derived maps
        instrument_keys.ISIN_TO_SYMBOL = {v: k for k, v in instrument_keys.NSE_EQ_KEYS.items()}
        instrument_keys.FO_STOCK_KEYS = list(dict.fromkeys(instrument_keys.NSE_EQ_KEYS.values()))
        print(f"[Init] Updated {updated} NSE_EQ keys from instrument master")


def normalize_mcx_response_key(key: str) -> str:
    """Convert name-based MCX key from API response to numeric instrument_key.
    e.g. 'MCX_FO|CRUDEOIL26APRFUT' → 'MCX_FO|486502'
    Returns original key if no mapping found.
    """
    if key.startswith("MCX_FO|") and not key.split("|")[1][:1].isdigit():
        return _mcx_name_to_numeric.get(key, key)
    return key


def normalize_nse_eq_response_key(key: str) -> str:
    """Convert symbol-based NSE_EQ key to ISIN instrument_key.
    Upstox REST API returns 'NSE_EQ|ITC' but we need 'NSE_EQ|INE154A01025'.
    ISIN format: INE + 3 alphanum + letter + 5 digits (e.g. INE154A01025)
    """
    if key.startswith("NSE_EQ|"):
        sym_part = key.split("|", 1)[1]
        # Check if already in ISIN format (INE followed by digits/letters, 12 chars)
        is_isin = (len(sym_part) == 12 and sym_part[:3] == "INE"
                   and sym_part[3:6].isalnum() and sym_part[6:12].isalnum())
        if not is_isin:
            isin_key = _nse_eq_sym_to_key.get(sym_part, "")
            if isin_key:
                return isin_key
    return key


def normalize_response_key(key: str) -> str:
    """Normalize any API response key to our canonical instrument_key format."""
    if key.startswith("MCX_FO|"):
        return normalize_mcx_response_key(key)
    if key.startswith("NSE_EQ|"):
        return normalize_nse_eq_response_key(key)
    return key


def _resolve_mcx_key(sym: str, months_ahead: int) -> str:
    """Resolve a commodity symbol to its correct Upstox instrument_key."""
    d = date.today() + timedelta(days=30 * months_ahead)
    trading_sym = f"{sym.upper()}{str(d.year)[2:]}{_M[d.month - 1]}FUT"
    # Try exact match first
    if trading_sym in _mcx_sym_to_key:
        return _mcx_sym_to_key[trading_sym]
    # Fallback: return name-based key (won't work but won't crash)
    return f"MCX_FO|{trading_sym}"


# MCX option underlying keys (may differ from spot futures key)
_mcx_option_underlying: dict = {}   # sym → instrument_key for option chain

async def validate_mcx_keys(token: str) -> dict:
    """Find correct MCX instrument keys from the instrument master."""
    global _validated_mcx, _mcx_option_underlying
    await _load_mcx_instrument_master()

    result = {}
    today = date.today()
    for sym in ["CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER"]:
        found = False
        # Search instrument master for nearest unexpired futures
        candidates = []
        for tsym, ikey in _mcx_sym_to_key.items():
            if tsym.startswith(sym) and tsym.endswith("FUT"):
                # Skip mini/micro variants
                suffix = tsym[len(sym):]
                if suffix[0:1] == "M" and suffix[1:2].isdigit():
                    # e.g. CRUDEOILM26APRFUT — skip mini
                    continue
                if any(v in tsym for v in ["PETAL", "GUINEA", "TEN", "MIC"]):
                    continue
                candidates.append((tsym, ikey))

        if candidates:
            # Sort by expiry proximity — parse month/year from trading symbol
            def _parse_expiry(tsym):
                s = tsym[len(sym):]  # e.g. "26APRFUT"
                try:
                    yr = int("20" + s[:2])
                    mon = _M.index(s[2:5]) + 1
                    return date(yr, mon, 1)
                except:
                    return date(2099, 1, 1)

            candidates.sort(key=lambda x: _parse_expiry(x[0]))
            # Pick the nearest future that hasn't expired
            for tsym, ikey in candidates:
                exp_approx = _parse_expiry(tsym)
                if exp_approx >= today.replace(day=1):
                    result[sym] = ikey
                    print(f"[MCX] {sym} = {ikey} ({tsym})")
                    found = True
                    break

        if not found:
            # Absolute fallback: try months_ahead 0, 1, 2
            for m in [0, 1, 2]:
                key = _resolve_mcx_key(sym, m)
                if key.startswith("MCX_FO|") and not key.split("|")[1][0].isdigit():
                    continue  # name-based fallback, skip
                result[sym] = key
                found = True
                break
            if not found:
                result[sym] = _resolve_mcx_key(sym, 1)
                print(f"[MCX] {sym} → fallback {result[sym]}")
    _validated_mcx = result

    # Find option underlying keys (may differ from spot futures for some commodities)
    if token:
        for sym in result:
            spot_key = result[sym]
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get(UPSTOX_CONTRACTS,
                                    params={"instrument_key": spot_key},
                                    headers=_h(token))
                    if r.status_code == 200:
                        contracts = r.json().get("data", [])
                        if contracts:
                            _mcx_option_underlying[sym] = spot_key
                            print(f"[MCX] {sym} options on {spot_key}: {len(contracts)} contracts")
                            continue
                # If spot key has no contracts, try other futures for this symbol
                candidates = []
                for tsym, ikey in _mcx_sym_to_key.items():
                    if tsym.startswith(sym) and tsym.endswith("FUT"):
                        suffix = tsym[len(sym):]
                        if suffix[0:1] == "M" and suffix[1:2].isdigit():
                            continue
                        if any(v in tsym for v in ["PETAL", "GUINEA", "TEN", "MIC"]):
                            continue
                        if ikey != spot_key:
                            candidates.append((tsym, ikey))
                # Sort by expiry proximity
                def _parse_exp(tsym):
                    s = tsym[len(sym):]
                    try: return date(int("20" + s[:2]), _M.index(s[2:5]) + 1, 1)
                    except: return date(2099, 1, 1)
                candidates.sort(key=lambda x: _parse_exp(x[0]))
                for tsym, ikey in candidates:
                    try:
                        async with httpx.AsyncClient(timeout=10) as c:
                            r = await c.get(UPSTOX_CONTRACTS,
                                            params={"instrument_key": ikey},
                                            headers=_h(token))
                            if r.status_code == 200:
                                contracts = r.json().get("data", [])
                                if contracts:
                                    _mcx_option_underlying[sym] = ikey
                                    print(f"[MCX] {sym} options on {ikey} ({tsym}): {len(contracts)} contracts")
                                    break
                    except:
                        pass
                    await asyncio.sleep(0.2)
            except Exception as e:
                print(f"[MCX] {sym} option key search: {e}")
            await asyncio.sleep(0.2)

    return result


async def fetch_intraday_candles(key: str, token: str,
                                  unit: str = "minutes", interval: int = 1) -> list:
    """Fetch today's intraday candles via /v3/historical-candle/intraday."""
    if not token: return []
    encoded = key.replace("|", "%7C")
    url = f"{UPSTOX_INTRADAY}/{encoded}/{unit}/{interval}"
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, headers=_h(token))
            if r.status_code == 200:
                raw = (r.json() or {}).get("data", {})
                if raw is None: return []
                candles_raw = raw.get("candles", []) or []
                result = []
                for candle in candles_raw:
                    if len(candle) < 5: continue
                    try:
                        from datetime import datetime
                        ts = int(datetime.fromisoformat(str(candle[0])).timestamp()*1000)
                    except:
                        ts = int(time.time()*1000)
                    result.append({
                        "t": ts,
                        "o": float(candle[1] or 0),
                        "h": float(candle[2] or 0),
                        "l": float(candle[3] or 0),
                        "c": float(candle[4] or 0),
                        "v": int(candle[5])  if len(candle)>5 else 0,
                    })
                return result
    except Exception as e:
        print(f"[Candle] {key}: {e}")
    return []


def get_current_and_next_expiry(expiries: list, symbol: str) -> dict:
    today  = date.today()
    future = sorted([e for e in expiries if e >= today.isoformat()])
    # Skip expiries within 3 days — near-expiry MCX contracts are often
    # delisted or have no open interest before their last day.
    near_cutoff = (today + timedelta(days=3)).isoformat()
    active = [e for e in future if e > near_cutoff] or future
    result = {"all": expiries, "default": active[0] if active else None}
    if symbol.upper() in MONTHLY_SYMBOLS:
        # Group unexpired expiries by year-month in order. The first live
        # month is "current_month" and the second is "next_month". Once the
        # calendar month's options have all expired this naturally rolls the
        # labels forward — e.g. on 17 Apr after Crude's 16 Apr options die,
        # current_month = 14 May and next_month = June expiry.
        months_order = []
        for e in future:
            ym = e[:7]
            if ym not in months_order:
                months_order.append(ym)
        def _last_in(ym): return [e for e in future if e.startswith(ym)][-1]
        result["current_month"] = _last_in(months_order[0]) if len(months_order) > 0 else None
        result["next_month"]    = _last_in(months_order[1]) if len(months_order) > 1 else None
    else:
        result["current_week"] = active[0] if len(active)>0 else None
        result["next_week"]    = active[1] if len(active)>1 else None
        result["far_week"]     = active[2] if len(active)>2 else None
    return result
