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
        # Prepend the currently-active validated spot key so it's always
        # queried even if it sits beyond the slice limit below. After a
        # monthly rollover the new futures key may be far down the sorted
        # master list; without this it gets cut off and we miss its expiries.
        active_spot = _validated_mcx.get(sym_upper) or spot_key
        existing_keys = [k for _, k in keys_to_try]
        if active_spot not in existing_keys:
            keys_to_try.insert(0, ("ACTIVE_SPOT", active_spot))
        elif active_spot in existing_keys:
            # Move it to front so it's never cut by the slice
            idx = existing_keys.index(active_spot)
            entry = keys_to_try.pop(idx)
            keys_to_try.insert(0, entry)
        # Fallback to spot key
        if not keys_to_try:
            keys_to_try.append(("SPOT", spot_key))

        try:
            async with httpx.AsyncClient(timeout=15) as c:
                for tsym, ikey in keys_to_try[:6]:  # try up to 6 futures keys
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
    """Pick the MCX futures underlying whose month matches the requested option expiry.

    All MCX commodities use same-month convention: expiry month Y → futures month Y.
    (Copper, Silver, NaturalGas all confirmed same-month from live contract data.)
    Returns "" if the instrument master hasn't been loaded or no match found.
    """
    if not expiry or len(expiry) < 7 or not _mcx_sym_to_key:
        return ""
    try:
        yr = int(expiry[:4]); mo = int(expiry[5:7])
    except Exception:
        return ""
    sym_upper = symbol.upper()
    mon_str = f"{str(yr)[2:]}{_M[mo - 1]}"   # e.g. "26JUL"
    target = f"{sym_upper}{mon_str}FUT"
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
    fallback_filtered: list = []
    fallback_expiry: str = ""
    fallback_key: str = ""

    try:
        today_str = date.today().isoformat()
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
                matched = [ct for ct in contracts
                           if isinstance(ct, dict) and ct.get("expiry") == expiry]
                print(f"[MCXChain] {symbol} underlying[{idx}]={ikey}: {len(contracts)} contracts, expiries={all_expiries[:5]}, matched={len(matched)}")
                if matched:
                    filtered = matched
                    option_key = ikey
                    break
                # Track nearest available future expiry as fallback.
                # Handles MCX date mismatches (e.g. expiry API says 2026-06-25
                # but contracts actually show 2026-06-23 on the JUNFUT).
                future_expiries = [e for e in all_expiries if e >= today_str]
                if future_expiries:
                    nearest = min(future_expiries,
                                  key=lambda e: abs((date.fromisoformat(e) - date.fromisoformat(expiry)).days))
                    nearest_cts = [ct for ct in contracts if isinstance(ct,dict) and ct.get("expiry") == nearest]
                    if nearest_cts and (not fallback_expiry or
                            abs((date.fromisoformat(nearest) - date.fromisoformat(expiry)).days) <
                            abs((date.fromisoformat(fallback_expiry) - date.fromisoformat(expiry)).days)):
                        fallback_filtered = nearest_cts
                        fallback_expiry   = nearest
                        fallback_key      = ikey
                        # Stop early only when the date mismatch is within tolerance:
                        # ≤7 days: NaturalGas/CrudeOil — small MCX date mismatches only.
                        #          A 31-day gap (June→July) must NOT be accepted; that
                        #          would load the wrong month's contracts entirely.
                        # ≤45 days: GOLD/SILVER/COPPER delivery-month convention —
                        #          expiry list returns last day of delivery month (e.g.
                        #          2026-08-31) but option contracts expire early that month
                        #          (~Aug 4). Gap can be 25-30 days; 45 covers it safely.
                        _MONTHLY_TIGHT = {"CRUDEOIL", "NATURALGAS"}
                        max_gap = 7 if sym_upper in _MONTHLY_TIGHT else 45
                        gap = abs((date.fromisoformat(nearest) - date.fromisoformat(expiry)).days)
                        if gap <= max_gap:
                            break
                await asyncio.sleep(0.15)
            if not filtered:
                if fallback_filtered:
                    filtered   = fallback_filtered
                    option_key = fallback_key
                    print(f"[MCXChain] {symbol} expiry mismatch — requested {expiry}, using nearest {fallback_expiry}")
                else:
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
            # Upstox quote API needs name-based key for MCX; numeric keys return empty
            _opt_name_key = _mcx_numeric_to_name.get(option_key, option_key)
            async with _quote_chunk_sem:
                await asyncio.sleep(2.0)
                for attempt in range(2):
                    r2 = await c.get(UPSTOX_QUOTE_V2,
                                     params={"instrument_key": _opt_name_key},
                                     headers=_h(token))
                    if r2.status_code == 200:
                        for _, v in (r2.json().get("data", {}) or {}).items():
                            if v:
                                spot_from_quote = float(v.get("last_price", 0))
                                break
                        break
                    elif r2.status_code == 429 and attempt == 0:
                        await asyncio.sleep(3)
                    else:
                        break

            # If API spot fetch failed, fall back to live price from WebSocket state
            if not spot_from_quote:
                try:
                    import sys as _sys_sp
                    _ms = _sys_sp.modules.get("backend.main")
                    if _ms:
                        _st = getattr(_ms, "state", None)
                        _md = getattr(_st, "market_data", {}) if _st else {}
                        for _sk in (option_key, _opt_name_key):
                            _td = _md.get(_sk)
                            if _td:
                                _lp = ((_td.get("ltpc") or {}).get("ltp") or
                                       (_td.get("efeed") or {}).get("ltp"))
                                if _lp:
                                    spot_from_quote = float(_lp)
                                    print(f"[MCXChain] {symbol}: spot fallback from market_data → {spot_from_quote}")
                                    break
                except Exception:
                    pass

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

            # MCX quote API returns empty for numeric keys; use name-based keys
            _qkey_to_ikey = {}
            quote_keys = []
            for s in nearby_strikes:
                for _sd in ("CE", "PE"):
                    if _sd not in strike_map[s]: continue
                    _ik = strike_map[s][_sd]
                    # Prefer instrument-master compact format (e.g. COPPER26AUG1275CE)
                    # over option-chain API spaced format (e.g. COPPER 1315 CE 24 AUG 26)
                    _nk = _mcx_numeric_to_name.get(_ik, "")
                    if not _nk:
                        _ts = key_to_tsym.get(_ik, "")
                        _nk = f"MCX_FO|{_ts}" if _ts else _ik
                    if _nk not in quote_keys:
                        quote_keys.append(_nk)
                    _qkey_to_ikey[_nk] = _ik

            # Step 5: Fetch quotes in chunks
            # MCX API returns name-based keys (MCX_FO:CRUDEOIL26APR9450CE)
            # even when we request numeric keys (MCX_FO|562412).
            # Use _mcx_numeric_to_name to map between formats.
            quotes = {}
            for i in range(0, len(quote_keys), 25):
                chunk = quote_keys[i:i+25]
                async with _quote_chunk_sem:
                  await asyncio.sleep(2.0)  # enforce ≥0.6s gap between any two chunk requests
                  # Retry on rate limit (429)
                  for attempt in range(4):
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
                    elif r3.status_code == 429 and attempt < 3:
                        wait = (attempt + 1) * 3
                        print(f"[MCXChain] Rate limited chunk {i//25+1} attempt {attempt+1}, retrying in {wait}s...")
                        await asyncio.sleep(wait)
                    else:
                        print(f"[MCXChain] Quote chunk {i//25+1} HTTP {r3.status_code}")
                        break
                await asyncio.sleep(0.3)

            # Back-map name-based quote keys → numeric keys for chain building
            for _nk, _ik in _qkey_to_ikey.items():
                if _nk in quotes and _ik not in quotes:
                    quotes[_ik] = quotes[_nk]

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
        from datetime import date as _d
        chain = await _fetch_mcx_option_chain(symbol, expiry, token)
        # If all LTPs are zero on expiry day the contract has settled.
        # Fall back to the next available expiry so LOC keeps showing data.
        if chain and expiry <= _d.today().isoformat():
            if not any(v.get("CE", {}).get("ltp") for v in chain.values()):
                try:
                    import sys as _sys
                    _main = _sys.modules.get("backend.main")
                    if _main:
                        all_exp = (getattr(_main, "state", None) and
                                   _main.state.expiry_cache.get(symbol.upper(), {}).get("all")) or []
                        next_exp = next((e for e in sorted(all_exp) if e > expiry), None)
                        if next_exp:
                            print(f"[MCXChain] {symbol}: {expiry} settled, falling back to {next_exp}")
                            chain = await _fetch_mcx_option_chain(symbol, next_exp, token)
                except Exception:
                    pass
        return chain

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            # Try ISIN-based key first, then NSE_FO symbol-based fallback.
            # Some stocks' option chains return empty on ISIN key but work on
            # the trading-symbol key (e.g. recently-added F&O names).
            keys_to_try = [spot_key]
            if spot_key.startswith("NSE_EQ|"):
                keys_to_try.append(f"NSE_FO|{symbol.upper()}")

            rows = []
            for attempt_key in keys_to_try:
                for attempt in range(2):  # retry once on 429
                    r = await c.get(UPSTOX_CHAIN,
                                    params={"instrument_key": attempt_key, "expiry_date": expiry},
                                    headers=_h(token))
                    print(f"[Chain] {symbol}/{expiry} key={attempt_key} HTTP {r.status_code}")
                    if r.status_code == 429:
                        print(f"[Chain] {symbol} rate-limited, retry in 1.5s")
                        await asyncio.sleep(1.5)
                        continue
                    if r.status_code != 200:
                        print(f"[Chain] error: {r.text[:200]}")
                        break
                    rows = r.json().get("data", []) or []
                    if rows:
                        break
                if rows:
                    break
            if not rows:
                print(f"[Chain] {symbol}/{expiry} empty after all attempts")
                return {}

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
                async with _quote_chunk_sem:
                    await asyncio.sleep(2.0)
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
            async with _quote_chunk_sem:
                await asyncio.sleep(2.0)
                for attempt in range(2):
                    r = await c.get(UPSTOX_QUOTE_V2,
                                    params={"instrument_key": ",".join(keys)},
                                    headers=_h(token))
                    if r.status_code == 200:
                        break
                    elif r.status_code == 429 and attempt == 0:
                        await asyncio.sleep(3)
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
_quote_chunk_sem = asyncio.Semaphore(1)  # serializes ALL Upstox quote API calls
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

# All MCX commodity options confirmed same-month convention (expiry Y → futures Y).
# _MCX_NEXT_MONTH_OPTION_SYMBOLS tracks when active option contracts live on a
# DIFFERENT futures key than the current spot (near-expiry transition). Used only
# to find the correct option chain via _mcx_option_underlying — NOT for
# expiry→futures month resolution (always same-month for all MCX instruments).
_MCX_NEXT_MONTH_SEED: frozenset = frozenset()   # empty — all MCX same-month convention
_MCX_NEXT_MONTH_OPTION_SYMBOLS: set = set(_MCX_NEXT_MONTH_SEED)

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
            # Pick the nearest futures whose calendar month is >= current month.
            # Using first-of-month comparison: even if this month's futures
            # expired mid-month, _align_mcx_spot_to_options() will advance the
            # spot to next month once expiry_cache shows the option default has
            # rolled forward. This is intentional — we want the closest anchor.
            for tsym, ikey in candidates:
                exp_approx = _parse_expiry(tsym)
                if exp_approx >= today.replace(day=1):
                    result[sym] = ikey
                    print(f"[MCX] validate: {sym} = {ikey} ({tsym}), today={today}")
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
    # IMPORTANT: filter to LIVE contracts only (expiry >= today). The Upstox API
    # returns expired contracts for a futures key even after they have settled.
    # Counting expired contracts as evidence would wrongly set _mcx_option_underlying
    # to the current spot key (e.g. JULFUT in July) when the true underlying for
    # the active options is the NEXT month's key (e.g. AUGFUT) — this would cause
    # NATURALGAS to be removed from _MCX_NEXT_MONTH_OPTION_SYMBOLS and result in
    # wrong spot alignment and incorrect BOP/LOC ratios after each monthly rollover.
    if token:
        today_str = today.isoformat()
        for sym in result:
            spot_key = result[sym]
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.get(UPSTOX_CONTRACTS,
                                    params={"instrument_key": spot_key},
                                    headers=_h(token))
                    if r.status_code == 200:
                        contracts = r.json().get("data", [])
                        # Only count non-expired option contracts as evidence
                        live = [ct for ct in contracts
                                if isinstance(ct, dict) and (ct.get("expiry") or "") >= today_str]
                        if live:
                            _mcx_option_underlying[sym] = spot_key
                            print(f"[MCX] {sym} options on {spot_key}: {len(live)} live contracts "
                                  f"({len(contracts)-len(live)} expired ignored)")
                            continue
                        elif contracts:
                            print(f"[MCX] {sym} spot_key={spot_key} has {len(contracts)} contracts "
                                  f"but ALL expired — searching other futures")
                # If spot key has no live contracts, try other futures for this symbol
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
                # Sort by expiry proximity; prefer future months over expired ones
                def _parse_exp(tsym):
                    s = tsym[len(sym):]
                    try: return date(int("20" + s[:2]), _M.index(s[2:5]) + 1, 1)
                    except: return date(2099, 1, 1)
                today_month = today.replace(day=1)
                candidates.sort(key=lambda x: (
                    0 if _parse_exp(x[0]) >= today_month else 1,
                    _parse_exp(x[0])
                ))
                for tsym, ikey in candidates:
                    try:
                        async with httpx.AsyncClient(timeout=10) as c:
                            r = await c.get(UPSTOX_CONTRACTS,
                                            params={"instrument_key": ikey},
                                            headers=_h(token))
                            if r.status_code == 200:
                                contracts = r.json().get("data", [])
                                # Same filter: only live contracts count as evidence
                                live = [ct for ct in contracts
                                        if isinstance(ct, dict) and (ct.get("expiry") or "") >= today_str]
                                if live:
                                    _mcx_option_underlying[sym] = ikey
                                    print(f"[MCX] {sym} options on {ikey} ({tsym}): "
                                          f"{len(live)} live contracts")
                                    break
                    except:
                        pass
                    await asyncio.sleep(0.2)
            except Exception as e:
                print(f"[MCX] {sym} option key search: {e}")
            await asyncio.sleep(0.2)

    # Determine which symbols use next-month convention (options listed under
    # the following month's futures). Confirmed by comparing the resolved spot
    # key with the option underlying key — if they differ, options trade on a
    # different (typically next) month's futures contract.
    global _MCX_NEXT_MONTH_OPTION_SYMBOLS
    detected_next_month = set()
    for sym, underlying in _mcx_option_underlying.items():
        spot_key = result.get(sym, "")
        if underlying and spot_key and underlying != spot_key:
            detected_next_month.add(sym)
            print(f"[MCX] {sym}: next-month option convention confirmed "
                  f"(spot={spot_key}, options_on={underlying})")
    if detected_next_month:
        _MCX_NEXT_MONTH_OPTION_SYMBOLS.update(detected_next_month)
    # Remove symbols where LIVE (non-expired) contracts confirmed same-month convention.
    # The expired-contract filter above means only genuine same-month evidence reaches here.
    # The NaturalGas convention is NOT always "next-month" — it depends on whether the
    # same-month futures expire before or after the options in that specific month:
    #   June 2026: JUNFUT expires Jun 25, June options expire Jun 23 → options CAN be
    #              on JUNFUT (same-month); live JUNFUT contracts confirm this.
    #   July 2026: JULFUT expires ~Jul 18, July options expire Jul 24 → options MUST be
    #              on AUGFUT (next-month); JULFUT has no live July contracts.
    # We trust live-contract evidence here; expired contracts are already filtered out above.
    same_month_confirmed = {
        sym for sym, underlying in _mcx_option_underlying.items()
        if underlying and result.get(sym) and underlying == result[sym]
    }
    _MCX_NEXT_MONTH_OPTION_SYMBOLS -= same_month_confirmed
    # Re-seed any symbol for which we found NO evidence at all (API failure / no contracts
    # returned). Without this, a completely blank result would leave the set in whatever
    # state a previous validate_mcx_keys() call left it — possibly wrong.
    no_evidence = {sym for sym in _MCX_NEXT_MONTH_SEED if sym not in _mcx_option_underlying}
    _MCX_NEXT_MONTH_OPTION_SYMBOLS.update(no_evidence)
    print(f"[MCX] Next-month option symbols: {_MCX_NEXT_MONTH_OPTION_SYMBOLS} "
          f"(detected={detected_next_month}, same_month={same_month_confirmed}, "
          f"no_evidence_reseeded={no_evidence})")

    return result


async def refresh_mcx_option_underlying(token: str, spot_keys: dict) -> None:
    """Lightweight post-rollover refresh of _mcx_option_underlying.

    After a monthly rollover the spot futures key changes (e.g. CRUDEOIL moves
    from JUNFUT to JULFUT). _mcx_option_underlying was set at startup and now
    points to the old month's futures, making _fetch_mcx_option_chain() try
    the wrong canonical underlying first.

    This re-queries only the current active spot keys (already resolved and
    passed in as `spot_keys`) and updates _mcx_option_underlying in place.
    It does NOT touch _MCX_NEXT_MONTH_OPTION_SYMBOLS — that is protected by
    the seed and only mutated by validate_mcx_keys().
    """
    global _mcx_option_underlying
    if not token or not _mcx_sym_to_key:
        return
    today_str = date.today().isoformat()
    for sym, spot_key in spot_keys.items():
        if not spot_key or not spot_key.startswith("MCX"):
            continue
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(UPSTOX_CONTRACTS,
                                params={"instrument_key": spot_key},
                                headers=_h(token))
                if r.status_code == 200:
                    contracts = r.json().get("data", [])
                    live = [ct for ct in contracts
                            if isinstance(ct, dict) and (ct.get("expiry") or "") >= today_str]
                    if live:
                        _mcx_option_underlying[sym] = spot_key
                        print(f"[MCX Refresh] {sym} underlying updated → {spot_key} "
                              f"({len(live)} live contracts)")
                        continue
                # Spot key has no live contracts — search other futures
                today_month = date.today().replace(day=1)
                candidates = sorted(
                    [(tsym, ikey) for tsym, ikey in _mcx_sym_to_key.items()
                     if (tsym.startswith(sym) and tsym.endswith("FUT")
                         and ikey != spot_key
                         and not (tsym[len(sym):][0:1] == "M" and tsym[len(sym):][1:2].isdigit())
                         and not any(v in tsym for v in ["PETAL", "GUINEA", "TEN", "MIC"]))],
                    key=lambda x: (
                        0 if date(int("20"+x[0][len(sym):len(sym)+2]),
                                  _M.index(x[0][len(sym)+2:len(sym)+5])+1, 1) >= today_month else 1,
                        x[0]
                    )
                )
                for tsym, ikey in candidates[:4]:
                    try:
                        async with httpx.AsyncClient(timeout=10) as c2:
                            r2 = await c2.get(UPSTOX_CONTRACTS,
                                              params={"instrument_key": ikey},
                                              headers=_h(token))
                            if r2.status_code == 200:
                                cts = r2.json().get("data", [])
                                live2 = [ct for ct in cts
                                         if isinstance(ct, dict) and (ct.get("expiry") or "") >= today_str]
                                if live2:
                                    _mcx_option_underlying[sym] = ikey
                                    print(f"[MCX Refresh] {sym} underlying updated → {ikey} "
                                          f"({tsym}, {len(live2)} live contracts)")
                                    break
                    except Exception:
                        pass
                    await asyncio.sleep(0.15)
        except Exception as e:
            print(f"[MCX Refresh] {sym} error: {e}")
        await asyncio.sleep(0.2)


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


def _is_past_market_close_ist() -> bool:
    """True when IST clock is past 15:35 — NSE options have settled for the day."""
    from datetime import datetime, timezone, timedelta as _td
    now_ist = datetime.now(timezone.utc) + _td(hours=5, minutes=30)
    return now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 35)


def _is_past_mcx_close_ist() -> bool:
    """True when IST clock is past 23:30 — MCX commodity options have settled.
    MCX closes at 23:30 IST (11:30 PM), distinct from NSE's 15:35 IST close.
    Rolling MCX contracts at the NSE close time (15:35) causes 8 hours of wrong
    spot prices on expiry day when MCX is still actively trading."""
    from datetime import datetime, timezone, timedelta as _td
    now_ist = datetime.now(timezone.utc) + _td(hours=5, minutes=30)
    return now_ist.hour == 23 and now_ist.minute >= 30


def get_current_and_next_expiry(expiries: list, symbol: str) -> dict:
    today  = date.today()
    today_s = today.isoformat()
    # future: all expiries on or after today (includes today's expiry so the
    # system stays on the live contract all day — roll only after it passes)
    future = sorted([e for e in expiries if e >= today_s])
    # active: expiries strictly after today (used for current/next labels and
    # default after the expiry date has passed at EOD).
    # On the expiry date itself, today's expiry is in `future` but NOT in
    # `active`, so the day-change rollover (midnight tick) moves the default
    # forward. Prior to the expiry date, today's expiry IS in `active`.
    # No near_cutoff buffer — the old +3-day cutoff caused premature switches
    # (e.g. NIFTY showed next-week options from Monday for a Thursday expiry).
    active = [e for e in future if e >= today_s] or future

    # Post-market intraday rollover: after NSE market close (15:35 IST) on
    # the expiry day itself, advance `future` to skip the settled contract so
    # LOC shows next-week data instead of settled/zero option prices.
    # MCX monthly symbols are excluded — they have their own 1-day buffer.
    if (symbol.upper() not in MONTHLY_SYMBOLS
            and future and future[0] == today_s
            and len(active) > 0 and active[0] != today_s
            and _is_past_market_close_ist()):
        future = active  # drop today's expired contract

    # default = future[0]: stay on the current expiry ALL DAY including expiry
    # day itself; roll forward only after the date passes (day-after rollover).
    # active[0] would skip to next expiry ON expiry day (wrong for intraday trading).
    result = {"all": expiries, "default": future[0] if future else None}

    if symbol.upper() in MONTHLY_SYMBOLS:
        # Use `active` (strictly after today) so expiry-day contracts are excluded.
        ref = active if active else future
        # Skip contracts expiring today or tomorrow — MCX monthly options lose
        # all liquidity on their final day (settlement at noon/afternoon). A
        # 1-day buffer ensures the system switches to the next live monthly
        # contract one day early (e.g. GOLD May 27 → June 30 on May 26).
        # No early-rollover buffer: stay on current options until they expire.
        # The old 1-day buffer caused premature rollover the day before options
        # expiry, which combined with the 1-2 day options-to-futures gap meant
        # CRUDEOIL/NATURALGAS/COPPER rolled 2-3 days before FUTURES expiry.
        months_order = []
        for e in ref:
            ym = e[:7]
            if ym not in months_order:
                months_order.append(ym)
        def _last_in(ym): return [e for e in ref if e.startswith(ym)][-1]
        result["current_month"] = _last_in(months_order[0]) if len(months_order) > 0 else None
        result["next_month"]    = _last_in(months_order[1]) if len(months_order) > 1 else None
        # default = current_month (the live monthly contract), consistent with label.
        result["default"] = result["current_month"]
    else:
        # Non-MCX (Index weekly/monthly + FO stocks):
        # current_week = live expiry (includes today on expiry day)
        # next/far = upcoming expiries AFTER current_week
        result["current_week"] = future[0] if future else None
        rest = [e for e in active if e != result["current_week"]]
        result["next_week"] = rest[0] if len(rest) > 0 else None
        result["far_week"]  = rest[1] if len(rest) > 1 else None
    return result
