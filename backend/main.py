"""
RAIMA Markets v9 — Zerodha Kite Connect backend (migrated from Upstox)
Key fixes from the original Upstox build, preserved through the migration:
1. MCX key validation at startup — finds working month
2. Option chain close_price field correct
3. Full OHLC (open,high,low,close) fetched for CE/PE via REST
4. Proper broadcast — attaches display_name to every stock tick

Market data now comes from Zerodha Kite Connect instead of Upstox — see
backend/instruments.py and the shared session in backend/history2/
(zerodha_client.py, ticker.py) for the provider-layer details. Business
logic (LOC formulas, routes, response shapes) is unchanged.
"""
import asyncio, json, os, time
from pathlib import Path
from typing import Set
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()
USE_MOCK = os.getenv("MOCK_MODE","false").lower() in ("true","1","yes")

app = FastAPI(title="RAIMA Markets v9")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# History 2 — shares this same Zerodha session/ticker with the LOC engine
# below (see backend/history2/zerodha_client.py, ticker.py); does not touch
# LOC state directly.
from .history2.routes import router as _history2_router, zerodha_router as _history2_zerodha_router
from .history2 import zerodha_client as zc
from .history2.state import state2
from .history2.ticker import ticker2
app.include_router(_history2_router)
app.include_router(_history2_zerodha_router)

FRONTEND_DIST   = Path(__file__).parent.parent / "frontend" / "dist"
FRONTEND_STATIC = Path(__file__).parent.parent / "frontend" / "static"
FRONTEND_STATIC.mkdir(parents=True, exist_ok=True)

# Zerodha Kite Connect app credentials — same shared app as History 2 (see
# zerodha_client.API_KEY/API_SECRET). ACCESS_TOKEN seeds from a restored
# disk-cached session if one exists (zerodha_client._restore_session_cache()
# runs at import time, above, before this line executes).
ACCESS_TOKEN = state2.access_token or ""
PASSWORD     = os.getenv("DASHBOARD_PASSWORD", "raima2024")

from .instruments import (
    get_spot_keys, mcx_key, mcx_key_for_month, get_current_and_next_expiry, get_itm2_strikes,
    fetch_expiry_list, fetch_option_chain, fetch_quotes_rest, fetch_index_quotes,
    fetch_option_ohlc_rest, fetch_intraday_candles, fetch_historical_candles,
    validate_mcx_keys, calculate_expiries_fallback,
    normalize_mcx_response_key, normalize_response_key,
    refresh_nse_eq_keys, refresh_mcx_option_underlying,
    token_to_key, key_to_token,
    STRIKE_STEPS, MONTHLY_SYMBOLS, _is_past_market_close_ist, _is_past_mcx_close_ist
)
from . import instruments as _instruments_mod
from . import instrument_keys as _ik
from . import calculator as calc_mod
from .instrument_keys import NSE_EQ_KEYS
from .loc_engine import LOCEngine, calc_loc_25

_INDEX_LOC  = ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","SENSEX","BANKEX"]
_MCX_LOC    = ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]
LOC_SYMBOLS = _INDEX_LOC + _MCX_LOC + [s for s in NSE_EQ_KEYS if s not in _INDEX_LOC + _MCX_LOC]
LOC_SYMBOLS_SET = set(LOC_SYMBOLS)

# ── Dynamic instrument keys ────────────────────────────────────────
# Upstox used human-readable literal strings as valid keys directly
# ("NSE_INDEX|Nifty 50", no resolution needed). Zerodha's index keys are
# "NSE_INDEX|<instrument_token>", resolved at runtime by
# instruments._resolve_indices() (called from validate_mcx_keys()) — so
# this can no longer be a static list computed once at import time; it must
# read whatever's currently in _instruments_mod._index_keys, which is empty
# until the first successful validate_mcx_keys() call after login.
def _get_index_keys() -> list:
    return list(_instruments_mod._index_keys.values())

# Will be updated at startup after validate_mcx_keys()
COMMODITY_KEYS = [mcx_key(s,0) for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]]
SPOT_KEYS_D: dict = {}   # filled at startup

# Feed key → LOC symbol (for routing to LOC engine)
FEED_KEY_TO_SYM: dict = {}
option_key_map:      dict = {}   # LOC option key (ITM-2) → (symbol, "CE"/"PE")
itm1_option_key_map: dict = {}   # LOC option key (ITM-1) → (symbol, "CE"/"PE")
otm2_option_key_map: dict = {}   # LOC option key (OTM-2, LTP-only reference) → (symbol, "CE"/"PE")
calc_option_key_map: dict = {}   # Calculator option key → (symbol, "CE"/"PE")
option_key_last_tick: dict = {}  # instrument_key → last WS tick timestamp (for stale detection)
_OPTION_KEY_MAX_AGE = 3600  # prune entries older than 1 hour to prevent unbounded growth


# ══════════════════════════════════════════════════════════════════
#  ZERODHA TICK ADAPTER
#  Converts KiteTicker's on_ticks payload into the same {"ltpc","efeed"}
#  shape the rest of this file (broadcast, _route_tick, _ex, _update_ohlc,
#  loc_engine) already expects — so everything downstream of this adapter
#  is unchanged from the Upstox version. Replaces the old protobuf decoder
#  entirely (Zerodha's ticker delivers already-parsed Python dicts, no wire
#  format to decode here).
# ══════════════════════════════════════════════════════════════════
async def _on_zerodha_tick(token: int, price: float, ts_ms: int):
    key = _instruments_mod.token_to_key(token)
    if not key:
        return
    full = ticker2.last_full_tick.get(token) or {}
    ohlc = full.get("ohlc") or {}
    cp = float(ohlc.get("close") or 0)
    feed = {
        "ltpc": {"ltp": price, "cp": cp},
        "efeed": {
            "ltp": price, "cp": cp,
            "open": float(ohlc.get("open") or price),
            "high": float(ohlc.get("high") or price),
            "low":  float(ohlc.get("low") or price),
            "oi":   float(full.get("oi") or 0),
        },
    }
    await broadcast({"type": "live_feed", "feeds": {key: feed}, "currentTs": str(ts_ms)})


async def _on_zerodha_status(status: str):
    print(f"[Feed] Zerodha ticker status: {status}")


def _ex(fv):
    ltpc=fv.get("ltpc",{}); ef=fv.get("efeed",{})
    ltp = float(ltpc.get("ltp",0))
    cp  = float(ltpc.get("cp",0) or ef.get("cp",0))
    # Return raw 0 when efeed fields are absent rather than falling back to ltp.
    # The `or ltp` fallback used to hide partial ticks from callers but caused
    # update_spot / update_option_from_feed to overwrite real session high/low
    # with the current ltp. Callers now treat 0 as "no update for this field".
    h   = float(ef.get("high",0))
    l   = float(ef.get("low",0))
    o   = float(ef.get("open",0))
    return ltp, cp, o, h, l


# ══════════════════════════════════════════════════════════════════
#  APP STATE
# ══════════════════════════════════════════════════════════════════
class AppState:
    access_token:  str  = ACCESS_TOKEN
    market_data:   dict = {}   # feed_key → {ltpc,efeed,ts,display_name}
    market_status: dict = {}
    ohlc:          dict = {}   # key → [{t,o,h,l,c,v}]
    loc_history:   dict = {}
    loc_hist_ts:   dict = {}
    sessions:      dict = {}
    expiry_cache:  dict = {}
    prev_close:    dict = {}
    connected_clients: Set[WebSocket] = set()
    feed_client = None   # the shared Zerodha ticker2 once started (was: raw Upstox websocket)
    feed_task  = None
    chain_task = None
    _index_poll_task = None
    frame_count: int = 0
    decode_ok:   int = 0
    subscribed_option_keys: set = set()
    # Spot-side MCX futures keys subscribed on demand by the calculator
    # endpoint (one per month per symbol). Tracked separately from
    # subscribed_option_keys so the LOC/option subscription logic stays
    # untouched.
    subscribed_calc_spot_keys: set = set()
    feed_log: list = []  # recent feed debug messages
    _feed_buffer: dict = {}       # buffered feeds for throttled broadcast
    _feed_buffer_lock = None      # asyncio.Lock, created at startup
    _flush_task = None
    _loc_dirty: set = set()       # symbols whose loc_result changed since last flush

state = AppState()

# Capture prints to feed_log
import builtins
_orig_print = builtins.print
def _log_print(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    if msg.startswith("[Feed]") or msg.startswith("[Decode]"):
        state.feed_log.append(f"{time.strftime('%H:%M:%S')} {msg}")
        if len(state.feed_log) > 50: state.feed_log.pop(0)
    _orig_print(*args, **kwargs)
builtins.print = _log_print
loc_engine = LOCEngine()

async def _on_loc(symbol: str, result: dict):
    _record_loc_hist(symbol, result)
    # Mark dirty so _flush_feed_buffer sends loc_results on next cycle,
    # even if no instrument feed ticks are buffered at that moment.
    state._loc_dirty.add(symbol)

loc_engine.on_loc_update = _on_loc
for sym in LOC_SYMBOLS:
    loc_engine.register(sym)

def _update_ohlc(key, ltp, ts_ms, o=0, h=0, l=0):
    if not ltp: return
    minute = (int(ts_ms)//60000)*60000
    hist = state.ohlc.setdefault(key, [])
    if hist and hist[-1]["t"] == minute:
        c = hist[-1]
        c["h"] = max(c["h"], h or ltp)
        c["l"] = min(c["l"], l or ltp) if (l or ltp) else c["l"]
        c["c"] = ltp; c["v"] = c.get("v",0)+1
    else:
        hist.append({"t":minute,"o":o or ltp,"h":h or ltp,"l":l or ltp,"c":ltp,"v":1})
        if len(hist) > 400: hist.pop(0)

def _record_loc_hist(sym, loc):
    if not loc: return
    now = int(time.time()//60)*60000
    if state.loc_hist_ts.get(sym) == now: return
    state.loc_hist_ts[sym] = now
    hist = state.loc_history.setdefault(sym, [])
    keep = ["ltp","bop","cep","pep","ul","ll","ful","fll","ful_diff","fll_diff",
            "call_cp_diff","put_cp_diff",
            "zone","change","direction","different",
            "call_cp_diff","put_cp_diff",
            "ce_strike","pe_strike","ce_ltp","pe_ltp","ce_iv","pe_iv",
            "itm1_ce_strike","itm1_pe_strike","itm1_ce_ltp","itm1_pe_ltp","itm1_ce_iv","itm1_pe_iv",
            "otm2_ce_ltp","otm2_pe_ltp","otm2_diff"]
    hist.insert(0, {"ts":int(time.time()*1000), **{k:loc[k] for k in keep if k in loc}})
    if len(hist) > 200: hist.pop()

def _route_tick(key, ltp, cp, o, h, l, ts):
    if not ltp: return
    sym = FEED_KEY_TO_SYM.get(key)
    if sym and sym in LOC_SYMBOLS_SET:
        # Sanity-check: log when an MCX key routes to a symbol but the key no
        # longer matches SPOT_KEYS_D (indicates a stale FEED_KEY_TO_SYM entry).
        if key.startswith("MCX_FO|") and SPOT_KEYS_D.get(sym) and SPOT_KEYS_D[sym] != key:
            print(f"[MCX Warn] Stale route: key={key} → {sym} but SPOT_KEYS_D has {SPOT_KEYS_D[sym]}")
        loc_engine.update_spot(sym, ltp, cp, h, l, ts, o)
        return
    # ── LOC option routing ─────────────────────────────────────────
    mapping = option_key_map.get(key)
    if not mapping:
        for st in loc_engine.symbols.values():
            if st.ce.instrument_key == key:
                mapping = (st.symbol, "CE"); break
            if st.pe.instrument_key == key:
                mapping = (st.symbol, "PE"); break
        if mapping:
            option_key_map[key] = mapping
    if mapping:
        sym_m, opt_type = mapping
        st = loc_engine.get_state(sym_m)
        if st:
            cur = st.ce.instrument_key if opt_type == "CE" else st.pe.instrument_key
            if key != cur:
                option_key_map.pop(key, None)
            else:
                loc_engine.update_option_from_feed(sym_m, opt_type, ltp, cp, h, l)
                option_key_last_tick[key] = time.time()  # record live tick for stale detection
    # ── LOC ITM-1 option routing (parallel, decoupled from ITM-2 above) ──
    itm1_mapping = itm1_option_key_map.get(key)
    if not itm1_mapping:
        for st in loc_engine.symbols.values():
            if st.itm1_ce.instrument_key == key:
                itm1_mapping = (st.symbol, "CE"); break
            if st.itm1_pe.instrument_key == key:
                itm1_mapping = (st.symbol, "PE"); break
        if itm1_mapping:
            itm1_option_key_map[key] = itm1_mapping
    if itm1_mapping:
        sym_i, opt_type_i = itm1_mapping
        st = loc_engine.get_state(sym_i)
        if st:
            cur_i = st.itm1_ce.instrument_key if opt_type_i == "CE" else st.itm1_pe.instrument_key
            if key != cur_i:
                itm1_option_key_map.pop(key, None)
            else:
                loc_engine.update_itm1_option_from_feed(sym_i, opt_type_i, ltp, cp, h, l)
                option_key_last_tick[key] = time.time()
    # ── LOC OTM-2 option routing (parallel, LTP-only reference data) ──
    otm2_mapping = otm2_option_key_map.get(key)
    if not otm2_mapping:
        for st in loc_engine.symbols.values():
            if st.otm2_ce_key == key:
                otm2_mapping = (st.symbol, "CE"); break
            if st.otm2_pe_key == key:
                otm2_mapping = (st.symbol, "PE"); break
        if otm2_mapping:
            otm2_option_key_map[key] = otm2_mapping
    if otm2_mapping:
        sym_o, opt_type_o = otm2_mapping
        st = loc_engine.get_state(sym_o)
        if st:
            cur_o = st.otm2_ce_key if opt_type_o == "CE" else st.otm2_pe_key
            if key != cur_o:
                otm2_option_key_map.pop(key, None)
            else:
                loc_engine.update_otm2_option_from_feed(sym_o, opt_type_o, ltp)
                option_key_last_tick[key] = time.time()
    # ── Calculator option routing (parallel, decoupled from LOC) ───
    cmap = calc_option_key_map.get(key)
    if not cmap:
        for sn, calc in loc_engine.calc_states.items():
            if calc.ce.instrument_key == key:
                cmap = (sn, "CE"); break
            if calc.pe.instrument_key == key:
                cmap = (sn, "PE"); break
        if cmap:
            calc_option_key_map[key] = cmap
    if cmap:
        sym_c, opt_type_c = cmap
        calc = loc_engine.calc_states.get(sym_c)
        if calc:
            cur_c = calc.ce.instrument_key if opt_type_c == "CE" else calc.pe.instrument_key
            if key != cur_c:
                calc_option_key_map.pop(key, None)
            else:
                loc_engine.update_calc_option(sym_c, opt_type_c, ltp, cp, h, l)


# ══════════════════════════════════════════════════════════════════
#  STARTUP INIT
# ══════════════════════════════════════════════════════════════════
async def startup_init():
    global COMMODITY_KEYS, SPOT_KEYS_D, FEED_KEY_TO_SYM

    # Wire up callback so chain refreshes trigger OHLC REST fetch immediately
    loc_engine.on_option_ohlc_needed = _refresh_option_ohlc_single
    loc_engine.on_calc_option_ohlc_needed = _refresh_calc_option_ohlc
    # Subscribe new option keys to Upstox WS the moment ATM shifts, so the
    # newly-selected CE/PE strike starts receiving live ticks without having
    # to wait for the next periodic_refresh cycle.
    loc_engine.on_option_keys_changed = _subscribe_new_option_keys
    loc_engine.on_calc_keys_changed   = _subscribe_new_option_keys

    print("[Init] Starting data init...")

    # Step 1: Validate MCX keys
    if state.access_token:
        print("[Init] Validating MCX keys...")
        valid_mcx = await validate_mcx_keys(state.access_token)
    else:
        valid_mcx = {s: mcx_key(s,0) for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]}

    # Step 2: Build SPOT_KEYS_D and COMMODITY_KEYS
    SPOT_KEYS_D = dict(get_spot_keys())
    for sym, key in valid_mcx.items():
        SPOT_KEYS_D[sym] = key
    # Add FNO stock keys to SPOT_KEYS_D
    for sym, key in _ik.NSE_EQ_KEYS.items():
        SPOT_KEYS_D[sym] = key

    COMMODITY_KEYS = list(dict.fromkeys(
        [valid_mcx.get(s, mcx_key(s,0)) for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]] +
        [mcx_key(s,1) for s in ["CRUDEOIL","NATURALGAS", "COPPER"]]
    ))

    # Step 3: Resolve NSE_EQ keys from the Zerodha instrument dump
    await refresh_nse_eq_keys()

    # Step 4: Build reverse map — only PRIMARY (current month) MCX key per symbol
    FEED_KEY_TO_SYM.clear()
    for sym, key in SPOT_KEYS_D.items():
        FEED_KEY_TO_SYM[key] = sym
    # Only map current month (m=0) MCX keys to LOC symbol — NOT next/far month
    # This prevents next-month prices (e.g. CRUDEOIL May=9118) from overwriting
    # current-month prices (e.g. CRUDEOIL Apr=10000) in the LOC engine
    for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]:
        FEED_KEY_TO_SYM[valid_mcx.get(s, mcx_key(s,0))] = s
    # Also map updated NSE_EQ keys
    for sym, key in _ik.NSE_EQ_KEYS.items():
        FEED_KEY_TO_SYM[key] = sym

    print(f"[Init] Commodity keys: {COMMODITY_KEYS}")
    print(f"[Init] LOC symbols: {len(LOC_SYMBOLS)} ({len(_INDEX_LOC)} idx + {len(_MCX_LOC)} mcx + {len(LOC_SYMBOLS)-len(_INDEX_LOC)-len(_MCX_LOC)} stocks)")

    # Step 5: Fetch expiries — parallel with concurrency limit
    expiry_sem = asyncio.Semaphore(5)

    async def _init_expiry(sym):
        async with expiry_sem:
            try:
                if state.access_token:
                    expiries = await fetch_expiry_list(sym, state.access_token)
                else:
                    expiries = calculate_expiries_fallback(sym)
                info = get_current_and_next_expiry(expiries, sym)
                state.expiry_cache[sym] = info
                default = info.get("default")
                if default:
                    loc_engine.set_expiry(sym, default, fetch_chain=False)
            except Exception as e:
                print(f"[Init] {sym} expiry: {e}")
            await asyncio.sleep(0.2)

    # Priority: indices + MCX first, then stocks
    priority = [s for s in LOC_SYMBOLS if s in _INDEX_LOC + _MCX_LOC]
    stock_syms = [s for s in LOC_SYMBOLS if s not in priority]
    await asyncio.gather(*[_init_expiry(s) for s in priority])
    print(f"[Init] Index/MCX expiries loaded: {len([s for s in priority if s in state.expiry_cache])}")
    await asyncio.gather(*[_init_expiry(s) for s in stock_syms])
    print(f"[Init] All expiries loaded: {len(state.expiry_cache)} symbols")

    # Step 5.5: Align MCX spot futures with the active option month.
    # validate_mcx_keys() picks the nearest futures by month >= today, so on
    # 17 Apr it still picks April futures even though April options died on
    # 16 Apr. Now that expiries are known, point the spot at whichever
    # month's options are actually live (info["default"]).
    global _last_rollover_check_date
    rolled_init = _align_mcx_spot_to_options()
    _update_mcx_spot_months()
    from datetime import date as _dc_init
    _last_rollover_check_date = _dc_init.today()

    # Re-fetch expiry list for any MCX symbols whose spot key rolled to a new
    # month. The initial fetch_expiry_list used the old spot key; the new
    # contract (e.g. GOLD August futures) has different available expiries
    # (e.g. June 30, July 29) that replace the stale default from the old key.
    if rolled_init and state.access_token:
        rolled_syms = {sym for sym, *_ in rolled_init}
        for sym in rolled_syms:
            try:
                expiries = await fetch_expiry_list(sym, state.access_token)
                info = get_current_and_next_expiry(expiries, sym)
                state.expiry_cache[sym] = info
                default = info.get("default")
                if default:
                    loc_engine.set_expiry(sym, default, fetch_chain=False)
                    print(f"[Init] {sym} post-rollover expiry re-fetched: default={default}")
            except Exception as e:
                print(f"[Init] {sym} post-rollover expiry re-fetch error: {e}")

    # Prime spot OHLC (open/high/low/close) for the rolled-over MCX futures
    # via REST so the LOC engine's spot isn't stuck at `ltp` when WS ticks
    # haven't arrived (e.g. weekends, pre-open). Runs unconditionally — if
    # the spot key didn't change this still refreshes stale prev-close data.
    _update_mcx_spot_months()
    await _prime_mcx_spot_from_rest()

    await broadcast({"type":"expiry_update","expiry_cache":state.expiry_cache})

    # Step 6: Fetch option chains — parallel with concurrency limit
    chain_sem = asyncio.Semaphore(3)
    chain_count = [0]

    async def _init_chain(sym):
        async with chain_sem:
            st = loc_engine.get_state(sym)
            if not st or not st.expiry: return
            try:
                chain = await fetch_option_chain(sym, st.expiry, state.access_token)
                if chain:
                    loc_engine.update_chain(sym, chain)
                    chain_count[0] += 1
            except Exception as e:
                print(f"[Init] {sym} chain: {e}")
            await asyncio.sleep(0.3)

    if state.access_token:
        # Priority chains first
        await asyncio.gather(*[_init_chain(s) for s in priority])
        print(f"[Init] Index/MCX chains loaded: {chain_count[0]}")
        # Stock chains in batches of 20
        for i in range(0, len(stock_syms), 20):
            batch = stock_syms[i:i+20]
            await asyncio.gather(*[_init_chain(s) for s in batch])
        print(f"[Init] All chains loaded: {chain_count[0]} symbols")

    # Step 7: Initial OHLC snapshot for stocks + indices
    if state.access_token:
        print("[Init] Fetching initial OHLC snapshot...")
        # Stocks and commodities via /v3/ohlc
        stock_comm_keys = list(dict.fromkeys(_ik.FO_STOCK_KEYS + COMMODITY_KEYS[:5]))
        data = await fetch_quotes_rest(stock_comm_keys, state.access_token)
        # Indices via /v2/market-quote/quotes
        idx_data = await fetch_index_quotes(_get_index_keys(), state.access_token)
        data.update(idx_data)
        for k, v in data.items():
            sym_name = _ik.ISIN_TO_SYMBOL.get(k, "")
            state.market_data[k] = {**v, "ts":str(int(time.time()*1000)),
                                     "display_name":sym_name}
            ltp = v.get("ltpc",{}).get("ltp",0)
            cp  = v.get("ltpc",{}).get("cp",0)
            if cp: state.prev_close[k] = cp
            if ltp:
                ef = v.get("efeed",{})
                _update_ohlc(k, ltp, int(time.time()*1000),
                             ef.get("open",ltp), ef.get("high",ltp), ef.get("low",ltp))
                # Feed OHLC into LOC engine so spot close/high/low are correct
                sym = FEED_KEY_TO_SYM.get(k)
                if sym and sym in LOC_SYMBOLS_SET:
                    loc_engine.update_spot(
                        sym, ltp, cp or ltp,
                        ef.get("high", ltp), ef.get("low", ltp),
                        int(time.time()*1000), ef.get("open", ltp))
        print(f"[Init] Snapshot loaded: {len(data)} instruments")
        await broadcast({
            "type":"snapshot_update",
            "market_data":state.market_data,
            "commodity_keys":COMMODITY_KEYS,
            "spot_keys":SPOT_KEYS_D,
            "expiry_cache":state.expiry_cache,
            "loc_results":loc_engine.get_all_results(),
            "market_status":state.market_status,
        })

    # Step 8: Re-subscribe feed to validated commodity + index keys.
    # Both COMMODITY_KEYS and _get_index_keys() are only resolved by
    # validate_mcx_keys() (Step 1, above) — start_feed()'s own initial
    # subscribe call can race ahead of that resolution and send an empty
    # list, so this re-subscribe (now that resolution is guaranteed done)
    # is the actual mechanism that gets them onto the live feed.
    if state.feed_client and COMMODITY_KEYS:
        try:
            await _sub_binary(state.feed_client, COMMODITY_KEYS, "full")
            print(f"[Init] Re-subscribed MCX keys: {COMMODITY_KEYS}")
        except Exception as e:
            print(f"[Init] MCX re-sub error: {e}")
    idx_keys = _get_index_keys()
    if state.feed_client and idx_keys:
        try:
            await _sub_binary(state.feed_client, idx_keys, "ltp")
            print(f"[Init] Re-subscribed index keys: {idx_keys}")
        except Exception as e:
            print(f"[Init] Index re-sub error: {e}")

    # Step 7: Fetch CE/PE OHLC from REST (since chain may have 0s)
    if state.access_token:
        await _refresh_all_option_ohlc()

    # Step 9: Subscribe all CE/PE option keys gathered from chains.
    # start_feed() subscribes option keys at WS-connect time, but the WS
    # connects before chains are loaded (3-second head-start), so
    # get_option_keys() returns [] there. After chains load here we must
    # explicitly push the keys to the live feed — otherwise CE/PE LTP
    # won't update until the first ATM shift or the 60-second periodic refresh.
    if state.feed_client:
        await _subscribe_new_option_keys()
        print(f"[Init] Option keys subscribed to WS: {len(loc_engine.get_option_keys())} keys")


async def _refresh_option_ohlc_single(symbol: str):
    """Fetch actual intraday OHLC for a single symbol's CE/PE via REST.
    Only updates close/high/low — does NOT overwrite LTP because the
    WebSocket feed provides real-time LTP which is more authoritative
    than the REST snapshot (which can be seconds to minutes stale).
    """
    if not state.access_token: return
    st = loc_engine.get_state(symbol)
    if not st or not st.ce.instrument_key: return
    data = await fetch_option_ohlc_rest(
        st.ce.instrument_key, st.pe.instrument_key, state.access_token)
    if not data: return
    ce_d = data.get(st.ce.instrument_key, {})
    pe_d = data.get(st.pe.instrument_key, {})
    changed = False
    # Upstox /v2/quotes ohlc.high / ohlc.low are the AUTHORITATIVE session
    # high/low as of the response timestamp — overwrite, don't max/min
    # accumulate across days. The old max/min caused yesterday's session
    # high to persist into today's values whenever today's high was lower.
    if ce_d:
        # Only seed LTP if WS hasn't provided one yet
        if not st.ce.ltp and ce_d.get("ltp"):
            st.ce.ltp = ce_d["ltp"]
            changed = True
        if ce_d.get("close"):
            st.ce.close = ce_d["close"]
        if ce_d.get("high"):
            st.ce.high = ce_d["high"]
        if ce_d.get("low"):
            st.ce.low = ce_d["low"]
        changed = True
    if pe_d:
        if not st.pe.ltp and pe_d.get("ltp"):
            st.pe.ltp = pe_d["ltp"]
            changed = True
        if pe_d.get("close"):
            st.pe.close = pe_d["close"]
        if pe_d.get("high"):
            st.pe.high = pe_d["high"]
        if pe_d.get("low"):
            st.pe.low = pe_d["low"]
        changed = True
    if changed:
        loc_engine.recalc(symbol)


async def _refresh_all_option_ohlc():
    """Fetch full OHLC for all CE/PE options via REST."""
    for sym in LOC_SYMBOLS:
        await _refresh_option_ohlc_single(sym)
        await asyncio.sleep(0.15)


async def _refresh_calc_option_ohlc(symbol: str):
    """REST OHLC refresh for a symbol's Calculator CE/PE view.
    Separate from the LOC path so Calculator preview stays independent."""
    if not state.access_token: return
    calc = loc_engine.calc_states.get(symbol)
    if not calc or not calc.ce.instrument_key: return
    data = await fetch_option_ohlc_rest(
        calc.ce.instrument_key, calc.pe.instrument_key, state.access_token)
    if not data: return
    ce_d = data.get(calc.ce.instrument_key, {})
    pe_d = data.get(calc.pe.instrument_key, {})
    changed = False
    if ce_d:
        if not calc.ce.ltp and ce_d.get("ltp"):
            calc.ce.ltp = ce_d["ltp"]; changed = True
        if ce_d.get("close"):
            calc.ce.close = ce_d["close"]; changed = True
        if ce_d.get("high"):
            calc.ce.high = max(calc.ce.high, ce_d["high"]) if calc.ce.high else ce_d["high"]
            changed = True
        if ce_d.get("low"):
            calc.ce.low = min(calc.ce.low, ce_d["low"]) if calc.ce.low else ce_d["low"]
            changed = True
    if pe_d:
        if not calc.pe.ltp and pe_d.get("ltp"):
            calc.pe.ltp = pe_d["ltp"]; changed = True
        if pe_d.get("close"):
            calc.pe.close = pe_d["close"]; changed = True
        if pe_d.get("high"):
            calc.pe.high = max(calc.pe.high, pe_d["high"]) if calc.pe.high else pe_d["high"]
            changed = True
        if pe_d.get("low"):
            calc.pe.low = min(calc.pe.low, pe_d["low"]) if calc.pe.low else pe_d["low"]
            changed = True
    if changed:
        loc_engine._recalc_calc(symbol)


async def _subscribe_new_option_keys():
    if not state.feed_client: return
    loc_keys  = [k for k in loc_engine.get_option_keys() if k]
    itm1_keys = [k for k in loc_engine.get_itm1_option_keys() if k]
    otm2_keys = [k for k in loc_engine.get_otm2_option_keys() if k]
    calc_keys = [k for k in loc_engine.get_calc_option_keys() if k]
    all_keys  = list(dict.fromkeys(loc_keys + itm1_keys + otm2_keys + calc_keys))
    all_keys_set = set(all_keys)
    new_keys  = [k for k in all_keys if k not in state.subscribed_option_keys]
    # get_option_keys()/get_calc_option_keys() only ever return each symbol's
    # CURRENT ce/pe instrument_key — so anything still in subscribed_option_keys
    # but no longer in all_keys_set is a superseded key from a strike roll or
    # expiry rollover that nothing references anymore. Unsubscribe it: leaving
    # it subscribed forever both wastes a slot against Upstox's per-connection
    # subscription cap and, once that cap is hit, silently blocks brand-new
    # strikes from subscribing at all (see _unsub_binary docstring).
    stale_keys = [k for k in state.subscribed_option_keys if k not in all_keys_set]
    if stale_keys:
        try:
            await _unsub_binary(state.feed_client, stale_keys)
        except Exception as e:
            print(f"[Options] Unsubscribe error: {e}")
        state.subscribed_option_keys.difference_update(stale_keys)
        for key in stale_keys:
            option_key_last_tick.pop(key, None)
    if new_keys:
        await _sub_binary(state.feed_client, new_keys, "full")
        state.subscribed_option_keys.update(new_keys)
        # Seed tick timestamps so stale monitor gives a 30s grace period before
        # REST fallback fires (prevents mass REST calls at startup for all symbols)
        now = time.time()
        for key in new_keys:
            option_key_last_tick[key] = now
        # Log every key individually so subscription can be verified in logs
        for key in new_keys:
            sym_info = option_key_map.get(key) or itm1_option_key_map.get(key) or otm2_option_key_map.get(key) or calc_option_key_map.get(key)
            label = f"{sym_info[0]}/{sym_info[1]}" if sym_info else "unknown"
            print(f"[Options] Subscribed key={key} → {label}")
        print(f"[Options] Total subscribed: {len(state.subscribed_option_keys)} option keys")
        # Immediately validate new keys via REST — confirms broker has them active
        # and primes LTP/OHLC before the first WS tick arrives
        if state.access_token:
            asyncio.create_task(_validate_and_prime_option_keys(new_keys))
    # Always rebuild BOTH maps from current engine state — even when neither
    # subscribe nor unsubscribe ran this call, in case some other path
    # mutated ce/pe instrument_key without going through this function.
    option_key_map.clear()
    for st in loc_engine.symbols.values():
        if st.ce.instrument_key: option_key_map[st.ce.instrument_key] = (st.symbol,"CE")
        if st.pe.instrument_key: option_key_map[st.pe.instrument_key] = (st.symbol,"PE")
    itm1_option_key_map.clear()
    for st in loc_engine.symbols.values():
        if st.itm1_ce.instrument_key: itm1_option_key_map[st.itm1_ce.instrument_key] = (st.symbol,"CE")
        if st.itm1_pe.instrument_key: itm1_option_key_map[st.itm1_pe.instrument_key] = (st.symbol,"PE")
    otm2_option_key_map.clear()
    for st in loc_engine.symbols.values():
        if st.otm2_ce_key: otm2_option_key_map[st.otm2_ce_key] = (st.symbol,"CE")
        if st.otm2_pe_key: otm2_option_key_map[st.otm2_pe_key] = (st.symbol,"PE")
    calc_option_key_map.clear()
    for sn, calc in loc_engine.calc_states.items():
        if calc.ce.instrument_key: calc_option_key_map[calc.ce.instrument_key] = (sn,"CE")
        if calc.pe.instrument_key: calc_option_key_map[calc.pe.instrument_key] = (sn,"PE")

async def _validate_and_prime_option_keys(keys: list):
    """After subscribing new option keys, immediately verify them via REST quote.
    Logs WARNING for any key not found (invalid/expired), and primes LTP/OHLC
    so the LOC engine has real data before the first WS tick arrives."""
    if not keys or not state.access_token: return
    batch = keys[:30]  # REST quote accepts up to ~30 keys at once
    try:
        data = await fetch_quotes_rest(batch, state.access_token)
        for k in batch:
            if k in data:
                ltp = data[k].get("ltpc", {}).get("ltp", 0)
                print(f"[Options] REST verify OK: {k} ltp={ltp}")
            else:
                print(f"[Options] WARNING: {k} not found in REST quote — key may be inactive or expired")
    except Exception as e:
        print(f"[Options] REST verify error: {e}")


async def _stale_fetch_option_ltp(symbol: str):
    """Force-fetch LTP for a symbol's CE and PE option keys via REST and inject
    into the LOC engine. Unlike _refresh_option_ohlc_single, this always
    overwrites LTP (not guarded by 'ltp==0') — used when WS ticks have been
    absent for >30 seconds so the LOC engine reflects the last traded price."""
    if not state.access_token: return
    st = loc_engine.get_state(symbol)
    if not st or not st.ce.instrument_key: return
    data = await fetch_option_ohlc_rest(
        st.ce.instrument_key, st.pe.instrument_key, state.access_token)
    if not data: return
    changed = False
    ce_d = data.get(st.ce.instrument_key, {})
    pe_d = data.get(st.pe.instrument_key, {})
    now = time.time()
    if ce_d:
        # Update tick timestamp regardless of ltp — prevents refiring every 30s
        # for illiquid keys where REST also returns ltp=0
        option_key_last_tick[st.ce.instrument_key] = now
        if ce_d.get("ltp"):  st.ce.ltp   = ce_d["ltp"];   changed = True
        if ce_d.get("close"): st.ce.close = ce_d["close"]
        if ce_d.get("high"):  st.ce.high  = ce_d["high"]
        if ce_d.get("low"):   st.ce.low   = ce_d["low"]
    if pe_d:
        option_key_last_tick[st.pe.instrument_key] = now
        if pe_d.get("ltp"):  st.pe.ltp   = pe_d["ltp"];   changed = True
        if pe_d.get("close"): st.pe.close = pe_d["close"]
        if pe_d.get("high"):  st.pe.high  = pe_d["high"]
        if pe_d.get("low"):   st.pe.low   = pe_d["low"]
    if changed:
        loc_engine.recalc(symbol)
        print(f"[Stale] {symbol} REST injected: ce_ltp={st.ce.ltp} pe_ltp={st.pe.ltp}")


async def _stale_option_monitor():
    """Background task: every 30 s check if any subscribed option key has
    received no WS tick. If stale, fall back to REST to fetch LTP and inject
    into the LOC engine — so LOC never freezes due to a dead WS key.

    Also prunes option_key_last_tick of keys no longer in use to prevent
    unbounded memory growth across ATM shifts over multiple days."""
    STALE_SEC = 30
    await asyncio.sleep(60)
    while True:
        await asyncio.sleep(STALE_SEC)
        if not state.access_token: continue
        now = time.time()

        # Prune stale entries from option_key_last_tick
        active_keys = set()
        for st in loc_engine.symbols.values():
            if st.ce.instrument_key: active_keys.add(st.ce.instrument_key)
            if st.pe.instrument_key: active_keys.add(st.pe.instrument_key)
        stale_keys = [k for k, ts in list(option_key_last_tick.items())
                      if k not in active_keys and (now - ts) > _OPTION_KEY_MAX_AGE]
        for k in stale_keys:
            option_key_last_tick.pop(k, None)
        if stale_keys:
            print(f"[Stale] Pruned {len(stale_keys)} inactive option_key_last_tick entries")

        for sym in list(loc_engine.symbols):
            st = loc_engine.get_state(sym)
            if not st: continue
            ce_key = st.ce.instrument_key
            pe_key = st.pe.instrument_key
            stale_parts = []
            if ce_key and (now - option_key_last_tick.get(ce_key, 0)) > STALE_SEC:
                age = int(now - option_key_last_tick.get(ce_key, 0))
                stale_parts.append(f"CE={ce_key}(age={age}s)")
            if pe_key and (now - option_key_last_tick.get(pe_key, 0)) > STALE_SEC:
                age = int(now - option_key_last_tick.get(pe_key, 0))
                stale_parts.append(f"PE={pe_key}(age={age}s)")
            if stale_parts:
                print(f"[Stale] {sym} no WS tick: {' | '.join(stale_parts)} — REST fallback")
                try:
                    await _stale_fetch_option_ltp(sym)
                except Exception as e:
                    print(f"[Stale] {sym} REST fallback error: {e}")
                await asyncio.sleep(0.2)


async def _refresh_prev_close_cache():
    """Re-derive state.prev_close from REST so MCX fallback doesn't go stale
    across trading days. net_change is authoritative (ltp - net_change = prev close)."""
    if not state.access_token: return
    try:
        stock_comm_keys = list(dict.fromkeys(_ik.FO_STOCK_KEYS + COMMODITY_KEYS[:5]))
        data = await fetch_quotes_rest(stock_comm_keys, state.access_token)
        idx_data = await fetch_index_quotes(_get_index_keys(), state.access_token)
        data.update(idx_data)
        updated = 0
        for k, v in data.items():
            cp = v.get("ltpc",{}).get("cp", 0)
            if cp:
                state.prev_close[k] = cp
                updated += 1
        print(f"[PrevClose] Refreshed cache: {updated} instruments")
    except Exception as e:
        print(f"[PrevClose] refresh error: {e}")


def _align_mcx_spot_to_options() -> list:
    """Re-point each MCX symbol's spot key at the futures contract that
    matches its currently-active option expiry month. Reads
    `state.expiry_cache` (must already be populated) and mutates
    `_validated_mcx`, `SPOT_KEYS_D`, `FEED_KEY_TO_SYM`, and `COMMODITY_KEYS`
    in place. Returns a list of (sym, old_key, new_key, default_expiry)
    tuples for rolled-over symbols so callers can decide whether to
    re-subscribe the WS feed.

    Name-based keys (instrument master not yet updated for a far-future month)
    are accepted for SPOT_KEYS_D / FEED_KEY_TO_SYM (REST quotes work fine)
    but are kept OUT of COMMODITY_KEYS (WS feed requires numeric keys).
    """
    global COMMODITY_KEYS
    from datetime import date as _dc
    today_d = _dc.today()
    print(f"[MCX Align] Running: today={today_d}, "
          f"next_month_syms={_instruments_mod._MCX_NEXT_MONTH_OPTION_SYMBOLS}")
    rolled = []
    for sym in _MCX_LOC:
        info = state.expiry_cache.get(sym) or {}
        defx = info.get("default") or ""
        if len(defx) < 7: continue
        try:
            yr = int(defx[:4]); mo = int(defx[5:7])
        except Exception:
            continue
        opt_underlying = _instruments_mod._mcx_option_underlying.get(sym) or ""
        if (yr, mo) == (today_d.year, today_d.month):
            # Same calendar month: normally no realignment needed.
            # Exception: NaturalGas and other next-month-convention symbols list
            # their options under the NEXT month's futures. Use
            # _mcx_underlying_for_expiry() (which knows about _MCX_NEXT_MONTH_OPTION_SYMBOLS)
            # rather than _mcx_option_underlying (which is set once at startup and
            # becomes stale after each monthly rollover without a restart).
            cur_spot = _instruments_mod._validated_mcx.get(sym) or SPOT_KEYS_D.get(sym) or ""
            correct_spot = _instruments_mod._mcx_underlying_for_expiry(sym, defx)
            if not correct_spot:
                # Fall back to startup-detected underlying only when month-resolution fails
                correct_spot = opt_underlying
            if not correct_spot or correct_spot == cur_spot:
                continue
            new_spot = correct_spot
            print(f"[MCX Align] {sym}: same-month branch realigning "
                  f"{cur_spot} → {new_spot} (expiry={defx})")
        else:
            # Resolve the futures key for the SPECIFIC option expiry month.
            # _mcx_underlying_for_expiry() is aware of _MCX_NEXT_MONTH_OPTION_SYMBOLS:
            #   CrudeOil/Gold/Silver/Copper: July options → July futures (same month)
            #   NaturalGas: July options → August futures (next month convention)
            # Falls back to mcx_key_for_month() (name-based) then opt_underlying.
            month_key = _instruments_mod._mcx_underlying_for_expiry(sym, defx)
            new_spot = month_key or mcx_key_for_month(sym, yr, mo) or opt_underlying
            print(f"[MCX Align] {sym}: diff-month branch expiry={defx} "
                  f"month_key={month_key or 'MISS'} → new_spot={new_spot or 'NONE'}")
        if not new_spot: continue
        old_spot = _instruments_mod._validated_mcx.get(sym) or SPOT_KEYS_D.get(sym)
        if old_spot == new_spot:
            continue
        tail = new_spot.split("|", 1)[1] if "|" in new_spot else ""
        is_numeric = bool(tail and tail[:1].isdigit())
        # Always update SPOT_KEYS_D / FEED_KEY_TO_SYM — REST quotes and
        # option chain fetch both accept name-based keys. WS feed requires
        # numeric keys, so name-based spot keys are NOT added to COMMODITY_KEYS.
        _instruments_mod._validated_mcx[sym] = new_spot
        SPOT_KEYS_D[sym] = new_spot
        if old_spot and FEED_KEY_TO_SYM.get(old_spot) == sym:
            del FEED_KEY_TO_SYM[old_spot]
        FEED_KEY_TO_SYM[new_spot] = sym
        rolled.append((sym, old_spot, new_spot, defx))
        print(f"[MCX Rollover] {sym}: {old_spot} → {new_spot} "
              f"(expiry={defx}, numeric={is_numeric})")

    # Always rebuild COMMODITY_KEYS so current spot keys are always subscribed
    # even if nothing rolled (e.g. after a restart or expiry fetch update).
    primary = []
    for s in _MCX_LOC:
        k = SPOT_KEYS_D.get(s, "")
        if not k: continue
        t = k.split("|", 1)[1] if "|" in k else ""
        # Only numeric keys can receive WS ticks
        if t and t[:1].isdigit():
            primary.append(k)
    # Pre-subscribe next-month futures for monthly MCX symbols (CrudeOil, NaturalGas,
    # Copper) so there is no WS subscription gap when the current month expires.
    # Without pre-subscription, the new month's spot has no WS feed for up to 60s
    # after rollover (the periodic_refresh interval), causing stale/zero LOC values.
    # GOLD/SILVER are excluded: bi-monthly/quarterly schedule means mcx_key(s,1)
    # may point to a non-existent intermediate month.
    _next_mo_syms = {"CRUDEOIL", "NATURALGAS", "COPPER"}
    next_mo = []
    next_mo_map = {}  # key → "SYM/YYYY-MM" for diagnostics
    for s in _MCX_LOC:
        if s not in _next_mo_syms:
            continue
        nm = (state.expiry_cache.get(s) or {}).get("next_month") or ""
        if len(nm) < 7: continue
        try:
            yr2 = int(nm[:4]); mo2 = int(nm[5:7])
        except Exception:
            continue
        # All MCX symbols: expiry month Y → futures month Y (same-month convention).
        k2 = mcx_key_for_month(s, yr2, mo2)
        t2 = k2.split("|", 1)[1] if "|" in k2 else ""
        if t2 and t2[:1].isdigit():
            next_mo.append(k2)
            next_mo_map[k2] = f"{s}/{nm[:7]}"
    COMMODITY_KEYS = list(dict.fromkeys(primary + next_mo))
    print(f"[MCX Align] primary={primary}")
    print(f"[MCX Align] next_mo={next_mo_map}")
    print(f"[MCX Align] COMMODITY_KEYS={COMMODITY_KEYS}")
    return rolled


def _update_mcx_spot_months():
    """Stamp spot_month (YYYY-MM) onto each MCX symbol's expiry_cache entry.
    Derived from the current validated spot key's trading symbol so the
    frontend can display the FUTURES contract month (e.g. Jun 2026) even
    when the options expiry has already rolled to the next month.
    """
    from .instruments import _mcx_numeric_to_name, _M
    for sym in _MCX_LOC:
        spot_key = SPOT_KEYS_D.get(sym, "")
        if not spot_key or not spot_key.startswith("MCX"):
            continue
        # Resolve numeric key → name-based key (e.g. "MCX_FO|COPPER26JUNFUT")
        name_key = _mcx_numeric_to_name.get(spot_key, spot_key)
        trading_sym = name_key.split("|", 1)[1] if "|" in name_key else name_key
        # Strip base symbol prefix: "COPPER26JUNFUT" → suffix "26JUNFUT"
        suffix = trading_sym[len(sym):]
        if len(suffix) < 5:
            continue
        try:
            yr = int("20" + suffix[:2])
            mon_code = suffix[2:5].upper()
            mon = _M.index(mon_code) + 1
            spot_month = f"{yr:04d}-{mon:02d}"
            if sym not in state.expiry_cache:
                state.expiry_cache[sym] = {}
            state.expiry_cache[sym]["spot_month"] = spot_month
        except (ValueError, IndexError):
            pass

_last_rollover_check_date = None  # set on first periodic tick


async def _prime_mcx_spot_from_rest():
    """Fetch live /v2 quotes for the 5 MCX spot futures and populate
    state.market_data + loc_engine spot OHLC. Without this, the LOC
    engine's spot high/low/close are 0 until the Upstox WS feed delivers
    a tick — which doesn't happen on weekends or outside market hours,
    so the UI shows spot_high = spot_low = spot_close = ltp. This is
    cheap (one /v2/quotes call for 5 keys) and idempotent."""
    if not state.access_token: return
    keys = list(dict.fromkeys([SPOT_KEYS_D[s] for s in _MCX_LOC if s in SPOT_KEYS_D]))
    if not keys: return
    try:
        data = await fetch_quotes_rest(keys, state.access_token)
    except Exception as e:
        print(f"[MCX Prime] fetch error: {e}")
        return
    if not data:
        print(f"[MCX Prime] /v2 quotes returned no data for keys={keys}")
        return
    updated = 0
    ts_ms = int(time.time() * 1000)
    for k, v in data.items():
        sym = FEED_KEY_TO_SYM.get(k, "")
        state.market_data[k] = {**v, "ts": str(ts_ms), "display_name": sym}
        ltp = v.get("ltpc", {}).get("ltp", 0)
        cp  = v.get("ltpc", {}).get("cp", 0)
        if cp: state.prev_close[k] = cp
        if ltp and sym and sym in LOC_SYMBOLS_SET:
            ef = v.get("efeed", {})
            loc_engine.update_spot(
                sym, ltp, cp or ltp,
                ef.get("high", ltp), ef.get("low", ltp),
                ts_ms, ef.get("open", ltp))
            updated += 1
    print(f"[MCX Prime] Refreshed spot OHLC for {updated} MCX symbols")


async def _process_expiry_rollovers(syms: list, parallel: bool = False) -> list:
    """For each symbol, detect whether its default option expiry has
    advanced since the last check. Returns a list of
    (sym, old_default, new_default) for symbols that rolled.

    Optimization — most days no symbol rolls, so we want to avoid hitting
    Upstox ~100 times. We first recompute `get_current_and_next_expiry()`
    from the CACHED expiry list. If the recomputed default equals the
    stored default, the symbol hasn't rolled — no HTTP call made. Only
    symbols whose cached default has advanced (or whose cache is empty)
    trigger a fresh fetch to pick up any newly-published far-future
    expiries from Upstox.

    When `parallel=True`, fetches run concurrently under a semaphore(5)
    — appropriate for the ~130 F&O stock sweep. Priority symbols (indices
    + MCX) run sequentially to guarantee predictable ordering.
    """
    rolled = []
    need_fetch = []

    for sym in syms:
        cached = state.expiry_cache.get(sym) or {}
        cached_default = cached.get("default") or ""
        cached_all = cached.get("all") or []
        if cached_all:
            recomputed = get_current_and_next_expiry(cached_all, sym)
            # Apply recompute to update current_week/next_week labels that may
            # have shifted even before an Upstox fetch (cheap, no HTTP).
            state.expiry_cache[sym] = recomputed
            if (recomputed.get("default") or "") == cached_default:
                continue  # default stable — no fetch, no rollover
        # Default advanced via the cached list, OR cache is empty — fetch fresh
        need_fetch.append((sym, cached_default))

    if not need_fetch: return rolled

    async def _fetch_one(sym, old_default):
        try:
            expiries = await fetch_expiry_list(sym, state.access_token)
            info = get_current_and_next_expiry(expiries, sym)
            state.expiry_cache[sym] = info
            new_default = info.get("default") or ""
            if new_default and new_default != old_default:
                loc_engine.set_expiry(sym, new_default, fetch_chain=False)
                print(f"[Rollover] {sym} expiry: {old_default or '—'} → {new_default}")
                return (sym, old_default, new_default)
        except Exception as e:
            print(f"[Rollover] {sym} expiry refresh: {e}")
        return None

    if parallel:
        sem = asyncio.Semaphore(5)
        async def _bounded(sym, old):
            async with sem:
                r = await _fetch_one(sym, old)
                await asyncio.sleep(0.1)
                return r
        results = await asyncio.gather(*[_bounded(s, d) for s, d in need_fetch])
        rolled = [r for r in results if r]
    else:
        for sym, old_default in need_fetch:
            r = await _fetch_one(sym, old_default)
            if r: rolled.append(r)
            await asyncio.sleep(0.15)
    return rolled


async def _refresh_chains_for_rolled(chain_refresh: dict, parallel: bool = False):
    """Fetch fresh option chain + CE/PE OHLC for each rolled symbol.
    `chain_refresh` maps sym → expiry to fetch. Uses a semaphore to avoid
    overwhelming Upstox when the F&O stock batch rolls on last-Thursday."""
    if not chain_refresh: return

    sem = asyncio.Semaphore(3)

    async def _one_chain(sym, expiry):
        async with sem:
            try:
                chain = await fetch_option_chain(sym, expiry, state.access_token)
                if chain:
                    loc_engine.update_chain(sym, chain)
            except Exception as e:
                print(f"[Rollover] {sym} chain: {e}")
            await asyncio.sleep(0.2)

    if parallel:
        await asyncio.gather(*[_one_chain(s, e) for s, e in chain_refresh.items()])
    else:
        for s, e in chain_refresh.items():
            await _one_chain(s, e)

    # CE/PE OHLC (close/high/low) via REST — chain data often has 0s.
    # Serial with small sleep: per-symbol cost is ~200 ms, so 100 stocks
    # is ~20 s but runs in a background task, so UI isn't blocked.
    for sym in chain_refresh:
        try:
            await _refresh_option_ohlc_single(sym)
        except Exception as e:
            print(f"[Rollover] {sym} option OHLC: {e}")
        await asyncio.sleep(0.1)


async def _daily_rollover_check():
    """Fires from periodic_refresh. Rolls over LOC symbols whose current
    `default` option expiry has passed:

      • Indices (NIFTY/SENSEX weekly + BANKNIFTY/FINNIFTY/MIDCPNIFTY/BANKEX monthly)
      • MCX commodities (monthly; spot futures realigned to active option month)
      • F&O stocks (~130 symbols, monthly)

    Gated to once per calendar day normally. Exception: re-fires within the
    same day when ANY priority symbol's default expiry is strictly in the
    past (< today) — handles intraday expiry (e.g. MCX options at 5 PM) so
    the system doesn't stay stuck on an expired contract until midnight.

    Phase A (priority, blocking): indices + MCX. Runs expiry check,
    MCX spot alignment + OHLC prime, chain + CE/PE OHLC refresh for rolled
    symbols, WS re-subscription. Broadcasts so UI updates within ~10 s.

    Phase B (background): F&O stocks. Runs in parallel under a
    semaphore(5) for expiries and semaphore(3) for chains. The cache-first
    optimization means on a typical day with no expiries passing, Phase B
    finishes in ~1 s with zero HTTP calls.
    """
    global _last_rollover_check_date
    from datetime import date as _dc
    today_d = _dc.today()
    today_s = today_d.isoformat()

    # Re-run even on the same calendar day if any priority symbol's default
    # expiry is strictly past — intraday rollover safety net.
    priority_syms = _INDEX_LOC + _MCX_LOC
    has_expired_default = any(
        (state.expiry_cache.get(s) or {}).get("default", "") < today_s
        for s in priority_syms
        if state.expiry_cache.get(s)
    )
    # Post-market intraday rollover — segment-aware:
    # NSE indices settle at 15:35 IST; MCX commodities at 23:30 IST.
    # Mixing these caused COPPER/NATURALGAS to roll 8 hours early, showing
    # next-month prices while the physical contract was still trading.
    if not has_expired_default and _is_past_market_close_ist():
        has_expired_default = any(
            (state.expiry_cache.get(s) or {}).get("default", "") == today_s
            for s in _INDEX_LOC          # NSE indices only
            if state.expiry_cache.get(s)
        )
    if not has_expired_default and _is_past_mcx_close_ist():
        has_expired_default = any(
            (state.expiry_cache.get(s) or {}).get("default", "") == today_s
            for s in _MCX_LOC            # MCX commodities only
            if state.expiry_cache.get(s)
        )
    if _last_rollover_check_date == today_d and not has_expired_default:
        return
    _last_rollover_check_date = today_d
    if not state.access_token: return

    # ── Phase A: indices + MCX (priority, sequential) ──
    priority = _INDEX_LOC + _MCX_LOC
    priority_rolled = await _process_expiry_rollovers(priority, parallel=False)

    mcx_spot_rolled = _align_mcx_spot_to_options()
    _update_mcx_spot_months()
    await _prime_mcx_spot_from_rest()

    # After spot alignment, refresh _mcx_option_underlying so chain fetches
    # use the correct new-month underlying (e.g. AUGFUT after NaturalGas rolls
    # from June to July). Without this, _fetch_mcx_option_chain() still tries
    # the old startup-derived underlying first and may use a stale canonical key.
    if mcx_spot_rolled and state.access_token:
        mcx_spot_keys = {sym: SPOT_KEYS_D[sym] for sym in _MCX_LOC if sym in SPOT_KEYS_D}
        try:
            await refresh_mcx_option_underlying(state.access_token, mcx_spot_keys)
        except Exception as e:
            print(f"[Rollover] refresh_mcx_option_underlying error: {e}")

    # Chain refresh set: union of expiry-rolled and MCX-spot-rolled
    chain_refresh = {sym: new_def for sym, _o, new_def in priority_rolled}
    for sym, _ok, _nk, defx in mcx_spot_rolled:
        chain_refresh.setdefault(sym, defx)

    await _refresh_chains_for_rolled(chain_refresh, parallel=False)

    if state.feed_client:
        if mcx_spot_rolled and COMMODITY_KEYS:
            try:
                await _sub_binary(state.feed_client, COMMODITY_KEYS, "full")
                print(f"[Rollover] Re-subscribed MCX keys: {COMMODITY_KEYS}")
            except Exception as e:
                print(f"[Rollover] MCX re-sub: {e}")
        try:
            await _subscribe_new_option_keys()
        except Exception as e:
            print(f"[Rollover] option re-sub: {e}")

    # Broadcast priority results so UI updates fast
    await broadcast({
        "type": "snapshot_update",
        "market_data": state.market_data,
        "commodity_keys": COMMODITY_KEYS,
        "spot_keys": SPOT_KEYS_D,
        "expiry_cache": state.expiry_cache,
        "loc_results": loc_engine.get_all_results(),
    })
    print(f"[Rollover] Phase A done: {len(priority_rolled)} priority expiry rolls, "
          f"{len(mcx_spot_rolled)} MCX spot rolls, {len(chain_refresh)} chains refreshed")

    # ── Phase B: F&O stocks (background, parallel) ──
    async def _stocks_background():
        stock_syms = [s for s in LOC_SYMBOLS if s not in priority]
        stock_rolled = await _process_expiry_rollovers(stock_syms, parallel=True)
        if not stock_rolled:
            # Possibly nothing rolled — still broadcast expiry_cache since
            # current_week/next_week labels may have shifted via recompute.
            await broadcast({
                "type": "snapshot_update",
                "expiry_cache": state.expiry_cache,
            })
            print(f"[Rollover] Phase B done: 0 stock rolls")
            return
        chain_refresh_b = {sym: new_def for sym, _o, new_def in stock_rolled}
        await _refresh_chains_for_rolled(chain_refresh_b, parallel=True)
        if state.feed_client:
            try:
                await _subscribe_new_option_keys()
            except Exception as e:
                print(f"[Rollover] Phase B option re-sub: {e}")
        await broadcast({
            "type": "snapshot_update",
            "expiry_cache": state.expiry_cache,
            "loc_results": loc_engine.get_all_results(),
        })
        print(f"[Rollover] Phase B done: {len(stock_rolled)} stock rolls, "
              f"{len(chain_refresh_b)} chains refreshed")

    asyncio.create_task(_stocks_background())


async def _supervise(name: str, coro_factory, restart_delay: float = 5.0):
    """Run a background task forever — if it ever exits (returns, or an
    exception escapes its own internal handling) log it and restart after
    `restart_delay`. Without this, a single uncaught exception in a
    long-running task like the feed flusher or periodic refresher would
    silently and permanently stop that task — FastAPI itself keeps running
    (so systemd sees no crash), but the data it was responsible for
    (broadcasts, chain/option refreshes) stops forever until a manual
    restart. asyncio.CancelledError is re-raised so intentional
    cancellation (e.g. during /auth/token restart) still works."""
    while True:
        try:
            await coro_factory()
            print(f"[Supervisor] {name} exited normally — restarting in {restart_delay}s")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            import traceback
            print(f"[Supervisor] {name} crashed: {e} — restarting in {restart_delay}s")
            traceback.print_exc()
        await asyncio.sleep(restart_delay)


async def periodic_refresh():
    tick = 0
    while True:
        await asyncio.sleep(60)
        if not state.access_token: continue
        try:
            # Day-change rollover: detect once per new calendar day.
            try:
                await _daily_rollover_check()
            except Exception as e:
                print(f"[Rollover] check error: {e}")
            await loc_engine.refresh_all_chains()
            await _subscribe_new_option_keys()
            tick += 1
            # Refresh prev_close cache every 10 minutes so MCX/stale-day values recover
            if tick % 10 == 0:
                await _refresh_prev_close_cache()
        except Exception as e:
            # Any uncaught exception here would silently kill this task
            # forever — chains/options would stop refreshing with no crash
            # and no log until the process is restarted.
            print(f"[PeriodicRefresh] error: {e}")


# ══════════════════════════════════════════════════════════════════
#  WS SUBSCRIPTION — thin wrappers over the shared Zerodha ticker2
#  `ws` param kept for call-site compatibility (every caller — startup_init,
#  calculator.py, rollover paths — still passes state.feed_client through
#  unchanged); the actual connection is the module-level `ticker2` singleton.
# ══════════════════════════════════════════════════════════════════
async def _sub_binary(ws, keys: list, mode: str = "full"):
    tokens = [t for t in (key_to_token(k) for k in keys) if t]
    if not tokens:
        return
    print(f"[Feed] Subscribing {len(tokens)} tokens, mode={mode}")
    ticker2.subscribe(tokens, mode)

async def _unsub_binary(ws, keys: list):
    """Mirrors _sub_binary. Without this, every ATM/ITM-2 strike roll and
    every expiry rollover only ever ADDS keys to the live subscription and
    never removes the superseded ones — the set grows unbounded for the
    life of the process. Two consequences: stale keys keep consuming a slot
    against Zerodha's per-connection subscription cap, and once that cap is
    hit, brand-new strikes (which are exactly what a strike roll or expiry
    rollover needs fresh ticks for) silently fail to subscribe — the
    frontend then shows a CE/PE LTP that never updates, indistinguishable
    from a live illiquid contract."""
    if not keys: return
    tokens = [t for t in (key_to_token(k) for k in keys) if t]
    if not tokens:
        return
    print(f"[Feed] Unsubscribing {len(tokens)} superseded tokens")
    ticker2.unsubscribe(tokens)


# ══════════════════════════════════════════════════════════════════
#  BROWSER WEBSOCKET
# ══════════════════════════════════════════════════════════════════
WS_HEARTBEAT_SEC = 15   # how often the server pings each client
WS_STALE_SEC     = 45   # no client activity (msg or pong) for this long → force-close

@app.websocket("/ws/feed")
async def ws_browser(ws: WebSocket):
    await ws.accept()
    state.connected_clients.add(ws)
    print(f"[WS] Client connected — total={len(state.connected_clients)}")
    last_seen = {"ts": time.time()}

    try:
        await ws.send_text(json.dumps({
            "type": "snapshot",
            "market_data": state.market_data,
            "market_status": state.market_status,
            "loc_results": loc_engine.get_all_results(),
            "calc_results": loc_engine.get_all_calc_results(),
            "expiry_cache": state.expiry_cache,
            "spot_keys": SPOT_KEYS_D,
            "commodity_keys": COMMODITY_KEYS,
            "mode": "mock" if USE_MOCK else "live",
        }))
    except Exception as e:
        print(f"[WS] Initial snapshot send failed: {e}")
        state.connected_clients.discard(ws)
        return

    async def _receiver():
        """Drain all incoming client frames (text, bytes, disconnect).
        Using receive() instead of receive_text() so pong/binary control
        frames don't raise and silently kill this task."""
        try:
            while True:
                msg = await ws.receive()
                last_seen["ts"] = time.time()
                if msg["type"] == "websocket.disconnect":
                    break
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print(f"[WS] receive error: {e}")

    async def _heartbeat():
        """Ping every WS_HEARTBEAT_SEC; force-close if the client has been
        silent for WS_STALE_SEC. Catches half-open connections (laptop
        sleep, mobile network switch, idle proxy drop) where the browser
        never fires onclose/onerror, so the frontend would otherwise never
        know to reconnect."""
        try:
            while True:
                await asyncio.sleep(WS_HEARTBEAT_SEC)
                if time.time() - last_seen["ts"] > WS_STALE_SEC:
                    print("[WS] Client stale (no activity) — closing")
                    return
                try:
                    await ws.send_text(json.dumps({"type": "ping", "ts": int(time.time()*1000)}))
                except Exception as e:
                    print(f"[WS] ping send failed, dropping client: {e}")
                    return
        except Exception as e:
            print(f"[WS] heartbeat error: {e}")

    recv_task = asyncio.create_task(_receiver())
    hb_task   = asyncio.create_task(_heartbeat())
    try:
        await asyncio.wait({recv_task, hb_task}, return_when=asyncio.FIRST_COMPLETED)
    except Exception as e:
        print(f"[WS] session error: {e}")
    finally:
        for t in (recv_task, hb_task):
            if not t.done(): t.cancel()
        state.connected_clients.discard(ws)
        try:
            await ws.close()
        except Exception:
            pass
        print(f"[WS] Client disconnected — total={len(state.connected_clients)}")

async def broadcast(msg: dict):
    if msg.get("type") == "live_feed":
        ts = int(msg.get("currentTs",0) or time.time()*1000)
        # Normalize response keys: MCX name→numeric, NSE_EQ symbol→ISIN
        raw_feeds = msg.get("feeds", {})
        feeds = {}
        for k, fv in raw_feeds.items():
            feeds[normalize_response_key(k)] = fv
        msg["feeds"] = feeds
        for k, fv in feeds.items():
            ltp, cp, o, h, l = _ex(fv)
            # WS ltpc.cp is the previous-day close for indices/stocks and
            # should be trusted. For MCX the WS value is unreliable (often
            # equals today's LTP), so fall back to the REST-derived value.
            # Also fall back if WS gave 0 or (suspiciously) == ltp.
            is_mcx = k.startswith("MCX")
            ws_cp_bad = (not cp) or (is_mcx and ltp and abs(cp - ltp) < 0.01)
            if ws_cp_bad and k in state.prev_close:
                cp = state.prev_close[k]
            # Keep the REST cache refreshed from trustworthy WS values so it
            # doesn't go stale across trading days.
            elif cp and not is_mcx:
                state.prev_close[k] = cp
            fv.setdefault("ltpc",{})["cp"] = cp
            # Merge efeed: preserve day open/high/low from REST snapshot
            existing = state.market_data.get(k, {})
            prev_ef = existing.get("efeed", {})
            new_ef  = fv.get("efeed", {})
            # Only update high/low if live value is valid (non-zero)
            merged_ef = {**prev_ef, **new_ef}
            if not merged_ef.get("open") or merged_ef["open"]==0: merged_ef["open"] = prev_ef.get("open",ltp)
            if not merged_ef.get("high") or merged_ef["high"]==0: merged_ef["high"] = prev_ef.get("high",ltp)
            if not merged_ef.get("low")  or merged_ef["low"] ==0: merged_ef["low"]  = prev_ef.get("low",ltp)
            merged_ef["ltp"] = ltp
            merged_ef["cp"]  = cp
            fv["efeed"] = merged_ef
            sym_name = _ik.ISIN_TO_SYMBOL.get(k,"")
            if sym_name: fv["display_name"] = sym_name
            state.market_data[k] = {**existing, **fv, "ts":str(ts)}
            if ltp: _update_ohlc(k, ltp, ts,
                                  merged_ef.get("open",ltp),
                                  merged_ef.get("high",ltp),
                                  merged_ef.get("low",ltp))
            # Route using raw values from this tick only (via _ex). If the
            # tick omits a field, pass 0 — update_spot / update_option_from_feed
            # treat 0 as "skip this field" and preserve the prior session
            # value. Previously we passed merged_ef.* which inherited
            # yesterday's efeed on the first partial tick of a new session,
            # clobbering today's real high/low via the `or ltp` fallback.
            _route_tick(k, ltp, cp, o, h, l, ts)
        # Buffer feeds for throttled broadcast to frontend
        for k, fv in feeds.items():
            state._feed_buffer[k] = fv
        return  # don't send immediately — _flush_feed_buffer will do it

    elif msg.get("type") == "market_info":
        si = msg.get("marketInfo",{}).get("segmentStatus",{})
        if si: state.market_status = si

    elif msg.get("type") in ("snapshot_update","expiry_update"):
        for k, v in msg.get("market_data",{}).items():
            if not state.market_data.get(k,{}).get("ltpc",{}).get("ltp"):
                state.market_data[k] = v

    # Non-live messages (snapshot_update, market_info, etc.) send immediately
    await _send_to_clients(msg)


async def _send_to_clients(msg: dict):
    """Send a message to all connected browser WebSocket clients.
    A send failure on one socket must never affect the others — each send
    is isolated in its own try/except and failed sockets are dropped."""
    if not state.connected_clients: return
    text = json.dumps(msg); dead = set()
    for ws in list(state.connected_clients):
        try:
            await ws.send_text(text)
        except Exception as e:
            print(f"[WS] send failed, dropping client: {e}")
            dead.add(ws)
    if dead:
        state.connected_clients -= dead


FEED_THROTTLE_MS = 300  # send at most ~3 updates/sec to frontend

async def _flush_feed_buffer():
    """Background task: flush buffered feed ticks + LOC results to frontend.

    Fires whenever:
    - There are buffered instrument feed ticks (live price ticks), OR
    - Any LOC symbol was recalculated since the last flush (_loc_dirty).

    This decouples LOC broadcasts (bop, cep, pep, zone…) from instrument
    ticks — so BOP updates reach the frontend even when an option key has
    no live WS tick (e.g. stale CRUDEOIL PE during illiquid periods).
    Bug 2 fix: loc_results are sent on every recalc, not just on feed ticks.
    """
    while True:
        await asyncio.sleep(FEED_THROTTLE_MS / 1000)
        try:
            if not state.connected_clients:
                continue
            has_feeds = bool(state._feed_buffer)
            has_loc   = bool(state._loc_dirty)
            if not has_feeds and not has_loc:
                continue
            # Swap buffers atomically before await so concurrent updates queue up
            feeds = state._feed_buffer
            state._feed_buffer = {}
            state._loc_dirty.clear()
            msg = {
                "type": "live_feed",
                "feeds": feeds,
                "currentTs": int(time.time() * 1000),
                "loc_results": loc_engine.get_all_results(),
                "calc_results": loc_engine.get_all_calc_results(),
            }
            await _send_to_clients(msg)
        except Exception as e:
            # Never let a single bad iteration (e.g. a transient KeyError in
            # loc_engine results) kill this task forever — an uncaught
            # exception here would silently end all future broadcasts to
            # every connected client until the process restarts.
            print(f"[Flush] error: {e}")


# ══════════════════════════════════════════════════════════════════
#  ZERODHA LIVE FEED
#  Reconnect/watchdog is handled internally by the shared ticker2 (see
#  backend/history2/ticker.py) — this function's job is just the one-time
#  subscribe sequence, then it idles forever so _supervise() doesn't treat
#  a normal return as a crash needing a restart-with-full-resubscribe.
# ══════════════════════════════════════════════════════════════════
_INDEX_POLL_INTERVAL = 7  # seconds

async def _index_ohlc_poll():
    """Zerodha's ticker only streams LTP for index tokens (no OHLC/depth
    mode for indices, unlike Upstox which streamed full index OHLC over the
    WS feed) — poll kite.quote() for the handful of index keys periodically
    so spot_high/spot_low stay populated for the LOC formulas that use them.
    Routed through broadcast() so state.market_data / _route_tick get the
    same update a WS tick would have produced."""
    while True:
        await asyncio.sleep(_INDEX_POLL_INTERVAL)
        if not state.access_token or not _get_index_keys():
            continue
        try:
            data = await fetch_index_quotes(_get_index_keys(), state.access_token)
        except Exception as e:
            print(f"[IndexPoll] error: {e}")
            continue
        if data:
            await broadcast({"type": "live_feed", "feeds": data,
                              "currentTs": str(int(time.time() * 1000))})

async def start_feed():
    if USE_MOCK:
        from backend.mock_feed import start_mock_feed
        await start_mock_feed(broadcast); return

    while not state.access_token:
        print("[Feed] No Zerodha access token yet — waiting for login")
        await asyncio.sleep(2)

    # Fresh (re)start: clear bookkeeping so a stale key from before a
    # restart doesn't look "already subscribed" — mirrors the old
    # per-reconnect clear, now done once here since ticker2 itself handles
    # reconnects transparently (subscribed_tokens survives those).
    state.subscribed_option_keys.clear()

    try:
        ticker2.start(_on_zerodha_tick, _on_zerodha_status)
    except Exception as e:
        print(f"[Feed] ticker2.start() failed: {e}")
        return
    state.feed_client = ticker2
    print("[Feed] Zerodha ticker connected/shared")

    # 1. Indices — LTP mode only (see _index_ohlc_poll for why)
    await _sub_binary(ticker2, _get_index_keys(), "ltp")
    await asyncio.sleep(0.2)

    # 2. Commodities — full mode (both current & next month)
    await _sub_binary(ticker2, COMMODITY_KEYS, "full")
    await asyncio.sleep(0.2)

    # 3. F&O stocks — full mode for OHLC
    stock_keys = list(dict.fromkeys(_ik.FO_STOCK_KEYS))
    for i in range(0, len(stock_keys), 100):
        await _sub_binary(ticker2, stock_keys[i:i+100], "full")
        await asyncio.sleep(0.2)

    # 4. Option CE/PE keys from chain (ITM-2, plus additive ITM-1, OTM-2)
    opt_keys  = loc_engine.get_option_keys()
    itm1_keys = loc_engine.get_itm1_option_keys()
    otm2_keys = loc_engine.get_otm2_option_keys()
    all_opt_keys = list(dict.fromkeys(opt_keys + itm1_keys + otm2_keys))
    if all_opt_keys:
        await _sub_binary(ticker2, all_opt_keys, "full")
        state.subscribed_option_keys.update(all_opt_keys)
        _now = time.time()
        for _k in all_opt_keys:
            option_key_last_tick[_k] = _now
        for st_sym in loc_engine.symbols.values():
            if st_sym.ce.instrument_key:
                option_key_map[st_sym.ce.instrument_key] = (st_sym.symbol,"CE")
            if st_sym.pe.instrument_key:
                option_key_map[st_sym.pe.instrument_key] = (st_sym.symbol,"PE")
            if st_sym.itm1_ce.instrument_key:
                itm1_option_key_map[st_sym.itm1_ce.instrument_key] = (st_sym.symbol,"CE")
            if st_sym.itm1_pe.instrument_key:
                itm1_option_key_map[st_sym.itm1_pe.instrument_key] = (st_sym.symbol,"PE")
            if st_sym.otm2_ce_key:
                otm2_option_key_map[st_sym.otm2_ce_key] = (st_sym.symbol,"CE")
            if st_sym.otm2_pe_key:
                otm2_option_key_map[st_sym.otm2_pe_key] = (st_sym.symbol,"PE")

    print(f"[Feed] Subscribed: {len(_get_index_keys())} idx | "
          f"{len(COMMODITY_KEYS)} mcx | {len(stock_keys)} stocks | "
          f"{len(all_opt_keys)} options ({len(opt_keys)} ITM2 + {len(itm1_keys)} ITM1 + {len(otm2_keys)} OTM2)")

    if not state._index_poll_task or state._index_poll_task.done():
        state._index_poll_task = asyncio.create_task(_index_ohlc_poll())

    # ticker2 owns reconnect/watchdog from here — idle forever so a normal
    # return here isn't mistaken by _supervise() for a crash.
    while True:
        await asyncio.sleep(3600)


# ══════════════════════════════════════════════════════════════════
#  AUTH ROUTES
# ══════════════════════════════════════════════════════════════════
@app.post("/auth/login")
async def login(payload: dict):
    if payload.get("password") == PASSWORD:
        token = f"sess_{int(time.time())}"
        state.sessions[token] = {"ts": time.time()}
        return {"status":"ok","token":token}
    raise HTTPException(401,"Invalid password")

@app.get("/auth/upstox/login")
async def zerodha_login_redirect():
    # Route path kept as-is (see migration plan) — now drives Zerodha's
    # login dialog instead of Upstox's. Zerodha's redirect_uri is fixed in
    # the Kite Connect app console, not passed here; it must point at
    # whichever of /auth/callback or /api/zerodha/callback (History 2's own,
    # already-registered route) you've configured there — both handlers do
    # the same request_token exchange, so either works.
    try:
        return RedirectResponse(zc.login_url())
    except RuntimeError as e:
        raise HTTPException(400, str(e))

@app.get("/auth/callback")
async def auth_cb(request_token: str = "", status: str = ""):
    if status and status != "success":
        raise HTTPException(400, f"Zerodha authentication failed (status={status})")
    if not request_token:
        raise HTTPException(400, "request_token required")
    try:
        await zc.generate_session(request_token)
    except Exception as e:
        raise HTTPException(400, str(e))
    state.access_token = state2.access_token
    loc_engine.access_token = state.access_token
    asyncio.create_task(_restart())
    return RedirectResponse("/?auth=success")

@app.post("/auth/token")
async def set_token(payload: dict):
    """Accepts either {"request_token": "..."} (drives the normal Zerodha
    exchange, same as /auth/callback) or {"access_token": "..."} (direct
    passthrough — advanced/scripted-login escape hatch, see
    zerodha_client.set_access_token)."""
    request_token = payload.get("request_token", "")
    access_token = payload.get("access_token", "")
    if request_token:
        try:
            await zc.generate_session(request_token)
        except Exception as e:
            raise HTTPException(400, str(e))
    elif access_token:
        zc.set_access_token(access_token)
    else:
        raise HTTPException(400, "request_token or access_token required")
    state.access_token = state2.access_token
    loc_engine.access_token = state.access_token
    asyncio.create_task(_restart())
    return {"status":"ok","message":"Feed restarting..."}

async def _restart():
    print("[Restart] Restarting...")
    if state.feed_task and not state.feed_task.done():
        state.feed_task.cancel(); await asyncio.sleep(1)
    state.feed_task = asyncio.create_task(_supervise("start_feed", start_feed))
    await asyncio.sleep(3)
    await startup_init()
    if state.chain_task and not state.chain_task.done(): state.chain_task.cancel()
    state.chain_task = asyncio.create_task(_supervise("periodic_refresh", periodic_refresh))
    print("[Restart] Done")


# ══════════════════════════════════════════════════════════════════
#  DATA API ROUTES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/feed-log")
async def feed_log():
    return {"log": state.feed_log[-30:]}

@app.get("/ping")
async def ping():
    """Keep-alive endpoint. Ping this every 5 minutes via UptimeRobot to prevent Render cold starts."""
    return {"ok": True, "ts": int(time.time() * 1000)}

@app.get("/api/status")
async def api_status():
    return {
        "auth": bool(state.access_token) or USE_MOCK,
        "feed_connected": state.feed_client is not None,
        "instruments": len(state.market_data), "frames": state.frame_count,
        "decoded": state.decode_ok, "mode": "mock" if USE_MOCK else "live",
        "option_keys": len(state.subscribed_option_keys),
        "spot_keys": SPOT_KEYS_D, "commodity_keys": COMMODITY_KEYS,
    }

@app.get("/api/market-data")
async def market_data_api():
    return {"market_data":state.market_data,"market_status":state.market_status,
            "timestamp":int(time.time()*1000)}

@app.get("/api/loc-all")
async def loc_all(): return loc_engine.get_all_results()

@app.get("/api/loc/{symbol}")
async def get_loc(symbol: str):
    st = loc_engine.get_state(symbol.upper())
    if not st: raise HTTPException(404,"Not found")
    return st.loc_result or {"error":"No data yet"}

@app.get("/api/loc-history/{symbol}")
async def get_loc_history(symbol: str):
    return {"symbol":symbol,"history":state.loc_history.get(symbol.upper(),[])}

@app.get("/api/expiry/{symbol}")
async def get_expiry(symbol: str):
    return state.expiry_cache.get(symbol.upper(), {"error":"Not loaded","all":[]})

@app.post("/api/expiry/{symbol}")
async def set_expiry(symbol: str, payload: dict):
    """Set the Calculator's active expiry for this symbol. The LOC table is
    always pinned to the symbol's default/current-week expiry and is NOT
    affected by this endpoint. Selecting the default expiry clears the
    Calculator view (frontend then falls back to LOC data)."""
    sym    = symbol.upper(); expiry = payload.get("expiry","")
    if not expiry: raise HTTPException(400,"expiry required")
    info    = state.expiry_cache.get(sym, {})
    default = info.get("default", "")
    state.expiry_cache.setdefault(sym, {})["selected"] = expiry
    if default and expiry == default:
        # Revert Calculator to the LOC default — discard the calc view.
        loc_engine.clear_calc_expiry(sym)
        return {"status":"ok","symbol":sym,"expiry":expiry,"mode":"loc"}
    # Non-default: set up a calc view for the requested expiry.
    asyncio.create_task(loc_engine.set_calc_expiry(sym, expiry))
    return {"status":"ok","symbol":sym,"expiry":expiry,"mode":"calc"}


# ── Dedicated Calculator endpoint ─────────────────────────────────────
# Frontend posts symbol+expiry; backend returns the 25 LOC formulas
# computed against a fresh chain for that expiry. Frontend polls every
# 5 s while a non-default expiry is selected. The actual chain fetch,
# spot-key resolution, WS subscribe, and LOC compute now live in
# backend/calculator.py — keeps that pipeline isolated from the live
# LOC table and from MCX/index rollover state.

@app.get("/api/calculator/{symbol}")
async def get_calculator(symbol: str, expiry: str = ""):
    """Fresh LOC snapshot for a user-selected expiry. Independent of the
    main LOC table — the table stays pinned to the default/current-week
    expiry; this endpoint runs the 25 LOC formulas against a freshly
    fetched option chain for the requested expiry, using the live WS-
    driven spot data, and returns the same field shape the frontend
    already renders for `locResults[sym]`.

    Params:
      symbol  — uppercase LOC symbol (NIFTY, SENSEX, CRUDEOIL, RELIANCE…)
      expiry  — ISO date (YYYY-MM-DD), must be present in expiry_cache.all

    Selecting the symbol's default expiry returns the existing live
    `loc_result` straight from the LOC engine (no Upstox round-trip).
    """
    sym = symbol.upper()
    if not expiry:
        raise HTTPException(400, "expiry query param required")
    if not state.access_token:
        raise HTTPException(503, "no Zerodha access token")

    info = state.expiry_cache.get(sym, {})
    valid_expiries = info.get("all") or []
    if valid_expiries and expiry not in valid_expiries:
        raise HTTPException(400, f"expiry {expiry} not available for {sym}")

    # Default expiry → return the LOC engine's live result (matches the
    # LOC table exactly — no need to refetch from Upstox).
    default = info.get("default", "")
    if default and expiry == default:
        st = loc_engine.get_state(sym)
        if st and st.loc_result:
            return {**st.loc_result, "expiry": expiry, "source": "loc"}

    # Non-default: delegate to the isolated calculator module. It pairs
    # the requested options expiry with the matching-month spot (futures
    # for MCX, unchanged spot for indices/stocks) and ensures that
    # spot key is on the live WS feed.
    res = await calc_mod.compute_calc_result(
        sym=sym, expiry=expiry,
        default_spot_keys=SPOT_KEYS_D,
        market_data=state.market_data,
        prev_close=state.prev_close,
        access_token=state.access_token,
        feed_client=state.feed_client,
        sub_binary=_sub_binary,
        subscribed_set=state.subscribed_calc_spot_keys,
    )
    if res is None:
        raise HTTPException(502, f"option chain unavailable for {sym}/{expiry}")
    return res

@app.get("/api/ohlc/{key:path}")
async def get_ohlc(key: str):
    """Return server-tracked OHLC candles."""
    return {"key":key,"candles":state.ohlc.get(key,[])}

@app.get("/api/ohlc-live/{key:path}")
async def get_ohlc_live(key: str, tf: str = "minutes/1"):
    """Fetch intraday candles from Zerodha."""
    if not state.access_token: return {"key":key,"candles":[]}
    # Parse tf like "minutes/1", "hours/1", "days/1"
    parts = tf.split("/")
    unit = parts[0] if len(parts)>0 else "minutes"
    interval = int(parts[1]) if len(parts)>1 else 1
    candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
    return {"key":key,"candles":candles}

@app.get("/api/ohlc-hist/{key:path}")
async def get_ohlc_hist(key: str, unit: str = "minutes", interval: int = 1,
                         to_date: str = "", from_date: str = ""):
    """
    Fetch historical candles via /v3/historical-candle/{key}/{unit}/{interval}/{to}/{from}
    Supports 1m/5m/15m/1h/1d with configurable date range.
    Frontend sends: /api/ohlc-hist/{instrKey}/{unit}/{interval}/{toDate}/{fromDate}
    FastAPI {key:path} captures everything, so parse the extra segments here.
    """
    # Parse path segments: key may contain unit/interval/dates appended by frontend
    parts = key.split("/")
    # The instrument key contains "|" (e.g. NSE_INDEX|Nifty 50) — find where extra segments start
    # Extra segments are: unit (minutes|hours|days), interval (int), toDate, fromDate
    _units = {"minutes", "hours", "days"}
    split_idx = None
    for i, p in enumerate(parts):
        if p in _units and i > 0:
            split_idx = i
            break
    if split_idx is not None:
        key = "/".join(parts[:split_idx])
        remaining = parts[split_idx:]
        if len(remaining) >= 1: unit = remaining[0]
        if len(remaining) >= 2:
            try: interval = int(remaining[1])
            except: pass
        if len(remaining) >= 3: to_date = remaining[2]
        if len(remaining) >= 4: from_date = remaining[3]

    if not state.access_token: return {"key":key,"candles":[]}
    from datetime import date, timedelta

    if not to_date:
        to_date = date.today().isoformat()
    if not from_date:
        from_date = (date.today()-timedelta(days=5)).isoformat()

    # For today's intraday, use intraday endpoint
    today = date.today().isoformat()
    if from_date == today and to_date == today and unit != "days":
        candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
        return {"key":key,"candles":candles}

    # Historical endpoint (Zerodha kite.historical_data(), see instruments.py)
    try:
        result = await fetch_historical_candles(key, state.access_token, unit, interval, from_date, to_date)
        # Merge with intraday for today so a range ending today includes it
        if to_date == today:
            today_candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
            existing_times = {c["t"] for c in result}
            result += [c for c in today_candles if c["t"] not in existing_times]
        result.sort(key=lambda c: c["t"])
        return {"key":key,"candles":result}
    except Exception as e:
        print(f"[Hist] {key}: {e}")
    # Fallback to intraday
    candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
    return {"key":key,"candles":candles}

@app.get("/api/debug/chain/{symbol}")
async def debug_chain(symbol: str):
    st = loc_engine.get_state(symbol.upper())
    if not st: return {"error":"not registered"}
    return {
        "symbol":symbol,"expiry":st.expiry,"spot_ltp":st.spot.ltp,
        "ce_strike":st.ce_strike,"ce_ltp":st.ce.ltp,"ce_close":st.ce.close,
        "ce_high":st.ce.high,"ce_low":st.ce.low,"ce_key":st.ce.instrument_key,
        "pe_strike":st.pe_strike,"pe_ltp":st.pe.ltp,"pe_close":st.pe.close,
        "pe_high":st.pe.high,"pe_low":st.pe.low,"pe_key":st.pe.instrument_key,
        "chain_size":len(st.option_chain),"loc":st.loc_result,
    }

@app.get("/api/debug/calc/{symbol}")
async def debug_calc(symbol: str):
    """Inspect the Calculator-only view for this symbol (independent of LOC)."""
    calc = loc_engine.get_calc_state(symbol.upper())
    if not calc: return {"active": False, "message": "no calc view set"}
    return {
        "active": True,
        "expiry": calc.expiry,
        "ce_strike": calc.ce_strike, "ce_ltp": calc.ce.ltp, "ce_close": calc.ce.close,
        "ce_high": calc.ce.high, "ce_low": calc.ce.low, "ce_key": calc.ce.instrument_key,
        "pe_strike": calc.pe_strike, "pe_ltp": calc.pe.ltp, "pe_close": calc.pe.close,
        "pe_high": calc.pe.high, "pe_low": calc.pe.low, "pe_key": calc.pe.instrument_key,
        "chain_size": len(calc.option_chain),
        "result": calc.result,
    }

@app.get("/api/debug/mcx")
async def debug_mcx():
    return {"commodity_keys":COMMODITY_KEYS,"spot_keys":SPOT_KEYS_D}

@app.post("/api/subscribe")
async def subscribe(payload: dict):
    keys=payload.get("instrumentKeys",[]); mode=payload.get("mode","full")
    if state.feed_client and keys: await _sub_binary(state.feed_client, keys, mode)
    return {"status":"ok"}

_watchlists: dict = {}

@app.get("/api/watchlist")
async def get_wl(): return _watchlists

@app.post("/api/watchlist")
async def save_wl(p: dict):
    _watchlists[p.get("name","default")] = p.get("keys",[]); return {"status":"ok"}

@app.delete("/api/watchlist/{name}")
async def del_wl(name: str):
    _watchlists.pop(name,None); return {"status":"ok"}


# ── Serve React build ─────────────────────────────────────────────
if FRONTEND_DIST.exists():
    try:
        app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST/"assets")),name="assets")
    except: pass

@app.get("/")
async def root():
    idx = FRONTEND_DIST/"index.html"
    if idx.exists(): return FileResponse(str(idx))
    return HTMLResponse("<h2>Build frontend: cd frontend && npm run build</h2>")

@app.get("/{path:path}")
async def spa(path: str):
    idx = FRONTEND_DIST/"index.html"
    # Resolve and require the result to stay inside FRONTEND_DIST (blocks
    # ../ path traversal out of the dist folder) AND reject any dotfile
    # segment (blocks serving a stray .env/.git/etc. that ended up inside
    # frontend/dist — e.g. via a file accidentally placed in frontend/public/
    # and copied verbatim into the build output by Vite). A real request for
    # /.env from the public internet returned 200 OK before this check existed.
    try:
        base = FRONTEND_DIST.resolve()
        f = (FRONTEND_DIST/path).resolve()
        rel = f.relative_to(base)
        if f.is_file() and not any(part.startswith(".") for part in rel.parts):
            return FileResponse(str(f))
    except (ValueError, OSError):
        pass
    if idx.exists(): return FileResponse(str(idx))
    return HTMLResponse("Not found",404)


# ══════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def on_startup():
    print(f"RAIMA Markets v9 | {'MOCK' if USE_MOCK else 'LIVE'} | broker=Zerodha")
    # Build initial key maps even without token
    global SPOT_KEYS_D, FEED_KEY_TO_SYM
    SPOT_KEYS_D = get_spot_keys()
    FEED_KEY_TO_SYM = {v:k for k,v in SPOT_KEYS_D.items()}
    # Only map current month (m=0) MCX keys — prevents next-month price mixing
    for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]:
        FEED_KEY_TO_SYM[mcx_key(s,0)] = s

    if USE_MOCK:
        from backend.mock_feed import start_mock_feed
        state.feed_task = asyncio.create_task(_supervise("start_mock_feed", lambda: start_mock_feed(broadcast)))
    elif state.access_token:
        loc_engine.access_token = state.access_token
        state.feed_task = asyncio.create_task(_supervise("start_feed", start_feed))
    else:
        print("[!] No Zerodha token yet — GET /auth/upstox/login to authenticate, "
              "or POST /auth/token with a request_token/access_token")

    # Start throttled feed flush task (supervised — never allowed to die silently)
    state._flush_task = asyncio.create_task(_supervise("flush_feed_buffer", _flush_feed_buffer))

    # Start stale-option monitor (Bug 1 fallback: REST fetch when WS tick absent)
    asyncio.create_task(_supervise("stale_option_monitor", _stale_option_monitor))

    # History 2 — autonomous background engine, independent of any frontend
    # connection, running alongside this file's own start_feed()/
    # periodic_refresh() autonomy on the SAME shared Zerodha session/ticker.
    # Waits internally for Zerodha auth before doing anything; supervised so
    # a crash restarts it instead of silently stopping recording.
    #
    # Re-enabled: this was disabled (2026-07-31) because its initial resolve
    # pass made ~200 synchronous (blocking) Zerodha SDK/instrument-scan calls
    # with no asyncio.to_thread offload, starving the event loop and
    # stalling the live feed for many minutes on every restart. Fixed as
    # part of the Upstox->Zerodha migration: history2/instruments.py's
    # _load()/resolve_*() and engine.py's _resolve_and_register_strikes()
    # are now async and offload every kite.* call via asyncio.to_thread.
    from .history2 import engine as _history2_engine
    asyncio.create_task(_supervise("history2_engine", _history2_engine.start))

    asyncio.create_task(_delayed_startup())

async def _delayed_startup():
    await asyncio.sleep(3)
    await startup_init()
    state.chain_task = asyncio.create_task(_supervise("periodic_refresh", periodic_refresh))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
