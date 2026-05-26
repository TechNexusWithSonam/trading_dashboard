"""
RAIMA Markets v9 — Complete Fixed Backend
Key fixes from screenshots (all 0.00 showing):
1. /v3/market-quote/ohlc used for initial snapshot (not v2)
2. MCX key validation at startup — finds working month
3. WS subscription as TEXT frame (not bytes)
4. Option chain close_price field correct
5. Full OHLC (open,high,low,close) fetched for CE/PE via REST
6. Intraday candle API v3 for chart data
7. Proper broadcast — attaches display_name to every stock tick
"""
import asyncio, inspect, json, os, time, struct as _struct
from pathlib import Path
from typing import Set
from urllib.parse import urlencode
import httpx, websockets
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

FRONTEND_DIST   = Path(__file__).parent.parent / "frontend" / "dist"
FRONTEND_STATIC = Path(__file__).parent.parent / "frontend" / "static"
FRONTEND_STATIC.mkdir(parents=True, exist_ok=True)

API_KEY      = os.getenv("UPSTOX_API_KEY", "")
API_SECRET   = os.getenv("UPSTOX_API_SECRET", "")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI", "http://localhost:8000/auth/callback")
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
FEED_URL     = "wss://api.upstox.com/v3/feed/market-data-feed"
PASSWORD     = os.getenv("DASHBOARD_PASSWORD", "raima2024")

from .instruments import (
    get_spot_keys, mcx_key, mcx_key_for_month, get_current_and_next_expiry, get_itm2_strikes,
    fetch_expiry_list, fetch_option_chain, fetch_quotes_rest, fetch_index_quotes,
    fetch_option_ohlc_rest, fetch_intraday_candles,
    validate_mcx_keys, calculate_expiries_fallback,
    normalize_mcx_response_key, normalize_response_key,
    refresh_nse_eq_keys, STRIKE_STEPS, MONTHLY_SYMBOLS
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
INDEX_KEYS = [
    "NSE_INDEX|Nifty 50","NSE_INDEX|Nifty Bank","NSE_INDEX|Nifty Fin Service",
    "NSE_INDEX|NIFTY MID SELECT","NSE_INDEX|Nifty Next 50",
    "BSE_INDEX|SENSEX","BSE_INDEX|BANKEX",
]

# Will be updated at startup after validate_mcx_keys()
COMMODITY_KEYS = [mcx_key(s,0) for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]]
SPOT_KEYS_D: dict = {}   # filled at startup

# Feed key → LOC symbol (for routing to LOC engine)
FEED_KEY_TO_SYM: dict = {}
option_key_map:      dict = {}   # LOC option key → (symbol, "CE"/"PE")
calc_option_key_map: dict = {}   # Calculator option key → (symbol, "CE"/"PE")


# ══════════════════════════════════════════════════════════════════
#  PROTO DECODER
# ══════════════════════════════════════════════════════════════════
def _rv(b, p):
    r = 0; s = 0
    while p < len(b):
        x = b[p]; p += 1; r |= (x & 127) << s
        if not (x & 128): break
        s += 7
    return r, p

def _pe(d):
    o = {}; i = 0
    dm = {1:"atp",2:"cp",6:"uc",7:"lc",8:"high52",9:"low52",10:"ltp",11:"high",12:"low"}
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 1 and i+8 <= len(d):
            v = _struct.unpack_from('<d',d,i)[0]; i += 8
            if fn in dm: o[dm[fn]] = round(v,2)
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 2: ln,i = _rv(d,i); i += ln
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return o

def _pl(d):
    o = {}; i = 0
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 1 and i+8 <= len(d):
            v = _struct.unpack_from('<d',d,i)[0]; i += 8
            if fn == 1: o["ltp"] = round(v,2)
            elif fn == 4: o["cp"] = round(v,2)
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 2: ln,i = _rv(d,i); i += ln
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return o

def _pmf(d):
    o = {}; i = 0
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 2:
            ln,i = _rv(d,i); ch = d[i:i+ln]; i += ln
            if fn == 1:
                lt = _pl(ch)
                if lt and lt.get("ltp") and "ltpc" not in o: o["ltpc"] = lt
            elif fn == 2:
                inn = _pmf(ch)
                for k,v in inn.items():
                    if k not in o: o[k] = v
                    elif k == "ltpc" and isinstance(v,dict):
                        [o["ltpc"].setdefault(fk,fv) for fk,fv in v.items()]
            elif fn == 4:
                ef = _pe(ch)
                if ef:
                    o["efeed"] = ef
                    if "ltpc" not in o: o["ltpc"] = {}
                    lv = ef.get("ltp"); cv = ef.get("cp")
                    if lv and lv != 0: o["ltpc"]["ltp"] = lv
                    if cv and cv != 0 and "cp" not in o["ltpc"]: o["ltpc"]["cp"] = cv
        elif wt == 1 and i+8 <= len(d): i += 8
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return o

def _pfd(d):
    o = {}; i = 0
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 2:
            ln,i = _rv(d,i); ch = d[i:i+ln]; i += ln
            if fn == 1: o["ltpc"] = _pl(ch)
            elif fn == 2: o.update(_pmf(ch))
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 1 and i+8 <= len(d): i += 8
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return o

def _pme(d):
    k = ""; v = {}; i = 0
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 2:
            ln,i = _rv(d,i); ch = d[i:i+ln]; i += ln
            if fn == 1: k = ch.decode("utf-8","replace")
            elif fn == 2: v = _pfd(ch)
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 1 and i+8 <= len(d): i += 8
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return k,v

def _pmi(d):
    seg = {}; i = 0
    ST = {0:"CLOSED",1:"NORMAL_OPEN",2:"NORMAL_OPEN",3:"PREOPEN",4:"CLOSED"}
    while i < len(d):
        t = d[i]; i += 1; fn = t>>3; wt = t&7
        if wt == 2:
            ln,i = _rv(d,i); ch = d[i:i+ln]; i += ln
            if fn == 1:
                si=0; sn=""; sv=0
                while si < len(ch):
                    st = ch[si]; si += 1; sf = st>>3; sw = st&7
                    if sw == 2:
                        sln,si = _rv(ch,si)
                        if sf == 1: sn = ch[si:si+sln].decode("utf-8","replace")
                        si += sln
                    elif sw == 0: sv,si = _rv(ch,si)
                    else: break
                if sn: seg[sn] = ST.get(sv,"NORMAL_OPEN")
        elif wt == 0: _, i = _rv(d,i)
        elif wt == 1 and i+8 <= len(d): i += 8
        elif wt == 5 and i+4 <= len(d): i += 4
        else: break
    return seg

try:
    from upstox_client.feeder.proto import MarketDataFeedV3_pb2 as _pb
    _HAS_PB = True
except ImportError:
    _HAS_PB = False

_TYPE_MAP = {0: "initial_feed", 1: "live_feed", 2: "market_info"}
_STATUS_MAP = {0:"PREOPEN",1:"PREOPEN",2:"NORMAL_OPEN",3:"CLOSED",4:"CLOSING",5:"CLOSED"}

def _ohlc_list_to_efeed(ohlc_list):
    """Extract day OHLC from MarketOHLC repeated field."""
    ef = {}
    for o in ohlc_list:
        if o.interval in ("1d", "I1"):
            ef["open"]  = round(o.open, 2) if o.open else 0
            ef["high"]  = round(o.high, 2) if o.high else 0
            ef["low"]   = round(o.low, 2) if o.low else 0
            ef["ltp"]   = round(o.close, 2) if o.close else 0
            ef["cp"]    = 0  # not in OHLC, comes from ltpc
            break
    return ef

def _feed_to_dict(feed):
    """Convert protobuf Feed message to dict compatible with frontend."""
    r = {}
    which = feed.WhichOneof("FeedUnion")
    if which == "ltpc":
        lt = feed.ltpc
        r["ltpc"] = {"ltp": round(lt.ltp, 2), "cp": round(lt.cp, 2)}
    elif which == "fullFeed":
        ff = feed.fullFeed
        ff_which = ff.WhichOneof("FullFeedUnion")
        if ff_which == "marketFF":
            mf = ff.marketFF
            r["ltpc"] = {"ltp": round(mf.ltpc.ltp, 2), "cp": round(mf.ltpc.cp, 2)}
            if mf.marketOHLC and mf.marketOHLC.ohlc:
                ef = _ohlc_list_to_efeed(mf.marketOHLC.ohlc)
                ef["ltp"] = round(mf.ltpc.ltp, 2)
                ef["cp"]  = round(mf.ltpc.cp, 2)
                ef["atp"] = round(mf.atp, 2) if mf.atp else 0
                r["efeed"] = ef
            if mf.optionGreeks and mf.optionGreeks.delta:
                r["greeks"] = {
                    "delta": round(mf.optionGreeks.delta, 4),
                    "theta": round(mf.optionGreeks.theta, 4),
                    "gamma": round(mf.optionGreeks.gamma, 6),
                    "vega": round(mf.optionGreeks.vega, 4),
                }
            if mf.iv: r.setdefault("efeed",{})["iv"] = round(mf.iv, 2)
            if mf.oi: r.setdefault("efeed",{})["oi"] = mf.oi
        elif ff_which == "indexFF":
            iff = ff.indexFF
            r["ltpc"] = {"ltp": round(iff.ltpc.ltp, 2), "cp": round(iff.ltpc.cp, 2)}
            if iff.marketOHLC and iff.marketOHLC.ohlc:
                ef = _ohlc_list_to_efeed(iff.marketOHLC.ohlc)
                ef["ltp"] = round(iff.ltpc.ltp, 2)
                ef["cp"]  = round(iff.ltpc.cp, 2)
                r["efeed"] = ef
    elif which == "firstLevelWithGreeks":
        fl = feed.firstLevelWithGreeks
        r["ltpc"] = {"ltp": round(fl.ltpc.ltp, 2), "cp": round(fl.ltpc.cp, 2)}
    return r

def decode_v3(raw):
    try: return json.loads(raw.decode("utf-8"))
    except: pass
    if _HAS_PB:
        try:
            resp = _pb.FeedResponse()
            resp.ParseFromString(raw)
            r = {"type": _TYPE_MAP.get(resp.type, "live_feed"),
                 "feeds": {}, "currentTs": str(resp.currentTs or int(time.time()*1000))}
            for key, feed in resp.feeds.items():
                fd = _feed_to_dict(feed)
                if fd: r["feeds"][key] = fd
            if resp.marketInfo and resp.marketInfo.segmentStatus:
                seg = {}
                for name, status in resp.marketInfo.segmentStatus.items():
                    seg[name] = _STATUS_MAP.get(status, "NORMAL_OPEN")
                r["marketInfo"] = {"segmentStatus": seg}
                r["type"] = "market_info"
            return r if (r["feeds"] or r.get("marketInfo")) else None
        except Exception as e:
            print(f"[Decode-pb] {e}")
    # Fallback to custom decoder
    try:
        r = {"type":"unknown","feeds":{},"currentTs":str(int(time.time()*1000))}
        i = 0; mt = 0
        while i < len(raw):
            t = raw[i]; i += 1; fn = t>>3; wt = t&7
            if wt == 0:
                v,i = _rv(raw,i)
                if fn == 1: mt = v
                elif fn == 3: r["currentTs"] = str(v)
            elif wt == 2:
                ln,i = _rv(raw,i); ch = raw[i:i+ln]; i += ln
                if fn == 2:
                    k,v = _pme(ch)
                    if k and v: r["feeds"][k] = v
                elif fn == 4: r["marketInfo"] = {"segmentStatus": _pmi(ch)}
            elif wt == 1 and i+8 <= len(raw): i += 8
            elif wt == 5 and i+4 <= len(raw): i += 4
            else: break
        if mt == 2 or r.get("marketInfo"): r["type"] = "market_info"
        elif mt == 1 or r["feeds"]: r["type"] = "live_feed"
        return r if (r["feeds"] or r.get("marketInfo")) else None
    except: return None

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
    upstox_ws  = None
    feed_task  = None
    chain_task = None
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
    # LOC results are sent via throttled live_feed flush (loc_results field)
    # No need to broadcast individually — avoids per-tick flickering

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
            "zone","change","direction",
            "ce_strike","pe_strike","ce_ltp","pe_ltp","ce_iv","pe_iv"]
    hist.insert(0, {"ts":int(time.time()*1000), **{k:loc[k] for k in keep if k in loc}})
    if len(hist) > 60: hist.pop()

def _route_tick(key, ltp, cp, o, h, l, ts):
    if not ltp: return
    sym = FEED_KEY_TO_SYM.get(key)
    if sym and sym in LOC_SYMBOLS_SET:
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
        [mcx_key(s,1) for s in ["CRUDEOIL","NATURALGAS"]]
    ))

    # Step 3: Refresh NSE_EQ keys from instrument master (fixes stale ISINs)
    refresh_nse_eq_keys()

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
        idx_data = await fetch_index_quotes(INDEX_KEYS, state.access_token)
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

    # Step 8: Re-subscribe Upstox feed to validated commodity keys
    if state.upstox_ws and COMMODITY_KEYS:
        try:
            await _sub_binary(state.upstox_ws, COMMODITY_KEYS, "full")
            print(f"[Init] Re-subscribed MCX keys: {COMMODITY_KEYS}")
        except Exception as e:
            print(f"[Init] MCX re-sub error: {e}")

    # Step 7: Fetch CE/PE OHLC from REST (since chain may have 0s)
    if state.access_token:
        await _refresh_all_option_ohlc()


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
    if not state.upstox_ws: return
    loc_keys  = [k for k in loc_engine.get_option_keys() if k]
    calc_keys = [k for k in loc_engine.get_calc_option_keys() if k]
    all_keys  = list(dict.fromkeys(loc_keys + calc_keys))
    new_keys  = [k for k in all_keys if k not in state.subscribed_option_keys]
    if new_keys:
        await _sub_binary(state.upstox_ws, new_keys, "full")
        state.subscribed_option_keys.update(new_keys)
        print(f"[Options] Subscribed {len(new_keys)} option keys: {new_keys[:2]}")
    # Always rebuild BOTH maps from current engine state — even when no new
    # WS subscription is needed. An ATM shift back to a previously-seen
    # strike leaves the strike in state.subscribed_option_keys (so new_keys
    # is empty) but changes the instrument_key, so without this rebuild
    # ticks for the current strike would silently miss and LTP would freeze.
    option_key_map.clear()
    for st in loc_engine.symbols.values():
        if st.ce.instrument_key: option_key_map[st.ce.instrument_key] = (st.symbol,"CE")
        if st.pe.instrument_key: option_key_map[st.pe.instrument_key] = (st.symbol,"PE")
    calc_option_key_map.clear()
    for sn, calc in loc_engine.calc_states.items():
        if calc.ce.instrument_key: calc_option_key_map[calc.ce.instrument_key] = (sn,"CE")
        if calc.pe.instrument_key: calc_option_key_map[calc.pe.instrument_key] = (sn,"PE")

async def _refresh_prev_close_cache():
    """Re-derive state.prev_close from REST so MCX fallback doesn't go stale
    across trading days. net_change is authoritative (ltp - net_change = prev close)."""
    if not state.access_token: return
    try:
        stock_comm_keys = list(dict.fromkeys(_ik.FO_STOCK_KEYS + COMMODITY_KEYS[:5]))
        data = await fetch_quotes_rest(stock_comm_keys, state.access_token)
        idx_data = await fetch_index_quotes(INDEX_KEYS, state.access_token)
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
    rolled = []
    for sym in _MCX_LOC:
        info = state.expiry_cache.get(sym) or {}
        defx = info.get("default") or ""
        if len(defx) < 7: continue
        try:
            yr = int(defx[:4]); mo = int(defx[5:7])
        except Exception:
            continue
        if (yr, mo) == (today_d.year, today_d.month):
            continue  # default option is in the current calendar month
        # Use API-confirmed option underlying when available — it correctly maps
        # options to their actual parent futures (e.g. SILVER Jun options live on
        # Jul futures, so we must NOT roll spot to a non-existent Jun futures key).
        # Fall back to month-based resolution only when underlying is unknown.
        opt_underlying = _instruments_mod._mcx_option_underlying.get(sym) or ""
        new_spot = opt_underlying if opt_underlying else mcx_key_for_month(sym, yr, mo)
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
    # Only pre-subscribe next-month futures for CrudeOil and NaturalGas —
    # these expire every month so we need the upcoming contract subscribed
    # a few days early. GOLD/SILVER/COPPER options live on the same futures
    # as their current spot (confirmed via _mcx_option_underlying) so their
    # next-month futures key is not needed in the WS feed and only creates
    # "unknown key" noise in market data.
    _next_mo_syms = {"CRUDEOIL", "NATURALGAS"}
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
    if _last_rollover_check_date == today_d and not has_expired_default:
        return
    _last_rollover_check_date = today_d
    if not state.access_token: return

    # ── Phase A: indices + MCX (priority, sequential) ──
    priority = _INDEX_LOC + _MCX_LOC
    priority_rolled = await _process_expiry_rollovers(priority, parallel=False)

    mcx_spot_rolled = _align_mcx_spot_to_options()
    await _prime_mcx_spot_from_rest()

    # Chain refresh set: union of expiry-rolled and MCX-spot-rolled
    chain_refresh = {sym: new_def for sym, _o, new_def in priority_rolled}
    for sym, _ok, _nk, defx in mcx_spot_rolled:
        chain_refresh.setdefault(sym, defx)

    await _refresh_chains_for_rolled(chain_refresh, parallel=False)

    if state.upstox_ws:
        if mcx_spot_rolled and COMMODITY_KEYS:
            try:
                await _sub_binary(state.upstox_ws, COMMODITY_KEYS, "full")
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
        if state.upstox_ws:
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


async def periodic_refresh():
    tick = 0
    while True:
        await asyncio.sleep(60)
        if not state.access_token: continue
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


# ══════════════════════════════════════════════════════════════════
#  WS SUBSCRIPTION — TEXT FRAME (not bytes!)
# ══════════════════════════════════════════════════════════════════
async def _sub_binary(ws, keys: list, mode: str = "full"):
    """Send subscription as BINARY frame — Upstox v3 requires binary opcode."""
    msg = json.dumps({
        "guid": f"raima_{int(time.time()*1000)}",
        "method": "sub",
        "data": {"mode": mode, "instrumentKeys": keys},
    }).encode("utf-8")
    print(f"[Feed] Subscribing {len(keys)} keys, mode={mode}")
    await ws.send(msg)   # BINARY frame (bytes)

def _ws_connect(url, headers):
    import ssl as _ssl
    sig = inspect.signature(websockets.connect); p = sig.parameters
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    kw  = dict(ping_interval=None, ping_timeout=None, max_size=2**24, ssl=ctx)
    if headers:
        if "extra_headers" in p: kw["extra_headers"] = headers
        else:                    kw["additional_headers"] = headers
    return websockets.connect(url, **kw)


# ══════════════════════════════════════════════════════════════════
#  BROWSER WEBSOCKET
# ══════════════════════════════════════════════════════════════════
@app.websocket("/ws/feed")
async def ws_browser(ws: WebSocket):
    await ws.accept(); state.connected_clients.add(ws)
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
        while True:
            await asyncio.sleep(15)
            await ws.send_text(json.dumps({"type":"ping","ts":int(time.time()*1000)}))
    except (WebSocketDisconnect, Exception): pass
    finally: state.connected_clients.discard(ws)

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
    """Send a message to all connected browser WebSocket clients."""
    if not state.connected_clients: return
    text = json.dumps(msg); dead = set()
    for ws in list(state.connected_clients):
        try: await ws.send_text(text)
        except: dead.add(ws)
    state.connected_clients -= dead


FEED_THROTTLE_MS = 300  # send at most ~3 updates/sec to frontend

async def _flush_feed_buffer():
    """Background task: flush buffered feed ticks to frontend at throttled rate."""
    while True:
        await asyncio.sleep(FEED_THROTTLE_MS / 1000)
        if not state._feed_buffer or not state.connected_clients:
            continue
        # Swap buffer atomically
        feeds = state._feed_buffer
        state._feed_buffer = {}
        msg = {
            "type": "live_feed",
            "feeds": feeds,
            "currentTs": str(int(time.time() * 1000)),
            "loc_results": loc_engine.get_all_results(),
            "calc_results": loc_engine.get_all_calc_results(),
        }
        await _send_to_clients(msg)


# ══════════════════════════════════════════════════════════════════
#  UPSTOX LIVE FEED
# ══════════════════════════════════════════════════════════════════
async def _get_authorized_ws_url(token: str) -> str:
    """Get authorized WebSocket URL from Upstox v3 authorize endpoint."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get("https://api.upstox.com/v3/feed/market-data-feed/authorize",
                        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
        if r.status_code == 200:
            data = (r.json() or {}).get("data", {})
            url = data.get("authorizedRedirectUri") or data.get("authorized_redirect_uri")
            if url:
                print(f"[Feed] Authorized WS URL obtained")
                return url
        print(f"[Feed] Authorize failed: {r.status_code} {r.text[:200]}")
    return FEED_URL  # fallback

async def start_feed():
    if USE_MOCK:
        from backend.mock_feed import start_mock_feed
        await start_mock_feed(broadcast); return

    headers = {"Authorization": f"Bearer {state.access_token}"}
    while True:
        try:
            async with _ws_connect(FEED_URL, headers) as ws:
                state.upstox_ws = ws
                print("[Feed] Connected to Upstox V3 WebSocket")

                # 1. Indices — full mode
                await _sub_binary(ws, INDEX_KEYS, "full")
                await asyncio.sleep(0.5)

                # 2. Commodities — full mode (both current & next month)
                await _sub_binary(ws, COMMODITY_KEYS, "full")
                await asyncio.sleep(0.5)

                # 3. F&O stocks (ISIN keys) — full mode for OHLC
                stock_keys = list(dict.fromkeys(_ik.FO_STOCK_KEYS))
                for i in range(0, len(stock_keys), 100):
                    await _sub_binary(ws, stock_keys[i:i+100], "full")
                    await asyncio.sleep(0.3)

                # 4. Option CE/PE keys from chain
                opt_keys = loc_engine.get_option_keys()
                if opt_keys:
                    await _sub_binary(ws, opt_keys, "full")
                    state.subscribed_option_keys.update(opt_keys)
                    for st_sym in loc_engine.symbols.values():
                        if st_sym.ce.instrument_key:
                            option_key_map[st_sym.ce.instrument_key] = (st_sym.symbol,"CE")
                        if st_sym.pe.instrument_key:
                            option_key_map[st_sym.pe.instrument_key] = (st_sym.symbol,"PE")

                print(f"[Feed] Subscribed: {len(INDEX_KEYS)} idx | "
                      f"{len(COMMODITY_KEYS)} mcx | {len(stock_keys)} stocks | "
                      f"{len(opt_keys)} options")

                tick_n = 0
                print(f"[Feed] Waiting for data...")
                async for raw in ws:
                    state.frame_count += 1
                    rtype = "binary" if isinstance(raw, bytes) else "text"
                    rlen = len(raw) if raw else 0
                    try:
                        msg = (decode_v3(raw) if isinstance(raw,bytes)
                               else (json.loads(raw) if raw else None))
                        if msg and (msg.get("feeds") or msg.get("marketInfo")):
                            state.decode_ok += 1
                            nf = len(msg.get("feeds",{}))
                            mt = msg.get("type","?")
                            if state.frame_count <= 10 or state.frame_count % 100 == 0:
                                print(f"[Feed] Frame #{state.frame_count}: {rtype} {rlen}B → type={mt} feeds={nf}")
                            await broadcast(msg)
                            tick_n += 1
                            if tick_n % 300 == 0:
                                await _subscribe_new_option_keys()
                        else:
                            print(f"[Feed] Frame #{state.frame_count}: {rtype} {rlen}B → decode returned None")
                    except Exception as ex:
                        print(f"[Decode] Frame #{state.frame_count}: {rtype} {rlen}B → error: {ex}")
                print("[Feed] WebSocket loop ended (server closed connection)")

        except asyncio.CancelledError: break
        except Exception as e:
            import traceback
            print(f"[Feed] Error: {e}")
            traceback.print_exc()
        finally: state.upstox_ws = None
        print("[Feed] Reconnecting in 5s...")
        await asyncio.sleep(5)


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
async def upstox_login():
    if not API_KEY: raise HTTPException(400,"API_KEY not set")
    params = {"client_id":API_KEY,"redirect_uri":REDIRECT_URI,"response_type":"code"}
    return RedirectResponse(f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}")

@app.get("/auth/callback")
async def auth_cb(code: str):
    async with httpx.AsyncClient() as c:
        r = await c.post("https://api.upstox.com/v2/login/authorization/token",
            data={"code":code,"client_id":API_KEY,"client_secret":API_SECRET,
                  "redirect_uri":REDIRECT_URI,"grant_type":"authorization_code"},
            headers={"Accept":"application/json"})
    d = r.json()
    if "access_token" not in d: raise HTTPException(400,str(d))
    state.access_token = d["access_token"]
    loc_engine.access_token = d["access_token"]
    asyncio.create_task(_restart()); return RedirectResponse("/?auth=success")

@app.post("/auth/token")
async def set_token(payload: dict):
    t = payload.get("access_token","")
    if not t: raise HTTPException(400,"access_token required")
    state.access_token = t; loc_engine.access_token = t
    asyncio.create_task(_restart())
    return {"status":"ok","message":"Feed restarting..."}

async def _restart():
    print("[Restart] Restarting...")
    if state.feed_task and not state.feed_task.done():
        state.feed_task.cancel(); await asyncio.sleep(1)
    state.feed_task = asyncio.create_task(start_feed())
    await asyncio.sleep(3)
    await startup_init()
    if state.chain_task and not state.chain_task.done(): state.chain_task.cancel()
    state.chain_task = asyncio.create_task(periodic_refresh())
    print("[Restart] Done")


# ══════════════════════════════════════════════════════════════════
#  DATA API ROUTES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/feed-log")
async def feed_log():
    return {"log": state.feed_log[-30:]}

@app.get("/api/status")
async def api_status():
    return {
        "auth": bool(state.access_token) or USE_MOCK,
        "feed_connected": state.upstox_ws is not None,
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
        raise HTTPException(503, "no Upstox access token")

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
        upstox_ws=state.upstox_ws,
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
    """Fetch intraday candles from Upstox API v3."""
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
    from .instruments import fetch_intraday_candles
    import httpx
    from datetime import date

    if not to_date:
        to_date = date.today().isoformat()
    if not from_date:
        # default: 5 days back
        from datetime import timedelta
        from_date = (date.today()-timedelta(days=5)).isoformat()

    # For today's intraday, use intraday endpoint
    today = date.today().isoformat()
    if from_date == today and to_date == today and unit != "days":
        candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
        return {"key":key,"candles":candles}

    # Historical endpoint
    try:
        encoded = key.replace("|", "%7C")
        url = f"https://api.upstox.com/v3/historical-candle/{encoded}/{unit}/{interval}/{to_date}/{from_date}"
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, headers={"Authorization":f"Bearer {state.access_token}","Accept":"application/json"})
            if r.status_code == 200:
                raw_candles = (r.json() or {}).get("data",{})
                if raw_candles is None: return {"key":key,"candles":[]}
                candles_raw = (raw_candles or {}).get("candles",[]) or []
                from datetime import datetime
                result = []
                for candle in candles_raw:
                    if len(candle) < 5: continue
                    try:
                        ts = int(datetime.fromisoformat(str(candle[0])).timestamp()*1000)
                    except:
                        ts = 0
                    if ts:
                        result.append({
                            "t":ts,"o":float(candle[1] or 0),"h":float(candle[2] or 0),
                            "l":float(candle[3] or 0),"c":float(candle[4] or 0),
                            "v":int(candle[5]) if len(candle)>5 else 0,
                        })
                # Also merge with intraday for today
                if to_date == today:
                    today_candles = await fetch_intraday_candles(key, state.access_token, unit, interval)
                    existing_times = {c["t"] for c in result}
                    result += [c for c in today_candles if c["t"] not in existing_times]
                result.sort(key=lambda c: c["t"])
                return {"key":key,"candles":result}
            else:
                print(f"[Hist] {key} HTTP {r.status_code}: {r.text[:100]}")
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
    if state.upstox_ws and keys: await _sub_binary(state.upstox_ws, keys, mode)
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
    f = FRONTEND_DIST/path
    if f.exists() and f.is_file(): return FileResponse(str(f))
    idx = FRONTEND_DIST/"index.html"
    if idx.exists(): return FileResponse(str(idx))
    return HTMLResponse("Not found",404)


# ══════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def on_startup():
    print(f"RAIMA Markets v9 | {'MOCK' if USE_MOCK else 'LIVE'} | ws={websockets.__version__}")
    # Build initial key maps even without token
    global SPOT_KEYS_D, FEED_KEY_TO_SYM
    SPOT_KEYS_D = get_spot_keys()
    FEED_KEY_TO_SYM = {v:k for k,v in SPOT_KEYS_D.items()}
    # Only map current month (m=0) MCX keys — prevents next-month price mixing
    for s in ["CRUDEOIL","NATURALGAS","GOLD","SILVER","COPPER"]:
        FEED_KEY_TO_SYM[mcx_key(s,0)] = s

    if USE_MOCK:
        from backend.mock_feed import start_mock_feed
        state.feed_task = asyncio.create_task(start_mock_feed(broadcast))
    elif state.access_token:
        loc_engine.access_token = state.access_token
        state.feed_task = asyncio.create_task(start_feed())
    else:
        print("[!] No token — POST /auth/token or set UPSTOX_ACCESS_TOKEN in .env")

    # Start throttled feed flush task
    state._flush_task = asyncio.create_task(_flush_feed_buffer())

    asyncio.create_task(_delayed_startup())

async def _delayed_startup():
    await asyncio.sleep(3)
    await startup_init()
    state.chain_task = asyncio.create_task(periodic_refresh())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
