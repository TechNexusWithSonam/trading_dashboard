"""Mock feed — simulates live Upstox data for local dev/testing"""
import asyncio, math, random, time
from datetime import date as _date

_M = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

def _cur_mcx(sym):
    """Current-month MCX name-based key — matches mcx_key(sym,0) when no instrument master."""
    d = _date.today()
    return f"MCX_FO|{sym.upper()}{str(d.year)[2:]}{_M[d.month-1]}FUT"

MOCK_INDEX = {
    "NSE_INDEX|Nifty 50":          {"base": 23200, "sym": "NIFTY"},
    "NSE_INDEX|Nifty Bank":        {"base": 48500, "sym": "BANKNIFTY"},
    "NSE_INDEX|Nifty Fin Service": {"base": 21800, "sym": "FINNIFTY"},
    "NSE_INDEX|NIFTY MID SELECT":  {"base": 11200, "sym": "MIDCPNIFTY"},
    "NSE_INDEX|Nifty Next 50":     {"base": 64000, "sym": "NXTFTY"},
    "BSE_INDEX|SENSEX":            {"base": 76800, "sym": "SENSEX"},
    "BSE_INDEX|BANKEX":            {"base": 58000, "sym": "BANKEX"},
}

MOCK_MCX_SYMS = {
    "CRUDEOIL":   7320,
    "NATURALGAS": 285,
    "GOLD":       93400,
    "SILVER":     97200,
}

MOCK_STOCK_SYMS = {
    "RELIANCE":   1380, "TCS":        3520, "HDFCBANK":   1685, "INFY":       1540,
    "ICICIBANK":  1310, "BHARTIARTL": 1720, "SBIN":        785, "ITC":         415,
    "KOTAKBANK":  1890, "LT":         3320, "AXISBANK":   1145, "BAJFINANCE":  6890,
    "WIPRO":       305, "TECHM":      1480, "NTPC":        335, "SUNPHARMA":  1780,
    "TATASTEEL":   168, "MARUTI":    11200, "TITAN":      3450, "ONGC":        267,
    "PFC":         398, "DRREDDY":   1245,  "ADANIENT":   2450, "ADANIPORTS": 1230,
    "POWERGRID":   302, "COALINDIA":   388, "DLF":         692, "TATAPOWER":   368,
    "JSWSTEEL":    920, "DALBHARAT":  1850,
}

MOCK_OPTIONS = {
    # Indices
    "NIFTY":      {"ce_step": 50,   "ce_premium": 580,  "pe_premium": 520},
    "BANKNIFTY":  {"ce_step": 100,  "ce_premium": 1240, "pe_premium": 1180},
    "FINNIFTY":   {"ce_step": 50,   "ce_premium": 420,  "pe_premium": 390},
    "MIDCPNIFTY": {"ce_step": 25,   "ce_premium": 380,  "pe_premium": 350},
    "SENSEX":     {"ce_step": 100,  "ce_premium": 1820, "pe_premium": 1750},
    "BANKEX":     {"ce_step": 100,  "ce_premium": 890,  "pe_premium": 840},
    # MCX
    "CRUDEOIL":   {"ce_step": 50,   "ce_premium": 145,  "pe_premium": 130},
    "NATURALGAS": {"ce_step": 10,   "ce_premium": 8.5,  "pe_premium": 7.8},
    "GOLD":       {"ce_step": 100,  "ce_premium": 520,  "pe_premium": 480},
    "SILVER":     {"ce_step": 1000, "ce_premium": 1200, "pe_premium": 1100},
    # Key F&O stocks
    "RELIANCE":   {"ce_step": 20,   "ce_premium": 35,   "pe_premium": 30},
    "HDFCBANK":   {"ce_step": 20,   "ce_premium": 28,   "pe_premium": 24},
    "INFY":       {"ce_step": 20,   "ce_premium": 22,   "pe_premium": 19},
    "TCS":        {"ce_step": 50,   "ce_premium": 55,   "pe_premium": 48},
    "ICICIBANK":  {"ce_step": 10,   "ce_premium": 18,   "pe_premium": 15},
    "SBIN":       {"ce_step": 10,   "ce_premium": 12,   "pe_premium": 10},
    "ITC":        {"ce_step": 5,    "ce_premium": 8,    "pe_premium": 7},
    "AXISBANK":   {"ce_step": 10,   "ce_premium": 15,   "pe_premium": 13},
    "BAJFINANCE": {"ce_step": 50,   "ce_premium": 85,   "pe_premium": 75},
    "LT":         {"ce_step": 50,   "ce_premium": 45,   "pe_premium": 40},
    "KOTAKBANK":  {"ce_step": 20,   "ce_premium": 30,   "pe_premium": 26},
    "MARUTI":     {"ce_step": 100,  "ce_premium": 120,  "pe_premium": 105},
    "TITAN":      {"ce_step": 50,   "ce_premium": 50,   "pe_premium": 44},
    "WIPRO":      {"ce_step": 5,    "ce_premium": 5,    "pe_premium": 4.5},
    "TECHM":      {"ce_step": 20,   "ce_premium": 18,   "pe_premium": 16},
    "NTPC":       {"ce_step": 5,    "ce_premium": 6,    "pe_premium": 5.5},
    "SUNPHARMA":  {"ce_step": 20,   "ce_premium": 22,   "pe_premium": 19},
    "ONGC":       {"ce_step": 5,    "ce_premium": 5,    "pe_premium": 4},
    "PFC":        {"ce_step": 5,    "ce_premium": 6,    "pe_premium": 5},
    "DRREDDY":    {"ce_step": 10,   "ce_premium": 14,   "pe_premium": 12},
    "ADANIENT":   {"ce_step": 50,   "ce_premium": 40,   "pe_premium": 35},
    "ADANIPORTS": {"ce_step": 20,   "ce_premium": 18,   "pe_premium": 16},
    "POWERGRID":  {"ce_step": 5,    "ce_premium": 5,    "pe_premium": 4},
    "COALINDIA":  {"ce_step": 5,    "ce_premium": 6,    "pe_premium": 5},
    "DLF":        {"ce_step": 10,   "ce_premium": 10,   "pe_premium": 9},
    "TATAPOWER":  {"ce_step": 5,    "ce_premium": 6,    "pe_premium": 5},
    "JSWSTEEL":   {"ce_step": 10,   "ce_premium": 10,   "pe_premium": 9},
    "DALBHARAT":  {"ce_step": 20,   "ce_premium": 20,   "pe_premium": 18},
    "TATASTEEL":  {"ce_step": 5,    "ce_premium": 5,    "pe_premium": 4.5},
}

# sym→spot_key for option update (built at start_mock_feed time)
_sym_to_spot_key: dict = {}


async def start_mock_feed(broadcast_fn):
    from backend.main import loc_engine
    from backend import instrument_keys as _ik

    # Build canonical spot key maps at runtime so MCX month and stock ISINs are correct.
    # MCX: use current-month name-based key — same as mcx_key(sym,0) without instrument master.
    # Stocks: use ISIN key from NSE_EQ_KEYS — matches FEED_KEY_TO_SYM after startup_init.
    MOCK_SYMBOLS: dict = {}
    for key, info in MOCK_INDEX.items():
        MOCK_SYMBOLS[key] = info
        _sym_to_spot_key[info["sym"]] = key

    for sym, base in MOCK_MCX_SYMS.items():
        key = _cur_mcx(sym)
        MOCK_SYMBOLS[key] = {"base": base, "sym": sym}
        _sym_to_spot_key[sym] = key

    for sym, base in MOCK_STOCK_SYMS.items():
        key = _ik.NSE_EQ_KEYS.get(sym, f"NSE_EQ|{sym}")
        MOCK_SYMBOLS[key] = {"base": base}
        _sym_to_spot_key[sym] = key

    prices = {k: v["base"] for k, v in MOCK_SYMBOLS.items()}
    closes = {k: v["base"] for k, v in MOCK_SYMBOLS.items()}

    print(f"[Mock] Starting mock feed: {len(MOCK_SYMBOLS)} spot keys, {len(MOCK_OPTIONS)} option symbols")

    t = 0
    while True:
        await asyncio.sleep(0.8)
        t += 1
        feeds = {}
        ts = int(time.time() * 1000)

        # Update spot prices — keys are canonical so _route_tick → loc_engine.update_spot fires correctly
        for key, info in MOCK_SYMBOLS.items():
            base = info["base"]
            drift = math.sin(t * 0.03 + base * 0.001) * 0.004
            noise = (random.random() - 0.5) * 0.002
            prices[key] = max(base * 0.85, prices[key] * (1 + drift + noise))
            ltp = round(prices[key], 2)
            cp = closes.get(key, ltp)
            feeds[key] = {
                "ltpc": {"ltp": ltp, "cp": cp},
                "efeed": {
                    "ltp": ltp, "cp": cp,
                    "high": round(ltp * 1.005, 2),
                    "low":  round(ltp * 0.995, 2),
                    "uc":   round(ltp * 1.02, 2),
                    "lc":   round(ltp * 0.98, 2),
                }
            }

        await broadcast_fn({"type": "live_feed", "feeds": feeds, "currentTs": str(ts)})

        # Inject CE/PE prices directly into the LOC engine every ~2.4 s.
        # broadcast() ignores loc_results in incoming messages — LOC engine is
        # the single source of truth, so drive it directly here.
        if t % 3 == 0:
            for sym, opts in MOCK_OPTIONS.items():
                spot_key = _sym_to_spot_key.get(sym)
                if not spot_key:
                    continue
                ltp = prices.get(spot_key, opts["ce_step"] * 460)
                step = opts["ce_step"]
                atm  = round(round(ltp / step) * step, 2)
                ce_s = atm - 2 * step
                pe_s = atm + 2 * step

                noise = (random.random() - 0.5) * 0.05
                ce_ltp = round(opts["ce_premium"] * (1 + noise + math.sin(t * 0.05) * 0.03), 2)
                pe_ltp = round(opts["pe_premium"] * (1 + noise - math.sin(t * 0.05) * 0.03), 2)
                ce_cl  = round(opts["ce_premium"] * 0.97, 2)
                pe_cl  = round(opts["pe_premium"] * 0.97, 2)
                ce_hi  = round(ce_ltp * 1.02, 2); ce_lo = round(ce_ltp * 0.95, 2)
                pe_hi  = round(pe_ltp * 1.02, 2); pe_lo = round(pe_ltp * 0.95, 2)

                loc_engine.update_option_from_feed(sym, "CE", ce_ltp, ce_cl, ce_hi, ce_lo)
                loc_engine.update_option_from_feed(sym, "PE", pe_ltp, pe_cl, pe_hi, pe_lo)

        # Market info every 30 s
        if t % 37 == 0:
            await broadcast_fn({"type": "market_info", "marketInfo": {
                "segmentStatus": {
                    "NSE_CM": "NORMAL_OPEN", "BSE_CM": "NORMAL_OPEN",
                    "MCX_FO": "NORMAL_OPEN", "NSE_FO": "NORMAL_OPEN",
                }
            }})
