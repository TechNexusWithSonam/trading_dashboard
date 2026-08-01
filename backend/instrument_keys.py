"""
instrument_keys.py — the F&O stock symbol universe for the LOC engine.

Values used to be hardcoded Upstox ISIN keys (source: Upstox instruments BOD
JSON). Since the migration to Zerodha, tokens are no longer hardcoded here —
they're resolved at startup against the live Zerodha NSE instrument dump by
backend/instruments.py's refresh_nse_eq_keys() (matching each symbol's
tradingsymbol), which fills in NSE_EQ_KEYS' values, ISIN_TO_SYMBOL, and
FO_STOCK_KEYS below. This also fixes a couple of stale/duplicate ISINs that
had crept into the old hardcoded dict (e.g. CDSL and BSE previously shared
one ISIN, as did JIOFIN/ETERNAL) — resolving live per-symbol makes that
class of drift impossible.

The SYMBOL NAMES (dict keys) are the actual product surface — this is the
list of ~150 F&O stocks the LOC table covers — and are unchanged by the
broker migration.
"""

_NSE_EQ_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "BHARTIARTL",
    "SBIN", "HINDUNILVR", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "ASIANPAINT", "DMART", "BAJFINANCE", "WIPRO", "ULTRACEMCO", "TITAN",
    "TECHM", "POWERGRID", "NTPC", "TATAPOWER", "JSWSTEEL", "VOLTAS",
    "DALBHARAT", "PFC", "DRREDDY", "SUNPHARMA", "ONGC", "TATACONSUM",
    "ADANIENT", "ADANIPORTS", "ADANIGREEN", "BAJAJ-AUTO", "BAJAJFINSV",
    "BANKBARODA", "BEL", "BHARATFORG", "BHEL", "BPCL", "BRITANNIA",
    "CANBK", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE",
    "DLF", "DIVISLAB", "EICHERMOT", "FEDERALBNK", "GAIL", "GRASIM",
    "HAL", "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDPETRO", "HINDZINC", "ICICIPRULI", "IDFCFIRSTB",
    "INDHOTEL", "INDIGO", "INDUSINDBK", "INDUSTOWER", "JIOFIN",
    "JSWENERGY", "LUPIN", "M&M", "MARICO", "MARUTI", "MOTHERSON",
    "MPHASIS", "MUTHOOTFIN", "NAUKRI", "NESTLEIND", "NHPC", "NMDC",
    "NYKAA", "OFSS", "PAGEIND", "PAYTM", "PERSISTENT", "PETRONET",
    "PIDILITIND", "PNB", "POLYCAB", "RECLTD", "RVNL", "SBICARD",
    "SBILIFE", "SHRIRAMFIN", "SIEMENS", "SUZLON", "TATASTEEL",
    "TATAELXSI", "TATATECH", "TORNTPHARM", "TORNTPOWER", "TRENT",
    "TVSMOTOR", "UPL", "VEDL", "YESBANK", "ZYDUSLIFE", "ASHOKLEY",
    "AUBANK", "AUROPHARMA", "BIOCON", "BSE", "CAMS", "CROMPTON",
    "DABUR", "DELHIVERY", "DIXON", "EXIDEIND", "FORTIS", "GLENMARK",
    "GODREJCP", "GODREJPROP", "HUDCO", "IDEA", "IEX", "INDIANB",
    "INOXWIND", "IOC", "IRFC", "JINDALSTEL", "KALYANKJIL", "KAYNES",
    "KEI", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA",
    "LTF", "MANAPPURAM", "MANKIND", "MAXHEALTH", "MCX", "NATIONALUM",
    "NBCC", "NUVAMA", "OIL", "PATANJALI", "PGEL", "PIIND", "PNBHOUSING",
    "POLICYBZR", "SAIL", "SHREECEM", "SOLARINDS", "SONACOMS", "SRF",
    "SUPREMEIND", "SYNGENE", "UNIONBANK", "UNOMINDA", "VBL", "ABB",
    "ABCAPITAL", "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APOLLOHOSP",
    "ASTRAL", "BANDHANBNK", "BDL", "BLUESTARCO", "BOSCHLTD", "COLPAL",
    "CUMMINSIND", "GMRAIRPORT", "MFSL", "TIINDIA", "OBEROIRLTY",
    "PHOENIXLTD", "PRESTIGE", "RBLBANK", "JUBLFOOD", "CDSL",
    "ADANIENSOL", "SWIGGY", "360ONE", "ETERNAL",
]

# sym -> internal key ("NSE_EQ|<zerodha_instrument_token>"), filled in by
# refresh_nse_eq_keys() at startup. Empty string until resolved.
NSE_EQ_KEYS = {sym: "" for sym in _NSE_EQ_SYMBOLS}

# Reverse: internal key -> display symbol name (rebuilt by refresh_nse_eq_keys())
ISIN_TO_SYMBOL = {}

# Deduplicated list of all resolved FO stock instrument keys (rebuilt by
# refresh_nse_eq_keys())
FO_STOCK_KEYS = []
