"""In-memory state for History 2 — completely separate from the `state`
object in main.py. Nothing here touches LOC/Upstox state, loc_history, or
loc_engine."""


class State2:
    def __init__(self):
        self.access_token: str | None = None
        self.user_id: str | None = None
        self.login_time: str | None = None
        # exchange -> list[dict] instrument rows, plus per-exchange load timestamp
        self.instruments_cache: dict[str, list[dict]] = {}
        self.instruments_loaded_at: dict[str, float] = {}
        # instrument_token -> {"lastPrice":.., "timestamp":..} — latest tick per token
        self.live_ticks: dict[int, dict] = {}

        # symbol -> {role: token} — role is "spot"/"ce"/"pe" (manual frontend
        # subscribe) or "itm1_ce"/"itm1_pe"/"itm2_ce"/"itm2_pe" (autonomous
        # engine, see engine.py). Populated via a MERGE (setdefault+update),
        # never a wholesale replace, so the manual per-page subscribe and the
        # always-on background engine can both register roles for the same
        # symbol without clobbering each other.
        self.symbol_context: dict[str, dict] = {}
        # symbol -> {"itm1_ce_strike":, "itm1_pe_strike":, "itm2_ce_strike":,
        # "itm2_pe_strike":, "last_atm":, "expiry":} — set by engine.py,
        # merged into recorded history rows by history.py.
        self.symbol_meta: dict[str, dict] = {}
        # instrument tokens explicitly subscribed by a connected frontend
        # client via the WS "history2:subscribe" message — used to avoid
        # broadcasting history2:tick for the much larger autonomous-engine
        # watchlist (recording-only, nobody's watching those tokens live).
        self.broadcast_tokens: set[int] = set()
        # symbol -> {"spot_ltp":, "ce_ltp":, "pe_ltp":, "itm1_ce_ltp":, ...,
        # "ts":} — latest known values, keyed by role_ltp
        self.live_by_symbol: dict[str, dict] = {}
        # symbol -> newest-first list of history rows, max 200
        self.history: dict[str, list[dict]] = {}
        # symbol -> minute bucket (int(ts_ms // 60000)) of the last recorded row
        self.last_minute: dict[str, int] = {}


state2 = State2()
