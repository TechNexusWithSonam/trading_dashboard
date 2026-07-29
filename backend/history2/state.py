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

        # symbol -> {"spot":token,"ce":token,"pe":token} — registered when the
        # frontend subscribes with a symbol context, so ticks can be routed
        # back to a symbol for minute-bucketed history recording.
        self.symbol_context: dict[str, dict] = {}
        # symbol -> {"spot_ltp":, "ce_ltp":, "pe_ltp":, "ts":} — latest known values
        self.live_by_symbol: dict[str, dict] = {}
        # symbol -> newest-first list of {"ts","spot_ltp","ce_ltp","pe_ltp"}, max 200
        self.history: dict[str, list[dict]] = {}
        # symbol -> minute bucket (int(ts_ms // 60000)) of the last recorded row
        self.last_minute: dict[str, int] = {}


state2 = State2()
