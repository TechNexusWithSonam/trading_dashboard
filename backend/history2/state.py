"""In-memory session/instrument-cache state for the shared Zerodha client
(access_token, instrument dumps). Separate from the `state` object in
main.py (LOC/market-data state) — the LOC engine reads `state2.access_token`
via zerodha_client.get_kite() but does not otherwise touch it here.

Was also home to History 2's recording-specific state (symbol_context,
symbol_meta, broadcast_tokens, live_by_symbol, history, last_minute,
live_ticks) — removed along with that feature; nothing in the codebase
reads them anymore."""


class State2:
    def __init__(self):
        self.access_token: str | None = None
        self.user_id: str | None = None
        self.login_time: str | None = None
        # exchange -> list[dict] instrument rows, plus per-exchange load timestamp
        self.instruments_cache: dict[str, list[dict]] = {}
        self.instruments_loaded_at: dict[str, float] = {}


state2 = State2()
