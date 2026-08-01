"""Shared Zerodha KiteTicker wrapper — one WebSocket connection to Zerodha
used by BOTH the main LOC engine (backend/main.py) and History 2. There is
exactly one Ticker2 instance (`ticker2` below); `start()` is idempotent and
safe to call from multiple owners — the first caller opens the connection,
later callers just register their own tick/status listener onto it.

KiteTicker runs its own background thread (Twisted reactor) when started with
connect(threaded=True) — its callbacks fire on that thread, not on the
FastAPI/uvicorn asyncio loop. _hop() bridges a callback back onto the main
loop via run_coroutine_threadsafe so listeners can safely await WebSocket
sends to connected frontend clients.

Different owners subscribe different token sets in different modes (History 2
uses MODE_LTP; the LOC engine needs MODE_FULL for OHLC/OI) on this one shared
connection — `token_modes` tracks each token's mode individually so a
reconnect resubscribes every token in the mode its owner actually asked for,
instead of clobbering everything with whichever mode was requested last.
"""
import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

from kiteconnect import KiteTicker

from .logger import log_h2, log_h2_error
from .state import state2

API_KEY = os.getenv("ZERODHA_API_KEY", "")


def _kite_mode(name: str):
    return {"ltp": KiteTicker.MODE_LTP,
            "quote": KiteTicker.MODE_QUOTE,
            "full": KiteTicker.MODE_FULL}.get(name, KiteTicker.MODE_LTP)

# No India timezone data assumed available on the host — IST is a fixed
# UTC+5:30 offset (no DST) so this needs no zoneinfo/pytz dependency.
IST = timezone(timedelta(hours=5, minutes=30))
_MARKET_OPEN = (9, 0)     # generous bound around NSE's real 9:15-15:30 window
_MARKET_CLOSE = (15, 35)

# If a subscribed ticker goes this long without a single tick during market
# hours, something's wrong with the connection regardless of what
# is_connected() claims — force a reconnect rather than trust it.
STALE_TICK_THRESHOLD_S = 150
WATCHDOG_INTERVAL_S = 60


def _within_market_hours(now: datetime | None = None) -> bool:
    now = now or datetime.now(IST)
    start = now.replace(hour=_MARKET_OPEN[0], minute=_MARKET_OPEN[1], second=0, microsecond=0)
    end = now.replace(hour=_MARKET_CLOSE[0], minute=_MARKET_CLOSE[1], second=0, microsecond=0)
    return start <= now <= end


class Ticker2:
    def __init__(self):
        self.kws: KiteTicker | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.subscribed_tokens: set[int] = set()
        # token -> "ltp"/"quote"/"full", so a reconnect resubscribes each
        # token in the mode its own owner asked for (History 2 vs the LOC
        # engine subscribe different token sets in different modes on this
        # one shared connection).
        self.token_modes: dict[int, str] = {}
        self.tick_listeners: list = []
        self.status_listeners: list = []
        # token -> most recent raw tick dict (ohlc/oi/depth included for
        # MODE_FULL tokens). Listeners that only need (token, price, ts) —
        # History 2's _on_tick — ignore this; the LOC engine's feed adapter
        # reads it to get OHLC without needing a wider callback signature.
        self.last_full_tick: dict[int, dict] = {}
        self.last_tick_at: float = 0
        self._watchdog_task: asyncio.Task | None = None

    def start(self, on_tick_async, on_status_async):
        """Register a tick/status listener and ensure the shared connection
        is running. Safe to call more than once from different owners — the
        first call opens the socket; later calls just add their listener."""
        if on_tick_async not in self.tick_listeners:
            self.tick_listeners.append(on_tick_async)
        if on_status_async not in self.status_listeners:
            self.status_listeners.append(on_status_async)
        if self.kws is not None:
            return  # connection already running — listener is registered on it
        if not state2.access_token:
            raise RuntimeError("Not authenticated with Zerodha")
        self.loop = asyncio.get_running_loop()
        self.last_tick_at = time.time()
        self._connect()
        if not self._watchdog_task:
            self._watchdog_task = self.loop.create_task(self._watchdog_loop())

    def _connect(self):
        self.kws = KiteTicker(API_KEY, state2.access_token, reconnect=True,
                               reconnect_max_tries=300, reconnect_max_delay=30)
        self.kws.on_ticks = self._on_ticks
        self.kws.on_connect = self._on_connect
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_reconnect = self._on_reconnect
        self.kws.on_noreconnect = self._on_noreconnect
        log_h2("Zerodha WebSocket connecting")
        self.kws.connect(threaded=True)

    def stop(self):
        if self._watchdog_task:
            self._watchdog_task.cancel()
            self._watchdog_task = None
        if self.kws:
            try:
                self.kws.close()
            except Exception as e:
                log_h2_error(f"Error closing Zerodha WebSocket: {e}")
            self.kws = None
        self.subscribed_tokens = set()

    def subscribe(self, tokens: list[int], mode: str = "ltp"):
        if not self.kws:
            raise RuntimeError("WebSocket not connected")
        kite_mode = _kite_mode(mode)
        new_tokens = [t for t in tokens if t not in self.subscribed_tokens]
        # Record intent BEFORE touching the wire — connect(threaded=True) returns
        # before the handshake finishes, so a subscribe() called right after
        # start() can race a connection that isn't open yet. If we send first
        # and only record on success, that race drops the tokens permanently
        # (on_connect's resubscribe only fires if subscribed_tokens is already
        # populated). Recording first means the pending tokens always get
        # picked up — either by the wire send below, or by on_connect once the
        # handshake actually completes.
        self.subscribed_tokens |= set(tokens)
        for t in tokens:
            self.token_modes[t] = mode
        if not self.kws.is_connected():
            log_h2(f"Queued tokens (Zerodha socket not yet open): {tokens} — on_connect will subscribe them")
            return
        try:
            if new_tokens:
                self.kws.subscribe(new_tokens)
            self.kws.set_mode(kite_mode, tokens)
            log_h2(f"Subscribed to tokens: {tokens} mode={mode} (new: {new_tokens})")
        except Exception as e:
            # is_connected() can lie — it only checks the socket's own state
            # flag, which can go stale if the connection zombied without a
            # clean close. A failure here means it really is dead; force a
            # fresh connection rather than leaving tokens subscribed-in-name
            # only on a socket that can't actually send.
            log_h2_error(f"subscribe()/set_mode() failed on a supposedly-open socket: {e} — forcing reconnect")
            self._force_reconnect()

    def unsubscribe(self, tokens: list[int]):
        if not self.kws:
            return
        self.subscribed_tokens -= set(tokens)
        for t in tokens:
            self.token_modes.pop(t, None)
            self.last_full_tick.pop(t, None)
        if not self.kws.is_connected():
            return
        try:
            self.kws.unsubscribe(tokens)
            log_h2(f"Unsubscribed from tokens: {tokens}")
        except Exception as e:
            log_h2_error(f"unsubscribe() failed: {e}")

    def _force_reconnect(self):
        log_h2_error(f"Forcing Zerodha ticker reconnect (last tick {int(time.time() - self.last_tick_at)}s ago)")
        try:
            if self.kws:
                self.kws.close()
        except Exception as e:
            log_h2_error(f"Error closing stale ticker before reconnect: {e}")
        self.kws = None
        self.last_tick_at = time.time()  # avoid the watchdog re-firing mid-reconnect
        self._connect()  # subscribed_tokens is untouched — on_connect resubscribes them all

    async def _watchdog_loop(self):
        # Runs on the main asyncio loop (not the ticker's Twisted thread).
        # is_connected() only reflects the socket's own state flag, which can
        # go stale if the connection dies without a clean close (observed in
        # production: hours of silence with no on_close/on_error logged at
        # all). This is the actual self-healing mechanism — subscribe()'s
        # exception path above only catches the (rarer) case where sending
        # itself throws.
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL_S)
            try:
                if not self.kws or not self.subscribed_tokens:
                    continue
                if not _within_market_hours():
                    continue
                idle_for = time.time() - self.last_tick_at
                if idle_for > STALE_TICK_THRESHOLD_S:
                    log_h2_error(f"No ticks for {int(idle_for)}s during market hours "
                                 f"on {len(self.subscribed_tokens)} tokens — reconnecting")
                    self._force_reconnect()
            except Exception as e:
                log_h2_error(f"Watchdog loop error (continuing): {e}")

    # ---- KiteTicker callbacks: fire on the ticker's background thread ----

    def _on_ticks(self, ws, ticks):
        try:
            self.last_tick_at = time.time()
            ts_ms = int(time.time() * 1000)
            for t in ticks:
                token = t.get("instrument_token")
                price = t.get("last_price")
                if token is None or price is None:
                    continue
                # Populate BEFORE hopping to listeners — by the time a hopped
                # coroutine actually runs on the main loop, this entry is
                # already there for it to read (ohlc/oi/depth for MODE_FULL
                # tokens; empty for MODE_LTP tokens).
                self.last_full_tick[token] = t
                for listener in list(self.tick_listeners):
                    self._hop(listener(token, price, ts_ms))
        except Exception as e:
            # An uncaught exception here could otherwise propagate into
            # autobahn/Twisted's callback dispatch and silently kill the
            # reactor thread — leaving a zombie connection with no error ever
            # logged. Catch, log, keep the thread alive.
            log_h2_error(f"on_ticks callback error (ticker thread stays alive): {e}")

    def _on_connect(self, ws, response):
        try:
            log_h2("Zerodha WebSocket connected")
            self.last_tick_at = time.time()
            if self.subscribed_tokens:
                tokens = list(self.subscribed_tokens)
                ws.subscribe(tokens)
                # Resubscribe each token in ITS OWN mode, not a single
                # connection-wide mode — History 2's LTP tokens and the LOC
                # engine's FULL tokens share this one connection.
                by_mode: dict[str, list[int]] = {}
                for tok in tokens:
                    by_mode.setdefault(self.token_modes.get(tok, "ltp"), []).append(tok)
                for mode_name, mode_tokens in by_mode.items():
                    ws.set_mode(_kite_mode(mode_name), mode_tokens)
                log_h2(f"Resubscribed to tokens after (re)connect: {tokens}")
            self._broadcast_status("connected")
        except Exception as e:
            log_h2_error(f"on_connect callback error (ticker thread stays alive): {e}")

    def _on_close(self, ws, code, reason):
        log_h2_error(f"Zerodha WebSocket closed (code={code}, reason={reason})")
        self._broadcast_status("disconnected")

    def _on_error(self, ws, code, reason):
        log_h2_error(f"Zerodha WebSocket error (code={code}, reason={reason})")

    def _on_reconnect(self, ws, attempts_count):
        log_h2(f"Zerodha WebSocket reconnecting (attempt {attempts_count})")
        self._broadcast_status("reconnecting")

    def _on_noreconnect(self, ws):
        log_h2_error("Zerodha WebSocket gave up reconnecting")
        self._broadcast_status("failed")

    def _broadcast_status(self, status: str):
        for listener in list(self.status_listeners):
            self._hop(listener(status))

    def _hop(self, coro):
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, self.loop)


ticker2 = Ticker2()
