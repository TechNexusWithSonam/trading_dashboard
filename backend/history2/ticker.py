"""Zerodha KiteTicker wrapper for History 2 live ticks.

KiteTicker runs its own background thread (Twisted reactor) when started with
connect(threaded=True) — its callbacks fire on that thread, not on the
FastAPI/uvicorn asyncio loop. _hop() bridges a callback back onto the main
loop via run_coroutine_threadsafe so it can safely await WebSocket sends to
connected History 2 frontend clients.

Isolated from the existing Upstox feed loop in main.py, which uses the
`websockets` library directly inside the asyncio loop — a different broker,
a different transport, kept deliberately separate.
"""
import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

from kiteconnect import KiteTicker

from .logger import log_h2, log_h2_error
from .state import state2

API_KEY = os.getenv("ZERODHA_HISTORY2_API_KEY", "")

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
        self.mode = None
        self.on_tick_async = None
        self.on_status_async = None
        self.last_tick_at: float = 0
        self._watchdog_task: asyncio.Task | None = None

    def start(self, on_tick_async, on_status_async):
        if self.kws is not None:
            return  # already running — never open a second connection
        if not state2.access_token:
            raise RuntimeError("Not authenticated with Zerodha")
        self.loop = asyncio.get_running_loop()
        self.on_tick_async = on_tick_async
        self.on_status_async = on_status_async
        self.mode = KiteTicker.MODE_LTP
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
        self.mode = KiteTicker.MODE_LTP if mode == "ltp" else KiteTicker.MODE_QUOTE
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
        if not self.kws.is_connected():
            log_h2(f"Queued tokens (Zerodha socket not yet open): {tokens} — on_connect will subscribe them")
            return
        try:
            if new_tokens:
                self.kws.subscribe(new_tokens)
            self.kws.set_mode(self.mode, tokens)
            log_h2(f"Subscribed to tokens: {tokens} (new: {new_tokens})")
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
            for t in ticks:
                token = t.get("instrument_token")
                price = t.get("last_price")
                if token is None or price is None:
                    continue
                self._hop(self.on_tick_async(token, price, int(time.time() * 1000)))
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
                ws.set_mode(self.mode, tokens)
                log_h2(f"Resubscribed to tokens after (re)connect: {tokens}")
            self._hop(self.on_status_async("connected"))
        except Exception as e:
            log_h2_error(f"on_connect callback error (ticker thread stays alive): {e}")

    def _on_close(self, ws, code, reason):
        log_h2_error(f"Zerodha WebSocket closed (code={code}, reason={reason})")
        self._hop(self.on_status_async("disconnected"))

    def _on_error(self, ws, code, reason):
        log_h2_error(f"Zerodha WebSocket error (code={code}, reason={reason})")

    def _on_reconnect(self, ws, attempts_count):
        log_h2(f"Zerodha WebSocket reconnecting (attempt {attempts_count})")
        self._hop(self.on_status_async("reconnecting"))

    def _on_noreconnect(self, ws):
        log_h2_error("Zerodha WebSocket gave up reconnecting")
        self._hop(self.on_status_async("failed"))

    def _hop(self, coro):
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, self.loop)


ticker2 = Ticker2()
