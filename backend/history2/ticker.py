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

from kiteconnect import KiteTicker

from .logger import log_h2, log_h2_error
from .state import state2

API_KEY = os.getenv("ZERODHA_HISTORY2_API_KEY", "")


class Ticker2:
    def __init__(self):
        self.kws: KiteTicker | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.subscribed_tokens: set[int] = set()
        self.mode = None
        self.on_tick_async = None
        self.on_status_async = None

    def start(self, on_tick_async, on_status_async):
        if self.kws is not None:
            return  # already running — never open a second connection
        if not state2.access_token:
            raise RuntimeError("Not authenticated with Zerodha")
        self.loop = asyncio.get_running_loop()
        self.on_tick_async = on_tick_async
        self.on_status_async = on_status_async
        self.mode = KiteTicker.MODE_LTP
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
        if new_tokens:
            self.kws.subscribe(new_tokens)
        self.kws.set_mode(self.mode, tokens)
        self.subscribed_tokens |= set(tokens)
        log_h2(f"Subscribed to tokens: {tokens} (new: {new_tokens})")

    def unsubscribe(self, tokens: list[int]):
        if not self.kws:
            return
        self.kws.unsubscribe(tokens)
        self.subscribed_tokens -= set(tokens)
        log_h2(f"Unsubscribed from tokens: {tokens}")

    # ---- KiteTicker callbacks: fire on the ticker's background thread ----

    def _on_ticks(self, ws, ticks):
        for t in ticks:
            token = t.get("instrument_token")
            price = t.get("last_price")
            if token is None or price is None:
                continue
            self._hop(self.on_tick_async(token, price, int(time.time() * 1000)))

    def _on_connect(self, ws, response):
        log_h2("Zerodha WebSocket connected")
        if self.subscribed_tokens:
            tokens = list(self.subscribed_tokens)
            ws.subscribe(tokens)
            ws.set_mode(self.mode, tokens)
            log_h2(f"Resubscribed to tokens after (re)connect: {tokens}")
        self._hop(self.on_status_async("connected"))

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
