"""Isolated Zerodha Kite Connect client for History 2. Independent of the
existing Upstox integration in main.py — separate credentials (see
ZERODHA_HISTORY2_* env vars), separate in-memory session state (state2).

Kite Connect's redirect URL is configured on Zerodha's app console (not
passed per-request), so login_url() only needs the api_key.
"""
import os

from kiteconnect import KiteConnect

from .logger import log_h2, log_h2_error
from .state import state2
from .ticker import ticker2

API_KEY = os.getenv("ZERODHA_HISTORY2_API_KEY", "")
API_SECRET = os.getenv("ZERODHA_HISTORY2_API_SECRET", "")


def get_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    if state2.access_token:
        kite.set_access_token(state2.access_token)
    return kite


def login_url() -> str:
    if not API_KEY:
        raise RuntimeError("ZERODHA_HISTORY2_API_KEY not set")
    return KiteConnect(api_key=API_KEY).login_url()


def generate_session(request_token: str) -> dict:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("ZERODHA_HISTORY2_API_KEY/SECRET not set")
    kite = KiteConnect(api_key=API_KEY)
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    state2.access_token = data["access_token"]
    state2.user_id = data.get("user_id")
    state2.login_time = str(data.get("login_time") or "")
    log_h2(f"Zerodha authentication successful (user_id={state2.user_id})")
    return data


def logout():
    ticker2.stop()  # access token is about to be invalidated — the live tick feed can't outlive it
    if state2.access_token:
        try:
            get_kite().invalidate_access_token()
        except Exception as e:
            log_h2_error(f"Zerodha logout warning: {e}")
    state2.access_token = None
    state2.user_id = None
    state2.login_time = None
    state2.instruments_cache = {}
    state2.instruments_loaded_at = {}
    log_h2("Zerodha session logged out")
