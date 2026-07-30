"""Isolated Zerodha Kite Connect client for History 2. Independent of the
existing Upstox integration in main.py — separate credentials (see
ZERODHA_HISTORY2_* env vars), separate in-memory session state (state2).

Kite Connect's redirect URL is configured on Zerodha's app console (not
passed per-request), so login_url() only needs the api_key.

The access_token is also cached to a local file so a backend restart
(routine deploys included) doesn't force a fresh login — Kite Connect
tokens are valid for the rest of the trading day regardless; only losing
the in-memory copy on restart was forcing unnecessary re-logins.
"""
import json
import os
from pathlib import Path

from kiteconnect import KiteConnect

from .logger import log_h2, log_h2_error
from .state import state2
from .ticker import ticker2

API_KEY = os.getenv("ZERODHA_HISTORY2_API_KEY", "")
API_SECRET = os.getenv("ZERODHA_HISTORY2_API_SECRET", "")

SESSION_CACHE_PATH = Path(__file__).parent / ".zerodha_session.json"


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
    _save_session_cache()
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
    _clear_session_cache()
    log_h2("Zerodha session logged out")


def _save_session_cache():
    try:
        SESSION_CACHE_PATH.write_text(json.dumps({
            "access_token": state2.access_token,
            "user_id": state2.user_id,
            "login_time": state2.login_time,
        }))
    except Exception as e:
        log_h2_error(f"Failed to save Zerodha session cache: {e}")


def _clear_session_cache():
    try:
        SESSION_CACHE_PATH.unlink(missing_ok=True)
    except Exception as e:
        log_h2_error(f"Failed to clear Zerodha session cache: {e}")


def _restore_session_cache():
    """Called once at import time. Restores a cached access_token if it's
    still valid (verified with a live profile() call — Kite tokens expire
    at the start of the next trading day, so a stale cache from a previous
    day must not be trusted blindly)."""
    if not SESSION_CACHE_PATH.exists() or not API_KEY:
        return
    try:
        data = json.loads(SESSION_CACHE_PATH.read_text())
        token = data.get("access_token")
        if not token:
            return
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(token)
        profile = kite.profile()  # raises if the token is invalid/expired
        state2.access_token = token
        state2.user_id = data.get("user_id") or profile.get("user_id")
        state2.login_time = data.get("login_time")
        log_h2(f"Restored Zerodha session from cache (user_id={state2.user_id}) — no re-login needed")
    except Exception as e:
        log_h2(f"Cached Zerodha session invalid/expired, ignoring: {e}")
        _clear_session_cache()


_restore_session_cache()
