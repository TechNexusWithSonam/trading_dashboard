"""Shared Zerodha OAuth callback route. History 2 (the product feature that
used to live here — its own UI page, autonomous recording engine, and
`/api/history-2/*` API) has been removed.

What's left is the OAuth callback, kept here because it's registered
directly in Zerodha's Kite Connect app console as
https://api.raimatheuniqueconcepts.com/api/zerodha/callback (and
.../api/zerodha/postback) — it cannot simply move without updating that
console setting, so it stays exactly where it's always been. This is the
ONE real OAuth callback the whole app's login flow depends on
(backend/main.py's /auth/upstox/login redirects to zerodha_client.login_url(),
and Zerodha always redirects back to whatever's registered there — this
route) — do not remove it.
"""
import os

import httpx
from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from . import zerodha_client as zc
from .logger import log_h2, log_h2_error
from .state import state2

# Internal loopback call, not a public URL — used only to hand the freshly
# generated token to the main LOC engine's own /auth/token route (see below).
_INTERNAL_BASE_URL = os.getenv("INTERNAL_BASE_URL", "http://127.0.0.1:8000")

# Kept outside any prefix — Kite Connect requires an exact match to the
# configured redirect URL.
zerodha_router = APIRouter(tags=["zerodha-oauth"])

# The post-login redirect must be an ABSOLUTE url to the frontend. A relative
# "/dashboard?..." resolves against whatever host the browser is currently
# on — which is this API host (api.raimatheuniqueconcepts.com), not the
# actual frontend (trading_raima, on Vercel).
FRONTEND_URL = os.getenv("ZERODHA_HISTORY2_FRONTEND_URL", "https://www.raimatheuniqueconcepts.com").rstrip("/")


@zerodha_router.get("/api/zerodha/callback")
async def zerodha_callback(request_token: str = "", status: str = ""):
    if status and status != "success":
        log_h2_error(f"Zerodha authentication failed (status={status})")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?auth=failed")
    if not request_token:
        log_h2_error("Zerodha callback missing request_token")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?auth=failed")
    try:
        await zc.generate_session(request_token)
    except Exception as e:
        log_h2_error(f"Zerodha authentication failed: {e}")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?auth=failed")
    log_h2(f"Zerodha authentication successful (user_id={state2.user_id})")

    # This route only updates the shared Zerodha session (state2) — the main
    # LOC engine in main.py keeps its own copy of the token (state.access_token,
    # a plain attribute, not something that reads through to state2 live) and
    # only refreshes/reconnects its feed when main.py's own /auth/token route
    # runs. Since Zerodha's registered redirect_uri points HERE (not at
    # main.py's /auth/callback), every login — manual or scripted — has to be
    # explicitly forwarded to that route or the LOC engine silently keeps
    # running on a stale token until the next full process restart.
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{_INTERNAL_BASE_URL}/auth/token",
                              json={"access_token": state2.access_token})
            r.raise_for_status()
        log_h2("Propagated fresh token to main LOC engine, feed restarting")
    except Exception as e:
        log_h2_error(f"Failed to propagate token to main LOC engine: {e}")

    return RedirectResponse(f"{FRONTEND_URL}/dashboard?auth=success")


@zerodha_router.post("/api/zerodha/postback")
async def zerodha_postback(payload: dict):
    # Order postbacks — this app is market-data only (no order placement),
    # so just acknowledge so Zerodha doesn't retry delivery.
    log_h2(f"Postback received (order_id={payload.get('order_id', '?')})")
    return {"status": "ok"}
