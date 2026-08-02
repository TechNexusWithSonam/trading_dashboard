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

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from . import zerodha_client as zc
from .logger import log_h2, log_h2_error
from .state import state2

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
    return RedirectResponse(f"{FRONTEND_URL}/dashboard?auth=success")


@zerodha_router.post("/api/zerodha/postback")
async def zerodha_postback(payload: dict):
    # Order postbacks — this app is market-data only (no order placement),
    # so just acknowledge so Zerodha doesn't retry delivery.
    log_h2(f"Postback received (order_id={payload.get('order_id', '?')})")
    return {"status": "ok"}
