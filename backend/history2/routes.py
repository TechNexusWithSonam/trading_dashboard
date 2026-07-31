"""FastAPI routes for History 2 — mounted at /api/history-2. Fully isolated
from every existing route in main.py; the only touch to that file is a
two-line import + include_router registration.
"""
import json
import os

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse

from . import history
from . import instruments as instr
from . import zerodha_client as zc
from .logger import log_h2, log_h2_error
from .state import state2
from .ticker import ticker2

router = APIRouter(prefix="/api/history-2", tags=["history2"])

# Registered with Zerodha's app console as
# https://api.raimatheuniqueconcepts.com/api/zerodha/callback and
# .../api/zerodha/postback (must be this API host, not the frontend host —
# Kite Connect requires an exact match to the configured redirect URL) — so
# these two routes live outside the /api/history-2 prefix instead of nesting
# under it.
zerodha_router = APIRouter(tags=["history2-zerodha-oauth"])

# The post-login redirect must be an ABSOLUTE url to the frontend. A relative
# "/dashboard?..." resolves against whatever host the browser is currently
# on — which is this API host (api.raimatheuniqueconcepts.com), not the
# actual frontend (trading_raima, on Vercel) — and this API process happens
# to also serve an unrelated legacy static dashboard at "/dashboard", so a
# relative redirect silently lands the user on the wrong page entirely.
FRONTEND_URL = os.getenv("ZERODHA_HISTORY2_FRONTEND_URL", "https://www.raimatheuniqueconcepts.com").rstrip("/")


@router.get("/zerodha/login")
async def zerodha_login():
    log_h2("Zerodha login started")
    try:
        url = zc.login_url()
    except RuntimeError as e:
        log_h2_error(str(e))
        raise HTTPException(400, str(e))
    return RedirectResponse(url)


@zerodha_router.get("/api/zerodha/callback")
async def zerodha_callback(request_token: str = "", status: str = ""):
    # trading_raima has no real per-page URL routing (History itself is a tab,
    # not a route) — "/dashboard" is a real top-level route that mounts
    # TraderApp, which reads ?history2_auth=... on load and switches to the
    # History 2 tab. Redirecting straight to "/history-2" would 404/blank
    # since react-router has no matching Route for it.
    if status and status != "success":
        log_h2_error(f"Zerodha authentication failed (status={status})")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?history2_auth=failed")
    if not request_token:
        log_h2_error("Zerodha callback missing request_token")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?history2_auth=failed")
    try:
        zc.generate_session(request_token)
    except Exception as e:
        log_h2_error(f"Zerodha authentication failed: {e}")
        return RedirectResponse(f"{FRONTEND_URL}/dashboard?history2_auth=failed")
    return RedirectResponse(f"{FRONTEND_URL}/dashboard?history2_auth=success")


@zerodha_router.post("/api/zerodha/postback")
async def zerodha_postback(payload: dict):
    # Order postbacks — History 2 is market-data only (no order placement),
    # so just acknowledge so Zerodha doesn't retry delivery.
    log_h2(f"Postback received (order_id={payload.get('order_id', '?')})")
    return {"status": "ok"}


@router.get("/zerodha/status")
async def zerodha_status():
    return {
        "authenticated": bool(state2.access_token),
        "userId": state2.user_id,
        "loginTime": state2.login_time,
    }


@router.post("/zerodha/logout")
async def zerodha_logout():
    zc.logout()
    return {"status": "ok"}


def _require_auth():
    if not state2.access_token:
        raise HTTPException(401, "Not authenticated with Zerodha")


@router.get("/instruments/search")
async def instruments_search(q: str = Query(default="")):
    _require_auth()
    return {"query": q, "results": instr.search(q)}


@router.get("/instruments/expiries")
async def instruments_expiries(underlying: str):
    _require_auth()
    return {"underlying": underlying.upper(), "expiries": instr.list_expiries(underlying)}


@router.get("/instruments/resolve")
async def instruments_resolve(underlying: str, expiry: str, strike: float):
    _require_auth()
    spot = instr.resolve_spot(underlying)
    ce = instr.resolve_option(underlying, expiry, strike, "CE")
    pe = instr.resolve_option(underlying, expiry, strike, "PE")
    if not spot:
        raise HTTPException(404, f"Spot instrument not found for {underlying}")
    if not ce:
        raise HTTPException(404, f"CE instrument not found for {underlying} {expiry} {strike}")
    if not pe:
        raise HTTPException(404, f"PE instrument not found for {underlying} {expiry} {strike}")
    return {"underlying": underlying.upper(), "expiry": expiry, "strike": strike, "spot": spot, "ce": ce, "pe": pe}


@router.get("/options/chain")
async def options_chain(underlying: str, expiry: str):
    _require_auth()
    return {"underlying": underlying.upper(), "expiry": expiry, "chain": instr.resolve_chain(underlying, expiry)}


@router.get("/history/{symbol}")
async def get_history(symbol: str):
    return {"symbol": symbol.upper(), "history": history.get_history(symbol)}


@router.get("/ltp")
async def get_ltp(instrument: str = Query(..., description="exchange:tradingsymbol, e.g. 'NSE:NIFTY 50'")):
    _require_auth()
    kite = zc.get_kite()
    try:
        return kite.ltp([instrument])
    except Exception as e:
        log_h2_error(f"LTP fetch failed for {instrument}: {e}")
        raise HTTPException(502, f"LTP fetch failed: {e}")


@router.post("/ltp")
async def get_ltp_multi(payload: dict):
    _require_auth()
    instruments = payload.get("instruments", [])
    if not instruments:
        raise HTTPException(400, "instruments required")
    kite = zc.get_kite()
    try:
        return kite.ltp(instruments)
    except Exception as e:
        log_h2_error(f"Multi-LTP fetch failed: {e}")
        raise HTTPException(502, f"LTP fetch failed: {e}")


# ══════════════════════════════════════════════════════════════════
#  LIVE WEBSOCKET — /api/history-2/ws
#  Isolated from the existing browser feed WebSocket (main.py's /ws/feed).
#  Plain JSON messages with a "type" field stand in for the spec's
#  history2:* Socket.IO event names, since this stack has no Socket.IO.
# ══════════════════════════════════════════════════════════════════
_clients: set[WebSocket] = set()


async def _broadcast(msg: dict):
    dead = []
    for ws in _clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _clients.discard(ws)


async def _on_tick(token: int, price: float, ts_ms: int):
    state2.live_ticks[token] = {"lastPrice": price, "timestamp": ts_ms}
    history.on_tick(token, price, ts_ms)  # minute-bucket history recording, if this token is registered to a symbol
    # Only broadcast to connected browser clients for tokens someone actually
    # subscribed to via this WS (manual Symbol/Expiry/Strike flow). The
    # autonomous engine (engine.py) subscribes ~200 symbols' worth of tokens
    # purely for background recording — broadcasting all of those to every
    # connected client would flood the browser with irrelevant ticks.
    if token in state2.broadcast_tokens:
        await _broadcast({"type": "history2:tick", "instrumentToken": token, "lastPrice": price, "timestamp": ts_ms})


async def _on_status(status: str):
    await _broadcast({"type": "history2:status", "status": status})


@router.websocket("/ws")
async def history2_ws(ws: WebSocket):
    await ws.accept()
    _clients.add(ws)
    log_h2(f"Frontend WS client connected (total={len(_clients)})")
    await ws.send_json({"type": "history2:connect", "status": "ok"})
    await ws.send_json({"type": "history2:status",
                         "status": "connected" if ticker2.kws else "idle",
                         "subscribedTokens": list(ticker2.subscribed_tokens)})
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send_json({"type": "history2:error", "message": "invalid JSON"})
                continue
            mtype = msg.get("type")
            if mtype == "history2:subscribe":
                tokens = msg.get("instrumentTokens", [])
                mode = msg.get("mode", "ltp")
                # Optional: {"symbol":"NIFTY","spotToken":..,"ceToken":..,"peToken":..}
                # Registers the token->role mapping so on_tick() can build minute-
                # bucketed history rows for this symbol. Additive to the spec's
                # documented subscribe payload — omit it and nothing changes.
                ctx = msg.get("context")
                if not state2.access_token:
                    await ws.send_json({"type": "history2:error", "message": "Not authenticated with Zerodha"})
                    continue
                try:
                    if not ticker2.kws:
                        ticker2.start(_on_tick, _on_status)
                    ticker2.subscribe(tokens, mode)
                    state2.broadcast_tokens.update(tokens)
                    if ctx and ctx.get("symbol"):
                        history.register_context(ctx["symbol"], ctx.get("spotToken"), ctx.get("ceToken"), ctx.get("peToken"))
                    await ws.send_json({"type": "history2:status", "status": "subscribed", "instrumentTokens": tokens})
                except Exception as e:
                    log_h2_error(f"Subscription failed: {e}")
                    await ws.send_json({"type": "history2:error", "message": str(e)})
            elif mtype == "history2:unsubscribe":
                tokens = msg.get("instrumentTokens", [])
                # Note: a token can coincidentally belong to more than one
                # owner (e.g. a user manually picks the exact strike the
                # autonomous engine in engine.py already tracks as an ITM-1/
                # ITM-2 leg for the same underlying — same Zerodha instrument
                # token either way). Unconditionally unsubscribing here can
                # therefore drop a token the engine still wants; that's
                # self-healing on the engine's next ATM-shift cycle (it
                # re-subscribes its current tokens whenever the strike set
                # changes) and is far safer than trying to guess ownership
                # from state2.symbol_context, whose manual-path entries only
                # get refreshed by the *next* subscribe message — which can
                # race this unsubscribe and make a false-positive "still
                # needed" check block a real strike-switch cleanup instead.
                ticker2.unsubscribe(tokens)
                state2.broadcast_tokens.difference_update(tokens)
                await ws.send_json({"type": "history2:status", "status": "unsubscribed", "instrumentTokens": tokens})
            else:
                await ws.send_json({"type": "history2:error", "message": f"unknown message type {mtype}"})
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)
        log_h2(f"Frontend WS client disconnected (total={len(_clients)})")
