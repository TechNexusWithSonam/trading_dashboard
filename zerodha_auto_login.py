"""
zerodha_auto_login.py — fully automated daily Zerodha Kite Connect login.

Kite Connect access tokens expire at the start of every trading day (Zerodha's
own security design, not something this app can opt out of) — normally that
means someone has to click the OAuth login link and enter password + TOTP by
hand every morning. This script does that same password+TOTP flow over HTTP
and rides Zerodha's own redirect chain straight into this app's already-
deployed callback route (/api/zerodha/callback), which does the normal
generate_session() exchange — so nothing on the backend needs to change or
know this script exists.

Meant to run once per trading day via cron, before market open. See
CRON_SETUP.md (or the deploy notes) for the crontab line.

Required environment variables (put these in the same .env this app already
reads, or export them in the cron environment):
  ZERODHA_API_KEY       — same Kite Connect app key already in use
  ZERODHA_USER_ID       — Zerodha login ID (e.g. "ZS4729")
  ZERODHA_PASSWORD      — Zerodha login password
  ZERODHA_TOTP_SECRET   — the TOTP secret key (the base32 string an
                           authenticator app was seeded with when 2FA was set
                           up — NOT a 6-digit code, which expires in 30s).
                           Found under Zerodha profile -> Password & Security
                           -> "External TOTP" setup (re-generating it there
                           invalidates the old secret, so save it somewhere
                           safe when you do).

SECURITY NOTE: unlike ZERODHA_API_KEY/SECRET (market-data-only access), the
password + TOTP secret are full account credentials — anyone who reads them
can log into the real Zerodha account, not just pull quotes. Keep .env's
permissions locked down (chmod 600, owned by the service user only) and
don't commit it or echo these values in logs.
"""
import os
import sys
import time

import pyotp
import requests
from dotenv import load_dotenv

load_dotenv()

KITE_LOGIN_URL = "https://kite.zerodha.com/api/login"
KITE_TWOFA_URL = "https://kite.zerodha.com/api/twofa"
KITE_CONNECT_LOGIN_URL = "https://kite.zerodha.com/connect/login"
REQUEST_TIMEOUT = 20


def _log(msg: str) -> None:
    print(f"[AutoLogin] {msg}", flush=True)


def main() -> int:
    api_key = os.getenv("ZERODHA_API_KEY", "")
    user_id = os.getenv("ZERODHA_USER_ID", "")
    password = os.getenv("ZERODHA_PASSWORD", "")
    totp_secret = os.getenv("ZERODHA_TOTP_SECRET", "")

    missing = [n for n, v in [
        ("ZERODHA_API_KEY", api_key), ("ZERODHA_USER_ID", user_id),
        ("ZERODHA_PASSWORD", password), ("ZERODHA_TOTP_SECRET", totp_secret),
    ] if not v]
    if missing:
        _log(f"Missing required env vars: {', '.join(missing)}")
        return 1

    session = requests.Session()

    # Step 1: password login -> gets a request_id, does NOT yet authenticate the session
    try:
        r1 = session.post(KITE_LOGIN_URL, data={"user_id": user_id, "password": password},
                           timeout=REQUEST_TIMEOUT)
        r1.raise_for_status()
        d1 = r1.json()
        if d1.get("status") != "success":
            _log(f"Password step rejected: {d1.get('message', d1)}")
            return 1
        request_id = d1["data"]["request_id"]
    except Exception as e:
        _log(f"Password step failed: {e}")
        return 1

    # Step 2: TOTP — codes rotate every 30s; retry once with a fresh code in
    # case the first attempt landed right on a rotation boundary.
    def _submit_totp() -> requests.Response:
        code = pyotp.TOTP(totp_secret).now()
        return session.post(KITE_TWOFA_URL, data={
            "user_id": user_id, "request_id": request_id,
            "twofa_value": code, "twofa_type": "totp",
        }, timeout=REQUEST_TIMEOUT)

    r2 = None
    for attempt in range(2):
        try:
            r2 = _submit_totp()
            d2 = r2.json()
            if d2.get("status") == "success":
                break
            if attempt == 0:
                _log(f"TOTP attempt 1 rejected ({d2.get('message', d2)}), retrying with a fresh code...")
                time.sleep(2)
                continue
            _log(f"TOTP step rejected: {d2.get('message', d2)}")
            return 1
        except Exception as e:
            if attempt == 0:
                _log(f"TOTP attempt 1 errored ({e}), retrying...")
                time.sleep(2)
                continue
            _log(f"TOTP step failed: {e}")
            return 1

    # Step 3: now-authenticated session requests the Connect login URL —
    # Zerodha redirects through consent and finally to this app's own
    # /api/zerodha/callback?request_token=...&status=success, which the
    # already-deployed backend handles exactly like a manual login would.
    try:
        r3 = session.get(KITE_CONNECT_LOGIN_URL, params={"api_key": api_key, "v": "3"},
                          timeout=REQUEST_TIMEOUT, allow_redirects=True)
    except Exception as e:
        _log(f"Final redirect step failed: {e}")
        return 1

    final_url = r3.url
    if "auth=success" in final_url:
        _log(f"Login successful (landed on {final_url})")
        return 0
    if "auth=failed" in final_url:
        _log(f"Backend reported auth failure (landed on {final_url})")
        return 1
    _log(f"Unexpected final redirect (couldn't confirm success): {final_url} "
         f"(HTTP {r3.status_code})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
