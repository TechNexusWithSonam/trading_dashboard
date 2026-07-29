"""Prefixed logging for History 2. Never pass api_secret / access_token here."""


def log_h2(msg: str):
    print(f"[History2] {msg}")


def log_h2_error(msg: str):
    print(f"[History2] ERROR: {msg}")
