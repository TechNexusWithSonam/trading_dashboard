"""Broker-independent strike calculation — shared by the Upstox LOC engine
(backend/instruments.py, backend/loc_engine.py) and the Zerodha History 2
engine (backend/history2/*). Pure math, no broker/API dependency.

ITM level N means the strike is N steps in-the-money from ATM:
  CE (call) is ITM below spot  -> atm - N*step
  PE (put)  is ITM above spot  -> atm + N*step
"""

STRIKE_STEPS = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25,
    "SENSEX": 100, "BANKEX": 100, "CRUDEOIL": 50, "NATURALGAS": 10,
    "GOLD": 100, "SILVER": 1000, "COPPER": 5,
}


def get_atm_strike(spot: float, symbol: str) -> float:
    step = STRIKE_STEPS.get(symbol.upper(), 50)
    return round(round(spot / step) * step, 2)


def get_itm_strikes(spot: float, symbol: str, level: int) -> tuple:
    """Returns (ce_strike, pe_strike) for the given ITM level (1 = 1st ITM,
    2 = 2nd ITM, ...)."""
    step = STRIKE_STEPS.get(symbol.upper(), 50)
    atm = get_atm_strike(spot, symbol)
    return atm - level * step, atm + level * step


def get_itm1_strikes(spot: float, symbol: str) -> tuple:
    return get_itm_strikes(spot, symbol, 1)


def get_itm2_strikes(spot: float, symbol: str) -> tuple:
    return get_itm_strikes(spot, symbol, 2)
