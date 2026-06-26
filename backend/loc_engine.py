"""
loc_engine.py v11 — Complete rewrite fixing:
1. ITM-2 strikes: CE = ATM-2*step (call IN the money = strike below spot)
                  PE = ATM+2*step (put IN the money = strike above spot)
2. Use close_price as fallback when ltp is near 0 (expiry day/weekend)
3. Real-time WS option price updates work correctly
4. ATM debounce to avoid thrashing on minor spot moves
5. chain_spot used correctly for initial strike calculation
"""
import asyncio, time
from dataclasses import dataclass, field
from typing import Optional, Callable, Dict
from .instruments import get_itm2_strikes, STRIKE_STEPS


@dataclass
class SpotData:
    ltp:float=0; close:float=0; high:float=0; low:float=0; open:float=0; ts:int=0

@dataclass
class OptionData:
    ltp:float=0; close:float=0; high:float=0; low:float=0
    oi:float=0; iv:float=0; instrument_key:str=""

    @property
    def effective_ltp(self) -> float:
        """Use close if ltp is near 0 (expiry day / market closed)."""
        if self.ltp and self.ltp >= 1.0:
            return self.ltp
        return self.close or self.ltp

    @property
    def effective_high(self) -> float:
        return self.high or self.effective_ltp

    @property
    def effective_low(self) -> float:
        return self.low or self.effective_ltp

@dataclass
class SymbolState:
    symbol:str=""
    spot:SpotData=field(default_factory=SpotData)
    ce:OptionData=field(default_factory=OptionData)
    pe:OptionData=field(default_factory=OptionData)
    ce_strike:float=0; pe_strike:float=0
    expiry:str=""
    option_chain:dict=field(default_factory=dict)
    loc_result:dict=field(default_factory=dict)
    last_atm:float=0
    chain_spot:float=0
    # Timestamps of last WS tick for CE/PE — used by stale-option REST fallback
    ce_last_tick:float=0; pe_last_tick:float=0


@dataclass
class CalcState:
    """Per-symbol, user-selectable expiry state for the Calculator page.
    Decoupled from SymbolState so the LOC table can stay pinned to the
    default/current-week expiry no matter what the user previews here."""
    expiry:str=""
    option_chain:dict=field(default_factory=dict)
    ce:OptionData=field(default_factory=OptionData)
    pe:OptionData=field(default_factory=OptionData)
    ce_strike:float=0; pe_strike:float=0
    last_atm:float=0
    chain_spot:float=0
    result:dict=field(default_factory=dict)


def calc_loc_25(spot_ltp, spot_close, spot_high, spot_low, spot_open,
                ce_ltp, ce_close, ce_high, ce_low,
                pe_ltp, pe_close, pe_high, pe_low) -> dict:
    """
    All 25 LOC formulas.
    Uses effective ltp (falls back to close when ltp ≈ 0).
    """
    s  = spot_ltp   or 1
    sc = spot_close or s
    sh = spot_high  or s
    sl = spot_low   or s

    # Use effective prices (fallback close when ltp near 0)
    ce_l = ce_ltp   if ce_ltp  >= 1.0 else (ce_close  or ce_ltp  or 0)
    ce_c = ce_close or ce_l
    ce_h = ce_high  or ce_l
    ce_lo= ce_low   or ce_l

    pe_l = pe_ltp   if pe_ltp  >= 1.0 else (pe_close  or pe_ltp  or 0)
    pe_c = pe_close or pe_l
    pe_h = pe_high  or pe_l
    pe_lo= pe_low   or pe_l

    def sd(a, b): return (a/b) if b else 0

    f1 = sd(max(ce_h, ce_c), max(sh, sc))
    f2 = sd(min(ce_lo, ce_c), min(sl, sc) or 1)
    f3 = sd(max(pe_h, pe_c), min(sl, sc) or 1)
    f4 = sd(min(pe_lo, pe_c), max(sh, sc))
    f5 = sd(ce_l, s)
    f6 = sd(pe_l, s)
    f7 = f1-f2; f8 = f3-f4; f9 = f7/2+f2; f10 = f8/2+f4
    ab = f5-f9; ac = f6-f10; f13 = f8-f7

    if   ab>0 and ac<0:             f15=abs(ab)+abs(ac)
    elif ab<0 and ac>0:             f15=abs(ab)+abs(ac)
    elif ab<0 and ac<0 and ab>ac:  f15=abs(ac)-abs(ab)
    elif ab<0 and ac<0 and ab<ac:  f15=abs(ab)-abs(ac)
    elif ab>0 and ac>0 and ab>ac:  f15=abs(ab-ac)
    else:                           f15=abs(ac-ab)

    f16=f15*s
    f17=s+f16 if ab<ac else(s-f16 if ab>ac else s)
    f18=s+abs(ab)*s if ab<0 else(s-abs(ab)*s if ab>0 else s)
    f19=s-abs(ac)*s if ac<0 else(s+abs(ac)*s if ac>0 else s)
    f20=f17*1.0; f21=f17*1.0; f22=f20-f18; f23=f21-f19
    f24=s+f22; f25=s+f23

    # zone=("CALL" if s>f24 and s>f25 and s>f20 else
    #       "PUT"  if s<f24 and s<f25 and s<f21 else "WAIT")
    zone=("CALL" if s> max(f20, f21,f24,f25) else
          "PUT"  if s< min(f20, f21,f24,f25) else "WAIT")
    chg=round(s-sc,2)
    r2=lambda x:round(x,2); r4=lambda x:round(x,4)
    return {
        "ltp":r2(s),"cp":r2(sc),"change":chg,
        "pct":round(chg/sc*100,2) if sc else 0,
        "bop":r2(f17),"cep":r2(f18),"pep":r2(f19),
        "ul":r2(f24),"ll":r2(f25),"ful":r2(f20),"fll":r2(f21),
        "ful_diff":r2(f22),"fll_diff":r2(f23),
        "dsl":r4(f15),"dsp":r2(f16),
        "call_move":r4(f7),"put_move":r4(f8),
        "call_cp":r4(f9),"put_cp":r4(f10),
        "call_cp_diff":r4(ab),"put_cp_diff":r4(ac),
        "different":r4(f13),
        "ceh_sh":r4(f1),"cel_sl":r4(f2),"peh_sl":r4(f3),"pel_sh":r4(f4),
        "c_ce_s":r4(f5),"c_pe_s":r4(f6),
        "zone":zone,"direction":"UP" if chg>=0 else "DOWN",
        "distance":r2(abs(s-f17)),
        "f1":f1,"f2":f2,"f3":f3,"f4":f4,"f5":f5,"f6":f6,"f7":f7,"f8":f8,"f9":f9,"f10":f10,"f13":f13,"ab":ab,"ac":ac,"f15":f15,"f16":f16,"f17":f17,"f18":f18,"f19":f19,"f20":f20,"f21":f21,"f22":f22,"f23":f23,"f24":f24,"f25":f25,
    }


class LOCEngine:
    def __init__(self):
        self.symbols:Dict[str,SymbolState]={}
        self.calc_states:Dict[str,CalcState]={}               # Calculator-only views
        self.access_token:str=""
        self.on_loc_update:Optional[Callable]=None
        self.on_option_ohlc_needed:Optional[Callable]=None   # (symbol) → fetch OHLC REST
        self.on_option_keys_changed:Optional[Callable]=None  # () → subscribe new WS keys
        self.on_calc_keys_changed:Optional[Callable]=None    # () → subscribe calc keys
        self.on_calc_option_ohlc_needed:Optional[Callable]=None  # (symbol) → fetch calc OHLC
        self.chain_fetch_time:Dict[str,float]={}
        self.calc_chain_fetch_time:Dict[str,float]={}

    def register(self, symbol:str):
        if symbol not in self.symbols:
            self.symbols[symbol]=SymbolState(symbol=symbol)

    def get_state(self, symbol:str)->Optional[SymbolState]:
        return self.symbols.get(symbol)

    def set_expiry(self, symbol:str, expiry:str, fetch_chain:bool=True):
        st=self.symbols.get(symbol)
        if st:
            st.expiry=expiry
            if fetch_chain:
                asyncio.create_task(self._refresh_chain(symbol))

    def update_spot(self, symbol:str, ltp:float, close:float,
                    high:float, low:float, ts:int, open_:float=0):
        st=self.symbols.get(symbol)
        if not st or not ltp: return
        st.spot.ltp  = ltp
        # OHLC accumulation: use WS-provided values when present (full marketFF
        # ticks); for partial ticks that send ltp only (high/low == 0), accumulate
        # rolling intraday max/min so frontend always sees real values, not ltp.
        if high > 0:
            st.spot.high = high
        elif st.spot.high > 0:
            st.spot.high = max(st.spot.high, ltp)
        else:
            st.spot.high = ltp
        if low > 0:
            st.spot.low = low
        elif st.spot.low > 0:
            st.spot.low = min(st.spot.low, ltp)
        else:
            st.spot.low = ltp
        if close > 0:
            st.spot.close = close
        elif not st.spot.close:
            st.spot.close = ltp   # seed until real prev-close arrives
        if open_ > 0: st.spot.open = open_
        st.spot.ts   = ts

        # ATM shift detection — use debounce (only act if ATM actually changes)
        step = STRIKE_STEPS.get(symbol.upper(), 50)
        new_atm = round(round(ltp / step) * step, 2)
        if new_atm != st.last_atm:
            st.last_atm = new_atm
            ce_s, pe_s = get_itm2_strikes(ltp, symbol)
            if ce_s != st.ce_strike or pe_s != st.pe_strike:
                prev_ce_key = st.ce.instrument_key
                prev_pe_key = st.pe.instrument_key
                st.ce_strike = ce_s
                st.pe_strike = pe_s
                print(f"[LOC] {symbol} ATM shift→{new_atm} CE:{ce_s} PE:{pe_s}")
                # Load from cached chain so the new strike's instrument_key is set.
                self._load_from_chain(symbol)
                # If the instrument key actually changed, push the new WS
                # subscription immediately — don't wait up to 60 s for the
                # periodic refresh. Also bypass the chain throttle so fresh
                # LTP/close/high/low arrive as soon as possible.
                keys_changed = (st.ce.instrument_key != prev_ce_key or
                                st.pe.instrument_key != prev_pe_key)
                if keys_changed and self.on_option_keys_changed:
                    asyncio.create_task(self.on_option_keys_changed())
                asyncio.create_task(self._refresh_chain(symbol, force=keys_changed))
        self._recalc(symbol)
        # Calculator parallel tracking — mirror the ATM-shift logic against
        # calc_state.last_atm if the user has a Calculator view open on
        # a different expiry. Only does work when calc_state exists.
        calc = self.calc_states.get(symbol)
        if calc:
            new_atm_c = round(round(ltp / step) * step, 2)
            if new_atm_c != calc.last_atm:
                calc.last_atm = new_atm_c
                ce_s_c, pe_s_c = get_itm2_strikes(ltp, symbol)
                if ce_s_c != calc.ce_strike or pe_s_c != calc.pe_strike:
                    prev_ce_key_c = calc.ce.instrument_key
                    prev_pe_key_c = calc.pe.instrument_key
                    calc.ce_strike = ce_s_c
                    calc.pe_strike = pe_s_c
                    self._load_calc_from_chain(symbol)
                    keys_changed_c = (calc.ce.instrument_key != prev_ce_key_c or
                                      calc.pe.instrument_key != prev_pe_key_c)
                    if keys_changed_c and self.on_calc_keys_changed:
                        asyncio.create_task(self.on_calc_keys_changed())
                    asyncio.create_task(self._refresh_calc_chain(symbol, force=keys_changed_c))
            self._recalc_calc(symbol)

    def update_option_from_feed(self, symbol:str, opt_type:str,
                                 ltp:float, close:float, high:float, low:float):
        """Real-time CE/PE price update from WS feed.
        Note: close (cp from WS ltpc) is the previous day's close and should
        only be set once. We do NOT overwrite it on every tick because for
        options, once set from REST (which derives it from net_change), the
        REST value is authoritative. WS cp can be used as initial seed only.

        High/low: Upstox WS feed sends the AUTHORITATIVE session high/low
        in efeed.high / efeed.low — overwrite directly when provided. The
        prior max/min accumulation caused yesterday's session high to
        persist when today's high was lower. Partial ticks (where efeed
        lacks these fields) are skipped via the `> 0` guard.
        """
        st=self.symbols.get(symbol)
        if not st: return
        opt = st.ce if opt_type=="CE" else st.pe
        if ltp and ltp>0:
            opt.ltp = ltp
            # Track last WS tick time for stale-option REST fallback (Bug 1)
            if opt_type == "CE":
                st.ce_last_tick = time.time()
            else:
                st.pe_last_tick = time.time()
            # Intraday high/low: use WS-provided values (full marketFF ticks)
            # when available; otherwise accumulate rolling max/min from ltp.
            # Option WS ticks are often firstLevelWithGreeks which omits efeed
            # high/low (sends 0), so without this accumulation ce_high == ce_ltp
            # forever (a snapshot price, not a true session high).
            if high > 0:
                opt.high = high
            elif opt.high > 0:
                opt.high = max(opt.high, ltp)
            else:
                opt.high = ltp
            if low > 0:
                opt.low = low
            elif opt.low > 0:
                opt.low = min(opt.low, ltp)
            else:
                opt.low = ltp
        # Only seed close from WS if we have no close yet (REST hasn't arrived)
        if close and close>0 and not opt.close:
            opt.close = close
        self._recalc(symbol)

    def update_chain(self, symbol:str, chain:dict):
        """Called after fresh chain fetch. Extracts spot and sets ITM-2 strikes."""
        st=self.symbols.get(symbol)
        if not st or not chain: return
        st.option_chain = chain

        # Auto-detect strike step from chain data
        strikes = sorted(chain.keys())
        if len(strikes) >= 3:
            diffs = [round(strikes[i+1] - strikes[i], 2)
                     for i in range(min(10, len(strikes)-1))]
            if diffs:
                step = max(set(diffs), key=diffs.count)
                if step > 0:
                    STRIKE_STEPS[symbol.upper()] = step

        # Extract underlying spot from chain rows
        chain_spot = 0.0
        for row in chain.values():
            sp = row.get("_spot", 0)
            if sp:
                chain_spot = float(sp)
                break

        if chain_spot:
            st.chain_spot = chain_spot
            # Use WS spot if available, else chain spot
            effective_spot = st.spot.ltp or chain_spot
            ce_s, pe_s = get_itm2_strikes(effective_spot, symbol)
            st.ce_strike = ce_s
            st.pe_strike = pe_s
            step = STRIKE_STEPS.get(symbol.upper(), 50)
            st.last_atm  = round(round(effective_spot / step) * step, 2)

            # Prime spot data from chain if WS hasn't arrived yet
            if not st.spot.ltp:
                st.spot.ltp   = chain_spot
                st.spot.close = chain_spot

        self._load_from_chain(symbol)

    def _load_from_chain(self, symbol:str):
        """Load CE/PE data from chain at the ITM-2 strikes."""
        st=self.symbols.get(symbol)
        if not st or not st.option_chain: return
        if not st.ce_strike:
            print(f"[LOC] {symbol}: no strikes set, skipping chain load")
            return

        ce_row = st.option_chain.get(st.ce_strike, {})
        pe_row = st.option_chain.get(st.pe_strike, {})

        strikes = sorted(st.option_chain.keys())
        step = STRIKE_STEPS.get(symbol.upper(), 50)

        if not ce_row or not pe_row:
            # Strikes not in chain — find nearest available strikes
            tolerance = step * 4
            if strikes:
                nearest_ce = min(strikes, key=lambda s: abs(s - st.ce_strike))
                nearest_pe = min(strikes, key=lambda s: abs(s - st.pe_strike))
                if abs(nearest_ce - st.ce_strike) < tolerance:
                    ce_row = st.option_chain.get(nearest_ce, {})
                    if ce_row: st.ce_strike = nearest_ce
                if abs(nearest_pe - st.pe_strike) < tolerance:
                    pe_row = st.option_chain.get(nearest_pe, {})
                    if pe_row: st.pe_strike = nearest_pe

        # MCX options are illiquid at ITM-2 — fall back to nearest strike
        # with non-zero LTP so the LOC engine has real data to work with.
        # Only jump to a different strike when BOTH ltp AND close are zero.
        # If close > 0 (prev-day settlement), the ITM-2 strike is valid and
        # we must not replace pe_strike with a different strike in the history.
        if strikes:
            ce_ltp   = float((ce_row.get("CE") or {}).get("ltp",   0) or 0)
            ce_close = float((ce_row.get("CE") or {}).get("close", 0) or 0)
            if ce_ltp == 0 and ce_close == 0:
                for s in sorted(strikes, key=lambda x: abs(x - st.ce_strike)):
                    row = st.option_chain.get(s, {})
                    if float((row.get("CE") or {}).get("ltp", 0) or 0) > 0:
                        ce_row = row
                        st.ce_strike = s
                        break
            pe_ltp   = float((pe_row.get("PE") or {}).get("ltp",   0) or 0)
            pe_close = float((pe_row.get("PE") or {}).get("close", 0) or 0)
            if pe_ltp == 0 and pe_close == 0:
                for s in sorted(strikes, key=lambda x: abs(x - st.pe_strike)):
                    row = st.option_chain.get(s, {})
                    if float((row.get("PE") or {}).get("ltp", 0) or 0) > 0:
                        pe_row = row
                        st.pe_strike = s
                        break

        def _best(*vals):
            for v in vals:
                try:
                    fv = float(v)
                    if fv > 0: return fv
                except: pass
            return 0.0

        # Every chain refresh gets today's authoritative prev-close + session
        # high/low from Upstox. Overwrite directly — the old max/min
        # accumulation and `not st.pe.close` guard caused yesterday's values
        # to persist into today's session whenever today's numbers were
        # smaller or a close had already been seeded.
        if ce_row.get("CE"):
            c = ce_row["CE"]
            new_ce_key = c.get("key", "")
            key_changed = (new_ce_key and new_ce_key != st.ce.instrument_key)
            chain_ltp   = _best(c.get("ltp"), c.get("close"))
            chain_close = _best(c.get("close"), c.get("ltp"))
            # Only overwrite LTP from chain if instrument changed (ATM shift)
            # or we have no WS data yet. WS LTP is real-time and authoritative.
            if key_changed or not st.ce.ltp:
                st.ce.ltp = chain_ltp
            # Close: chain close_price is prev day's close for NSE, net_change-
            # derived for MCX. Always refresh when chain returns a value.
            if chain_close:
                st.ce.close = chain_close
            # High/low: chain reports today's session extremes. Overwrite when
            # the chain has a value; fall back to ltp only when both chain is
            # empty and we have no prior high/low (first-load or ATM shift).
            chain_high = _best(c.get("high"))
            chain_low  = _best(c.get("low"))
            if chain_high:
                st.ce.high = chain_high
            elif key_changed or not st.ce.high:
                st.ce.high = chain_ltp
            if chain_low:
                st.ce.low = chain_low
            elif key_changed or not st.ce.low:
                st.ce.low = chain_ltp
            st.ce.oi    = float(c.get("oi") or 0)
            st.ce.iv    = float(c.get("iv") or 0)
            st.ce.instrument_key = new_ce_key or st.ce.instrument_key

        if pe_row.get("PE"):
            p = pe_row["PE"]
            new_pe_key = p.get("key", "")
            key_changed = (new_pe_key and new_pe_key != st.pe.instrument_key)
            chain_ltp   = _best(p.get("ltp"), p.get("close"))
            chain_close = _best(p.get("close"), p.get("ltp"))
            if key_changed or not st.pe.ltp:
                st.pe.ltp = chain_ltp
            if chain_close:
                st.pe.close = chain_close
            chain_high = _best(p.get("high"))
            chain_low  = _best(p.get("low"))
            if chain_high:
                st.pe.high = chain_high
            elif key_changed or not st.pe.high:
                st.pe.high = chain_ltp
            if chain_low:
                st.pe.low = chain_low
            elif key_changed or not st.pe.low:
                st.pe.low = chain_ltp
            st.pe.oi    = float(p.get("oi") or 0)
            st.pe.iv    = float(p.get("iv") or 0)
            st.pe.instrument_key = new_pe_key or st.pe.instrument_key

        print(f"[LOC] {symbol} loaded: "
              f"CE@{st.ce_strike}=ltp:{st.ce.ltp} close:{st.ce.close} "
              f"eff:{st.ce.effective_ltp} "
              f"key:{st.ce.instrument_key[:20] if st.ce.instrument_key else 'MISS'} | "
              f"PE@{st.pe_strike}=ltp:{st.pe.ltp} close:{st.pe.close} "
              f"eff:{st.pe.effective_ltp} "
              f"key:{st.pe.instrument_key[:20] if st.pe.instrument_key else 'MISS'}")
        # Seed tick timestamps so REST fallback waits 30s after each chain load
        # before assuming the key is silent (gives WS feed a chance to deliver)
        now = time.time()
        st.ce_last_tick = now
        st.pe_last_tick = now
        self._recalc(symbol)

    def _recalc(self, symbol:str):
        """Run all 25 LOC formulas and notify."""
        st = self.symbols.get(symbol)
        if not st: return
        spot_ltp = st.spot.ltp or st.chain_spot
        if not spot_ltp: return

        # Use effective ltp (falls back to close when near 0)
        res = calc_loc_25(
            spot_ltp,
            st.spot.close or spot_ltp,
            st.spot.high  or spot_ltp,
            st.spot.low   or spot_ltp,
            st.spot.open  or spot_ltp,
            st.ce.effective_ltp,  st.ce.close,
            st.ce.effective_high, st.ce.effective_low,
            st.pe.effective_ltp,  st.pe.close,
            st.pe.effective_high, st.pe.effective_low,
        )
        res.update({
            "symbol":     symbol,
            "spot_ltp":   round(spot_ltp, 2),
            "spot_close": round(st.spot.close or spot_ltp, 2),
            "spot_high":  round(st.spot.high  or spot_ltp, 2),
            "spot_low":   round(st.spot.low   or spot_ltp, 2),
            "spot_open":  round(st.spot.open  or spot_ltp, 2),
            "ce_strike": st.ce_strike,
            "pe_strike": st.pe_strike,
            "expiry":    st.expiry,
            "ce_ltp":    round(st.ce.effective_ltp, 2),
            "pe_ltp":    round(st.pe.effective_ltp, 2),
            "ce_close":  round(st.ce.close, 2),
            "pe_close":  round(st.pe.close, 2),
            "ce_high":   round(st.ce.effective_high, 2),
            "ce_low":    round(st.ce.effective_low, 2),
            "pe_high":   round(st.pe.effective_high, 2),
            "pe_low":    round(st.pe.effective_low, 2),
            "ce_iv":     round(st.ce.iv, 2),
            "pe_iv":     round(st.pe.iv, 2),
            "ts":        int(time.time() * 1000),
        })
        st.loc_result = res
        if self.on_loc_update:
            asyncio.create_task(self.on_loc_update(symbol, res))

    def recalc(self, symbol:str):
        return self._recalc(symbol)

    async def _refresh_chain(self, symbol:str, force:bool=False):
        if not self.access_token: return
        st = self.symbols.get(symbol)
        if not st or not st.expiry: return
        cache_key = f"{symbol}|{st.expiry}"
        # Throttle: at most once per 55 seconds — unless forced (ATM shift).
        if not force and time.time() - self.chain_fetch_time.get(cache_key, 0) < 55: return
        self.chain_fetch_time[cache_key] = time.time()
        from .instruments import fetch_option_chain
        chain = await fetch_option_chain(symbol, st.expiry, self.access_token)
        if chain:
            self.update_chain(symbol, chain)
            # Immediately fetch actual OHLC (chain API lacks intraday high/low)
            if self.on_option_ohlc_needed:
                await self.on_option_ohlc_needed(symbol)

    async def refresh_all_chains(self):
        for sym in list(self.symbols.keys()):
            await self._refresh_chain(sym)
            await asyncio.sleep(0.3)

    def get_all_results(self) -> dict:
        return {s: st.loc_result for s, st in self.symbols.items() if st.loc_result}

    def get_option_keys(self) -> list:
        keys = []
        for st in self.symbols.values():
            if st.ce.instrument_key: keys.append(st.ce.instrument_key)
            if st.pe.instrument_key: keys.append(st.pe.instrument_key)
        return [k for k in keys if k]

    # ── Calculator-only API (decoupled from LOC table) ─────────────────
    def get_calc_option_keys(self) -> list:
        keys = []
        for calc in self.calc_states.values():
            if calc.ce.instrument_key: keys.append(calc.ce.instrument_key)
            if calc.pe.instrument_key: keys.append(calc.pe.instrument_key)
        return [k for k in keys if k]

    def get_all_calc_results(self) -> dict:
        return {sym: calc.result for sym, calc in self.calc_states.items() if calc.result}

    def get_calc_state(self, symbol:str) -> Optional[CalcState]:
        return self.calc_states.get(symbol)

    async def set_calc_expiry(self, symbol:str, expiry:str):
        """Create (or reset) the per-symbol Calculator state for this expiry.
        Never touches SymbolState — the LOC table stays locked on default."""
        st = self.symbols.get(symbol)
        if not st or not expiry: return
        calc = self.calc_states.get(symbol)
        if calc and calc.expiry == expiry and calc.option_chain:
            return  # already set
        # Fresh calc state for this expiry
        self.calc_states[symbol] = CalcState(expiry=expiry)
        await self._refresh_calc_chain(symbol, force=True)

    def clear_calc_expiry(self, symbol:str):
        """Remove the Calculator view for this symbol. Frontend falls back
        to the LOC table (default expiry) data."""
        self.calc_states.pop(symbol, None)

    async def _refresh_calc_chain(self, symbol:str, force:bool=False):
        if not self.access_token: return
        calc = self.calc_states.get(symbol)
        if not calc or not calc.expiry: return
        ck = f"{symbol}|{calc.expiry}|calc"
        if not force and time.time() - self.calc_chain_fetch_time.get(ck, 0) < 55: return
        self.calc_chain_fetch_time[ck] = time.time()
        from .instruments import fetch_option_chain
        chain = await fetch_option_chain(symbol, calc.expiry, self.access_token)
        if chain:
            calc.option_chain = chain
            chain_spot = 0.0
            for row in chain.values():
                sp = row.get("_spot", 0)
                if sp:
                    chain_spot = float(sp); break
            if chain_spot:
                calc.chain_spot = chain_spot
            self._load_calc_from_chain(symbol)
            if self.on_calc_option_ohlc_needed:
                await self.on_calc_option_ohlc_needed(symbol)
            if self.on_calc_keys_changed:
                await self.on_calc_keys_changed()

    def _load_calc_from_chain(self, symbol:str):
        """Populate calc.ce / calc.pe from the cached calc chain at current
        ITM-2 strikes (strikes derived from LOC spot)."""
        st = self.symbols.get(symbol)
        calc = self.calc_states.get(symbol)
        if not st or not calc or not calc.option_chain: return
        spot_ltp = st.spot.ltp or calc.chain_spot
        if not spot_ltp: return
        step = STRIKE_STEPS.get(symbol.upper(), 50)
        ce_s, pe_s = get_itm2_strikes(spot_ltp, symbol)
        calc.ce_strike = ce_s
        calc.pe_strike = pe_s
        calc.last_atm = round(round(spot_ltp / step) * step, 2)

        ce_row = calc.option_chain.get(ce_s, {})
        pe_row = calc.option_chain.get(pe_s, {})
        if not ce_row or not pe_row:
            strikes = sorted(calc.option_chain.keys())
            tolerance = step * 4
            if strikes:
                nearest_ce = min(strikes, key=lambda s: abs(s - ce_s))
                nearest_pe = min(strikes, key=lambda s: abs(s - pe_s))
                if abs(nearest_ce - ce_s) < tolerance:
                    ce_row = calc.option_chain.get(nearest_ce, {})
                    if ce_row: calc.ce_strike = nearest_ce
                if abs(nearest_pe - pe_s) < tolerance:
                    pe_row = calc.option_chain.get(nearest_pe, {})
                    if pe_row: calc.pe_strike = nearest_pe

        def _best(*vals):
            for v in vals:
                try:
                    fv = float(v)
                    if fv > 0: return fv
                except: pass
            return 0.0

        if ce_row.get("CE"):
            c = ce_row["CE"]
            new_key = c.get("key", "")
            key_changed = (new_key and new_key != calc.ce.instrument_key)
            chain_ltp   = _best(c.get("ltp"), c.get("close"))
            chain_close = _best(c.get("close"), c.get("ltp"))
            if key_changed or not calc.ce.ltp:
                calc.ce.ltp = chain_ltp
            if chain_close and (key_changed or not calc.ce.close):
                calc.ce.close = chain_close
            chain_high = _best(c.get("high"))
            chain_low  = _best(c.get("low"))
            if chain_high:
                if key_changed: calc.ce.high = chain_high
                else: calc.ce.high = max(calc.ce.high, chain_high) if calc.ce.high else chain_high
            elif key_changed or not calc.ce.high:
                calc.ce.high = chain_ltp
            if chain_low:
                if key_changed: calc.ce.low = chain_low
                else: calc.ce.low = min(calc.ce.low, chain_low) if calc.ce.low else chain_low
            elif key_changed or not calc.ce.low:
                calc.ce.low = chain_ltp
            calc.ce.oi = float(c.get("oi") or 0)
            calc.ce.iv = float(c.get("iv") or 0)
            calc.ce.instrument_key = new_key or calc.ce.instrument_key

        if pe_row.get("PE"):
            p = pe_row["PE"]
            new_key = p.get("key", "")
            key_changed = (new_key and new_key != calc.pe.instrument_key)
            chain_ltp   = _best(p.get("ltp"), p.get("close"))
            chain_close = _best(p.get("close"), p.get("ltp"))
            if key_changed or not calc.pe.ltp:
                calc.pe.ltp = chain_ltp
            if chain_close and (key_changed or not calc.pe.close):
                calc.pe.close = chain_close
            chain_high = _best(p.get("high"))
            chain_low  = _best(p.get("low"))
            if chain_high:
                if key_changed: calc.pe.high = chain_high
                else: calc.pe.high = max(calc.pe.high, chain_high) if calc.pe.high else chain_high
            elif key_changed or not calc.pe.high:
                calc.pe.high = chain_ltp
            if chain_low:
                if key_changed: calc.pe.low = chain_low
                else: calc.pe.low = min(calc.pe.low, chain_low) if calc.pe.low else chain_low
            elif key_changed or not calc.pe.low:
                calc.pe.low = chain_ltp
            calc.pe.oi = float(p.get("oi") or 0)
            calc.pe.iv = float(p.get("iv") or 0)
            calc.pe.instrument_key = new_key or calc.pe.instrument_key

        print(f"[CALC] {symbol} loaded expiry={calc.expiry} "
              f"CE@{calc.ce_strike}=ltp:{calc.ce.ltp} "
              f"key:{calc.ce.instrument_key[:20] if calc.ce.instrument_key else 'MISS'} | "
              f"PE@{calc.pe_strike}=ltp:{calc.pe.ltp} "
              f"key:{calc.pe.instrument_key[:20] if calc.pe.instrument_key else 'MISS'}")
        self._recalc_calc(symbol)

    def _recalc_calc(self, symbol:str):
        st = self.symbols.get(symbol)
        calc = self.calc_states.get(symbol)
        if not st or not calc: return
        spot_ltp = st.spot.ltp or calc.chain_spot
        if not spot_ltp: return
        res = calc_loc_25(
            spot_ltp,
            st.spot.close or spot_ltp,
            st.spot.high  or spot_ltp,
            st.spot.low   or spot_ltp,
            st.spot.open  or spot_ltp,
            calc.ce.effective_ltp,  calc.ce.close,
            calc.ce.effective_high, calc.ce.effective_low,
            calc.pe.effective_ltp,  calc.pe.close,
            calc.pe.effective_high, calc.pe.effective_low,
        )
        res.update({
            "symbol":     symbol,
            "spot_high":  round(st.spot.high or spot_ltp, 2),
            "spot_low":   round(st.spot.low  or spot_ltp, 2),
            "ce_strike":  calc.ce_strike,
            "pe_strike":  calc.pe_strike,
            "expiry":     calc.expiry,
            "ce_ltp":     round(calc.ce.effective_ltp, 2),
            "pe_ltp":     round(calc.pe.effective_ltp, 2),
            "ce_close":   round(calc.ce.close, 2),
            "pe_close":   round(calc.pe.close, 2),
            "ce_high":    round(calc.ce.effective_high, 2),
            "ce_low":     round(calc.ce.effective_low, 2),
            "pe_high":    round(calc.pe.effective_high, 2),
            "pe_low":     round(calc.pe.effective_low, 2),
            "ce_iv":      round(calc.ce.iv, 2),
            "pe_iv":      round(calc.pe.iv, 2),
        })
        calc.result = res

    def update_calc_option(self, symbol:str, opt_type:str,
                            ltp:float, close:float, high:float, low:float):
        """WS-tick update for a Calculator view's CE or PE."""
        calc = self.calc_states.get(symbol)
        if not calc: return
        opt = calc.ce if opt_type == "CE" else calc.pe
        if ltp and ltp > 0: opt.ltp = ltp
        if close and close > 0 and not opt.close: opt.close = close
        if high and high > 0:
            opt.high = max(opt.high, high) if opt.high else high
        if low and low > 0:
            opt.low = min(opt.low, low) if opt.low else low
        self._recalc_calc(symbol)
