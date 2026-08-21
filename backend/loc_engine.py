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
from .instruments import get_itm2_strikes, get_itm1_strikes, STRIKE_STEPS


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
    # Edge-trigger latch for CP-Flip detection (backend-authoritative — see
    # LOCEngine._recalc). True while the existing flip condition
    # (|call_cp_diff|>0.0020 AND |put_cp_diff|>0.0020 AND equal magnitude)
    # holds; a flip event fires only on the false→true transition so a
    # condition that stays true across many ticks emits exactly one event.
    cp_flip_active:bool=False
    # 1st ITM strikes/data — additive alongside the ce/pe (2nd ITM) pair above.
    itm1_ce:OptionData=field(default_factory=OptionData)
    itm1_pe:OptionData=field(default_factory=OptionData)
    itm1_ce_strike:float=0; itm1_pe_strike:float=0
    # 2nd OTM pair — additive, LTP-only (no OHLC tracked, per product decision).
    # OTM-2 CE is the CE contract at pe_strike (ATM+2*step — OTM for calls);
    # OTM-2 PE is the PE contract at ce_strike (ATM-2*step — OTM for puts).
    # Same strikes already tracked above, just the opposite option side —
    # no new strike math, just an opposite-side chain lookup.
    otm2_ce_ltp:float=0; otm2_pe_ltp:float=0
    otm2_ce_key:str=""; otm2_pe_key:str=""
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
        self.on_cp_flip:Optional[Callable]=None      # (symbol, flip_event) → notify on new CP flip
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
            itm1_ce_s, itm1_pe_s = get_itm1_strikes(ltp, symbol)
            # Coalesce ITM-2 and ITM-1 into a single on_option_keys_changed()/
            # _refresh_chain() dispatch per ATM shift instead of up to two —
            # both strikes shift from the same spot tick almost every time, so
            # firing the callback twice back-to-back just doubled load on the
            # Upstox WS send path for no benefit (observed in production logs
            # as an occasional unretrieved-task-exception when a transient
            # keepalive hiccup landed on the second, redundant send).
            any_keys_changed = False
            any_strike_changed = False
            if ce_s != st.ce_strike or pe_s != st.pe_strike:
                any_strike_changed = True
                prev_ce_key = st.ce.instrument_key
                prev_pe_key = st.pe.instrument_key
                prev_otm2_ce_key = st.otm2_ce_key
                prev_otm2_pe_key = st.otm2_pe_key
                st.ce_strike = ce_s
                st.pe_strike = pe_s
                print(f"[LOC] {symbol} ATM shift→{new_atm} CE:{ce_s} PE:{pe_s} (ITM1 CE:{itm1_ce_s} PE:{itm1_pe_s})")
                # Load from cached chain so the new strike's instrument_key is set.
                self._load_from_chain(symbol)
                # OTM-2 uses the same ce_strike/pe_strike (opposite side) —
                # additive, does not read or write anything _load_from_chain touches.
                self._load_otm2_from_chain(symbol)
                if (st.ce.instrument_key != prev_ce_key or
                        st.pe.instrument_key != prev_pe_key or
                        st.otm2_ce_key != prev_otm2_ce_key or
                        st.otm2_pe_key != prev_otm2_pe_key):
                    any_keys_changed = True
            if itm1_ce_s != st.itm1_ce_strike or itm1_pe_s != st.itm1_pe_strike:
                any_strike_changed = True
                prev_itm1_ce_key = st.itm1_ce.instrument_key
                prev_itm1_pe_key = st.itm1_pe.instrument_key
                st.itm1_ce_strike = itm1_ce_s
                st.itm1_pe_strike = itm1_pe_s
                self._load_itm1_from_chain(symbol)
                if (st.itm1_ce.instrument_key != prev_itm1_ce_key or
                        st.itm1_pe.instrument_key != prev_itm1_pe_key):
                    any_keys_changed = True
            if any_strike_changed:
                # If any instrument key actually changed, push the new WS
                # subscription immediately — don't wait up to 60 s for the
                # periodic refresh. Also bypass the chain throttle so fresh
                # LTP/close/high/low arrive as soon as possible.
                if any_keys_changed and self.on_option_keys_changed:
                    asyncio.create_task(self.on_option_keys_changed())
                asyncio.create_task(self._refresh_chain(symbol, force=any_keys_changed))
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

    def update_itm1_option_from_feed(self, symbol:str, opt_type:str,
                                      ltp:float, close:float, high:float, low:float):
        """Real-time CE/PE price update from WS feed for the 1st-ITM pair.
        Mirrors update_option_from_feed exactly, writing into itm1_ce/itm1_pe
        instead of ce/pe — kept as a separate method (not a shared branch)
        so the two pairs' OptionData targets can never be confused."""
        st=self.symbols.get(symbol)
        if not st: return
        opt = st.itm1_ce if opt_type=="CE" else st.itm1_pe
        if ltp and ltp>0:
            opt.ltp = ltp
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
            itm1_ce_s, itm1_pe_s = get_itm1_strikes(effective_spot, symbol)
            st.itm1_ce_strike = itm1_ce_s
            st.itm1_pe_strike = itm1_pe_s
            step = STRIKE_STEPS.get(symbol.upper(), 50)
            st.last_atm  = round(round(effective_spot / step) * step, 2)

            # Prime spot data from chain if WS hasn't arrived yet
            if not st.spot.ltp:
                st.spot.ltp   = chain_spot
                st.spot.close = chain_spot

        self._load_from_chain(symbol)
        self._load_itm1_from_chain(symbol)
        self._load_otm2_from_chain(symbol)

    def _load_strikes_from_chain(self, symbol:str, ce_strike:float, pe_strike:float,
                                  ce_opt:'OptionData', pe_opt:'OptionData', label:str='ITM2'):
        """Shared chain-lookup logic for a CE/PE strike pair: nearest-strike
        fallback when the exact strike isn't in the chain, MCX-illiquid jump
        to the nearest non-zero-LTP strike, and field mapping into the given
        OptionData targets. Used identically by the ITM-2 (ce/pe) and ITM-1
        (itm1_ce/itm1_pe) pairs so both get exactly the same correctness
        guarantees. Returns the (possibly nearest-strike-adjusted) strikes."""
        st=self.symbols.get(symbol)
        if not st or not st.option_chain: return ce_strike, pe_strike
        if not ce_strike:
            print(f"[LOC] {symbol}: no {label} strikes set, skipping chain load")
            return ce_strike, pe_strike

        ce_row = st.option_chain.get(ce_strike, {})
        pe_row = st.option_chain.get(pe_strike, {})

        strikes = sorted(st.option_chain.keys())
        step = STRIKE_STEPS.get(symbol.upper(), 50)

        if not ce_row or not pe_row:
            # Strikes not in chain — find nearest available strikes
            tolerance = step * 4
            if strikes:
                nearest_ce = min(strikes, key=lambda s: abs(s - ce_strike))
                nearest_pe = min(strikes, key=lambda s: abs(s - pe_strike))
                if abs(nearest_ce - ce_strike) < tolerance:
                    ce_row = st.option_chain.get(nearest_ce, {})
                    if ce_row: ce_strike = nearest_ce
                if abs(nearest_pe - pe_strike) < tolerance:
                    pe_row = st.option_chain.get(nearest_pe, {})
                    if pe_row: pe_strike = nearest_pe

        # MCX options are illiquid at ITM strikes — fall back to nearest strike
        # with non-zero LTP so the LOC engine has real data to work with.
        # Only jump to a different strike when BOTH ltp AND close are zero.
        # If close > 0 (prev-day settlement), the ITM strike is valid and
        # we must not replace pe_strike with a different strike in the history.
        if strikes:
            ce_ltp   = float((ce_row.get("CE") or {}).get("ltp",   0) or 0)
            ce_close = float((ce_row.get("CE") or {}).get("close", 0) or 0)
            if ce_ltp == 0 and ce_close == 0:
                for s in sorted(strikes, key=lambda x: abs(x - ce_strike)):
                    row = st.option_chain.get(s, {})
                    if float((row.get("CE") or {}).get("ltp", 0) or 0) > 0:
                        ce_row = row
                        ce_strike = s
                        break
            pe_ltp   = float((pe_row.get("PE") or {}).get("ltp",   0) or 0)
            pe_close = float((pe_row.get("PE") or {}).get("close", 0) or 0)
            if pe_ltp == 0 and pe_close == 0:
                for s in sorted(strikes, key=lambda x: abs(x - pe_strike)):
                    row = st.option_chain.get(s, {})
                    if float((row.get("PE") or {}).get("ltp", 0) or 0) > 0:
                        pe_row = row
                        pe_strike = s
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
        # accumulation and `not opt.close` guard caused yesterday's values
        # to persist into today's session whenever today's numbers were
        # smaller or a close had already been seeded.
        if ce_row.get("CE"):
            c = ce_row["CE"]
            new_ce_key = c.get("key", "")
            key_changed = (new_ce_key and new_ce_key != ce_opt.instrument_key)
            chain_ltp   = _best(c.get("ltp"), c.get("close"))
            chain_close = _best(c.get("close"), c.get("ltp"))
            # Only overwrite LTP from chain if instrument changed (ATM shift)
            # or we have no WS data yet. WS LTP is real-time and authoritative.
            if key_changed or not ce_opt.ltp:
                ce_opt.ltp = chain_ltp
            # Close: chain close_price is prev day's close for NSE, net_change-
            # derived for MCX. Always refresh when chain returns a value.
            if chain_close:
                ce_opt.close = chain_close
            # High/low: chain reports today's session extremes. Overwrite when
            # the chain has a value; fall back to ltp only when both chain is
            # empty and we have no prior high/low (first-load or ATM shift).
            chain_high = _best(c.get("high"))
            chain_low  = _best(c.get("low"))
            if chain_high:
                ce_opt.high = chain_high
            elif key_changed or not ce_opt.high:
                ce_opt.high = chain_ltp
            if chain_low:
                ce_opt.low = chain_low
            elif key_changed or not ce_opt.low:
                ce_opt.low = chain_ltp
            ce_opt.oi    = float(c.get("oi") or 0)
            ce_opt.iv    = float(c.get("iv") or 0)
            ce_opt.instrument_key = new_ce_key or ce_opt.instrument_key

        if pe_row.get("PE"):
            p = pe_row["PE"]
            new_pe_key = p.get("key", "")
            key_changed = (new_pe_key and new_pe_key != pe_opt.instrument_key)
            chain_ltp   = _best(p.get("ltp"), p.get("close"))
            chain_close = _best(p.get("close"), p.get("ltp"))
            if key_changed or not pe_opt.ltp:
                pe_opt.ltp = chain_ltp
            if chain_close:
                pe_opt.close = chain_close
            chain_high = _best(p.get("high"))
            chain_low  = _best(p.get("low"))
            if chain_high:
                pe_opt.high = chain_high
            elif key_changed or not pe_opt.high:
                pe_opt.high = chain_ltp
            if chain_low:
                pe_opt.low = chain_low
            elif key_changed or not pe_opt.low:
                pe_opt.low = chain_ltp
            pe_opt.oi    = float(p.get("oi") or 0)
            pe_opt.iv    = float(p.get("iv") or 0)
            pe_opt.instrument_key = new_pe_key or pe_opt.instrument_key

        print(f"[LOC] {symbol} {label} loaded: "
              f"CE@{ce_strike}=ltp:{ce_opt.ltp} close:{ce_opt.close} "
              f"eff:{ce_opt.effective_ltp} "
              f"key:{ce_opt.instrument_key[:20] if ce_opt.instrument_key else 'MISS'} | "
              f"PE@{pe_strike}=ltp:{pe_opt.ltp} close:{pe_opt.close} "
              f"eff:{pe_opt.effective_ltp} "
              f"key:{pe_opt.instrument_key[:20] if pe_opt.instrument_key else 'MISS'}")
        return ce_strike, pe_strike

    def _load_from_chain(self, symbol:str):
        """Load CE/PE data from chain at the ITM-2 strikes."""
        st=self.symbols.get(symbol)
        if not st or not st.option_chain: return
        if not st.ce_strike:
            print(f"[LOC] {symbol}: no strikes set, skipping chain load")
            return
        st.ce_strike, st.pe_strike = self._load_strikes_from_chain(
            symbol, st.ce_strike, st.pe_strike, st.ce, st.pe, label='ITM2')
        # Seed tick timestamps so REST fallback waits 30s after each chain load
        # before assuming the key is silent (gives WS feed a chance to deliver)
        now = time.time()
        st.ce_last_tick = now
        st.pe_last_tick = now
        self._recalc(symbol)

    def _load_itm1_from_chain(self, symbol:str):
        """Load CE/PE data from chain at the ITM-1 strikes. Additive sibling
        of _load_from_chain — does not touch ce_last_tick/pe_last_tick (those
        stay scoped to the ITM-2 pair the stale-option REST monitor tracks)."""
        st=self.symbols.get(symbol)
        if not st or not st.option_chain: return
        if not st.itm1_ce_strike:
            print(f"[LOC] {symbol}: no ITM1 strikes set, skipping chain load")
            return
        st.itm1_ce_strike, st.itm1_pe_strike = self._load_strikes_from_chain(
            symbol, st.itm1_ce_strike, st.itm1_pe_strike, st.itm1_ce, st.itm1_pe, label='ITM1')
        self._recalc(symbol)

    def _load_otm2_from_chain(self, symbol:str):
        """Load 2nd-OTM CE/PE LTP from the cached chain. OTM-2 CE = the CE
        contract at pe_strike (ATM+2*step — OTM for calls); OTM-2 PE = the
        PE contract at ce_strike (ATM-2*step — OTM for puts). Same strikes
        ce_strike/pe_strike already track, just the opposite option side —
        no new strike math. LTP-only (no OHLC) by product decision.

        Deliberately NOT routed through _load_strikes_from_chain — that
        function's nearest-strike-fallback and MCX-illiquid-jump logic
        exists for the ITM-2/ITM-1 pairs the 25 LOC formulas actually
        depend on. This stays a plain, isolated dict lookup so a bug here
        can never touch ce/pe/itm1_ce/itm1_pe state."""
        st = self.symbols.get(symbol)
        if not st or not st.option_chain: return
        ce_row = st.option_chain.get(st.pe_strike, {}).get("CE") or {}
        pe_row = st.option_chain.get(st.ce_strike, {}).get("PE") or {}
        if ce_row:
            ltp = float(ce_row.get("ltp") or 0)
            if ltp: st.otm2_ce_ltp = ltp
            st.otm2_ce_key = ce_row.get("key", "") or st.otm2_ce_key
        if pe_row:
            ltp = float(pe_row.get("ltp") or 0)
            if ltp: st.otm2_pe_ltp = ltp
            st.otm2_pe_key = pe_row.get("key", "") or st.otm2_pe_key
        self._recalc(symbol)

    def update_otm2_option_from_feed(self, symbol:str, opt_type:str, ltp:float):
        """Real-time LTP-only update for the OTM-2 pair from a WS tick.
        Mirrors update_option_from_feed's shape but writes only otm2_ce_ltp/
        otm2_pe_ltp — no high/low/close tracked for this pair."""
        st = self.symbols.get(symbol)
        if not st or not ltp or ltp <= 0: return
        if opt_type == "CE": st.otm2_ce_ltp = ltp
        else: st.otm2_pe_ltp = ltp
        self._recalc(symbol)

    def get_otm2_option_keys(self) -> list:
        keys = []
        for st in self.symbols.values():
            if st.otm2_ce_key: keys.append(st.otm2_ce_key)
            if st.otm2_pe_key: keys.append(st.otm2_pe_key)
        return [k for k in keys if k]

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
            # spot_ts is the REAL last spot-tick time (WS or REST-fallback
            # write into st.spot.ts) — distinct from "ts" below, which is
            # merely this recalc's wall-clock time and gets re-stamped by
            # unrelated CE/PE ticks even while spot itself is frozen. Do not
            # treat "ts" as proof of spot freshness.
            "spot_ts":    st.spot.ts,
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
            # 1st ITM pair — additive, does not alter any key above (2nd ITM).
            "itm1_ce_strike": st.itm1_ce_strike,
            "itm1_pe_strike": st.itm1_pe_strike,
            "itm1_ce_ltp":    round(st.itm1_ce.effective_ltp, 2),
            "itm1_pe_ltp":    round(st.itm1_pe.effective_ltp, 2),
            "itm1_ce_close":  round(st.itm1_ce.close, 2),
            "itm1_pe_close":  round(st.itm1_pe.close, 2),
            "itm1_ce_high":   round(st.itm1_ce.effective_high, 2),
            "itm1_ce_low":    round(st.itm1_ce.effective_low, 2),
            "itm1_pe_high":   round(st.itm1_pe.effective_high, 2),
            "itm1_pe_low":    round(st.itm1_pe.effective_low, 2),
            "itm1_ce_iv":     round(st.itm1_ce.iv, 2),
            "itm1_pe_iv":     round(st.itm1_pe.iv, 2),
            # 2nd OTM pair — additive, LTP-only reference fields (client request).
            "otm2_ce_ltp":  round(st.otm2_ce_ltp, 2),
            "otm2_pe_ltp":  round(st.otm2_pe_ltp, 2),
            "otm2_diff":    round(st.otm2_ce_ltp - st.otm2_pe_ltp, 2),
            "ts":        int(time.time() * 1000),
        })
        st.loc_result = res

        # ── CP-Flip detection (backend-authoritative) ──────────────────────
        # Runs on EVERY recalc (every tick), before the 300ms broadcast
        # throttle — this is the fix for flips that occur transiently between
        # two throttled frontend broadcasts and would otherwise never be
        # observed. Condition is unchanged from the prior frontend logic in
        # useStore.js's _applyFlips: both diffs must clear 0.0020 in
        # magnitude and be equal to each other within 1e-9. call_cp_diff/
        # put_cp_diff here are already the same r4()-rounded values the
        # frontend used to receive, so the comparison is bit-for-bit
        # equivalent to the old frontend check.
        c = abs(res["call_cp_diff"]); p = abs(res["put_cp_diff"])
        flip_ok = c > 0.0020 and p > 0.0020 and abs(c - p) < 1e-9
        if flip_ok and not st.cp_flip_active:
            dir_ = (1 if res["call_cp_diff"] > res["put_cp_diff"] else
                    -1 if res["call_cp_diff"] < res["put_cp_diff"] else 0)
            flip_event = {
                "type":     "cp_flip",
                "symbol":   symbol,
                "ts":       res["ts"],
                "distance": res["different"],
                "cltp":     res["ltp"],
                "dir":      dir_,
            }
            if self.on_cp_flip:
                ft = asyncio.create_task(self.on_cp_flip(symbol, flip_event))
                ft.add_done_callback(lambda f: f.exception() if not f.cancelled() and f.exception() else None)
        st.cp_flip_active = flip_ok

        if self.on_loc_update:
            t = asyncio.create_task(self.on_loc_update(symbol, res))
            t.add_done_callback(lambda f: f.exception() if not f.cancelled() and f.exception() else None)

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

    def get_itm1_option_keys(self) -> list:
        keys = []
        for st in self.symbols.values():
            if st.itm1_ce.instrument_key: keys.append(st.itm1_ce.instrument_key)
            if st.itm1_pe.instrument_key: keys.append(st.itm1_pe.instrument_key)
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
