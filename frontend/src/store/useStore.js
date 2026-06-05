import{create}from'zustand'
import{setMcxKeyMap}from'../utils'

const useStore=create((set,get)=>({
  authed:!!sessionStorage.getItem("raima_auth"),
  setAuthed:(v)=>{
    if(v)sessionStorage.setItem("raima_auth","1")
    else sessionStorage.removeItem("raima_auth")
    set({authed:v})
  },

  wsState:"CONNECTING", setWsState:(v)=>set({wsState:v}),
  mode:"live",          setMode:  (v)=>set({mode:v}),
  frames:0, tickCount:0, instrCount:0,

  marketData:{},
  marketStatus:{},
  locResults:{},
  expiryCache:{},
  locHistory:{},
  watchlists:{},
  commodityKeys:[],
  spotKeys:{},

  // Full snapshot on connect
  onSnapshot:(d)=>{
    const md = d.market_data || {}
    const sk = d.spot_keys || {}
    if(Object.keys(sk).length) setMcxKeyMap(sk)
    set({
      marketData:   {...md},
      marketStatus: d.market_status || {},
      locResults:   {...(d.loc_results || {})},
      expiryCache:  d.expiry_cache   || {},
      commodityKeys:d.commodity_keys || [],
      spotKeys:     sk,
      mode:         d.mode           || "live",
      instrCount:   Object.keys(md).length,
    })
  },

  // Live feed tick — FORCE new object references so React re-renders
  onLiveFeed:(msg)=>set(s=>{
    const feeds = msg.feeds || {}
    const hasFeeds = Object.keys(feeds).length > 0
    const newLoc   = msg.loc_results || {}
    const hasLoc   = Object.keys(newLoc).length > 0
    if(!hasFeeds && !hasLoc) return {}   // nothing changed

    // Only LOC recalculated (chain refresh, stale REST) — no price ticks this cycle
    if(!hasFeeds) return { locResults: { ...s.locResults, ...newLoc } }

    // Build completely new marketData object
    const md = Object.assign({}, s.marketData)

    for(const [k,v] of Object.entries(feeds)){
      const prev = md[k] || {}
      const prevLtpc = prev.ltpc || {}
      const prevEf   = prev.efeed || {}
      const newLtpc  = v.ltpc  || {}
      const newEf    = v.efeed || {}

      // ALWAYS preserve cp (prev close) — feed often doesn't send it
      const ltp = newLtpc.ltp || 0
      const cp  = newLtpc.cp  || prevLtpc.cp || prevEf.cp || 0

      // Merge efeed: keep REST snapshot day OHLC, update with live values
      const mergedEf = {
        ...prevEf,
        ...newEf,
        ltp,
        cp,
        // Only update open/high/low if new value is valid (>0)
        open: (newEf.open  > 0 ? newEf.open  : prevEf.open)  || ltp,
        high: (newEf.high  > 0 ? newEf.high  : prevEf.high)  || ltp,
        low:  (newEf.low   > 0 ? newEf.low   : prevEf.low)   || ltp,
      }

      // New object for this instrument — forces React to see change
      md[k] = {
        ...prev,
        ...v,
        ltpc:  { ...newLtpc, ltp, cp },
        efeed: mergedEf,
        display_name: v.display_name || prev.display_name || "",
        ts:    msg.currentTs || Date.now(),
      }
    }

    return {
      marketData: md,
      locResults: hasLoc ? { ...s.locResults, ...newLoc } : s.locResults,
      frames:    s.frames + 1,
      tickCount: s.tickCount + Object.keys(feeds).length,
      instrCount:Object.keys(md).length,
    }
  }),

  // Individual LOC update — pushed on every recalc
  onLiveLoc:(msg)=>set(s=>({
    locResults:{ ...s.locResults, [msg.symbol]: msg.loc },
  })),

  onMarketInfo:(msg)=>set({
    marketStatus: msg.marketInfo?.segmentStatus || {},
  }),

  setLocHistory:(sym,hist)=>set(s=>({locHistory:{...s.locHistory,[sym]:hist}})),
  setWatchlists:(wl)=>set({watchlists:wl}),
  addWatchlist:(name)=>set(s=>({watchlists:{...s.watchlists,[name]:[]}})),
  addToWatchlist:(name,key)=>set(s=>{
    const wl={...s.watchlists}
    wl[name]=[...(wl[name]||[])].filter(k=>k!==key).concat(key)
    return{watchlists:wl}
  }),
  removeFromWatchlist:(name,key)=>set(s=>{
    const wl={...s.watchlists}
    wl[name]=(wl[name]||[]).filter(k=>k!==key)
    return{watchlists:wl}
  }),
  deleteWatchlist:(name)=>set(s=>{
    const wl={...s.watchlists}
    delete wl[name]
    return{watchlists:wl}
  }),
}))
export default useStore
