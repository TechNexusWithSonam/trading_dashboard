import{useEffect,useRef}from'react'
import useStore from'../store/useStore'
import{setMcxKeyMap}from'../utils'

const BASE_DELAY_MS = 1000   // first reconnect attempt
const MAX_DELAY_MS  = 30000  // cap for exponential backoff
const STALE_MS      = 40000  // no message of any kind (incl. server ping every 15s) → assume dead

export default function useWebSocket(){
  const ws=useRef(null); const reconnectTimer=useRef(null); const staleTimer=useRef(null)
  const attempt=useRef(0)
  const{setWsState,onSnapshot,onLiveFeed,onLiveLoc,onMarketInfo,setMode}=useStore()

  function armStaleWatchdog(){
    clearTimeout(staleTimer.current)
    staleTimer.current=setTimeout(()=>{
      // No activity (not even a server heartbeat ping) for STALE_MS — the
      // socket is likely half-open (sleep/wake, network switch, idle proxy
      // drop) and the browser may never fire onclose/onerror on its own.
      // Force-close so onclose runs and reconnection kicks in.
      console.warn("WS stale — forcing reconnect")
      ws.current?.close()
    },STALE_MS)
  }

  function scheduleReconnect(){
    setWsState("RECONNECTING")
    clearTimeout(reconnectTimer.current)
    const delay=Math.min(BASE_DELAY_MS*(2**attempt.current),MAX_DELAY_MS)
    attempt.current++
    reconnectTimer.current=setTimeout(connect,delay)
  }

  function connect(){
    setWsState("CONNECTING")
    const proto=location.protocol==="https:"?"wss":"ws"
    const url=`${proto}://${location.host}/ws/feed`
    try{
      const w=new WebSocket(url); ws.current=w
      w.onopen=()=>{
        attempt.current=0
        setWsState("OPEN")
        armStaleWatchdog()
      }
      w.onmessage=e=>{
        armStaleWatchdog()
        try{
          const msg=JSON.parse(e.data)
          if(msg.type==="ping"){
            try{w.send(JSON.stringify({type:"pong",ts:msg.ts}))}catch{}
            return
          }
          if(msg.type==="snapshot")         onSnapshot(msg)
          else if(msg.type==="live_feed")   onLiveFeed(msg)
          else if(msg.type==="loc_update")  onLiveLoc(msg)
          else if(msg.type==="market_info") onMarketInfo(msg)
          else if(msg.type==="snapshot_update"){
            // Partial snapshot - merge market_data, update metadata only if present
            const s=useStore.getState()
            const merged={...s.marketData,...(msg.market_data||{})}
            const upd={marketData:merged, instrCount:Object.keys(merged).length}
            if(msg.commodity_keys?.length) upd.commodityKeys=msg.commodity_keys
            if(msg.spot_keys&&Object.keys(msg.spot_keys).length){upd.spotKeys=msg.spot_keys;setMcxKeyMap(msg.spot_keys)}
            if(msg.expiry_cache&&Object.keys(msg.expiry_cache).length) upd.expiryCache=msg.expiry_cache
            if(msg.loc_results&&Object.keys(msg.loc_results).length) upd.locResults={...s.locResults,...msg.loc_results}
            if(msg.market_status&&Object.keys(msg.market_status).length) upd.marketStatus=msg.market_status
            useStore.setState(upd)
          }
        }catch(err){console.warn("WS parse:",err)}
      }
      w.onclose=()=>{
        clearTimeout(staleTimer.current)
        scheduleReconnect()
      }
      w.onerror=()=>w.close()
    }catch{
      scheduleReconnect()
    }
  }

  useEffect(()=>{
    connect()
    // Tab was backgrounded (browsers throttle timers there, delaying the
    // stale watchdog) — on foregrounding, immediately verify the socket is
    // actually usable and reconnect right away if not, instead of waiting
    // for a throttled timer to eventually fire.
    const onVisible=()=>{
      if(document.visibilityState==="visible" && ws.current?.readyState!==WebSocket.OPEN){
        clearTimeout(reconnectTimer.current)
        attempt.current=0
        connect()
      }
    }
    document.addEventListener("visibilitychange",onVisible)
    return()=>{
      document.removeEventListener("visibilitychange",onVisible)
      clearTimeout(reconnectTimer.current); clearTimeout(staleTimer.current)
      ws.current?.close()
    }
  },[])
  return ws
}
