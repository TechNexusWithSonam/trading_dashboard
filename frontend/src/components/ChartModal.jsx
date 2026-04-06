import { useState, useEffect, useRef, useCallback } from 'react'
import { createChart, CrosshairMode, LineStyle } from 'lightweight-charts'
import useStore from '../store/useStore'
import { fmt, pct, sign, arr, absn, getMeta, SYM_TO_KEY } from '../utils'

const LOC_LINES = [
  { k: "ul", label: "UL", color: "#80deea", dash: false, side: "right" },
  { k: "ful", label: "FUL", color: "#ff3d5a", dash: true, side: "left" },
  { k: "bop", label: "BOP", color: "#ffc94d", dash: false, side: "right" },
  { k: "fll", label: "FLL", color: "#00e676", dash: true, side: "left" },
  { k: "ll", label: "LL", color: "#b39ddb", dash: false, side: "right" },
]

const TF_OPTS = [
  { label: "1m", unit: "minutes", interval: 1, days: 1 },
  { label: "5m", unit: "minutes", interval: 5, days: 3 },
  { label: "15m", unit: "minutes", interval: 15, days: 5 },
  { label: "1h", unit: "hours", interval: 1, days: 14 },
  { label: "1d", unit: "days", interval: 1, days: 90 },
]

export default function ChartModal({ instrKey: rawKey, onClose }) {
  const { marketData, locResults, spotKeys } = useStore()
  const instrKey = rawKey.includes("|") ? rawKey : (spotKeys[rawKey] || SYM_TO_KEY[rawKey] || rawKey)
  const [tfIdx, setTfIdx] = useState(0)
  const [candles, setCandles] = useState([])
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState("")
  const chartRef = useRef(null); const ctRef = useRef(null)
  const seriesRef = useRef(null)
  const dummySeriesRef = useRef(null) // Ref for our invisible syncing series
  const liveCandleRef = useRef(null) // tracks current candle {time,open,high,low,close}

  const meta = getMeta(instrKey, marketData[instrKey])
  const sym = meta.s || rawKey
  const d = marketData[instrKey] || {}
  const ltpc = d.ltpc || {}; const ef = d.efeed || {}
  const ltp = ltpc.ltp || ef.ltp || 0
  const cp = ltpc.cp || ef.cp || 0
  const high = ef.high || ltp; const low = (ef.low && ef.low > 0) ? ef.low : ltp
  const open_ = ef.open || ltp
  const p = pct(ltp, cp); const dir = sign(p)
  const loc = locResults[sym] || null
  const tf = TF_OPTS[tfIdx]

  const loadCandles = useCallback(async () => {
    setLoading(true); setErr("")
    try {
      const today = new Date()
      const toDate = today.toISOString().slice(0, 10)
      const fromD = new Date(today)
      fromD.setDate(fromD.getDate() - tf.days)
      const fromDate = fromD.toISOString().slice(0, 10)

      let data = []
      const enc = encodeURIComponent(instrKey)
      if (tf.unit === "days") {
        const r = await fetch(`/api/ohlc-hist/${enc}/${tf.unit}/${tf.interval}/${toDate}/${fromDate}`,
          { headers: { Accept: "application/json" } })
        if (r.ok) { const j = await r.json(); data = j.candles || [] }
      } else {
        let r = await fetch(`/api/ohlc-hist/${enc}/${tf.unit}/${tf.interval}/${toDate}/${fromDate}`,
          { headers: { Accept: "application/json" } })
        if (r.ok) { const j = await r.json(); data = j.candles || [] }
        if (data.length === 0) {
          r = await fetch(`/api/ohlc-live/${enc}?tf=${tf.unit}/${tf.interval}`,
            { headers: { Accept: "application/json" } })
          if (r.ok) { const j = await r.json(); data = j.candles || [] }
        }
      }
      setCandles(data)
      liveCandleRef.current = null // reset live candle tracking on fresh load
      if (data.length === 0) setErr("No candle data available")
    } catch (e) {
      setErr("Failed to load: " + e.message)
    }
    setLoading(false)
  }, [instrKey, tfIdx])

  useEffect(() => { loadCandles() }, [loadCandles])

  useEffect(() => {
    if (!chartRef.current) return
    ctRef.current?.remove(); ctRef.current = null; seriesRef.current = null; dummySeriesRef.current = null; liveCandleRef.current = null

    const chart = createChart(chartRef.current, {
      layout: { background: { color: "#0b1018" }, textColor: "#4a5568" },
      grid: { vertLines: { visible: false }, horzLines: { color: "#0f1624" } },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#162033", textColor: "#4a5568", visible: true },
      
      // Keep left scale logically active but completely invisible
      leftPriceScale: { 
        visible: true, 
        borderColor: "transparent", 
        textColor: "transparent", // Hides the scale numbers
        drawTicks: false,         // Hides the tick marks
      },
      
      timeScale: {
        borderColor: "#162033", timeVisible: true,
        secondsVisible: tf.unit === "minutes" && tf.interval === 1
      },
      width: chartRef.current.clientWidth, height: 310,
    })
    ctRef.current = chart

    if (candles.length > 0) {
      // 1. Main Candlestick Series (Right Scale)
      const cs = chart.addCandlestickSeries({
        upColor: "#00e676", downColor: "#ff3d5a",
        wickUpColor: "#00e676", wickDownColor: "#ff3d5a",
        borderVisible: false,
      })
      seriesRef.current = cs

      // 2. Invisible Dummy Series (Left Scale) - Synchronizes left axis with the right axis
      const dummyCs = chart.addCandlestickSeries({
        priceScaleId: "left",
        upColor: "transparent", downColor: "transparent",
        wickUpColor: "transparent", wickDownColor: "transparent",
        borderVisible: false,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      dummySeriesRef.current = dummyCs

      const cdata = candles
        .filter(c => c.o && c.h && c.l && c.c)
        .map(c => ({ time: Math.floor(c.t / 1000), open: c.o, high: c.h, low: c.l, close: c.c }))
        .sort((a, b) => a.time - b.time)
        .filter((c, i, arr) => i === 0 || c.time !== arr[i - 1].time)

      if (cdata.length > 0) {
        cs.setData(cdata)
        dummyCs.setData(cdata) // Feed exactly the same data to the invisible left-scale series

        if (loc) {
          LOC_LINES.forEach(({ k, label, color, dash, side }) => {
            const v = loc[k]
            if (!v) return
            
            const opts = {
              color, 
              lineWidth: 1,
              lineStyle: dash ? LineStyle.Dashed : LineStyle.Solid,
              priceLineVisible: false,
              lastValueVisible: true, 
              title: label,
              crosshairMarkerVisible: false,
            }
            
            if (side === "left") opts.priceScaleId = "left"
            
            const ls = chart.addLineSeries(opts)
            ls.setData(cdata.map(d => ({ time: d.time, value: v })))
          })
        }
      }
    }

    const ro = new ResizeObserver(() => {
      if (chartRef.current && ctRef.current)
        ctRef.current.applyOptions({ width: chartRef.current.clientWidth })
    })
    if (chartRef.current) ro.observe(chartRef.current)
    return () => { ro.disconnect(); chart.remove(); ctRef.current = null }
  }, [candles, loc, tf])

  useEffect(() => {
    if (!seriesRef.current || !ltp || candles.length === 0) return
    if (tf.unit === "days") return // don't live-update daily candles

    const bucketSize = tf.interval * (tf.unit === "hours" ? 3600 : 60)
    const nowSec = Math.floor(Date.now() / 1000)
    const calcTime = Math.floor(nowSec / bucketSize) * bucketSize

    // Find last loaded candle time to avoid going backwards
    const lastLoaded = candles[candles.length - 1]
    const lastLoadedSec = lastLoaded ? Math.floor((lastLoaded.t || 0) / 1000) : 0
    const lastLoadedBucket = lastLoadedSec ? Math.floor(lastLoadedSec / bucketSize) * bucketSize : 0

    // Use max of client time vs last loaded time — prevents silent update() failure
    const candleTime = Math.max(calcTime, lastLoadedBucket)

    const prev = liveCandleRef.current
    let candle
    if (prev && prev.time === candleTime) {
      // Same candle period — only update high/low/close, keep open stable
      candle = {
        time: candleTime, open: prev.open,
        high: Math.max(prev.high, ltp), low: Math.min(prev.low, ltp), close: ltp
      }
    } else if (!prev && lastLoaded && lastLoadedBucket === candleTime) {
      // First tick after load, same bucket as last API candle — merge with its OHLC
      candle = {
        time: candleTime, open: lastLoaded.o || ltp,
        high: Math.max(lastLoaded.h || ltp, ltp),
        low: Math.min((lastLoaded.l && lastLoaded.l > 0 ? lastLoaded.l : ltp), ltp), close: ltp
      }
    } else {
      // New candle period
      candle = { time: candleTime, open: ltp, high: ltp, low: ltp, close: ltp }
    }
    liveCandleRef.current = candle
    try {
      seriesRef.current.update(candle)
      dummySeriesRef.current?.update(candle) // Keep left scale synced
    } catch (e) { }
  }, [ltp, tf])

  const zoneColor = loc?.zone === "CALL" ? "#00e676" : loc?.zone === "PUT" ? "#ff3d5a" : "#ffc94d"

  return (
    <div style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,.8)", zIndex: 300,
      display: "flex", alignItems: "center", justifyContent: "center", padding: 16
    }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div className="chart-modal-container" style={{
        background: "#0b1018", border: "1px solid #162033", borderRadius: 12,
        width: "95vw", maxWidth: 1100, maxHeight: "90vh", overflow: "hidden",
        display: "flex", flexDirection: "column"
      }}>

        {/* Header */}
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "12px 16px", borderBottom: "1px solid #162033", gap: 12, flexWrap: "wrap"
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
            <div>
              <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 15, fontWeight: 700, color: "#ffc94d" }}>
                {meta.n}
              </div>
              <div style={{ fontSize: 8, color: "#4a5568", fontFamily: "'JetBrains Mono',monospace" }}>
                {instrKey}
              </div>
            </div>
            <div>
              <div style={{
                fontFamily: "'JetBrains Mono',monospace", fontSize: 22, fontWeight: 700,
                color: ltp > 0 ? (dir === "up" ? "#00e676" : "#ff3d5a") : "#dde4ef"
              }}>
                {ltp > 0 ? fmt(ltp) : "—"}
              </div>
              {ltp > 0 && cp > 0 && <div style={{
                fontSize: 11, fontFamily: "'JetBrains Mono',monospace",
                color: dir === "up" ? "#00e676" : "#ff3d5a"
              }}>
                {arr(p)} {absn(p)}% ({dir === "up" ? "+" : ""}{fmt(ltp - cp)})
              </div>}
            </div>
            {high > 0 && <div style={{ display: "flex", gap: 12, fontSize: 9, fontFamily: "'JetBrains Mono',monospace", color: "#4a5568" }}>
              {[["O", open_], ["H", high, "#00e676"], ["L", low, "#ff3d5a"]].map(([l, v, c]) => (
                <div key={l}><span>{l} </span><span style={{ color: c || "#dde4ef", fontWeight: 600 }}>{fmt(v)}</span></div>
              ))}
            </div>}
            {loc?.zone && <div style={{
              padding: "4px 12px", borderRadius: 4, fontFamily: "'JetBrains Mono',monospace",
              fontSize: 11, fontWeight: 700, color: zoneColor,
              background: `${zoneColor}18`, border: `1px solid ${zoneColor}40`
            }}>
              {loc.zone}
            </div>}
          </div>
          <button onClick={onClose} style={{ background: "none", border: "none", color: "#4a5568", fontSize: 22, cursor: "pointer" }}>✕</button>
        </div>

        {/* Body */}
        <div className="chart-modal-body" style={{
          flex: 1, padding: 14, overflowY: "auto", display: "grid",
          gridTemplateColumns: "1fr 230px", gap: 14, minHeight: 0
        }}>
          <div>
            <div style={{ display: "flex", gap: 5, marginBottom: 8, alignItems: "center" }}>
              {TF_OPTS.map((t, i) => (
                <button key={t.label} onClick={() => setTfIdx(i)} style={{
                  padding: "3px 10px", borderRadius: 3, cursor: "pointer",
                  border: `1px solid ${tfIdx === i ? "rgba(255,201,77,.4)" : "#162033"}`,
                  background: tfIdx === i ? "rgba(255,201,77,.1)" : "none",
                  color: tfIdx === i ? "#ffc94d" : "#4a5568", fontSize: 10, fontFamily: "'JetBrains Mono',monospace"
                }}>
                  {t.label}
                </button>
              ))}
              {loading && <span style={{ fontSize: 9, color: "#4a5568", fontFamily: "'JetBrains Mono',monospace" }}>loading...</span>}
              <span style={{ fontSize: 9, color: "#4a5568", marginLeft: "auto", fontFamily: "'JetBrains Mono',monospace" }}>
                {candles.length} candles · {tf.days}d
              </span>
            </div>

            <div style={{ background: "#0b1018", border: "1px solid #162033", borderRadius: 6 }}>
              <div ref={chartRef} style={{ height: 310, width: "100%" }} />
            </div>

            {sym && loc && (
              <div style={{ marginTop: 8, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 7 }}>
                <div style={{ background: "rgba(255,61,90,.04)", border: "1px solid rgba(255,61,90,.15)", borderRadius: 6, padding: "8px 10px" }}>
                  <div style={{ fontSize: 9, color: "#ff3d5a", fontFamily: "'JetBrains Mono',monospace", marginBottom: 4 }}>FUL Diff</div>
                  <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 14, fontWeight: 700, color: "#ff3d5a" }}>{fmt(loc.ful_diff || 0)}</div>
                </div>
                <div style={{ background: "rgba(0,230,118,.04)", border: "1px solid rgba(0,230,118,.15)", borderRadius: 6, padding: "8px 10px" }}>
                  <div style={{ fontSize: 9, color: "#00e676", fontFamily: "'JetBrains Mono',monospace", marginBottom: 4 }}>FLL Diff</div>
                  <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 14, fontWeight: 700, color: "#00e676" }}>{fmt(loc.fll_diff || 0)}</div>
                </div>
              </div>
            )}
          </div>

          {/* LOC Panel */}
          <div style={{ background: "#060a0f", border: "1px solid #162033", borderRadius: 6, padding: 12, overflowY: "auto" }}>
            <div style={{
              fontFamily: "'JetBrains Mono',monospace", fontSize: 10, color: "#8a9bb5",
              textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 8, fontWeight: 700,
              borderBottom: "1px solid #1a2538", paddingBottom: 6
            }}>LOC Analysis</div>
            {loc ? (
              <>
                <div style={{
                  padding: "5px 10px", borderRadius: 4, textAlign: "center", margin: "0 0 8px",
                  fontFamily: "'JetBrains Mono',monospace", fontSize: 11, fontWeight: 700,
                  color: zoneColor, background: `${zoneColor}18`, border: `1px solid ${zoneColor}30`
                }}>
                  {loc.zone} ZONE
                </div>
                {[["UL", loc.ul, "#80deea"], ["CEP", loc.cep, "#00e676"], ["BOP", loc.bop, "#ffc94d"],
                ["PEP", loc.pep, "#ff3d5a"], ["LL", loc.ll, "#b39ddb"], ["LTP", loc.ltp, "#4fc3f7"]
                ].map(([k, v, c]) => (
                  <div key={k} style={{
                    display: "flex", justifyContent: "space-between", padding: "4px 0",
                    fontFamily: "'JetBrains Mono',monospace", fontSize: 11, borderBottom: "1px solid #1a2538"
                  }}>
                    <span style={{ color: "#8a9bb5", fontWeight: 600 }}>{k}</span>
                    <span style={{ fontWeight: 700, color: c }}>{fmt(v)}</span>
                  </div>
                ))}
                <div style={{ marginTop: 8 }}>
                  {[["FUL", loc.ful, 2, "#ff3d5a"], ["FLL", loc.fll, 2, "#00e676"],
                  ["FUL Diff", loc.ful_diff, 2, "#ff3d5a"], ["FLL Diff", loc.fll_diff, 2, "#00e676"],
                  ].map(([k, v, dp, c]) => (
                    <div key={k} style={{
                      display: "flex", justifyContent: "space-between", padding: "4px 0",
                      fontFamily: "'JetBrains Mono',monospace", fontSize: 10, borderBottom: "1px solid #1a2538"
                    }}>
                      <span style={{ color: "#8a9bb5", fontWeight: 600 }}>{k}</span>
                      <span style={{ fontWeight: 700, color: c }}>{fmt(v, dp)}</span>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <div style={{ color: "#4a5568", fontSize: 10, textAlign: "center", padding: "20px 0", lineHeight: 1.8 }}>
                No LOC data yet<br />
                <span style={{ fontSize: 9 }}>Waiting for spot+option data...</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}