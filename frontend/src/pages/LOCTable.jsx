import{useState,useMemo}from'react'
import useStore from'../store/useStore'
import{fmt,absn,getMeta,SYM_TO_KEY}from'../utils'

const TH=({label,k,sort,setSort,num,sticky})=>(
  <th onClick={()=>setSort(s=>({k,d:s.k===k?-s.d:1}))}
    style={{padding:"7px 8px",textAlign:num?"right":"left",fontFamily:"'JetBrains Mono',monospace",fontSize:8,
      textTransform:"uppercase",letterSpacing:".06em",color:sort.k===k?"#4fc3f7":"#4a5568",
      background:"#0f1624",borderBottom:"1px solid #162033",whiteSpace:"nowrap",cursor:"pointer",userSelect:"none",
      position:"sticky",top:0,zIndex:sticky?4:3,
      ...(sticky?{left:0}:{})}}>
    {label}{sort.k===k?(sort.d>0?" ↑":" ↓"):""}
  </th>
)

const fmt4=v=>(v==null||isNaN(v))?"—":v.toFixed(4)
const fmt2=v=>(v==null||isNaN(v))?"—":fmt(v)

export default function LOCTable({onOpenChart}){
  const{locResults,spotKeys,flipTimes}=useStore()
  const[filter,setFilter]=useState("all")
  const[search,setSearch]=useState("")
  const[sort,setSort]=useState({k:"symbol",d:1})
  const rows=useMemo(()=>{
    let arr=Object.entries(locResults).map(([sym,loc])=>({sym,loc,meta:getMeta(sym),ft:flipTimes[sym]?.time||""})) 
    if(search){const q=search.toUpperCase();arr=arr.filter(r=>r.sym.toUpperCase().includes(q))}
    if(filter==="CALL"||filter==="PUT"||filter==="WAIT")arr=arr.filter(r=>r.loc.zone===filter)
    arr.sort((a,b)=>{
      if(sort.k==="flip_time")return sort.d*a.ft.localeCompare(b.ft)
      if(sort.k==="symbol")return sort.d*a.sym.localeCompare(b.sym)
      const av=a.loc[sort.k]||0,bv=b.loc[sort.k]||0
      return sort.d*(av-bv)
    })
    return arr
  },[locResults,filter,sort,search])

  const FC=({id,label})=>(
    <button onClick={()=>setFilter(id)} style={{padding:"3px 9px",borderRadius:4,fontSize:10,
      border:`1px solid ${filter===id?"#4fc3f7":"#162033"}`,
      background:filter===id?"rgba(79,195,247,.1)":"none",color:filter===id?"#4fc3f7":"#4a5568"}}>
      {label}
    </button>
  )

  const thP={sort,setSort}
  return(
    <div style={{padding:"14px 18px 60px"}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:10,flexWrap:"wrap",gap:8}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:9,color:"#4a5568",textTransform:"uppercase",letterSpacing:".1em"}}>LOC Analysis — All Symbols</div>
          <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search symbol..."
            style={{padding:"4px 10px",borderRadius:4,border:"1px solid #162033",background:"rgba(15,22,36,.8)",
              color:"#dde4ef",fontFamily:"'JetBrains Mono',monospace",fontSize:10,outline:"none",width:160}}/>
        </div>
        <div style={{display:"flex",gap:5}}><FC id="all" label="All"/><FC id="CALL" label="CALL"/><FC id="PUT" label="PUT"/><FC id="WAIT" label="WAIT"/></div>
      </div>
      <div style={{overflow:"auto",border:"1px solid #162033",borderRadius:8,maxHeight:"calc(100vh - 110px)"}}>
        <table style={{width:"max-content",minWidth:"100%",borderCollapse:"collapse",fontSize:10}}>
          <thead><tr>
            <TH label="Flip Time" k="flip_time" {...thP}/>
            {/* Spot Data */}
            <TH label="Symbol" k="symbol" {...thP} sticky/>
            <TH label="Spot LTP" k="ltp" {...thP} num/>
            <TH label="Spot Close" k="cp" {...thP} num/>
            <TH label="Chg%" k="pct" {...thP} num/>
            <TH label="Spot High" k="spot_high" {...thP} num/>
            <TH label="Spot Low" k="spot_low" {...thP} num/>
            {/* CE Data */}
            <TH label="CE ITM-2" k="ce_strike" {...thP} num/>
            <TH label="CE LTP" k="ce_ltp" {...thP} num/>
            <TH label="CE High" k="ce_high" {...thP} num/>
            <TH label="CE Low" k="ce_low" {...thP} num/>
            <TH label="CE Close" k="ce_close" {...thP} num/>
            {/* PE Data */}
            <TH label="PE ITM-2" k="pe_strike" {...thP} num/>
            <TH label="PE LTP" k="pe_ltp" {...thP} num/>
            <TH label="PE High" k="pe_high" {...thP} num/>
            <TH label="PE Low" k="pe_low" {...thP} num/>
            <TH label="PE Close" k="pe_close" {...thP} num/>
            {/* Ratio Formulas */}
            <TH label="CEH/SH" k="ceh_sh" {...thP} num/>
            <TH label="CEL/SL" k="cel_sl" {...thP} num/>
            <TH label="PEH/SL" k="peh_sl" {...thP} num/>
            <TH label="PEL/SH" k="pel_sh" {...thP} num/>
            <TH label="C-CE/S" k="c_ce_s" {...thP} num/>
            <TH label="C-PE/S" k="c_pe_s" {...thP} num/>
            {/* Move & CP */}
            <TH label="Call Move" k="call_move" {...thP} num/>
            <TH label="Put Move" k="put_move" {...thP} num/>
            <TH label="Call CP" k="call_cp" {...thP} num/>
            <TH label="Put CP" k="put_cp" {...thP} num/>
            {/* Diffs */}
            <TH label="Call CP Diff" k="call_cp_diff" {...thP} num/>
            <TH label="Put CP Diff" k="put_cp_diff" {...thP} num/>
            <TH label="Different" k="different" {...thP} num/>
            {/* Zone */}
            <TH label="Zone" k="zone" {...thP}/>
            {/* DSL / DSP */}
            <TH label="D.S.L" k="dsl" {...thP} num/>
            <TH label="D.S.P" k="dsp" {...thP} num/>
            {/* Levels */}
            <TH label="BOP" k="bop" {...thP} num/>
            <TH label="CEP" k="cep" {...thP} num/>
            <TH label="PEP" k="pep" {...thP} num/>
            <TH label="FUL" k="ful" {...thP} num/>
            <TH label="FLL" k="fll" {...thP} num/>
            <TH label="FUL Diff" k="ful_diff" {...thP} num/>
            <TH label="FLL Diff" k="fll_diff" {...thP} num/>
            <TH label="UL" k="ul" {...thP} num/>
            <TH label="LL" k="ll" {...thP} num/>
            {/* Extra */}
            <TH label="Distance" k="distance" {...thP} num/>
            <TH label="Dir" k="direction" {...thP}/>
          </tr></thead>
          <tbody>
            {rows.map(({sym,loc})=>{
              const zoneColor=loc.zone==="CALL"?"#00e676":loc.zone==="PUT"?"#ff3d5a":"#ffc94d"
              const zoneBg=loc.zone==="CALL"?"rgba(0,230,118,.12)":loc.zone==="PUT"?"rgba(255,61,90,.12)":"rgba(255,201,77,.1)"
              const chgColor=loc.change>=0?"#00e676":"#ff3d5a"
              const bb="1px solid rgba(22,32,51,.4)"
              const mono="'JetBrains Mono',monospace"
              const cellBase={padding:"7px 8px",borderBottom:bb,fontFamily:mono,fontSize:10}
              const numR={...cellBase,textAlign:"right"}
              return(
              <tr key={sym} onClick={()=>onOpenChart(spotKeys[sym]||SYM_TO_KEY[sym]||sym)} style={{cursor:"pointer"}}
                onMouseEnter={e=>e.currentTarget.style.background="rgba(255,255,255,.015)"}
                onMouseLeave={e=>e.currentTarget.style.background=""}>
                {/* Flip Time */}
                <td style={{...cellBase,color:"#ffc94d",whiteSpace:"nowrap"}}>
                  {flipTimes[sym] ? `${flipTimes[sym].time} (${flipTimes[sym].from||'?'}→${flipTimes[sym].to||'?'})` : "—"}
                </td>
                {/* Symbol - sticky */}
                <td style={{...cellBase,fontWeight:700,position:"sticky",left:0,background:"#0a0f1a",zIndex:1}}>{sym}</td>
                {/* Spot */}
                <td style={{...numR,color:"#4fc3f7"}}>{fmt2(loc.ltp)}</td>
                <td style={{...numR,color:"#4a5568"}}>{fmt2(loc.cp)}</td>
                <td style={{...numR,color:chgColor}}>{absn(loc.pct)}%</td>
                <td style={numR}>{fmt2(loc.spot_high)}</td>
                <td style={numR}>{fmt2(loc.spot_low)}</td>
                {/* CE */}
                <td style={{...numR,color:"#546e7a"}}>{fmt2(loc.ce_strike)}</td>
                <td style={{...numR,color:"#00e676"}}>{fmt2(loc.ce_ltp)}</td>
                <td style={numR}>{fmt2(loc.ce_high)}</td>
                <td style={numR}>{fmt2(loc.ce_low)}</td>
                <td style={numR}>{fmt2(loc.ce_close)}</td>
                {/* PE */}
                <td style={{...numR,color:"#546e7a"}}>{fmt2(loc.pe_strike)}</td>
                <td style={{...numR,color:"#ff3d5a"}}>{fmt2(loc.pe_ltp)}</td>
                <td style={numR}>{fmt2(loc.pe_high)}</td>
                <td style={numR}>{fmt2(loc.pe_low)}</td>
                <td style={numR}>{fmt2(loc.pe_close)}</td>
                {/* Ratios */}
                <td style={numR}>{fmt4(loc.ceh_sh)}</td>
                <td style={numR}>{fmt4(loc.cel_sl)}</td>
                <td style={numR}>{fmt4(loc.peh_sl)}</td>
                <td style={numR}>{fmt4(loc.pel_sh)}</td>
                <td style={numR}>{fmt4(loc.c_ce_s)}</td>
                <td style={numR}>{fmt4(loc.c_pe_s)}</td>
                {/* Moves */}
                <td style={numR}>{fmt4(loc.call_move)}</td>
                <td style={numR}>{fmt4(loc.put_move)}</td>
                <td style={numR}>{fmt4(loc.call_cp)}</td>
                <td style={numR}>{fmt4(loc.put_cp)}</td>
                {/* Diffs */}
                <td style={{...numR,color:loc.call_cp_diff<0?"#ff3d5a":"#00e676"}}>{fmt4(loc.call_cp_diff)}</td>
                <td style={{...numR,color:loc.put_cp_diff<0?"#ff3d5a":"#00e676"}}>{fmt4(loc.put_cp_diff)}</td>
                <td style={numR}>{fmt4(loc.different)}</td>
                {/* Zone */}
                <td style={cellBase}>
                  <span style={{padding:"2px 6px",borderRadius:2,fontSize:9,fontFamily:mono,fontWeight:700,
                    background:zoneBg,color:zoneColor}}>{loc.zone}</span>
                </td>
                {/* DSL / DSP */}
                <td style={numR}>{fmt4(loc.dsl)}</td>
                <td style={numR}>{fmt2(loc.dsp)}</td>
                {/* Levels */}
                <td style={{...numR,color:"#ffc94d"}}>{fmt2(loc.bop)}</td>
                <td style={{...numR,color:"#00e676"}}>{fmt2(loc.cep)}</td>
                <td style={{...numR,color:"#ff3d5a"}}>{fmt2(loc.pep)}</td>
                <td style={numR}>{fmt2(loc.ful)}</td>
                <td style={numR}>{fmt2(loc.fll)}</td>
                <td style={numR}>{fmt2(loc.ful_diff)}</td>
                <td style={numR}>{fmt2(loc.fll_diff)}</td>
                <td style={{...numR,color:"#80deea"}}>{fmt2(loc.ul)}</td>
                <td style={{...numR,color:"#b39ddb"}}>{fmt2(loc.ll)}</td>
                {/* Extra */}
                <td style={numR}>{fmt2(loc.distance)}</td>
                <td style={{...cellBase,color:loc.direction==="UP"?"#00e676":"#ff3d5a"}}>
                  {loc.direction==="UP"?"▲":"▼"}
                </td>
              </tr>
              )})}
          </tbody>
        </table>
      </div>
    </div>
  )
}
