import{useState,useEffect,useMemo}from'react'
import useStore from'../store/useStore'
import{fmt,ts2t,LOC_SYMS}from'../utils'
export default function History(){
  const{locHistory,setLocHistory,locResults}=useStore()
  const[sym,setSym]=useState("NIFTY")
  const[search,setSearch]=useState("")
  const hist=locHistory[sym]||[]
  // All symbols with LOC data — indices/MCX first, then stocks sorted
  const allSyms=useMemo(()=>{
    const fromLoc=Object.keys(locResults)
    const merged=[...new Set([...LOC_SYMS,...fromLoc])]
    const priority=new Set(LOC_SYMS)
    return merged.sort((a,b)=>{
      const ap=priority.has(a)?0:1, bp=priority.has(b)?0:1
      if(ap!==bp) return ap-bp
      return a.localeCompare(b)
    })
  },[locResults])
  const filteredSyms=search?allSyms.filter(s=>s.toLowerCase().includes(search.toLowerCase())):allSyms
  useEffect(()=>{
    if(!locHistory[sym]){
      fetch(`/api/loc-history/${sym}`).then(r=>r.json()).then(d=>setLocHistory(sym,d.history||[])).catch(()=>{})
    }
  },[sym])
  function exportCSV(){
    if(!hist.length)return
    const hdr="Time,LTP,CEP,BOP,PEP,UL,LL,Zone,Change,Different,CE Strike,PE Strike\n"
    const rows=hist.map(h=>[ts2t(h.ts),h.ltp,h.cep,h.bop,h.pep,h.ul,h.ll,h.zone,h.change,
      (h.different!=null&&h.different!==0)?(-h.different).toFixed(4):"",
      h.ce_strike,h.pe_strike].join(",")).join("\n")
    const blob=new Blob([hdr+rows],{type:"text/csv"})
    const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=`loc_${sym}.csv`;a.click()
  }
  return(
    <div style={{padding:"14px 18px 60px"}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12,flexWrap:"wrap",gap:8}}>
        <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:9,color:"#4a5568",textTransform:"uppercase",letterSpacing:".1em"}}>LOC Record — Every 1 Minute</div>
        <div style={{display:"flex",gap:8}}>
          <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Filter symbols..." style={{padding:"4px 9px",fontSize:10,width:100}}/>
          <select value={sym} onChange={e=>setSym(e.target.value)} style={{padding:"4px 9px",fontSize:10}}>
            {filteredSyms.map(s=><option key={s}>{s}</option>)}
          </select>
          <button onClick={exportCSV} style={{padding:"4px 10px",border:"1px solid #162033",background:"none",color:"#4a5568",borderRadius:4,fontSize:10}}>Export CSV</button>
        </div>
      </div>
      <div style={{overflowX:"auto",border:"1px solid #162033",borderRadius:8}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
          <thead>
            <tr>{["Time","LTP","CEP","BOP","PEP","UL","LL","Zone","Chg","Diff","CE Stk","PE Stk"].map(h=>(
              <th key={h} style={{padding:"7px 10px",textAlign:h==="Time"||h==="Zone"?"left":"right",fontFamily:"'JetBrains Mono',monospace",
                fontSize:8,textTransform:"uppercase",color:"#4a5568",background:"#0f1624",borderBottom:"1px solid #162033"}}>{h}</th>
            ))}</tr>
          </thead>
          <tbody>
            {hist.length===0?<tr><td colSpan={12} style={{padding:20,textAlign:"center",color:"#4a5568",fontFamily:"'JetBrains Mono',monospace",fontSize:10}}>No history yet — records every minute</td></tr>
            :hist.map((h,i)=>(
              <tr key={i} onMouseEnter={e=>e.currentTarget.style.background="rgba(255,255,255,.015)"}
                onMouseLeave={e=>e.currentTarget.style.background=""}>
                <td style={{padding:"7px 10px",fontFamily:"'JetBrains Mono',monospace",borderBottom:"1px solid rgba(22,32,51,.4)"}}>{ts2t(h.ts)}</td>
                <Td v={h.ltp} c="#4fc3f7"/><Td v={h.cep} c="#00e676"/><Td v={h.bop} c="#ffc94d"/>
                <Td v={h.pep} c="#ff3d5a"/><Td v={h.ul} c="#80deea"/><Td v={h.ll} c="#b39ddb"/>
                <td style={{padding:"7px 10px",borderBottom:"1px solid rgba(22,32,51,.4)"}}>
                  <span style={{padding:"2px 6px",borderRadius:2,fontSize:9,fontFamily:"'JetBrains Mono',monospace",fontWeight:700,
                    color:h.zone==="CALL"?"#00e676":h.zone==="PUT"?"#ff3d5a":"#ffc94d"}}>{h.zone}</span>
                </td>
                <Td v={h.change} c={h.change>=0?"#00e676":"#ff3d5a"}/>
                <td style={{padding:"7px 10px",textAlign:"right",borderBottom:"1px solid rgba(22,32,51,.4)",fontFamily:"'JetBrains Mono',monospace",
                  color:(h.different!=null&&h.different!==0)?((-h.different)<0?"#ff3d5a":"#00e676"):"#4a5568"}}>
                  {(h.different==null||isNaN(h.different)||h.different===0)?"—":(-h.different).toFixed(4)}
                </td>
                <Td v={h.ce_strike} d={0}/><Td v={h.pe_strike} d={0}/>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
const Td=({v,c,d=2})=><td style={{padding:"7px 10px",textAlign:"right",borderBottom:"1px solid rgba(22,32,51,.4)",fontFamily:"'JetBrains Mono',monospace",color:c||"#dde4ef"}}>{fmt(v,d)}</td>
