import{useState,useEffect}from'react'
import useStore from'../store/useStore'
import{fmt,LOC_SYMS,SYM_TO_KEY}from'../utils'

const RES=[
  ["BOP","bop","#ffc94d"],["CEP","cep","#00e676"],["PEP","pep","#ff3d5a"],
  ["UL","ul","#80deea"],["LL","ll","#b39ddb"],["FUL","ful",""],["FLL","fll",""],
  ["FUL Diff","ful_diff",""],["FLL Diff","fll_diff",""],["DSL","dsl",""],["DSP","dsp",""],
  ["Call Move","call_move","#00e676"],["Put Move","put_move","#ff3d5a"],
  ["Call CP","call_cp","#00e676"],["Put CP","put_cp","#ff3d5a"],
  ["Call CP Diff","call_cp_diff","#00e676"],["Put CP Diff","put_cp_diff","#ff3d5a"],
  ["Different","different",""],["Distance","distance",""],
  ["CE IV","ce_iv","#ffab40"],["PE IV","pe_iv","#ffab40"],
]

export default function Calculator(){
  const{locResults,expiryCache,marketData,spotKeys}=useStore()
  const[sym,setSym]=useState("NIFTY")
  const[expiry,setExpiry]=useState("")

  // LOC result has everything we need
  const loc=locResults[sym]||{}
  const exInfo=expiryCache[sym]||{}

  // Spot data from market data — use dynamic spotKeys (has MCX), fallback to static SYM_TO_KEY
  const spotKey=spotKeys[sym]||SYM_TO_KEY[sym]||""
  const spotD=marketData[spotKey]||{}
  const ltpc=spotD.ltpc||{}; const ef=spotD.efeed||{}
  const ltp=ltpc.ltp||ef.ltp||loc.ltp||0
  const cp=ltpc.cp||ef.cp||loc.cp||0
  const high=ef.high||ltp
  const low=(ef.low&&ef.low>0)?ef.low:ltp
  const open_=ef.open||ltp
  const chg=ltp&&cp?(ltp-cp):0

  // CE/PE data comes directly from loc result (includes effective_ltp fallback)
  const ce_ltp  =loc.ce_ltp  ||0
  const ce_close=loc.ce_close||0
  const ce_high =loc.ce_high ||ce_ltp
  const ce_low  =loc.ce_low  ||ce_ltp
  const ce_iv   =loc.ce_iv   ||0
  const pe_ltp  =loc.pe_ltp  ||0
  const pe_close=loc.pe_close||0
  const pe_high =loc.pe_high ||pe_ltp
  const pe_low  =loc.pe_low  ||pe_ltp
  const pe_iv   =loc.pe_iv   ||0

  useEffect(()=>{
    if(exInfo.default&&!expiry) setExpiry(exInfo.default||"")
  },[exInfo.default])

  async function changeExpiry(e){
    const exp=e.target.value; setExpiry(exp)
    if(!exp||exp==="Loading...") return
    await fetch(`/api/expiry/${sym}`,{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({expiry:exp})
    })
  }

  const zoneColor=loc.zone==="CALL"?"#00e676":loc.zone==="PUT"?"#ff3d5a":"#ffc94d"

  return(
    <div style={{padding:"14px 18px 60px",maxWidth:1100}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12,flexWrap:"wrap",gap:8}}>
        <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:9,color:"#4a5568",textTransform:"uppercase",letterSpacing:".1em"}}>
          LOC Calculator — Fully Dynamic
        </div>
        <div style={{fontSize:9,padding:"3px 10px",borderRadius:4,
          background:"rgba(0,230,118,.1)",color:"#00e676",
          border:"1px solid rgba(0,230,118,.2)",fontFamily:"'JetBrains Mono',monospace"}}>
          ● AUTO — All data from Upstox API
        </div>
      </div>

      {/* Symbol & Expiry */}
      <div style={{background:"#0f1624",border:"1px solid #162033",borderRadius:8,padding:14,marginBottom:12}}>
        <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:9,color:"#4a5568",
          textTransform:"uppercase",marginBottom:10}}>Symbol & Expiry</div>
        <div style={{display:"flex",gap:12,flexWrap:"wrap",alignItems:"flex-end"}}>
          <div>
            <label style={{fontSize:9,color:"#4a5568",display:"block",marginBottom:3,fontFamily:"'JetBrains Mono',monospace"}}>Symbol</label>
            <select value={sym} onChange={e=>{setSym(e.target.value);setExpiry("")}}
              style={{padding:"6px 12px",fontSize:11,fontFamily:"'JetBrains Mono',monospace",
                background:"#060a0f",border:"1px solid #162033",color:"#dde4ef",minWidth:140,borderRadius:4}}>
              {LOC_SYMS.map(s=><option key={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label style={{fontSize:9,color:"#4a5568",display:"block",marginBottom:3,fontFamily:"'JetBrains Mono',monospace"}}>Expiry</label>
            <select value={expiry} onChange={changeExpiry}
              style={{padding:"6px 12px",fontSize:11,fontFamily:"'JetBrains Mono',monospace",
                background:"#060a0f",border:"1px solid #162033",color:"#dde4ef",minWidth:140,borderRadius:4}}>
              {(exInfo.all||[]).map(e=><option key={e}>{e}</option>)}
              {!(exInfo.all?.length)&&<option>Loading...</option>}
            </select>
          </div>
          {/* ITM-2 badge */}
          <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:10,color:"#4a5568",
            padding:"6px 12px",background:"#060a0f",border:"1px solid #162033",borderRadius:4}}>
            ITM-2: CE={fmt(loc.ce_strike||0,0)} | PE={fmt(loc.pe_strike||0,0)}
          </div>
          {exInfo.current_week&&<ExpBtn label="Curr Week" val={exInfo.current_week}
            active={expiry===exInfo.current_week}
            onClick={()=>changeExpiry({target:{value:exInfo.current_week}})}/>}
          {exInfo.next_week&&<ExpBtn label="Next Week" val={exInfo.next_week}
            active={expiry===exInfo.next_week}
            onClick={()=>changeExpiry({target:{value:exInfo.next_week}})}/>}
          {exInfo.current_month&&<ExpBtn label="Curr Month" val={exInfo.current_month}
            active={expiry===exInfo.current_month}
            onClick={()=>changeExpiry({target:{value:exInfo.current_month}})}/>}
          {exInfo.next_month&&<ExpBtn label="Next Month" val={exInfo.next_month}
            active={expiry===exInfo.next_month}
            onClick={()=>changeExpiry({target:{value:exInfo.next_month}})}/>}
        </div>
      </div>

      {/* Live Input Data */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10,marginBottom:12}}>
        <DataCard title="Spot Data" titleColor="#4fc3f7" rows={[
          ["LTP",  fmt(ltp),   "#4fc3f7"],
          ["Close",fmt(cp),    "#dde4ef"],
          ["High", fmt(high),  "#00e676"],
          ["Low",  fmt(low),   "#ff3d5a"],
          ["Change",`${chg>=0?"▲":"▼"} ${fmt(Math.abs(chg))}`,chg>=0?"#00e676":"#ff3d5a"],
        ]}/>
        <DataCard title={`CE ITM-2 — ${fmt(loc.ce_strike||0,0)}`} titleColor="#00e676" rows={[
          ["LTP",  fmt(ce_ltp),  "#00e676"],
          ["Close",fmt(ce_close),"#dde4ef"],
          ["High", fmt(ce_high), "#00e676"],
          ["Low",  fmt(ce_low),  "#ff3d5a"],
          ["IV",   fmt(ce_iv,1)+"%","#ffab40"],
        ]}/>
        <DataCard title={`PE ITM-2 — ${fmt(loc.pe_strike||0,0)}`} titleColor="#ff3d5a" rows={[
          ["LTP",  fmt(pe_ltp),  "#ff3d5a"],
          ["Close",fmt(pe_close),"#dde4ef"],
          ["High", fmt(pe_high), "#00e676"],
          ["Low",  fmt(pe_low),  "#ff3d5a"],
          ["IV",   fmt(pe_iv,1)+"%","#ffab40"],
        ]}/>
      </div>

      {/* Zone Banner */}
      {loc.zone&&(
        <div style={{padding:"10px 20px",borderRadius:6,
          fontFamily:"'JetBrains Mono',monospace",fontSize:16,fontWeight:700,
          textAlign:"center",marginBottom:12,
          background:`${zoneColor}18`,color:zoneColor,
          border:`1px solid ${zoneColor}30`}}>
          {loc.zone} ZONE
        </div>
      )}

      {/* All 25 Results */}
      {loc.bop&&(
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(130px,1fr))",gap:7}}>
          {RES.map(([label,key,color])=>(
            <div key={key} style={{background:"#0f1624",border:"1px solid #162033",
              borderRadius:6,padding:"8px 10px",textAlign:"center"}}>
              <div style={{fontSize:8,color:"#4a5568",fontFamily:"'JetBrains Mono',monospace",
                textTransform:"uppercase",marginBottom:3}}>{label}</div>
              <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:11,fontWeight:700,
                color:color||"#dde4ef"}}>{fmt(loc[key],4)}</div>
            </div>
          ))}
        </div>
      )}
      {!loc.bop&&(
        <div style={{padding:24,textAlign:"center",color:"#4a5568",fontFamily:"'JetBrains Mono',monospace",fontSize:11}}>
          Waiting for option chain data...
          {!spotKey&&<span> (Select a valid symbol)</span>}
        </div>
      )}
    </div>
  )
}

function DataCard({title,titleColor,rows}){
  return(
    <div style={{background:"#0f1624",border:"1px solid #162033",borderRadius:8,padding:12}}>
      <div style={{fontFamily:"'JetBrains Mono',monospace",fontSize:9,color:titleColor||"#4a5568",
        textTransform:"uppercase",letterSpacing:".08em",
        marginBottom:8,borderBottom:"1px solid #162033",paddingBottom:6}}>{title}</div>
      {rows.map(([k,v,c])=>(
        <div key={k} style={{display:"flex",justifyContent:"space-between",padding:"3px 0",
          fontFamily:"'JetBrains Mono',monospace",fontSize:10,
          borderBottom:"1px solid rgba(22,32,51,.3)"}}>
          <span style={{color:"#4a5568"}}>{k}</span>
          <span style={{fontWeight:600,color:c||"#dde4ef"}}>{v}</span>
        </div>
      ))}
    </div>
  )
}

function ExpBtn({label,val,active,onClick}){
  return(
    <button onClick={onClick} style={{padding:"5px 10px",borderRadius:4,fontSize:9,
      fontFamily:"'JetBrains Mono',monospace",cursor:"pointer",
      border:`1px solid ${active?"rgba(255,201,77,.4)":"#162033"}`,
      background:active?"rgba(255,201,77,.1)":"none",
      color:active?"#ffc94d":"#4a5568"}}>
      {label}
    </button>
  )
}
