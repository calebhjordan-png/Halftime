import { google } from "googleapis";

/** ===== Config ===== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();

if (!SHEET_ID || !CREDS_RAW) {
  console.error("Missing secrets.");
  process.exit(1);
}

/** ===== Helpers ===== */
function parseServiceAccount(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET_TZ = "America/New_York";
function fmtETDate(dLike) {
  return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(dLike));
}
function yyyymmddInET(d=new Date()){
  const p=new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const g=k=>p.find(x=>x.type===k)?.value||"";return`${g("year")}${g("month")}${g("day")}`;
}
async function fetchJson(url){
  const r=await fetch(url,{headers:{"User-Agent":"orchestrator/2.2"}});
  if(!r.ok)throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const normLeague=l=>(l==="ncaaf"||l==="college-football")?"college-football":"nfl";
const scoreboardUrl=(l,d)=>{
  const lg=normLeague(l);
  const extra=lg==="college-football"?"&groups=80&limit=300":"";
  return`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const pickOdds=a=>{
  if(!Array.isArray(a)||!a.length)return null;
  return a.find(o=>/espn\s*bet/i.test(o.provider?.name||""))||a[0];
};
const numOrBlank=v=>{
  if(v==null)return"";
  const s=String(v).trim();
  const n=parseFloat(s.replace(/[^\d.+-]/g,""));
  if(!Number.isFinite(n))return"";
  return s.startsWith("+")?`+${n}`:`${n}`;
};

/** ===== Columns ===== */
const COLS=[
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ===== Week Label ===== */
function resolveWeekLabel(sb,iso,lg){
  if(normLeague(lg)==="nfl"){
    const w=sb?.week?.number;
    if(Number.isFinite(w))return`Week ${w}`;
  }else{
    const t=(sb?.week?.text||"").trim();
    if(t)return t;
  }
  return"Regular Season";
}

/** ===== Main ===== */
(async()=>{
  const CREDS=parseServiceAccount(CREDS_RAW);
  const auth=new google.auth.GoogleAuth({
    credentials:{client_email:CREDS.client_email,private_key:CREDS.private_key},
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets=google.sheets({version:"v4",auth});

  // headers
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const tabs=(meta.data.sheets||[]).map(s=>s.properties?.title);
  if(!tabs.includes(TAB_NAME)){
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId:SHEET_ID,
      requestBody:{requests:[{addSheet:{properties:{title:TAB_NAME}}}]}
    });
  }
  const read=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z`});
  const vals=read.data.values||[];
  let header=vals[0]||[];
  if(header.length===0){
    await sheets.spreadsheets.values.update({
      spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW",requestBody:{values:[COLS]}
    });
    header=COLS.slice();
  }

  const hmap={};header.forEach((h,i)=>hmap[(h||"").toLowerCase()]=i);

  const datesList=RUN_SCOPE==="week"
    ?Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)))
    :[yyyymmddInET(new Date())];

  let firstSB=null,events=[];
  for(const d of datesList){
    const sb=await fetchJson(scoreboardUrl(LEAGUE,d));
    if(!firstSB)firstSB=sb;
    events.push(...(sb?.events||[]));
  }
  const seen=new Set();events=events.filter(e=>!seen.has(e.id)&&seen.add(e.id));
  console.log(`Events: ${events.length}`);

  const rows=vals.slice(1);
  const keyToRow=new Map();
  rows.forEach((r,i)=>{
    const k=`${r[hmap["date"]]}__${r[hmap["matchup"]]}`;
    keyToRow.set(k,i+2);
  });

  const append=[];
  for(const e of events){
    const comp=e.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||"Home";
    const matchup=`${awayName} @ ${homeName}`;
    const dateET=fmtETDate(e.date);
    const key=`${dateET}__${matchup}`;
    if(keyToRow.has(key))continue;

    const o=pickOdds(comp.odds||e.odds||[]);
    let as="",hs="",tot="",aml="",hml="";
    if(o){
      tot=o.overUnder??o.total??"";
      const favId=String(o.favorite||o.favoriteTeamId||"");
      const spreadVal=parseFloat(o.spread??o.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1]??NaN);
      if(!Number.isNaN(spreadVal)){
        // assign by favorite
        if(favId){
          if(String(away?.team?.id)===favId){as=`-${Math.abs(spreadVal)}`;hs=`+${Math.abs(spreadVal)}`;}
          else if(String(home?.team?.id)===favId){hs=`-${Math.abs(spreadVal)}`;as=`+${Math.abs(spreadVal)}`;}
        }else{
          // fallback: use sign in details text
          const det=(o.details||"").toLowerCase();
          if(det.includes(awayName.toLowerCase())){as=`-${Math.abs(spreadVal)}`;hs=`+${Math.abs(spreadVal)}`;}
          else if(det.includes(homeName.toLowerCase())){hs=`-${Math.abs(spreadVal)}`;as=`+${Math.abs(spreadVal)}`;}
        }
      }
      aml=numOrBlank(o.awayTeamOdds?.moneyLine??"");
      hml=numOrBlank(o.homeTeamOdds?.moneyLine??"");
    }
    const week=resolveWeekLabel(firstSB,e.date,LEAGUE);
    append.push([String(e.id),dateET,week,fmtETDate(e.date),matchup,"",
      as,aml,hs,hml,String(tot),"","","","","",""]);
  }
  if(append.length){
    await sheets.spreadsheets.values.append({
      spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW",requestBody:{values:append}
    });
    console.log(`Added ${append.length} pregame row(s).`);
  }

  // Finals sweep
  const snap=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z`});
  const hdr2=snap.data.values?.[0]||header;
  const h2={};hdr2.forEach((h,i)=>h2[h.toLowerCase()]=i);
  const rowsNow=(snap.data.values||[]).slice(1);
  const rowByKey=new Map();
  rowsNow.forEach((r,i)=>rowByKey.set(`${r[h2["date"]]}__${r[h2["matchup"]]}`,i+2));

  let finals=0;
  for(const e of events){
    const comp=e.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const matchup=`${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
    const dateET=fmtETDate(e.date);
    const row=rowByKey.get(`${dateET}__${matchup}`);
    if(!row)continue;

    const status=(e.status?.type?.name||comp.status?.type?.name||"").toUpperCase();
    if(!/FINAL/.test(status))continue;

    const score=`${away?.score??""}-${home?.score??""}`;
    const updates=[];
    const add=(name,val)=>{
      const idx=h2[name.toLowerCase()];if(idx==null)return;
      const col=String.fromCharCode("A".charCodeAt(0)+idx);
      updates.push({range:`${TAB_NAME}!${col}${row}`,values:[[val]]});
    };
    add("final score",score);
    add("status","Final");
    if(updates.length){
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId:SHEET_ID,
        requestBody:{valueInputOption:"RAW",data:updates}
      });
      finals++;
    }
  }
  console.log(`✅ Finals written: ${finals}`);
})().catch(e=>{console.error("❌ Orchestrator fatal:",e);process.exit(1);});
