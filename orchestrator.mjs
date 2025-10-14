import { google } from "googleapis";

/** CONFIG **/
const SHEET_ID  = process.env.GOOGLE_SHEET_ID?.trim();
const CREDS_RAW = process.env.GOOGLE_SERVICE_ACCOUNT?.trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();

if(!SHEET_ID||!CREDS_RAW){console.error("Missing secrets.");process.exit(1);}

/** Helpers **/
function parseSA(raw){return raw.trim().startsWith("{")?JSON.parse(raw):JSON.parse(Buffer.from(raw,"base64").toString("utf8"));}
const ET="America/New_York";
const fmtETDate=d=>new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(d));
const yyyymmddInET=(d=new Date())=>{
  const p=new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const g=t=>p.find(x=>x.type===t)?.value||"";return`${g("year")}${g("month")}${g("day")}`;};
async function fetchJson(url){const r=await fetch(url,{headers:{"User-Agent":"orchestrator/2.0"}});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();}
const normLeague=l=>l==="college-football"||l==="ncaaf"?"college-football":"nfl";
const scoreboardUrl=(l,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/scoreboard?dates=${d}${normLeague(l)==="college-football"?"&groups=80&limit=300":""}`;
const pickOdds=o=>Array.isArray(o)?(o.find(x=>/espn bet/i.test(x.provider?.name||""))||o[0]):null;
const numOrBlank=v=>{if(v===0)return"0";if(v==null)return"";const s=String(v).trim();const n=parseFloat(s.replace(/[^\d.+-]/g,""));if(!Number.isFinite(n))return"";return s.startsWith("+")?`+${n}`:`${n}`;};

/** Columns (Game ID first, no Last Pre-Half Status) **/
const COLS=["Game ID","Date","Week","Status","Matchup","Final Score",
"Away Spread","Away ML","Home Spread","Home ML","Total",
"Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"];

(async()=>{
  const CREDS=parseSA(CREDS_RAW);
  const auth=new google.auth.GoogleAuth({credentials:{client_email:CREDS.client_email,private_key:CREDS.private_key},scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sheets=google.sheets({version:"v4",auth});
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const tabs=(meta.data.sheets||[]).map(s=>s.properties?.title);
  if(!tabs.includes(TAB_NAME)){
    await sheets.spreadsheets.batchUpdate({spreadsheetId:SHEET_ID,requestBody:{requests:[{addSheet:{properties:{title:TAB_NAME}}}]}});}
  const read=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z`});
  const vals=read.data.values||[];let header=vals[0]||[];
  if(header.length===0){
    await sheets.spreadsheets.values.update({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,valueInputOption:"RAW",requestBody:{values:[COLS]}});
    header=COLS.slice();
  }else{
    const need=COLS.filter(c=>!header.map(h=>h.toLowerCase()).includes(c.toLowerCase()));
    if(need.length){header=[...header,...need];await sheets.spreadsheets.values.update({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,valueInputOption:"RAW",requestBody:{values:[header]}});}
  }

  const map={};header.forEach((h,i)=>map[h.toLowerCase()]=i);
  const datesList=RUN_SCOPE==="week"?Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000))):[yyyymmddInET(new Date())];

  let firstSB=null,events=[];
  for(const d of datesList){const sb=await fetchJson(scoreboardUrl(LEAGUE,d));if(!firstSB)firstSB=sb;events=events.concat(sb?.events||[]);}
  const seen=new Set();events=events.filter(e=>!seen.has(e.id)&&seen.add(e.id));
  console.log(`Events found: ${events.length}`);

  const rows=vals.slice(1);const keyToRow=new Map();
  rows.forEach((r,i)=>keyToRow.set(`${r[map["date"]]}__${r[map["matchup"]]}`,i+2));

  const append=[];
  for(const e of events){
    const comp=e.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const matchup=`${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
    const dateET=fmtETDate(e.date);
    const key=`${dateET}__${matchup}`;
    if(keyToRow.has(key))continue;
    const o=pickOdds(comp.odds||e.odds||[]);
    let as="",hs="",tot="",aml="",hml="";
    if(o){tot=o.overUnder??o.total??"";const fav=String(o.favorite||o.favoriteTeamId||"");
      const sp=parseFloat(o.spread??o.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1]??NaN);
      if(!Number.isNaN(sp)&&fav){
        if(String(away?.team?.id)===fav){as=`-${Math.abs(sp)}`;hs=`+${Math.abs(sp)}`;}
        else if(String(home?.team?.id)===fav){hs=`-${Math.abs(sp)}`;as=`+${Math.abs(sp)}`;}
      }
      const ml={awayML:numOrBlank(o.awayTeamOdds?.moneyLine??""),homeML:numOrBlank(o.homeTeamOdds?.moneyLine??"")};
      aml=ml.awayML;hml=ml.homeML;
    }
    const week=(firstSB?.week?.text||"").trim()||`Week ${firstSB?.week?.number||""}`;
    append.push([String(e.id),dateET,week,fmtETDate(e.date),matchup,"",
      as,aml,hs,hml,String(tot),"","","","","",""]);
  }
  if(append.length){
    await sheets.spreadsheets.values.append({
      spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW",requestBody:{values:append}});
    console.log(`✅ Added ${append.length} pregame row(s).`);
  }

  // finals sweep
  const fresh=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z`});
  const hdr2=fresh.data.values?.[0]||header;const h2={};hdr2.forEach((h,i)=>h2[h.toLowerCase()]=i);
  const current=(fresh.data.values||[]).slice(1);const rowByKey=new Map();
  current.forEach((r,i)=>rowByKey.set(`${r[h2["date"]]}__${r[h2["
