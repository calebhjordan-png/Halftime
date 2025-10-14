import { google } from "googleapis";

/** ===== Config ===== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();           // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();      // "today" | "week"

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
  const r=await fetch(url,{headers:{"User-Agent":"orchestrator/2.3","Referer":"https://www.espn.com/"}});
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
  return a.find(o=>/espn\s*bet/i.test(o.provider?.name||"") || /espn\s*bet/i.test(o.provider?.displayName||"")) || a[0];
};
const numOrBlank=v=>{
  if(v==null)return"";
  const s=String(v).trim();
  const n=parseFloat(s.replace(/[^\d.+-]/g,""));
  if(!Number.isFinite(n))return"";
  return s.startsWith("+")?`+${n}`:`${n}`;
};
const toLowerMap=(arr=[])=>{const m={};arr.forEach((h,i)=>m[(h||"").trim().toLowerCase()]=i);return m;};
const keyOf=(dateStr,matchup)=>`${(dateStr||"").trim()}__${(matchup||"").trim()}`;

/** ===== Columns (Game ID first) ===== */
const COLS=[
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ===== Week label (simple) ===== */
function resolveWeekLabel(sb,iso,lg){
  if(normLeague(lg)==="nfl"){
    const w=sb?.week?.number;
    if(Number.isFinite(w))return`Week ${w}`;
  }else{
    const t=(sb?.week?.text||"").trim();
    if(t)return t;
  }
  return "Regular Season";
}

/** ===== Pregame spread assignment (team-true) ===== */
function assignTeamSpreads(odds, away, home){
  let awaySpread="", homeSpread="";
  if(!odds) return {awaySpread,homeSpread};

  // spread magnitude (abs value, from numeric or 'details')
  const spreadNum = (() => {
    if (odds.spread != null && `${odds.spread}`.trim() !== "") {
      const n = parseFloat(`${odds.spread}`.trim());
      if (!Number.isNaN(n)) return Math.abs(n);
    }
    const det = `${odds.details||""}`;
    const m = det.match(/([+-]?\d+(\.\d+)?)/);
    if (m) return Math.abs(parseFloat(m[1]));
    return NaN;
  })();

  if (Number.isNaN(spreadNum)) return {awaySpread,homeSpread};

  const favId = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || "");
  const homeId = String(home?.team?.id || "");

  if (favId) {
    if (favId === awayId) {
      awaySpread = `-${spreadNum}`; homeSpread = `+${spreadNum}`;
      return {awaySpread,homeSpread};
    }
    if (favId === homeId) {
      homeSpread = `-${spreadNum}`; awaySpread = `+${spreadNum}`;
      return {awaySpread,homeSpread};
    }
  }

  // Fallback: infer by team name in details
  const det = `${odds.details||""}`.toLowerCase();
  const awayName = (away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "").toLowerCase();
  const homeName = (home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "").toLowerCase();

  if (awayName && det.includes(awayName)) { awaySpread = `-${spreadNum}`; homeSpread = `+${spreadNum}`; return {awaySpread,homeSpread}; }
  if (homeName && det.includes(homeName)) { homeSpread = `-${spreadNum}`; awaySpread = `+${spreadNum}`; return {awaySpread,homeSpread}; }

  // Unknown favorite → don't guess
  return {awaySpread,homeSpread};
}

/** ===== Main ===== */
(async()=>{
  const CREDS=parseServiceAccount(CREDS_RAW);
  const auth=new google.auth.GoogleAuth({
    credentials:{client_email:CREDS.client_email,private_key:CREDS.private_key},
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets=google.sheets({version:"v4",auth});

  // Ensure tab + headers
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
  }else{
    const lower=header.map(h=>(h||"").toLowerCase());
    for(const want of COLS){ if(!lower.includes(want.toLowerCase())) header.push(want); }
    if(header.length!==vals[0].length){
      await sheets.spreadsheets.values.update({
        spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,
        valueInputOption:"RAW",requestBody:{values:[header]}
      });
    }
  }
  const hmap = toLowerMap(header);

  // dates
  const datesList = RUN_SCOPE==="week"
    ? Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)))
    : [yyyymmddInET(new Date())];

  let firstSB=null,events=[];
  for(const d of datesList){
    const sb=await fetchJson(scoreboardUrl(LEAGUE,d));
    if(!firstSB) firstSB=sb;
    events.push(...(Array.isArray(sb?.events)?sb.events:[]));
  }
  const seen=new Set(); events=events.filter(e=>e && !seen.has(e.id) && seen.add(e.id));
  console.log(`Events: ${events.length}`);

  // existing rows index
  const existing = vals.slice(1);
  const keyToRow = new Map();
  const idToRow  = new Map();
  existing.forEach((r,i)=>{
    const row=i+2;
    const gameId=(r[hmap["game id"]]||"").toString().trim();
    if(gameId) idToRow.set(gameId,row);
    keyToRow.set(keyOf(r[hmap["date"]], r[hmap["matchup"]]), row);
  });

  // append pregame rows
  const append=[];
  for(const e of events){
    const comp=e.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||"Home";
    const matchup=`${awayName} @ ${homeName}`;
    const dateET=fmtETDate(e.date);
    const key=keyOf(dateET,matchup);
    const gid=String(e.id);

    if(idToRow.has(gid) || keyToRow.has(key)) continue;

    const o=pickOdds(comp.odds||e.odds||[]);
    const {awaySpread,homeSpread}=assignTeamSpreads(o,away,home);
    const total = (o?.overUnder ?? o?.total) ?? "";
    const awayML = numOrBlank(o?.awayTeamOdds?.moneyLine ?? o?.awayTeamOdds?.moneyline);
    const homeML = numOrBlank(o?.homeTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyline);
    const week = resolveWeekLabel(firstSB,e.date,LEAGUE);

    append.push([
      gid, dateET, week, fmtETDate(e.date), matchup, "",
      awaySpread, awayML, homeSpread, homeML, String(total || ""),
      "", "", "", "", "", ""
    ]);
  }
  if(append.length){
    await sheets.spreadsheets.values.append({
      spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW",requestBody:{values:append}
    });
    console.log(`Added ${append.length} pregame row(s).`);
  }

  // finals sweep — use Game ID first, fallback to Date+Matchup
  const snap=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z`});
  const hdr2=snap.data.values?.[0]||header;
  const h2=toLowerMap(hdr2);
  const rowsNow=(snap.data.values||[]).slice(1);

  const idToRowNow=new Map();
  const keyToRowNow=new Map();
  rowsNow.forEach((r,i)=>{
    const row=i+2;
    const gid=(r[h2["game id"]]||"").toString().trim();
    if(gid) idToRowNow.set(gid,row);
    keyToRowNow.set(keyOf(r[h2["date"]], r[h2["matchup"]]), row);
  });

  let finals=0;
  for(const e of events){
    const comp=e.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||"Away";
    const homeName=home?.team?.shortDisplayName||"Home";
    const matchup=`${awayName} @ ${homeName}`;
    const dateET=fmtETDate(e.date);

    const status=(e.status?.type?.name||comp.status?.type?.name||"").toUpperCase();
    const isFinal=/FINAL/.test(status);
    if(!isFinal) continue;

    const row = idToRowNow.get(String(e.id)) || keyToRowNow.get(keyOf(dateET,matchup));
    if(!row) continue;

    const score=`${away?.score??""}-${home?.score??""}`;
    const updates=[];
    const add=(name,val)=>{
      const idx=h2[name.toLowerCase()]; if(idx==null) return;
      const col=String.fromCharCode("A".charCodeAt(0)+idx);
      updates.push({range:`${TAB_NAME}!${col}${row}`, values:[[val]]});
    };
    add("final score", score);
    add("status", "Final");

    if(updates.length){
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId:SHEET_ID,
        requestBody:{ valueInputOption:"RAW", data:updates }
      });
      finals++;
    }
  }

  console.log(`✅ Finals written: ${finals}`);
})().catch(e=>{console.error("❌ Orchestrator fatal:",e);process.exit(1);});
