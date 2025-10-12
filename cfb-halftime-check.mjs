/**
 * CFB-halftime-check.mjs
 * Watches ONE CFB game (TARGET_GAME_ID). Adaptive cadence pre-halftime; at halftime,
 * captures Half Score (and can be extended to pull Live Odds) and writes once to the sheet.
 *
 * ENV (required): GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT (raw JSON), TAB_NAME, TARGET_GAME_ID
 * ENV (optional): MAX_TOTAL_MIN=200, DEBUG_MODE=true (short sleeps ~12s)
 */
import { google } from "googleapis";
// import { chromium } from "playwright"; // keep available if you later add live odds

// ---------- ENV ----------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;
const TAB_NAME = process.env.TAB_NAME || "CFB";
const EVENT_ID = process.env.TARGET_GAME_ID;
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

// ---------- HTTP ----------
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "cfb-halftime-check/1.0" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const pick = (o,p)=>p.replace(/\[(\d+)\]/g,'.$1').split('.').reduce((a,k)=>a?.[k],o);

// ---------- ESPN parsing ----------
function parseStatus(sum){
  const a = pick(sum,"header.competitions.0.status") || {};
  const b = pick(sum,"competitions.0.status") || {};
  const s = a?.type?.shortDetail ? a : b;
  const type = s.type || {};
  return {
    shortDetail: type.shortDetail || "", // e.g. "Halftime", "4:20 - 3rd"
    state: type.state || "",             // "pre" | "in" | "post"
    period: Number(s.period ?? 0),
    displayClock: s.displayClock ?? "0:00",
  };
}

// Try header/competitions, competitions, then boxscore.teams
function getTeams(sum){
  let away, home;

  let cps = pick(sum,"header.competitions.0.competitors");
  if (!Array.isArray(cps) || cps.length<2) cps = pick(sum,"competitions.0.competitors");
  if (Array.isArray(cps) && cps.length>=2) {
    away = cps.find(c=>c.homeAway==="away");
    home = cps.find(c=>c.homeAway==="home");
  }
  if ((!away || !home) && Array.isArray(pick(sum,"boxscore.teams"))) {
    const bs = pick(sum,"boxscore.teams");
    away = bs.find(t=>t.homeAway==="away") || away;
    home = bs.find(t=>t.homeAway==="home") || home;
  }

  const tn = (t)=> t?.team?.shortDisplayName || t?.team?.abbreviation || t?.team?.displayName || "Team";
  const ts = (t)=> Number(t?.score ?? t?.statistics?.find?.(s=>s.name==="points")?.value ?? 0);

  const awayName = tn(away), homeName = tn(home);
  const awayScore = ts(away),  homeScore = ts(home);
  return { awayName, homeName, awayScore, homeScore };
}

const normalize=n=>String(n).trim().replace(/\./g,"").replace(/\bState\b/gi,"St").replace(/\s+/g," ");
const matchup=(a,h)=>`${normalize(a)} @ ${normalize(h)}`;
function kickoffISO(sum){ return pick(sum,"header.competitions.0.date") || pick(sum,"competitions.0.date") || null; }
function etDateStr(iso){
  if(!iso) return "";
  const dt=new Date(iso);
  const parts=new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(dt);
  const fx=t=>parts.find(x=>x.type===t)?.value||"";
  return `${fx("month")}/${fx("day")}/${fx("year")}`;
}
const ms=m=>Math.max(60_000,Math.round(m*60_000));
function decideSleep(p,clock){
  let mins=20;
  if(Number(p)===2){
    const m=/(\d{1,2}):(\d{2})/.exec(clock||""); if(m){ const left=+m[1]+(+m[2])/60; if(left<=10) mins=Math.max(1,Math.ceil(left*2)); }
  }
  if (DEBUG_MODE) mins=Math.min(mins,0.2); // ~12s
  return mins;
}

// ---------- Sheets ----------
async function sheetsClient(){
  const creds=JSON.parse(SA_JSON);
  const jwt=new google.auth.JWT(creds.client_email,null,creds.private_key,["https://www.googleapis.com/auth/spreadsheets"]);
  await jwt.authorize();
  return google.sheets({version:"v4",auth:jwt});
}
async function findRowIndex(sheets, dateET, mu){
  const res=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:P2000`});
  const rows=res.data.values||[];
  if(!rows.length) return -1;
  const hdr=rows[0].map(h=>h.toLowerCase());
  const iDate=hdr.findIndex(h=>h.startsWith("date"));
  const iMu=hdr.findIndex(h=>h==="matchup");
  if(iDate<0||iMu<0) return -1;
  for(let i=1;i<rows.length;i++){
    const r=rows[i]||[], d=(r[iDate]||"").trim(), m=(r[iMu]||"").trim();
    if(d===dateET && m===mu) return i;
  }
  return -1;
}
async function writeHalftime(sheets,rowIdx,vals){
  const hdr=(await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z1`})).data.values?.[0]||[];
  const map={
    "Status": vals.status,
    "Half Score": vals.halfScore,
    "Live Away Spread": vals.liveAwaySpread,
    "Live Away ML": vals.liveAwayMl,
    "Live Home Spread": vals.liveHomeSpread,
    "Live Home ML": vals.liveHomeMl,
    "Live Total": vals.liveTotal,
    "Away Spread": vals.liveAwaySpread,
    "Away ML": vals.liveAwayMl,
    "Home Spread": vals.liveHomeSpread,
    "Home ML": vals.liveHomeMl,
    "Total": vals.liveTotal,
  };
  const updates=[];
  Object.entries(map).forEach(([k,v])=>{
    if(v===undefined||v===null||v==="") return;
    const j=hdr.findIndex(h=>h.trim().toLowerCase()===k.toLowerCase());
    if(j>=0){
      const col=String.fromCharCode("A".charCodeAt(0)+j);
      const row=rowIdx+2;
      updates.push({range:`${TAB_NAME}!${col}${row}`, values:[[v]]});
    }
  });
  if(!updates.length){ console.log("No writable columns or nothing to write."); return 0; }
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ valueInputOption:"USER_ENTERED", data:updates }
  });
  return updates.length;
}

// ---------- MAIN ----------
(async()=>{
  const sheets=await sheetsClient();
  let total=0;
  for(;;){
    const sum=await fetchJson(`https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=${EVENT_ID}`);
    const st=parseStatus(sum);
    const tm=getTeams(sum);
    const dateET=etDateStr(kickoffISO(sum));
    const mu=matchup(tm.awayName, tm.homeName);

    console.log(`[${st.shortDetail || st.state || "unknown"}] ${mu}  Q${st.period||0}  (clock ${st.displayClock})`);

    const isHalftime = /halftime/i.test(st.shortDetail)
      || (st.state==="in" && Number(st.period)===2 && /^0?:?0{1,2}\b/.test(st.displayClock||""));

    if (isHalftime) {
      const halfScore = `${tm.awayScore}-${tm.homeScore}`;
      const rowIdx = await findRowIndex(sheets, dateET, mu);
      if (rowIdx<0){ console.log(`No matching row for Date=${dateET} & Matchup="${mu}".`); return; }
      const wrote = await writeHalftime(sheets,rowIdx,{status:"Halftime",halfScore});
      console.log(`✅ wrote ${wrote} cell(s).`);
      return;
    }

    const sleepM=decideSleep(st.period, st.displayClock);
    console.log(`Sleeping ${sleepM}m (${sleepM*60}s)…`);
    await new Promise(r=>setTimeout(r, ms(sleepM)));
    total+=sleepM;
    if(total>=MAX_TOTAL_MIN){ console.log("⏹ Reached MAX_TOTAL_MIN, exiting."); return; }
  }
})().catch(e=>{ console.error("Fatal:", e); process.exit(1); });
