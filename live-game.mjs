/* live-game.mjs — Status + current score + Live Odds (strict Game ID)
   - Writes:
       Status
       Half Score           -> current score (away-home) on every tick
       Live Away Spread     -> number (e.g. +13.5 / -3.5)
       Live Away ML         -> number (e.g. +140 / -180)
       Live Home Spread     -> number
       Live Home ML         -> number
       Live Total           -> number (over/under)
   - ONESHOT=true : single fetch/write and exit
   - DEBUG_ODDS=true : log raw odds blocks for mapping/verification
*/
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;  // JSON string for SA
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();

const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE  = String(process.env.DEBUG_MODE  || "").toLowerCase()==="true";
const ONESHOT     = String(process.env.ONESHOT     || "").toLowerCase()==="true";
const DEBUG_ODDS  = String(process.env.DEBUG_ODDS  || "").toLowerCase()==="true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* ---------- ESPN helpers ---------- */
const pick = (o,p)=>p.replace(/\[(\d+)\]/g,'.$1').split('.').reduce((a,k)=>a?.[k],o);

async function fetchJson(url, tries=3) {
  for (let i=1;i<=tries;i++){
    try{
      const r = await fetch(url,{headers:{"User-Agent":"halftime-live/1.2"}});
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    }catch(e){
      if(i===tries) throw e;
      await new Promise(r=>setTimeout(r, 500*i));
    }
  }
}
const summaryUrl = id => `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${id}`;

function parseStatus(sum){
  const a = pick(sum,"header.competitions.0.status") || {};
  const b = pick(sum,"competitions.0.status") || {};
  const s = a?.type?.shortDetail ? a : b;
  const t = s.type || {};
  return {
    shortDetail: t.shortDetail || "",
    state: t.state || "",
    name: (t.name || "").toUpperCase(),
    period: Number(s.period ?? 0),
    displayClock: s.displayClock ?? "0:00",
  };
}

function getTeams(sum){
  let away,home;
  let cps = pick(sum,"header.competitions.0.competitors");
  if(!Array.isArray(cps)||cps.length<2) cps = pick(sum,"competitions.0.competitors");
  if(Array.isArray(cps)&&cps.length>=2){
    away = cps.find(c=>c.homeAway==="away");
    home = cps.find(c=>c.homeAway==="home");
  }
  const tn=t=>t?.team?.shortDisplayName||t?.team?.abbreviation||t?.team?.displayName||"Team";
  const keys=t=>{
    const n1=t?.team?.shortDisplayName||"";
    const n2=t?.team?.abbreviation||"";
    const n3=t?.team?.displayName||"";
    return [n1,n2,n3].filter(Boolean).map(x=>x.toLowerCase());
  };
  const ts=t=>Number(t?.score ?? 0);
  return {
    awayName: tn(away), homeName: tn(home),
    awayKeys: keys(away), homeKeys: keys(home),
    awayScore: ts(away), homeScore: ts(home)
  };
}

/* ----- Live Odds extraction ----- */
function coerceNum(v){
  if (v===null||v===undefined||v==="") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Decide spreads from favorite flags / details
function spreadsFrom(spread, ctx){
  if (!Number.isFinite(spread)) return {awaySpread:null, homeSpread:null};
  const abs = Math.abs(spread);
  if (ctx.awayFav === true && ctx.homeFav !== true) {
    return { awaySpread: -abs, homeSpread: +abs };
  }
  if (ctx.homeFav === true && ctx.awayFav !== true) {
    return { awaySpread: +abs, homeSpread: -abs };
  }
  // Try details text to infer favorite
  const d = (ctx.detail||"").toLowerCase();
  const hit = (keys)=>keys?.some(k=>k && d.includes(k));
  if (hit(ctx.awayKeys) && !hit(ctx.homeKeys)) return { awaySpread: -abs, homeSpread: +abs };
  if (hit(ctx.homeKeys) && !hit(ctx.awayKeys)) return { awaySpread: +abs, homeSpread: -abs };
  // Fallback: assume positive for away, negative for home (won't bias favorite)
  return { awaySpread: +abs, homeSpread: -abs };
}

function extractOdds(sum, tm){
  // Try competitions[0].odds[0]
  let o = pick(sum,"competitions.0.odds.0") || pick(sum,"header.competitions.0.odds.0");
  let src = "competitions.odds[0]";
  if (DEBUG_ODDS && o) console.log("DEBUG_ODDS competitions.odds[0]:", JSON.stringify(o, null, 2));

  // If not useful, try pickcenter[0]
  if (!o || (o.spread==null && !o.awayTeamOdds && !o.homeTeamOdds && o.overUnder==null)){
    const pc = (pick(sum,"pickcenter")||[])[0];
    if (DEBUG_ODDS && pc) console.log("DEBUG_ODDS pickcenter[0]:", JSON.stringify(pc, null, 2));
    if (pc) { o = pc; src = "pickcenter[0]"; }
  }

  if (!o) return { src:null, awaySpread:null, awayML:null, homeSpread:null, homeML:null, total:null, detail:null };

  const detail = o.details || o.detail || "";
  const overUnder = coerceNum(o.overUnder ?? o.total);
  const spread = coerceNum(o.spread);

  const awayFav = o.awayTeamOdds?.favorite === true || o.awayTeamOdds?.underdog === false;
  const homeFav = o.homeTeamOdds?.favorite === true || o.homeTeamOdds?.underdog === false;

  const awayML = coerceNum(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline);
  const homeML = coerceNum(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline);

  const { awaySpread, homeSpread } = spreadsFrom(spread, {
    awayFav, homeFav, detail, awayKeys: tm.awayKeys, homeKeys: tm.homeKeys
  });

  return { src, awaySpread, awayML, homeSpread, homeML, total: overUnder, detail };
}

/* ---------- Sheets helpers ---------- */
async function sheetsClient(){
  const creds=JSON.parse(SA_JSON);
  const jwt=new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  await jwt.authorize();
  return google.sheets({version:"v4",auth:jwt});
}
function colMap(hdr=[]){const m={}; hdr.forEach((h,i)=>m[(h||"").trim().toLowerCase()]=i); return m;}
function A1(colIndex){ return String.fromCharCode("A".charCodeAt(0) + colIndex); }

async function findRowByGameId(sheets){
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:A2000` });
  const col = res.data.values || [];
  for (let i=1;i<col.length;i++){
    if ((col[i][0]||"").toString().trim() === EVENT_ID) return i+1; // 1-based row number
  }
  return -1;
}

async function writeValues(sheets,rowNumber,kv){
  const header = (await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`
  })).data.values?.[0] || [];
  const h = colMap(header);
  const data = [];
  for (const [name,val] of Object.entries(kv)){
    if (val == null && val !== "") continue; // allow empty string clears
    const j = h[name.toLowerCase()];
    if (j == null) continue; // only to existing columns
    data.push({ range: `${TAB_NAME}!${A1(j)}${rowNumber}`, values: [[val]] });
  }
  if (!data.length) return 0;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data }
  });
  return data.length;
}

/* pacing (loop mode) */
function decideSleep(period, clock){
  let m = 20;
  if (Number(period) === 2) {
    const mm = /(\d{1,2}):(\d{2})/.exec(clock || "");
    if (mm) {
      const left = +mm[1] + (+mm[2])/60;
      if (left <= 10) m = Math.max(1, Math.ceil(left * 2));
    }
  }
  if (DEBUG_MODE) m = Math.min(m, 0.2);
  return m;
}
const ms = m => Math.max(60_000, Math.round(m * 60_000));

async function tickOnce(sheets){
  const row = await findRowByGameId(sheets);
  if (row < 0) { console.log(`No row with Game ID ${EVENT_ID} in ${TAB_NAME} (A)`); return; }

  const sum = await fetchJson(summaryUrl(EVENT_ID));
  const st  = parseStatus(sum);
  const tm  = getTeams(sum);
  const odds = extractOdds(sum, tm);

  const currentStatus = st.shortDetail || st.state || "unknown";
  const scoreStr = `${tm.awayScore}-${tm.homeScore}`;
  const logLine = `[${currentStatus}] ${tm.awayName} @ ${tm.homeName} Q${st.period} (${st.displayClock})` +
                  (odds && (odds.awaySpread!=null || odds.total!=null) ? ` | odds:${odds.src}` : "");
  console.log(`${logLine} → row ${row}`);

  if (DEBUG_ODDS && odds) {
    console.log("DEBUG_ODDS parsed:", JSON.stringify(odds, null, 2));
  }

  // always keep current score in "Half Score"
  const commonPayload = {
    "Status": currentStatus,
    "Half Score": scoreStr,
    "Live Away Spread": odds.awaySpread,
    "Live Away ML": odds.awayML,
    "Live Home Spread": odds.homeSpread,
    "Live Home ML": odds.homeML,
    "Live Total": odds.total
  };

  // Avoid overwriting Half/Final in Status (still update score/odds)
  const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`});
  const hdr = headerRes.data.values?.[0] || [];
  const h   = colMap(hdr);
  const rowRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A${row}:Z${row}`});
  const cells  = rowRes.data.values?.[0] || [];
  const curStatus = (cells[h["status"]] || "").toString().trim();
  if (/^(Half|Final)$/i.test(curStatus)) {
    delete commonPayload["Status"];
  }

  // Halftime detection (still log, but we already keep score current every tick)
  const isHalf = /HALF/i.test(st.name) || (st.state==="in" && Number(st.period)===2 && /^0?:?0{1,2}\b/.test(st.displayClock||""));
  if (isHalf) {
    commonPayload["Status"] = "Half"; // force set at halftime
  }

  await writeValues(sheets, row, commonPayload);

  if (isHalf) {
    console.log(`✅ Halftime write done (score ${scoreStr})`);
    return "half";
  }
  return "continue";
}

(async ()=>{
  const sheets = await sheetsClient();

  if (ONESHOT) {
    await tickOnce(sheets);
    return;
  }

  let totalMin = 0;
  for(;;){
    const res = await tickOnce(sheets);
    if (res === "half") return;

    const sum = await fetchJson(summaryUrl(EVENT_ID));
    const st  = parseStatus(sum);
    const sleepM = decideSleep(st.period, st.displayClock);
    console.log(`Sleeping ${sleepM}m`);
    await new Promise(r=>setTimeout(r, ms(sleepM)));
    totalMin += sleepM;
    if (totalMin >= MAX_TOTAL_MIN) { console.log("⏹ MAX_TOTAL_MIN reached."); return; }
  }
})().catch(e=>{ console.error("Fatal:", e); process.exit(1); });
