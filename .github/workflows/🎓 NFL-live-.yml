/* live-game.mjs — Status + current score + ESPN BET LIVE odds (strict Game ID)
   Columns written (must already exist):
     Status
     Half Score                 -> current score (away-home) every tick
     Live Away Spread
     Live Away ML
     Live Home Spread
     Live Home ML
     Live Total
   Env:
     LEAGUE=nfl|college-football
     TAB_NAME=NFL|CFB
     GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT (JSON)
     TARGET_GAME_ID=4017...
     ONESHOT=true              -> single fetch/write and exit
     DEBUG_ODDS=true           -> print ALL odds candidates + chosen one
     FORCE_STATUS_WRITE=true   -> always write Status (except Final)
*/
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();

const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE  = String(process.env.DEBUG_MODE  || "").toLowerCase()==="true";
const ONESHOT     = String(process.env.ONESHOT     || "").toLowerCase()==="true";
const DEBUG_ODDS  = String(process.env.DEBUG_ODDS  || "").toLowerCase()==="true";
const FORCE_STATUS_WRITE = String(process.env.FORCE_STATUS_WRITE || "").toLowerCase()==="true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* ---------- ESPN helpers ---------- */
const pick = (o,p)=>p.replace(/\[(\d+)\]/g,'.$1').split('.').reduce((a,k)=>a?.[k],o);
async function fetchJson(url, tries=3) {
  for (let i=1;i<=tries;i++){
    try{
      const r = await fetch(url,{headers:{"User-Agent":"halftime-live/1.5"}});
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
    name: (t.name || "").toUpperCase(),   // e.g., IN, STATUS_HALFTIME
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
  const keys=t=>[t?.team?.shortDisplayName,t?.team?.abbreviation,t?.team?.displayName].filter(Boolean).map(x=>x.toLowerCase());
  const ts=t=>Number(t?.score ?? 0);
  return {
    awayName: tn(away), homeName: tn(home),
    awayKeys: keys(away), homeKeys: keys(home),
    awayScore: ts(away),  homeScore: ts(home)
  };
}

/* ---- Live Odds helpers ---- */
const ESPNBET = /espn\s*bet/i;
function coerceNum(v){ if(v===null||v===undefined||v==="") return null; const n=Number(v); return Number.isFinite(n)?n:null; }
function toTs(x){ if(!x) return 0; const n=Date.parse(x); return Number.isFinite(n)?n:0; }
function looksLive(obj){
  const s = (obj?.type || obj?.name || obj?.displayName || obj?.market || "").toLowerCase();
  return obj?.live === true || /live|in[-\s]?game|ingame/.test(s);
}
function providerName(o){ return (o?.provider?.name || o?.provider?.displayName || o?.name || "").trim(); }
function updatedTs(o){
  return Math.max(
    toTs(o?.lastModified), toTs(o?.updated), toTs(o?.updateTime), toTs(o?.lastUpdate),
    toTs(o?.lastModifiedDate), toTs(o?.lastUpdated)
  );
}
function spreadsFrom(spread, ctx){
  if (!Number.isFinite(spread)) return {awaySpread:null, homeSpread:null};
  const abs = Math.abs(spread);
  if (ctx.awayFav === true && ctx.homeFav !== true) return { awaySpread: -abs, homeSpread: +abs };
  if (ctx.homeFav === true && ctx.awayFav !== true) return { awaySpread: +abs, homeSpread: -abs };
  const d=(ctx.detail||"").toLowerCase();
  const hit=(keys)=>keys?.some(k=>k && d.includes(k));
  if (hit(ctx.awayKeys) && !hit(ctx.homeKeys)) return { awaySpread:-abs, homeSpread:+abs };
  if (hit(ctx.homeKeys) && !hit(ctx.awayKeys)) return { awaySpread:+abs, homeSpread:-abs };
  // neutral fallback
  return { awaySpread:+abs, homeSpread:-abs };
}

function extractOdds(sum, tm){
  const compsOdds = pick(sum,"competitions.0.odds") || [];
  const pickCenter = pick(sum,"pickcenter") || [];
  const candidates = [];

  // Prefer ESPN BET live objects from competitions[0].odds[*]
  if (Array.isArray(compsOdds)) {
    compsOdds.forEach((o,i)=>candidates.push({src:`competitions.odds[${i}]`, o, group:"comp"}));
  }
  // We can still see pickcenter (pregame/consensus) for debugging
  if (Array.isArray(pickCenter)) {
    pickCenter.forEach((o,i)=>candidates.push({src:`pickcenter[${i}]`, o, group:"pc"}));
  }

  if (DEBUG_ODDS) {
    console.log("DEBUG_ODDS candidates:", candidates.length);
    for (const c of candidates) {
      const o=c.o;
      console.log(
        `# ${c.src} | provider="${providerName(o)}" live=${looksLive(o)} updatedTs=${updatedTs(o)} ` +
        `spread=${o?.spread} ou=${o?.overUnder ?? o?.total} ` +
        `awayML=${o?.awayTeamOdds?.moneyLine ?? o?.awayTeamOdds?.moneyline} homeML=${o?.homeTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyline}`
      );
    }
    // Full JSON (use once to identify correct source)
    for (const c of candidates) {
      console.log(`DEBUG_ODDS FULL ${c.src}:`, JSON.stringify(c.o, null, 2));
    }
  }

  // Selection:
  // 1) competitions.odds where provider is ESPN BET and 'live'
  let chosen = candidates.find(c => c.group==="comp" && ESPNBET.test(providerName(c.o)) && looksLive(c.o));

  // 2) competitions.odds latest ESPN BET (even if not explicitly flagged live)
  if (!chosen) {
    chosen = candidates
      .filter(c => c.group==="comp" && ESPNBET.test(providerName(c.o)))
      .map(c => ({...c, ts:updatedTs(c.o)}))
      .sort((a,b)=>b.ts-a.ts)[0];
  }

  // 3) any competitions.odds live
  if (!chosen) chosen = candidates.find(c => c.group==="comp" && looksLive(c.o));

  // 4) last resort: most recent competitions.odds (still better than pickcenter for in-game)
  if (!chosen) {
    chosen = candidates
      .filter(c => c.group==="comp")
      .map(c => ({...c, ts:updatedTs(c.o)}))
      .sort((a,b)=>b.ts-a.ts)[0];
  }

  // 5) absolute fallback: pickcenter[0] (pregame) — not ideal but avoids blanks
  if (!chosen) chosen = candidates[0];

  if (!chosen) {
    return { src:null, awaySpread:null, awayML:null, homeSpread:null, homeML:null, total:null, detail:null };
  }

  const o = chosen.o;
  const detail = o?.details || o?.detail || "";
  const total  = coerceNum(o?.overUnder ?? o?.total);
  const spread = coerceNum(o?.spread);

  const awayFav = o?.awayTeamOdds?.favorite === true || o?.awayTeamOdds?.underdog === false;
  const homeFav = o?.homeTeamOdds?.favorite === true || o?.homeTeamOdds?.underdog === false;

  const awayML = coerceNum(o?.awayTeamOdds?.moneyLine ?? o?.awayTeamOdds?.moneyline);
  const homeML = coerceNum(o?.homeTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyline);

  const { awaySpread, homeSpread } = spreadsFrom(spread, {
    awayFav, homeFav, detail, awayKeys: tm.awayKeys, homeKeys: tm.homeKeys
  });

  return { src: chosen.src, awaySpread, awayML, homeSpread, homeML, total, detail };
}

/* ---------- Sheets ---------- */
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
    if ((col[i][0]||"").toString().trim() === EVENT_ID) return i+1; // 1-based
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
    if (val == null && val !== "") continue; // allow "" clears
    const j = h[name.toLowerCase()];
    if (j == null) continue;
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

  console.log(`[${currentStatus}] ${tm.awayName} @ ${tm.homeName} Q${st.period} (${st.displayClock})${odds?.src?` | odds:${odds.src}`:""} → row ${row}`);

  const payload = {
    "Status": currentStatus,
    "Half Score": scoreStr,                 // keep current score in this column
    "Live Away Spread": odds.awaySpread,
    "Live Away ML": odds.awayML,
    "Live Home Spread": odds.homeSpread,
    "Live Home ML": odds.homeML,
    "Live Total": odds.total
  };

  // Don't clobber Final; optionally avoid clobbering Half unless forced
  const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`});
  const hdr = headerRes.data.values?.[0] || [];
  const h   = colMap(hdr);
  const rowRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A${row}:Z${row}`});
  const cells  = rowRes.data.values?.[0] || [];
  const curStatus = (cells[h["status"]] || "").toString().trim();

  if (/^Final$/i.test(curStatus)) delete payload["Status"];
  if (!FORCE_STATUS_WRITE && /^Half$/i.test(curStatus)) delete payload["Status"];

  // Halftime detection
  const isHalf = /HALF/i.test(st.name) || (st.state==="in" && Number(st.period)===2 && /^0?:?0{1,2}\b/.test(st.displayClock||""));
  if (isHalf) payload["Status"] = "Half";

  await writeValues(sheets, row, payload);
  if (isHalf) { console.log(`✅ Halftime write done (score ${scoreStr})`); return "half"; }
  return "continue";
}

(async ()=>{
  const sheets = await sheetsClient();
  if (ONESHOT) { await tickOnce(sheets); return; }

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
