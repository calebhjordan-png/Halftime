/* live-game.mjs — Status + Half Score + Live Line (strict Game ID, no new rows) */
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;  // JSON string
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" or "college-football"
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200"); // safety cap
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* ---------------- ESPN helpers ---------------- */
const pick = (o,p)=>p.replace(/\[(\d+)\]/g,'.$1').split('.').reduce((a,k)=>a?.[k],o);
async function fetchJson(url, tries=3) {
  for (let i=1;i<=tries;i++){
    try{
      const r = await fetch(url,{headers:{"User-Agent":"halftime-live/restore-1.0"}});
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
  const ts=t=>Number(t?.score ?? 0);
  return { awayName: tn(away), homeName: tn(home), awayScore: ts(away), homeScore: ts(home) };
}

/* Live line: return a display string like "Chiefs -3.5" (or null if none) */
function getLiveLine(sum){
  // primary: competitions[0].odds[0]
  const odds = pick(sum,"competitions.0.odds.0") || pick(sum,"header.competitions.0.odds.0");
  let spread = odds?.spread != null ? Number(odds.spread) : null;
  let detail = odds?.details || "";

  // fallback: pickcenter[0]
  if (spread==null) {
    const pc = (pick(sum,"pickcenter")||[])[0];
    if (pc?.spread != null) spread = Number(pc.spread);
    if (!detail && pc?.details) detail = pc.details;
  }

  if (spread==null && typeof detail === "string") {
    const m = detail.match(/(-?\d+(\.\d+)?)/);
    if (m) spread = Number(m[1]);
  }
  if (spread==null) return null;

  // choose favorite name for display from details if present
  const d = (detail||"").trim();
  if (d) return d;                // e.g., "KC -3.5" or "Chiefs -3.5"
  // if details missing, fall back to neutral formatting
  return `Line ${spread > 0 ? `+${spread}` : spread}`;
}

/* ---------------- Sheets helpers ---------------- */
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

/* strict: find row by Game ID in column A only */
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
    if (val == null) continue;
    const j = h[name.toLowerCase()];
    if (j == null) continue; // only write to existing columns
    data.push({ range: `${TAB_NAME}!${A1(j)}${rowNumber}`, values: [[val]] });
  }
  if (!data.length) return 0;

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data }
  });
  return data.length;
}

/* pacing */
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

/* ---------------- Main loop ---------------- */
(async ()=>{
  const sheets = await sheetsClient();
  const row    = await findRowByGameId(sheets);
  if (row < 0) { console.log(`No row with Game ID ${EVENT_ID} in ${TAB_NAME} (column A).`); return; }

  let totalMin = 0;
  for(;;){
    const sum = await fetchJson(summaryUrl(EVENT_ID));
    const st  = parseStatus(sum);
    const tm  = getTeams(sum);
    const lineDisplay = getLiveLine(sum); // may be null
    const currentStatus = st.shortDetail || st.state || "unknown";

    console.log(`[${currentStatus}] ${tm.awayName} @ ${tm.homeName} Q${st.period} (${st.displayClock}) → row ${row}${lineDisplay?` | ${lineDisplay}`:""}`);

    // write rolling Status (unless already Half/Final)
    const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`});
    const hdr = headerRes.data.values?.[0] || [];
    const h   = colMap(hdr);
    const rowRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A${row}:Z${row}`});
    const cells  = rowRes.data.values?.[0] || [];
    const cur = (cells[h["status"]] || "").toString().trim();

    if (!/^(Half|Final)$/i.test(cur)) {
      const payload = { "Status": currentStatus };
      if (lineDisplay && h["live line"] != null) payload["Live Line"] = lineDisplay; // only if column exists
      await writeValues(sheets, row, payload);
    }

    // halftime action -> write and exit
    const isHalf = /HALF/i.test(st.name) || (st.state==="in" && Number(st.period)===2 && /^0?:?0{1,2}\b/.test(st.displayClock||""));
    if (isHalf){
      const halfScore = `${tm.awayScore}-${tm.homeScore}`;
      const payload = { "Status": "Half", "Half Score": halfScore };
      if (lineDisplay && h["live line"] != null) payload["Live Line"] = lineDisplay;
      await writeValues(sheets, row, payload);
      console.log(`✅ Halftime written (${halfScore})`);
      return;
    }

    const sleepM = decideSleep(st.period, st.displayClock);
    console.log(`Sleeping ${sleepM}m`);
    await new Promise(r=>setTimeout(r, ms(sleepM)));
    totalMin += sleepM;
    if (totalMin >= MAX_TOTAL_MIN) { console.log("⏹ MAX_TOTAL_MIN reached."); return; }
  }
})().catch(e=>{ console.error("Fatal:", e); process.exit(1); });
