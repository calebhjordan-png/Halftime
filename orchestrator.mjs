// orchestrator.mjs — Node 20+ ESM
// npm deps: axios, googleapis

import axios from "axios";
import { google } from "googleapis";

/* ===== Env ===== */
const SHEET_ID  = process.env.GOOGLE_SHEET_ID;
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" | "college-football"
const TAB_TITLE = process.env.TAB_NAME || (LEAGUE === "nfl" ? "NFL" : "CFB");
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase(); // kept for compatibility
const DATE_FMT  = (process.env.DATE_FMT || "MM/DD/YY").toUpperCase();

if (!SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!process.env.GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");

const PROVIDER_PREFERENCE = ["ESPN BET", "CAESARS", "DRAFTKINGS", "FANDUEL"];
const CONCURRENCY = 5;

/* ===== Columns (0-based) ===== */
const COL = {
  gameId: 0, date: 1, week: 2, status: 3, matchup: 4, finalScore: 5,
  awaySpread: 6, awayML: 7, homeSpread: 8, homeML: 9, total: 10,
  // live columns exist to the right but are intentionally untouched here
};

/* ===== Colors ===== */
const GREEN = { red: 0.85, green: 0.95, blue: 0.85 };
const RED   = { red: 0.98, green: 0.88, blue: 0.88 };
const WHITE = { red: 1, green: 1, blue: 1 };

/* ===== Google Sheets ===== */
function getAuth() {
  const sa = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  return new google.auth.JWT(
    sa.client_email, null, sa.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
}
async function getSheets() {
  return google.sheets({ version: "v4", auth: getAuth() });
}
async function getSheetIdByTitle(sheets, spreadsheetId, title) {
  const { data } = await sheets.spreadsheets.get({ spreadsheetId });
  const tab = (data.sheets || []).find(s => s.properties.title === title);
  if (!tab) throw new Error(`Tab "${title}" not found`);
  return tab.properties.sheetId;
}
async function readGridMap(sheets, spreadsheetId, title) {
  const range = `'${title}'!A2:K`;
  const { data } = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const rows = data.values || [];
  const map = new Map();
  rows.forEach((vals, i) => {
    const gid = vals?.[0] ? String(vals[0]) : "";
    if (gid) map.set(gid, { rowIndex0: 1 + i, values: vals });
  });
  return { rowsCount: rows.length, map };
}

/* ===== Small utils ===== */
function pLimit(n) {
  let active = 0, q = [];
  const next = () => { active--; if (q.length) q.shift()(); };
  return fn => new Promise((res, rej) => {
    const run = () => { active++; Promise.resolve(fn()).then(v=>{res(v);next();},e=>{rej(e);next();}); };
    active < n ? run() : q.push(run);
  });
}
const limit = pLimit(CONCURRENCY);

function pad2(n){ return n<10 ? `0${n}` : `${n}`; }
function fmtMMDDYY(d){ const t=new Date(d); return `${pad2(t.getMonth()+1)}/${pad2(t.getDate())}/${String(t.getFullYear()).slice(-2)}`; }
function fmtDDMMYY(d){ const t=new Date(d); return `${pad2(t.getDate())}/${pad2(t.getMonth()+1)}/${String(t.getFullYear()).slice(-2)}`; }
function fmtDate(d){ return DATE_FMT==="DD/MM/YY" ? fmtDDMMYY(d) : fmtMMDDYY(d); }

function toLocalET(iso){
  try{
    const dt=new Date(iso);
    return new Intl.DateTimeFormat("en-US",{hour:"numeric",minute:"2-digit",hour12:true,timeZone:"America/New_York"}).format(dt);
  }catch{return "";}
}
function inferWeekTxt(league, wk){
  let n=null;
  if (typeof wk==="number") n=wk;
  else if (wk && typeof wk==="object" && wk.number!=null) n=wk.number;
  return n?`Week ${n}`:"Week";
}
function parseNum(v){
  if (v==null) return null;
  if (typeof v==="number") return v;
  const s=String(v).trim();
  return /^[+-]?\d+(\.\d+)?$/.test(s)?Number(s):null;
}
const isLiveish = (s) => /in progress|1st|2nd|3rd|4th|ot|half|end of/i.test(String(s||""));

/* Favorite underline helpers */
function teamLabel(away, home, favKey){
  const a=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
  const h=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
  const text=`${a} @ ${h}`;
  let uStart=-1,uEnd=-1;
  if (favKey==="away"){ uStart=0; uEnd=a.length; }
  else if (favKey==="home"){ const at=text.indexOf("@"); if (at>=0){ uStart=at+2; uEnd=text.length; } }
  return {text,uStart,uEnd};
}
function textRuns(text,uStart,uEnd){
  const L=text.length, runs=[];
  runs.push({startIndex:0,format:{underline:false}});
  if (uStart>=0 && uStart<L){
    runs.push({startIndex:uStart,format:{underline:true}});
    if (uEnd>uStart && uEnd<L) runs.push({startIndex:uEnd,format:{underline:false}});
  }
  return runs;
}

/* Score helpers */
function scorePair(s){
  const m = s && String(s).match(/^\s*(\d+)\s*-\s*(\d+)\s*$/);
  return m?{a:+m[1],h:+m[2]}:{a:null,h:null};
}
function winnerKey(a,h){ if(a>h) return "away"; if(h>a) return "home"; return null; }

/* ===== ESPN fetch ===== */
const leaguePath = () => LEAGUE==="college-football" ? "football/college-football" : "football/nfl";

/* Include groups=80 for CFB so we get ALL FBS, not just Top 25. */
async function fetchScoreboard(){
  const url=`https://site.api.espn.com/apis/site/v2/sports/${leaguePath()}/scoreboard`;
  const params = {};

  // ensure we get the full FBS slate for college football
  if (LEAGUE === "college-football") {
    params.groups = "80";   // FBS
    params.limit  = 300;    // plenty for full week
  }

  const { data } = await axios.get(url, { timeout: 15000, params });
  return data;
}

function pickProvider(oddsArr){
  if (!Array.isArray(oddsArr)||!oddsArr.length) return null;
  const byName = n => oddsArr.find(o => (o?.provider?.name||"").toUpperCase()===n.toUpperCase());
  for (const n of PROVIDER_PREFERENCE){ const x=byName(n); if (x) return x; }
  return oddsArr[0];
}
function extractOdds(comp){
  const o = pickProvider(comp?.odds);
  let spreadAway=null, spreadHome=null, total=null, awayML=null, homeML=null;
  if (o){
    const s=parseNum(o.spread);
    if (s!=null){
      const mag=Math.abs(s), homeFav=s<0;
      spreadAway = homeFav ? +mag : -mag;
      spreadHome = homeFav ? -mag : +mag;
    }
    total  = parseNum(o.overUnder);
    awayML = parseNum(o?.awayTeamOdds?.moneyLine);
    homeML = parseNum(o?.homeTeamOdds?.moneyLine);
  }
  return {spreadAway,spreadHome,total,awayML,homeML};
}
async function fetchMlFallback(eventId){
  try{
    const url=`https://site.api.espn.com/apis/site/v2/sports/${leaguePath()}/summary?event=${eventId}`;
    const {data}=await axios.get(url,{timeout:12000});
    const pools=[];
    if (Array.isArray(data?.pickcenter)) pools.push(...data.pickcenter);
    if (Array.isArray(data?.odds)) pools.push(...data.odds);
    const upper=s=>(s||"").toUpperCase();
    let pick = PROVIDER_PREFERENCE
      .map(p => pools.find(x=>upper(x?.provider?.name)===upper(p) && (x?.awayTeamOdds?.moneyLine!=null || x?.homeTeamOdds?.moneyLine!=null)))
      .find(Boolean);
    if (!pick) pick = pools.find(x => x?.awayTeamOdds?.moneyLine!=null || x?.homeTeamOdds?.moneyLine!=null);
    if (!pick) return {awayML:null,homeML:null};
    return {
      awayML: parseNum(pick?.awayTeamOdds?.moneyLine),
      homeML: parseNum(pick?.homeTeamOdds?.moneyLine)
    };
  }catch{ return {awayML:null,homeML:null}; }
}
function favoriteKeyFromOdds(o){
  if (o.spreadAway!=null && o.spreadHome!=null){
    if (o.spreadAway<0) return "away";
    if (o.spreadHome<0) return "home";
  }
  if (o.awayML!=null && o.homeML!=null){
    if (o.awayML<0 && (o.homeML>=0 || o.awayML<o.homeML)) return "away";
    if (o.homeML<0 && (o.awayML>=0 || o.homeML<o.awayML)) return "home";
  }
  return null;
}

/* ===== A1 helpers ===== */
function colA1(n){ let x=n,s=""; while(x>=0){s=String.fromCharCode((x%26)+65)+s; x=Math.floor(x/26)-1;} return s; }
const val = v =>
  v==null || v==="" ? { userEnteredValue:{stringValue:""} } :
  (typeof v==="number" ? { userEnteredValue:{numberValue:v} } : { userEnteredValue:{stringValue:String(v)} });

/* ===== MAIN ===== */
async function main(){
  const sheets = await getSheets();
  const sheetId = await getSheetIdByTitle(sheets, SHEET_ID, TAB_TITLE);
  const { map: sheetMap, rowsCount } = await readGridMap(sheets, SHEET_ID, TAB_TITLE);

  const board = await fetchScoreboard();
  const events = Array.isArray(board?.events) ? board.events : [];

  const requests = [];
  let appendCursor = 1 + rowsCount;

  for (const ev of events){
    const comp = ev?.competitions?.[0];
    if (!comp) continue;

    const gameId = String(comp.id || ev.id || "");
    if (!gameId) continue;

    const statusName = (comp?.status?.type?.name || ev?.status?.type?.name || "").toLowerCase();
    const isFinal = statusName.includes("final");
    const isLive = !isFinal && isLiveish(comp?.status?.type?.description || comp?.status?.type?.state);

    // Odds (+ CFB ML fallback)
    let odds = extractOdds(comp);
    if (LEAGUE==="college-football" && (odds.awayML==null || odds.homeML==null)){
      const ml = await limit(()=>fetchMlFallback(comp.id));
      if (odds.awayML==null) odds.awayML = ml.awayML;
      if (odds.homeML==null) odds.homeML = ml.homeML;
    }

    // Teams & favorite underline
    const away = comp?.competitors?.find(c=>c.homeAway==="away");
    const home = comp?.competitors?.find(c=>c.homeAway==="home");
    const favKey = favoriteKeyFromOdds(odds);
    const { text: matchupText, uStart, uEnd } = teamLabel(away, home, favKey);

    // Score/time/labels
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";
    const weekTxt = inferWeekTxt(LEAGUE, ev?.week || comp?.week || {});
    const kickoffLabel = toLocalET(comp?.date); // used only for brand-new rows

    const existing = sheetMap.get(gameId);

    // Helper: push a single-cell value update (safe – does not blank)
    const setCell = (rowIndex0, colIndex, value) => {
      if (value === undefined) return;
      requests.push({
        updateCells: {
          range: { sheetId, startRowIndex: rowIndex0, endRowIndex: rowIndex0+1, startColumnIndex: colIndex, endColumnIndex: colIndex+1 },
          rows: [{ values: [val(value)] }],
          fields: "userEnteredValue"
        }
      });
    };

    if (existing){
      const r = existing.values;
      const row0 = existing.rowIndex0;

      const existingStatus = r[COL.status] ?? "";
      const currentlyLiveish = isLiveish(existingStatus);

      // 1) Finalization path — only now do we set Status=Final & Final Score
      if (isFinal){
        setCell(row0, COL.finalScore, finalScore);
        setCell(row0, COL.status, "Final");

        // finals grading backgrounds
        const {a,h}=scorePair(finalScore||"");
        if (a!=null && h!=null){
          const win = winnerKey(a,h);

          const colorAt = (col, bg) => {
            if (!bg) return;
            requests.push({
              updateCells: {
                range: { sheetId, startRowIndex: row0, endRowIndex: row0+1, startColumnIndex: col, endColumnIndex: col+1 },
                rows: [{ values: [{ userEnteredFormat:{ backgroundColor: bg } }] }],
                fields: "userEnteredFormat.backgroundColor"
              }
            });
          };

          // ML bg
          if (win){
            colorAt(COL.awayML, win==="away" ? GREEN : RED);
            colorAt(COL.homeML, win==="home" ? GREEN : RED);
          }

          // Spread bg (cover logic)
          if (typeof odds.spreadAway==="number" && typeof odds.spreadHome==="number"){
            const awayCovers = (a + odds.spreadAway) > h;
            const homeCovers = (h + odds.spreadHome) > a;
            colorAt(COL.awaySpread, awayCovers ? GREEN : RED);
            colorAt(COL.homeSpread, homeCovers ? GREEN : RED);
          }

          // Total bg
          if (typeof odds.total==="number"){
            const sum = a+h;
            colorAt(COL.total, sum>odds.total ? GREEN : sum<odds.total ? RED : WHITE);
          }
        }
      } else {
        // 2) Prefill odds – ONLY if cells are blank AND row is NOT live/final
        const canPrefill = !currentlyLiveish && !/final/i.test(existingStatus);
        if (canPrefill){
          if ((r[COL.awaySpread] ?? "") === "" && odds.spreadAway!=null) setCell(row0, COL.awaySpread, odds.spreadAway);
          if ((r[COL.homeSpread] ?? "") === "" && odds.spreadHome!=null) setCell(row0, COL.homeSpread, odds.spreadHome);
          if ((r[COL.awayML] ?? "") === "" && odds.awayML!=null) setCell(row0, COL.awayML, odds.awayML);
          if ((r[COL.homeML] ?? "") === "" && odds.homeML!=null) setCell(row0, COL.homeML, odds.homeML);
          if ((r[COL.total] ?? "") === "" && odds.total!=null) setCell(row0, COL.total, odds.total);
        }
        // We do NOT overwrite Status here (preserve kickoff string or any manual note)
      }

      // Underline favorite in the matchup cell (non-destructive)
      const runs = textRuns(matchupText, uStart, uEnd);
      if (runs.length){
        requests.push({
          updateCells: {
            range: { sheetId, startRowIndex: row0, endRowIndex: row0+1, startColumnIndex: COL.matchup, endColumnIndex: COL.matchup+1 },
            rows: [{ values: [{ textFormatRuns: runs, userEnteredValue: { stringValue: matchupText } }] }],
            fields: "userEnteredValue,textFormatRuns"
          }
        });
      }

      // Keep Date/Week fresh only if the existing cell is blank (avoid stomping)
      if ((r[COL.date] ?? "") === "" && comp?.date) setCell(row0, COL.date, fmtDate(comp.date));
      if ((r[COL.week] ?? "") === "" && weekTxt) setCell(row0, COL.week, weekTxt);

      // Never touch pre-existing Final Score unless we’re finalizing above
    } else {
      // New row -> write all base values
      const row0 = appendCursor;
      appendCursor += 1;

      // Base cells
      [
        [COL.gameId, gameId],
        [COL.date, fmtDate(comp?.date || ev?.date)],
        [COL.week, weekTxt],
        [COL.status, kickoffLabel || ""],
        [COL.matchup, matchupText],
      ].forEach(([c,v]) => setCell(row0, c, v));

      // Prefill odds if we have them (new rows are not live)
      if (odds.spreadAway!=null) setCell(row0, COL.awaySpread, odds.spreadAway);
      if (odds.spreadHome!=null) setCell(row0, COL.homeSpread, odds.spreadHome);
      if (odds.awayML!=null)    setCell(row0, COL.awayML, odds.awayML);
      if (odds.homeML!=null)    setCell(row0, COL.homeML, odds.homeML);
      if (odds.total!=null)     setCell(row0, COL.total, odds.total);

      // If the game is already final when first seen, finalize now
      if (isFinal){
        setCell(row0, COL.finalScore, finalScore);
        setCell(row0, COL.status, "Final");
      }

      // Favorite underline on the fresh cell
      const runs = textRuns(matchupText, uStart, uEnd);
      if (runs.length){
        requests.push({
          updateCells: {
            range: { sheetId, startRowIndex: row0, endRowIndex: row0+1, startColumnIndex: COL.matchup, endColumnIndex: COL.matchup+1 },
            rows: [{ values: [{ textFormatRuns: runs }] }],
            fields: "textFormatRuns"
          }
        });
      }
    }
  }

  if (requests.length === 0){
    console.log("No changes to apply.");
    return;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests }
  });

  console.log("Batch update complete. Ops:", requests.length);
}

main().catch(err => {
  console.error("Orchestrator fatal:", err?.response?.status ? `HTTP ${err.response.status}` : err);
  process.exit(1);
});
