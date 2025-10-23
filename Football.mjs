// Football.mjs
// One pass: Prefill + Finals formatting/grading (live odds hooks kept OFF)

import { google } from "googleapis";
import axios from "axios";

/* ============= ENV ============= */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN  = (process.env.LEAGUE || "nfl").toLowerCase();        // nfl | college-football
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE_IN === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "week").toLowerCase();    // week | today
const GAME_IDS   = (process.env.GAME_IDS || process.env.TARGET_GAME_ID || "").trim();

const GHA = String(process.env.GHA_JSON || "0") === "1";
const ET  = "America/New_York";

/* ========== SHEET COLUMNS ========== */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total"
];

/* ============= UTIL ============= */
const log  = (...a)=> GHA ? console.error(...a) : console.log(...a);
const warn = (...a)=> GHA ? console.error(...a) : console.warn(...a);

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
function fmtET(d, opt) {
  return new Intl.DateTimeFormat("en-US",{ timeZone: ET, ...opt }).format(new Date(d));
}
function fmtStatusDateTime(d){
  // "MM/DD - h:mm AM/PM" (no year)
  const mmdd = fmtET(d,{month:"2-digit",day:"2-digit"});
  const tm   = fmtET(d,{hour:"numeric",minute:"2-digit",hour12:true});
  return `${mmdd} - ${tm}`;
}
function yyyymmddET(d=new Date()){
  const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(d));
  const g = k => parts.find(p=>p.type===k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}

function leagueKey(x){
  return (x==="ncaaf"||x==="college-football")?"college-football":"nfl";
}
function sbUrl(lg, date){
  lg = leagueKey(lg);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${date}${extra}`;
}
function sumUrl(lg, id){
  lg = leagueKey(lg);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
}

async function fetchJSON(url){
  const r = await axios.get(url, { headers: { "User-Agent":"football-bot/1.1" }, timeout: 15000 });
  return r.data;
}

/* Week label from scoreboard calendar */
function resolveWeekLabel(sb, eventDateISO, lg){
  if (Number.isFinite(sb?.week?.number)) return `Week ${sb.week.number}`;
  const txt = (sb?.week?.text || "").trim();
  if (txt) return /^week\s+\d+/i.test(txt) ? txt.replace(/^\w/, c=>c.toUpperCase()) : txt;

  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(eventDateISO).getTime();
  for (const item of cal) {
    const entries = Array.isArray(item?.entries) ? item.entries : [item];
    for (const e of entries) {
      const s = new Date(e?.startDate || e?.start || 0).getTime();
      const ed= new Date(e?.endDate   || e?.end   || 0).getTime();
      const label = (e?.label || e?.detail || e?.text || "").trim();
      if (Number.isFinite(s) && Number.isFinite(ed) && t>=s && t<=ed) {
        if (/week\s+\d+/i.test(label)) return label;
        const m = label.match(/\d+/);
        return m? `Week ${m[0]}` : label || (lg==="college-football"?"Week":"Week");
      }
    }
  }
  return "Week";
}

/* Odds helpers (pregame) */
function pickESPNBet(oddsArr=[]) {
  if (!Array.isArray(oddsArr)) return null;
  return oddsArr.find(o=>/espn\s*bet/i.test(o?.provider?.name || o?.provider?.displayName || "")) || oddsArr[0] || null;
}
function extractBaseOdds(event){
  const comp = event?.competitions?.[0] || {};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const o = pickESPNBet(comp.odds || event.odds || []);
  let spread = null, favId = null, total = null, aML="", hML="";
  if (o) {
    total = Number.isFinite(+o.overUnder) ? +o.overUnder : (Number.isFinite(+o.total)? +o.total : null);
    if (o?.details) {
      const m = o.details.match(/([+-]?\d+(?:\.\d+)?)/);
      if (m) spread = Math.abs(parseFloat(m[1]));
    }
    if (Number.isFinite(+o.spread)) spread = Math.abs(+o.spread);
    favId = String(o.favoriteTeamId || o.favorite || "");
    const a = o?.awayTeamOdds || {};
    const h = o?.homeTeamOdds || {};
    aML = a?.moneyLine ?? a?.moneyline ?? a?.money_line ?? "";
    hML = h?.moneyLine ?? h?.moneyline ?? h?.money_line ?? "";
  }
  return { spread, favId, total, aML: aML===""?"":String(aML), hML: hML===""?"":String(hML), away, home };
}
function buildSpreads(spread, favId, awayId, homeId){
  let aS="", hS="";
  if (!Number.isFinite(spread) || !favId) return {aS, hS};
  if (String(awayId) === String(favId)) { aS = `-${spread}`; hS = `+${spread}`; }
  else if (String(homeId) === String(favId)) { hS = `-${spread}`; aS = `+${spread}`; }
  return {aS,hS};
}

/* Bold/underline runs for Matchup cell */
function makeMatchupRuns(matchup, favoriteSide, winnerSide){
  const sepIdx = matchup.indexOf(" @ ");
  if (sepIdx < 0) return [];
  const awayStart = 0, awayEnd = sepIdx;
  const homeStart = sepIdx + 3, homeEnd = matchup.length;

  const segs = [
    {start: awayStart, end: awayEnd, underline: favoriteSide==="away", bold: winnerSide==="away"},
    {start: homeStart, end: homeEnd, underline: favoriteSide==="home", bold: winnerSide==="home"},
  ];
  const dedup = new Map();
  dedup.set(0, { startIndex:0, format:{bold:false,underline:false} });
  for (const s of segs) {
    const fmt = {};
    if (s.bold) fmt.bold = true;
    if (s.underline) fmt.underline = true;
    dedup.set(s.start, { startIndex: s.start, format: Object.keys(fmt).length? fmt : {bold:false,underline:false} });
    if (s.end < matchup.length) dedup.set(s.end, { startIndex: s.end, format:{bold:false,underline:false} });
  }
  return [...dedup.values()].sort((a,b)=>a.startIndex-b.startIndex).filter(r=>r.startIndex < matchup.length);
}

/* Grade G..K based on final score */
function gradeRow(finalScore, aSpread, aML, hSpread, hML, total){
  const out = { G:null, H:null, I:null, J:null, K:null };
  const m = (finalScore||"").match(/(\d+)\s*-\s*(\d+)/);
  if (!m) return out;
  const a = +m[1], h = +m[2];

  if (aSpread){
    const aLine = parseFloat(aSpread);
    const aAdj = a + (aLine||0);
    out.G = (aAdj > h) ? "green" : "red";
  }
  if (hSpread){
    const hLine = parseFloat(hSpread);
    const hAdj = h + (hLine||0);
    out.I = (hAdj > a) ? "green" : "red";
  }
  if (aML) out.H = (a>h) ? "green" : "red";
  if (hML) out.J = (h>a) ? "green" : "red";
  if (total){
    const t = parseFloat(total);
    if (Number.isFinite(t)){
      const sum = a + h;
      out.K = (sum > t) ? "green" : (sum < t ? "red" : null);
    }
  }
  return out;
}

/* ======= Final-state helpers (robust) ======= */
function isFinalFromType(t){
  const name = (t?.name || "").toLowerCase();
  const state = (t?.state || "").toLowerCase();      // e.g. 'post'
  const desc  = (t?.description || "").toLowerCase();
  return Boolean(t?.completed) || state.includes("post") || name.includes("final") || desc.includes("final");
}
async function finalPairFromEventOrSummary(ev, lg){
  const comp = ev?.competitions?.[0] || {};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const aS = away?.score, hS = home?.score;
  if (Number.isFinite(+aS) && Number.isFinite(+hS)) return `${aS}-${hS}`;

  // Fallback: summary endpoint
  try{
    const s = await fetchJSON(sumUrl(lg, ev.id));
    const box = s?.boxscore;
    const A = box?.teams?.find(t=>t.homeAway==="away")?.score;
    const H = box?.teams?.find(t=>t.homeAway==="home")?.score;
    if (Number.isFinite(+A) && Number.isFinite(+H)) return `${A}-${H}`;
  }catch(_){/* ignore */}
  return "";
}

/* ============= Sheets wrapper ============= */
class Sheets {
  constructor(auth, sheetId, tab){
    this.api  = google.sheets({version:"v4", auth});
    this.id   = sheetId;
    this.tab  = tab;
    this._meta = null;
    this._values = null;
    this.hmap = {};
  }
  async meta(){
    if (!this._meta) this._meta = await this.api.spreadsheets.get({ spreadsheetId: this.id });
    return this._meta;
  }
  async ensureTabAndHeader(){
    const meta = await this.meta();
    const present = (meta.data.sheets||[]).find(s=>s.properties?.title===this.tab);
    if (!present) {
      await this.api.spreadsheets.batchUpdate({
        spreadsheetId: this.id,
        requestBody: { requests: [{ addSheet: { properties: { title: this.tab } } }] }
      });
      this._meta = null; await this.meta();
    }
    const cur = await this.readAll();
    if ((cur[0]||[]).length === 0) {
      await this.api.spreadsheets.values.update({
        spreadsheetId: this.id, range: `${this.tab}!A1`,
        valueInputOption:"RAW",
        requestBody: { values: [HEADERS] }
      });
      this._values = null; await this.readAll();
    }
  }
  async readAll(){
    if (!this._values) {
      const r = await this.api.spreadsheets.values.get({
        spreadsheetId: this.id, range: `${this.tab}!A1:Z`
      });
      this._values = r.data.values || [];
      const header = this._values[0] || [];
      this.hmap = {};
      header.forEach((h,i)=>{ this.hmap[h.trim().toLowerCase()] = i; });
    }
    return this._values;
  }
  col(c){ return String.fromCharCode("A".charCodeAt(0)+c); }
}

/* ============= MAIN ============= */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing Google creds / sheet id");
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth  = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);
  await sheets.ensureTabAndHeader();
  const values = await sheets.readAll();
  const h = sheets.hmap;

  // Map Game ID -> row number
  const keyRow = new Map();
  (values.slice(1)).forEach((r,i)=>{ const id=(r[h["game id"]]||"").toString().trim(); if(id) keyRow.set(id, i+2); });

  // Dates to pull
  const dates = RUN_SCOPE==="today"
    ? [yyyymmddET()]
    : Array.from({length:7},(_,i)=>{ const d=new Date(); d.setDate(d.getDate()+i); return yyyymmddET(d); });

  const forcedIds = GAME_IDS ? GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean) : [];

  // Pull events
  let masterSB=null, events=[];
  for (const d of dates){
    const sb = await fetchJSON(sbUrl(LEAGUE_IN, d));
    if (!masterSB) masterSB = sb;
    events = events.concat(sb?.events || []);
  }
  const seen = new Set();
  events = events.filter(e=>!seen.has(e?.id) && seen.add(e.id));
  if (forcedIds.length) {
    const keep = new Set(forcedIds);
    events = events.filter(e=>keep.has(String(e.id)));
  }
  log(`Events found: ${events.length}`);

  const valueUpdates = [];
  const formatRequests = [];
  const sheetId = (await sheets.meta()).data.sheets.find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;

  function updCell(row, colIdx, val){
    const A = sheets.col(colIdx), range = `${TAB_NAME}!${A}${row}:${A}${row}`;
    valueUpdates.push({ range, values:[[val==null?"":val]] });
  }

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup  = `${awayName} @ ${homeName}`;

    const gameId = String(ev.id);
    const rowNum = keyRow.get(gameId);

    const t = comp.status?.type || ev.status?.type || {};
    const isFinal = isFinalFromType(t);
    const displayWhen = isFinal ? "Final" : fmtStatusDateTime(ev.date);

    const weekLabel = resolveWeekLabel(masterSB, ev.date, LEAGUE_IN);
    const dateET   = fmtET(ev.date, {year:"numeric",month:"2-digit",day:"2-digit"});

    const { spread, favId, total, aML, hML } = extractBaseOdds(ev);
    const { aS, hS } = buildSpreads(spread, favId, away?.team?.id, home?.team?.id);

    if (!rowNum){
      // New row (append)
      const row = new Array(HEADERS.length).fill("");
      row[h["game id"]] = gameId;
      row[h["date"]]    = dateET;
      row[h["week"]]    = weekLabel;
      row[h["status"]]  = displayWhen;
      row[h["matchup"]] = matchup;
      row[h["final score"]] = isFinal ? (await finalPairFromEventOrSummary(ev, LEAGUE_IN)) : "";

      row[h["a spread"]] = aS || "";
      row[h["a ml"]]     = aML || "";
      row[h["h spread"]] = hS || "";
      row[h["h ml"]]     = hML || "";
      row[h["total"]]    = Number.isFinite(total)? String(total):"";

      valueUpdates.push({ range:`${TAB_NAME}!A1`, values:[row] });

      let favSide = null;
      if (favId) favSide = String(favId)===String(away?.team?.id) ? "away" : "home";
      const runs = makeMatchupRuns(matchup, favSide, null);
      if (runs.length){
        formatRequests.push({
          updateCells: {
            range: { sheetId, startRowIndex: (values.length), endRowIndex: (values.length+1), startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
            rows: [{ values: [{ userEnteredValue:{ stringValue: matchup }, textFormatRuns: runs }] }],
            fields: "userEnteredValue,textFormatRuns"
          }
        });
      }

    } else {
      // Existing row, update cells
      const row = values[rowNum-1] || [];
      if ((row[h["week"]]||"") !== weekLabel) updCell(rowNum, h["week"], weekLabel);
      if ((row[h["date"]]||"") !== dateET)     updCell(rowNum, h["date"], dateET);

      const curStatus = (row[h["status"]]||"").toString().trim();
      if (!/final/i.test(curStatus)) updCell(rowNum, h["status"], displayWhen);

      // Only move lines before kickoff
      const state = (t?.state || "").toLowerCase();
      const gameStarted = isFinal || state==="in" || /progress|live|half|q\d/.test(state);
      if (!gameStarted){
        if (aS)   updCell(rowNum, h["a spread"], aS);
        if (hS)   updCell(rowNum, h["h spread"], hS);
        if (aML!=="") updCell(rowNum, h["a ml"], aML);
        if (hML!=="") updCell(rowNum, h["h ml"], hML);
        if (Number.isFinite(total)) updCell(rowNum, h["total"], String(total));

        let favSide = null;
        if (favId) favSide = String(favId)===String(away?.team?.id) ? "away" : "home";
        const runs = makeMatchupRuns(matchup, favSide, null);
        if (runs.length){
          formatRequests.push({
            updateCells: {
              range: { sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
              rows: [{ values: [{ userEnteredValue:{ stringValue: matchup }, textFormatRuns: runs }] }],
              fields: "userEnteredValue,textFormatRuns"
            }
          });
        }
      }

      if (isFinal){
        const finalPair = await finalPairFromEventOrSummary(ev, LEAGUE_IN);
        if ((row[h["final score"]]||"") !== finalPair && finalPair) updCell(rowNum, h["final score"], finalPair);

        // Bold winner + underline favorite
        let winner = null;
        const m = finalPair.match(/(\d+)-(\d+)/);
        if (m) {
          const a = +m[1], hsc = +m[2];
          winner = a>hsc ? "away" : (hsc>a ? "home" : null);
        }
        const favSide = favId ? (String(favId)===String(away?.team?.id) ? "away" : "home") : null;
        const runs = makeMatchupRuns(matchup, favSide, winner);
        if (runs.length){
          formatRequests.push({
            updateCells: {
              range: { sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
              rows: [{ values: [{ userEnteredValue:{ stringValue: matchup }, textFormatRuns: runs }] }],
              fields: "userEnteredValue,textFormatRuns"
            }
          });
        }

        // Grade G..K only
        const grading = gradeRow(finalPair, row[h["a spread"]], row[h["a ml"]], row[h["h spread"]], row[h["h ml"]], row[h["total"]]);
        const colorMap = { green:{red:0.85,green:0.95,blue:0.85}, red:{red:0.98,green:0.85,blue:0.85} };
        const colIdx = { G:h["a spread"], H:h["a ml"], I:h["h spread"], J:h["h ml"], K:h["total"] };
        for (const key of ["G","H","I","J","K"]){
          const ci = colIdx[key];
          if (ci==null) continue;
          const bg = grading[key];
          formatRequests.push({
            repeatCell:{
              range:{ sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: ci, endColumnIndex: ci+1 },
              cell:{ userEnteredFormat:{ backgroundColor: bg ? colorMap[bg] : {red:1,green:1,blue:1} } },
              fields:"userEnteredFormat.backgroundColor"
            }
          });
        }
        updCell(rowNum, h["status"], "Final");
      }
    }
  }

  // Flush value updates (append first)
  if (valueUpdates.length){
    const appends = valueUpdates.filter(d=>d.range.endsWith("!A1"));
    const updates = valueUpdates.filter(d=>!d.range.endsWith("!A1"));

    if (appends.length){
      await sheets.api.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: appends.flatMap(x=>x.values) }
      });
      sheets._values = null; const newVals = await sheets.readAll();
      const h2 = sheets.hmap; // refreshed
      // rebuild keyRow (so later passes in same run would find new rows, if any)
      keyRow.clear();
      (newVals.slice(1)).forEach((r,i)=>{ const id=(r[h2["game id"]]||"").toString().trim(); if(id) keyRow.set(id,i+2); });
    }

    if (updates.length){
      await sheets.api.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody:{ valueInputOption:"RAW", data: updates }
      });
    }
  }

  if (formatRequests.length){
    await sheets.api.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody:{ requests: formatRequests }
    });
  }

  log("Done.");
  if (GHA) process.stdout.write(JSON.stringify({ok:true, tab:TAB_NAME, events:events.length})+"\n");
})().catch(err=>{
  warn("Fatal:", err?.message || err);
  if (GHA) process.stdout.write(JSON.stringify({ok:false, error:String(err?.message||err)})+"\n");
  process.exit(1);
});
