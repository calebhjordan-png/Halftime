// Football.mjs
// One pass: Prefill + Finals formatting/grading (+ Live odds when enabled)

import { google } from "googleapis";
import axios from "axios";

/* =========================
   Env / knobs
   ========================= */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN  = (process.env.LEAGUE || "nfl").toLowerCase();        // nfl | college-football
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE_IN === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "week").toLowerCase();    // week | today
const GAME_IDS   = (process.env.GAME_IDS || process.env.TARGET_GAME_ID || "").trim();

// 🔒 Feature switches
const PREFILL_ENABLED = true;
const FINALS_ENABLED  = true;
const LIVE_ENABLED    = true; // Live odds now enabled (L–Q columns)

// Optional GHA output toggle
const GHA = String(process.env.GHA_JSON || "0") === "1";
const ET  = "America/New_York";

/* =========================
   Sheet column model
   ========================= */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* =========================
   Utilities
   ========================= */
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
const sleep = ms=> new Promise(r=>setTimeout(r,ms));

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

/* =========================
   ESPN pulls
   ========================= */
async function fetchJSON(url){
  const r = await axios.get(url, { headers: { "User-Agent":"football-bot/1.0" }, timeout: 15000 });
  return r.data;
}
function resolveWeekLabel(sb, eventDateISO, lg){
  if (Number.isFinite(sb?.week?.number)) return `Week ${sb.week.number}`;
  const txt = (sb?.week?.text || "").trim();
  if (txt) {
    return /^week\s+\d+/i.test(txt) ? txt.replace(/^\w/,(c)=>c.toUpperCase()) : txt;
  }
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

/* =========================
   Odds helpers
   ========================= */
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
    if (aML==="" || hML==="") {
      if (o.moneyline?.away?.close?.odds || o.moneyline?.away?.open?.odds) {
        aML = (o.moneyline.away.close?.odds ?? o.moneyline.away.open?.odds ?? aML);
      }
      if (o.moneyline?.home?.close?.odds || o.moneyline?.home?.open?.odds) {
        hML = (o.moneyline.home.close?.odds ?? o.moneyline.home.open?.odds ?? hML);
      }
    }
  }
  return { spread, favId, total, aML: aML===""?"":String(aML), hML: hML===""?"":String(hML), away, home };
}
function buildSpreads(spread, favId, awayId, homeId){
  let aS="", hS="";
  if (!Number.isFinite(spread) || !favId) return {aS, hS};
  if (String(awayId) === String(favId)) {
    aS = `-${spread}`; hS = `+${spread}`;
  } else if (String(homeId) === String(favId)) {
    hS = `-${spread}`; aS = `+${spread}`;
  }
  return {aS,hS};
}

/* =========================
   Sheets wrapper + alias map
   ========================= */
const HEADER_ALIASES = {
  "h score":        "half score",
  "h a spread":     "live away spread",
  "h a ml":         "live away ml",
  "h h spread":     "live home spread",
  "h h ml":         "live home ml",
  "h total":        "live total",
};

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
    if (!this._meta) {
      this._meta = await this.api.spreadsheets.get({ spreadsheetId: this.id });
    }
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
      // alias remap
      for (const [alias, canon] of Object.entries(HEADER_ALIASES)) {
        if (this.hmap[alias]!=null && this.hmap[canon]==null) {
          this.hmap[canon] = this.hmap[alias];
        }
      }
    }
    return this._values;
  }
  col(c){ return String.fromCharCode("A".charCodeAt(0)+c); }
}

/* =========================
   Matchup formatting
   ========================= */
function makeMatchupRuns(matchup, favoriteSide, winnerSide){
  const sepIdx = matchup.indexOf(" @ ");
  if (sepIdx < 0) return [];
  const awayStart = 0, awayEnd = sepIdx;
  const homeStart = sepIdx + 3, homeEnd = matchup.length;

  const segs = [
    {start: awayStart, end: awayEnd, underline: favoriteSide==="away", bold: winnerSide==="away"},
    {start: homeStart, end: homeEnd, underline: favoriteSide==="home", bold: winnerSide==="home"},
  ];
  const runs = [];
  runs.push({ startIndex:0, format:{ bold:false, underline:false }});
  for (const s of segs) {
    const fmt = {};
    if (s.bold) fmt.bold = true;
    if (s.underline) fmt.underline = true;
    runs.push({ startIndex: s.start, format: Object.keys(fmt).length? fmt : { bold:false, underline:false }});
    if (s.end < matchup.length) {
      runs.push({ startIndex: s.end, format: { bold:false, underline:false }});
    }
  }
  const map = new Map();
  for (const r of runs) map.set(r.startIndex, r);
  return [...map.values()].sort((a,b)=>a.startIndex-b.startIndex).filter(r=>r.startIndex < matchup.length);
}

/* =========================
   Grading helpers
   ========================= */
function toNum(v){ const n=Number(v); return Number.isFinite(n)?n:null; }

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

/* =========================
   Main
   ========================= */
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

  const keyRow = new Map();
  (values.slice(1)).forEach((r,i)=>{
    const id = (r[h["game id"]]||"").toString().trim();
    if (id) keyRow.set(id, i+2);
  });

  const dates = RUN_SCOPE==="today"
    ? [yyyymmddET()]
    : Array.from({length:7},(_,i)=>{ const d=new Date(); d.setDate(d.getDate()+i); return yyyymmddET(d); });

  const forcedIds = GAME_IDS ? GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean) : [];
  let masterSB=null, events=[];
  for (const d of dates){
    const sb = await fetchJSON(sbUrl(LEAGUE_IN, d));
    if (!masterSB) masterSB = sb;
    events = events.concat(sb?.events || []);
  }
  const seen = new Set();
  events = events.filter(e=>!seen.has(e?.id) && seen.add(e.id));
  const filterSet = forcedIds.length ? new Set(forcedIds) : null;
  if (filterSet) events = events.filter(e=>filterSet.has(String(e.id)));

  log(`Events found: ${events.length}`);

  const valueUpdates = [];
  const formatRequests = [];
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

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const isFinal = statusName.includes("FINAL");
    const displayWhen = isFinal ? "Final" : fmtStatusDateTime(ev.date);
    const weekLabel = resolveWeekLabel(masterSB, ev.date, LEAGUE_IN);
    const dateET   = fmtET(ev.date, {year:"numeric",month:"2-digit",day:"2-digit"});
    const { spread, favId, total, aML, hML } = extractBaseOdds(ev);
    const { aS, hS } = buildSpreads(spread, favId, away?.team?.id, home?.team?.id);

    if (PREFILL_ENABLED && !rowNum){
      const row = new Array(HEADERS.length).fill("");
      row[h["game id"]] = gameId;
      row[h["date"]]    = dateET;
      row[h["week"]]    = weekLabel;
      row[h["status"]]  = displayWhen;
      row[h["matchup"]] = matchup;
      row[h["final score"]] = isFinal ? `${away?.score??""}-${home?.score??""}` : "";
      row[h["a spread"]] = aS || "";
      row[h["a ml"]]     = aML || "";
      row[h["h spread"]] = hS || "";
      row[h["h ml"]]     = hML || "";
      row[h["total"]]    = Number.isFinite(total)? String(total):"";
      valueUpdates.push({ range:`${TAB_NAME}!A1`, values:[row] });
    } 
    else if (rowNum) {
      const row = values[rowNum-1] || [];
      const curStatus = (row[h["status"]]||"").toString().trim();

      if (!/final/i.test(curStatus) && !/(\d+:\d+\s*-\s*\d+)/i.test(curStatus))
        updCell(rowNum, h["status"], displayWhen);
      if ((row[h["week"]]||"") !== weekLabel) updCell(rowNum, h["week"], weekLabel);
      if ((row[h["date"]]||"") !== dateET)     updCell(rowNum, h["date"], dateET);

      const gameStarted = /in_progress|live|halftime|final/i.test(statusName);
      if (PREFILL_ENABLED && !gameStarted){
        if (aS)   updCell(rowNum, h["a spread"], aS);
        if (hS)   updCell(rowNum, h["h spread"], hS);
        if (aML!=="") updCell(rowNum, h["a ml"], aML);
        if (hML!=="") updCell(rowNum, h["h ml"], hML);
        if (Number.isFinite(total)) updCell(rowNum, h["total"], String(total));
      }

      if (FINALS_ENABLED && isFinal){
        const finalPair = `${away?.score??""}-${home?.score??""}`;
        if ((row[h["final score"]]||"") !== finalPair) updCell(rowNum, h["final score"], finalPair);
        let winner = null;
        if (Number.isFinite(+away?.score) && Number.isFinite(+home?.score)) {
          winner = (+away.score > +home.score) ? "away" : (+home.score > +away.score ? "home" : null);
        }
        const favSide = favId ? (String(favId)===String(away?.team?.id) ? "away" : "home") : null;
        const runs = makeMatchupRuns(matchup, favSide, winner);
        if (runs.length){
          formatRequests.push({
            updateCells: {
              range: { sheetId: getSheetIdFromMeta(sheets._meta, TAB_NAME), startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
              rows: [{ values: [{ userEnteredValue:{ stringValue: matchup }, textFormatRuns: runs }] }],
              fields: "userEnteredValue,textFormatRuns"
            }
          });
        }
        const grading = gradeRow(finalPair, row[h["a spread"]], row[h["a ml"]], row[h["h spread"]], row[h["h ml"]], row[h["total"]]);
        const colorMap = { green:{red:0.85,green:0.95,blue:0.85}, red:{red:0.98,green:0.85,blue:0.85} };
        const colIdx = { G:h["a spread"], H:h["a ml"], I:h["h spread"], J:h["h ml"], K:h["total"] };
        for (const key of ["G","H","I","J","K"]){
          const ci = colIdx[key];
          if (ci==null) continue;
          const bg = grading[key];
          formatRequests.push({
            repeatCell:{
              range:{ sheetId: getSheetIdFromMeta(sheets._meta, TAB_NAME), startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: ci, endColumnIndex: ci+1 },
              cell:{ userEnteredFormat:{ backgroundColor: bg ? colorMap[bg] : {red:1,green:1,blue:1} } },
              fields:"userEnteredFormat.backgroundColor"
            }
          });
        }
        updCell(rowNum, h["status"], "Final");
      }

      // 🎯 LIVE ODDS BLOCK (Halftime-only scrape placeholder)
      if (LIVE_ENABLED && /HALF/i.test(statusName)) {
        const liveCells = ["h score","h a spread","h a ml","h h spread","h h ml","h total"];
        for (const key of liveCells) {
          if (h[key]==null) continue;
          const existing = (row[h[key]]||"").toString().trim();
          if (!existing) {
            updCell(rowNum, h[key], "—"); // placeholder until Playwright odds inserted
          }
        }
      }
    }
  }

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
      const h2 = sheets.hmap;
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

/* =========================
   Helpers needing sheetId
   ========================= */
function getSheetIdFromMeta(meta, title){
  const s = (meta?.data?.sheets||[]).find(x=>x.properties?.title===title);
  return s?.properties?.sheetId;
}
