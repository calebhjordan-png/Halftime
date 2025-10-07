// Halftime Bot — VERBOSE + BATCHED + CFB Week Fallback (2025)
// - Pregame append (today|week)
// - Halftime-only capture (status->"Half", "Half Score", live odds)
// - Finals (batched to avoid 429)
// - College Football week label fallback using 2025 ranges
// - Keeps public/free ESPN + Google Sheets approach

import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG (from GitHub Actions env) ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const WEEK_OVERRIDE = process.env.WEEK_OVERRIDE ? Number(process.env.WEEK_OVERRIDE) : null;

/** ====== SHEET COLUMNS (order matters) ====== */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ====== Logging ====== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

/** ====== Utilities ====== */
function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw);
  const json = Buffer.from(raw, "base64").toString("utf8");
  return JSON.parse(json);
}
function toET(dateLike) {
  return new Date(new Date(dateLike).toLocaleString("en-US", { timeZone: "America/New_York" }));
}
function yyyymmddInET(d=new Date()) {
  const et = toET(d);
  const y = et.getFullYear();
  const m = String(et.getMonth()+1).padStart(2,"0");
  const day = String(et.getDate()).padStart(2,"0");
  return `${y}${m}${day}`;
}
async function fetchJson(url) {
  log("GET", url);
  const res = await fetch(url, {
    headers: {
      "User-Agent": "halftime-bot",
      "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
      "Referer": "https://www.espn.com/"
    }
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, { dates, week }) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  if (week != null && Number.isFinite(week)) {
    return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?week=${week}${extra}`;
  }
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function summaryUrl(league, eventId) {
  const lg = normLeague(league);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${eventId}`;
}
function gameUrl(league, gameId) {
  const lg = normLeague(league);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}
function pickOdds(oddsArr=[]) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}
function mapHeadersToIndex(headerRow) {
  const map = {};
  headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }
const sleep = ms => new Promise(r=>setTimeout(r,ms));
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** ====== CFB Week Fallback (2025) ======
 * Ranges are ET inclusive on both ends.
 * If ESPN returns "Regular Season", we map by date below.
 * (You can tweak/extend later or swap per-season constants.)
 */
const CFB_2025_RANGES = [
  // From screenshots: Week 4..16, Bowls, CFP
  { label: "Week 4",  start: "2025-09-15", end: "2025-09-21" },
  { label: "Week 5",  start: "2025-09-22", end: "2025-09-28" },
  { label: "Week 6",  start: "2025-09-29", end: "2025-10-05" },
  { label: "Week 7",  start: "2025-10-06", end: "2025-10-12" },
  { label: "Week 8",  start: "2025-10-13", end: "2025-10-19" },
  { label: "Week 9",  start: "2025-10-20", end: "2025-10-26" },
  { label: "Week 10", start: "2025-10-27", end: "2025-11-02" },
  { label: "Week 11", start: "2025-11-03", end: "2025-11-09" },
  { label: "Week 12", start: "2025-11-10", end: "2025-11-16" },
  { label: "Week 13", start: "2025-11-17", end: "2025-11-23" },
  { label: "Week 14", start: "2025-11-24", end: "2025-11-30" },
  { label: "Week 15", start: "2025-12-01", end: "2025-12-07" },
  { label: "Week 16", start: "2025-12-08", end: "2025-12-13" },
  { label: "Bowls",   start: "2025-12-13", end: "2026-01-20" },
  { label: "CFP",     start: "2025-12-19", end: "2026-01-19" }, // overlay
];
function inRangeET(d, startISO, endISO) {
  const x = toET(d).getTime();
  const s = toET(startISO + "T00:00:00").getTime();
  const e = toET(endISO   + "T23:59:59").getTime();
  return x >= s && x <= e;
}
function cfbWeekFallbackLabel(dateObj) {
  // Prefer specific CFP overlay label first
  for (const r of CFB_2025_RANGES.filter(r=>r.label==="CFP")) {
    if (inRangeET(dateObj, r.start, r.end)) return r.label;
  }
  // Then regular weeks/bowls
  for (const r of CFB_2025_RANGES.filter(r=>r.label!=="CFP")) {
    if (inRangeET(dateObj, r.start, r.end)) return r.label;
  }
  return "Regular Season";
}
function computeWeekLabel(league, espnWeekText, dateStrET) {
  if (/Week\s*\d+/i.test(espnWeekText || "")) return espnWeekText;
  if (normLeague(league) === "college-football") {
    const parsed = toET(dateStrET);
    return cfbWeekFallbackLabel(parsed);
  }
  // NFL: ESPN week text is usually fine; fallback to "Regular Season"
  return espnWeekText || "Regular Season";
}

/** ====== Week math (Tue→Mon ET) for RUN_SCOPE=week ====== */
function startOfLeagueWeekET(d=new Date()) {
  const et = toET(d);
  const dow = et.getDay(); // 0 Sun ... 6 Sat
  const offsetToTue = ((dow - 2) + 7) % 7;
  const start = new Date(et);
  start.setDate(et.getDate() - offsetToTue);
  start.setHours(0,0,0,0);
  return start;
}
function datesForWeekET(ref=new Date()) {
  const start = startOfLeagueWeekET(ref);
  const out = [];
  for (let i=0;i<7;i++){
    const d = new Date(start);
    d.setDate(start.getDate()+i);
    out.push(yyyymmddInET(d));
  }
  return out;
}
function uniqueById(events) {
  const seen = new Set();
  const out = [];
  for (const e of events) {
    const id = String(e?.id || "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(e);
  }
  return out;
}

/** ===== Moneylines extractor ===== */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";
  const byId = (tid, ml) => {
    if (!ml) return;
    if (String(tid) === String(awayId)) awayML = awayML || ml;
    if (String(tid) === String(homeId)) homeML = homeML || ml;
  };
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      byId(tid, ml);
    }
  }
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);
  if (!awayML || !homeML) {
    const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
    const favML = numOrBlank(o?.favoriteMoneyLine);
    const dogML = numOrBlank(o?.underdogMoneyLine);
    if (favId && (favML || dogML)) {
      if (String(awayId) === favId) { awayML = awayML || favML; homeML = homeML || dogML; }
      else if (String(homeId) === favId) { homeML = homeML || favML; awayML = awayML || dogML; }
    }
  }
  if ((!awayML || !homeML) && Array.isArray(competitors)) {
    for (const c of competitors) {
      const cand = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!cand) continue;
      if (c.homeAway === "away") awayML = awayML || cand;
      if (c.homeAway === "home") homeML = homeML || cand;
    }
  }
  return { awayML, homeML };
}

/** ===== Build pregame row ===== */
function pregameRow(event, weekTextFromESPN) {
  const comp = event.competitions?.[0] || {};
  const status = event.status?.type?.name || comp.status?.type?.name || "";
  const shortStatus = event.status?.type?.shortDetail || comp.status?.type?.shortDetail || "";
  const competitors = comp?.competitors || [];
  const away = competitors.find(c => c.homeAway === "away");
  const home = competitors.find(c => c.homeAway === "home");
  const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  const matchup = `${awayName} @ ${homeName}`;
  const finalScore = /final/i.test(status) ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

  const o = pickOdds(comp.odds || event.odds || []);
  let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";
  if (o) {
    total = (o.overUnder ?? o.total) ?? "";
    const favId = String(o.favorite || "");
    const spread = Number.isFinite(o.spread) ? o.spread : (typeof o.spread === "string" ? parseFloat(o.spread) : NaN);
    if (!Number.isNaN(spread) && favId) {
      if (String(away?.team?.id||"") === favId) { awaySpread = `-${Math.abs(spread)}`; homeSpread = `+${Math.abs(spread)}`; }
      else if (String(home?.team?.id||"") === favId) { homeSpread = `-${Math.abs(spread)}`; awaySpread = `+${Math.abs(spread)}`; }
    } else if (o.details) {
      const m = o.details.match(/([+-]?\d+(\.\d+)?)/);
      if (m) { const line = parseFloat(m[1]);
        awaySpread = line > 0 ? `+${Math.abs(line)}` : `${line}`;
        homeSpread = line > 0 ? `-${Math.abs(line)}` : `+${Math.abs(line)}`;
      }
    }
    const ids = { awayId: away?.team?.id, homeId: home?.team?.id };
    const ml = extractMoneylines(o, ids.awayId, ids.homeId, competitors);
    awayML = ml.awayML || ""; homeML = ml.homeML || "";
  }

  const dateET = toET(event.date);
  const dateETStr = dateET.toLocaleDateString("en-US", { timeZone: "America/New_York" });
  const weekText = computeWeekLabel(LEAGUE, weekTextFromESPN, dateET);

  return {
    values: [dateETStr, weekText || "", shortStatus || status, matchup, finalScore,
      awaySpread || "", String(awayML||""), homeSpread || "", String(homeML||""), String(total||""),
      "", "", "", "", "", ""],
    dateET: dateETStr,
    matchup
  };
}

/** ===== Halftime detection & snapshot ===== */
function isHalftimeLike(evtOrSnap) {
  const t = (evtOrSnap?.status?.type?.name || evtOrSnap?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short = (evtOrSnap?.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /HALF\s*TIME/i.test(short);
}
async function getEventSnapshot(league, eventId) {
  try {
    const sum = await fetchJson(summaryUrl(league, eventId));
    const comp = sum?.header?.competitions?.[0] || {};
    const status = comp?.status || {};
    const competitors = comp?.competitors || [];
    const away = competitors.find(c => c.homeAway === "away");
    const home = competitors.find(c => c.homeAway === "home");
    const aScore = away?.score != null ? String(away.score) : "";
    const hScore = home?.score != null ? String(home.score) : "";
    return { status: { type: { name: status?.type?.name, shortDetail: status?.type?.shortDetail } }, scores: { half: `${aScore}-${hScore}` } };
  } catch { return null; }
}

/** ===== Live odds scraping ===== */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  log("Scrape odds:", url);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(()=>{});
    await page.waitForTimeout(750);

    const table = page.locator('table:has-text("ML"):has-text("Total")').first();
    if (await table.count()) {
      const txt = (await table.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();
      const ml = [...txt.matchAll(/\bML\s*([+-]?\d{2,4})\b/gi)].map(m => m[1]);
      const totals = [...txt.matchAll(/\bTotal\s*([0-9]+(?:\.[0-9])?)\b/gi)].map(m => m[1]);
      const spreads = [...txt.matchAll(/(^|\s)([+-]\d+(?:\.\d+)?)(?!\s*(?:o|u)\b)/gi)]
        .map(m => Number(m[2])).filter(n => Math.abs(n)<=40);
      return {
        liveAwaySpread: spreads[0]!=null ? (spreads[0]>0?`+${spreads[0]}`:`${spreads[0]}`) : "",
        liveHomeSpread:  spreads[1]!=null ? (spreads[1]>0?`+${spreads[1]}`:`${spreads[1]}`) : "",
        liveAwayML: ml[0]||"", liveHomeML: ml[1]||"", liveTotal: totals[0]||""
      };
    }

    const primaryFooter = page.getByText(/All Live Odds on ESPN BET Sportsbook/i).first();
    const altFooter = page.getByText(/^Odds by$/i).first();
    const container = await nearestOddsContainer(primaryFooter) || await nearestOddsContainer(altFooter);
    if (!container) { warn("LIVE ODDS container not found"); return { liveAwaySpread:"",liveHomeSpread:"",liveAwayML:"",liveHomeML:"",liveTotal:"" }; }
    const raw = (await container.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

    const totalMatch =
      raw.match(/\b(?:o\/?u|total)\s*([0-9]+(?:\.[0-9])?)/i) ||
      raw.match(/\bo\s*([0-9]+(?:\.[0-9])?)\b/i) ||
      raw.match(/\bu\s*([0-9]+(?:\.[0-9])?)\b/i);
    const liveTotal = totalMatch ? totalMatch[1] : "";

    const spreadNums = [...raw.matchAll(/([+-]\d+(?:\.\d+)?)(?!\s*(?:o|u)\b)/gi)]
      .map(m => Number(m[1])).filter(n => Number.isFinite(n) && Math.abs(n) <= 40);
    const liveAwaySpread = (spreadNums[0]!=null) ? (spreadNums[0]>0?`+${spreadNums[0]}`:`${spreadNums[0]}`) : "";
    const liveHomeSpread = (spreadNums[1]!=null) ? (spreadNums[1]>0?`+${spreadNums[1]}`:`${spreadNums[1]}`) : "";

    const mlTokens = [...raw.matchAll(/\bML\s*([+-]?\d{2,4})\b/gi)].map(m => m[1]);
    let liveAwayML = mlTokens[0] || "";
    let liveHomeML = mlTokens[1] || "";
    if (!liveAwayML || !liveHomeML) {
      const bare = [...raw.matchAll(/\s([+-]\d{2,4})\s/g)].map(m => m[1]);
      liveAwayML = liveAwayML || bare[0] || "";
      liveHomeML = liveHomeML || bare[1] || "";
    }
    return { liveAwaySpread, liveHomeSpread, liveAwayML, liveHomeML, liveTotal };

  } catch (err) {
    warn("Live DOM scrape failed:", err.message);
    return { liveAwaySpread:"", liveHomeSpread:"", liveAwayML:"", liveHomeML:"", liveTotal:"" };
  } finally {
    await page.close().catch(()=>{});
    await browser.close().catch(()=>{});
  }

  async function nearestOddsContainer(locator) {
    try {
      await locator.wait({ timeout: 4000 });
      const el = await locator.elementHandle();
      if (!el) return null;
      return await el.evaluateHandle(node => {
        let cur = node;
        for (let i=0; i<6 && cur && cur.parentElement; i++) {
          cur = cur.parentElement;
          if (cur?.querySelector && (cur.querySelector('table') || cur.querySelector('[role="table"]'))) return cur;
        }
        return node.parentElement || node;
      });
    } catch { return null; }
  }
}

/** ====== Batch update manager (avoid 429s) ====== */
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0) + i); }
class BatchWriter {
  constructor(tabName){ this.tab = tabName; this.data = []; }
  addCell(rowNumber, colIndex, value){
    if (colIndex == null || colIndex < 0) return;
    const range = `${this.tab}!${colLetter(colIndex)}${rowNumber}:${colLetter(colIndex)}${rowNumber}`;
    this.data.push({ range, values: [[value]] });
  }
  async flush(sheets){
    if (!this.data.length) return;
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: this.data }
    });
    log(`🟩 Batched ${this.data.length} cell update(s).`);
    this.data = [];
  }
}

/** ====== MAIN ====== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) { console.error("Missing secrets."); process.exit(1); }
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Ensure tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets || []).map(s => s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    log("Creating sheet tab:", TAB_NAME);
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    log("Writing header row");
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [COLS] }
    });
    header = COLS.slice();
  }
  const hmap = mapHeadersToIndex(header);
  const rows = values.slice(1);
  const keyToRowNum = new Map();
  rows.forEach((r, i) => {
    const k = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
    keyToRowNum.set(k, i + 2);
  });

  // Fetch events (today|week|override)
  let events = [];
  let weekTextFromESPN = "Regular Season";
  if (WEEK_OVERRIDE != null && Number.isFinite(WEEK_OVERRIDE)) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, { week: WEEK_OVERRIDE }));
    weekTextFromESPN = sb?.week?.text || (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
    events = sb?.events || [];
  } else if (RUN_SCOPE === "week") {
    const all = datesForWeekET(new Date());
    let agg = [];
    for (const d of all) {
      const sb = await fetchJson(scoreboardUrl(LEAGUE, { dates: d }));
      if (weekTextFromESPN === "Regular Season") {
        weekTextFromESPN = sb?.week?.text || (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
      }
      agg = agg.concat(sb?.events || []);
    }
    events = uniqueById(agg);
  } else {
    const d = yyyymmddInET(new Date());
    const sb = await fetchJson(scoreboardUrl(LEAGUE, { dates: d }));
    weekTextFromESPN = sb?.week?.text || (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
    events = sb?.events || [];
  }
  log(`Events found: ${events.length}, ESPN week: ${weekTextFromESPN}`);

  // Pregame append
  let appendBatch = [];
  for (const ev of events) {
    const { values: rowVals, dateET, matchup } = pregameRow(ev, weekTextFromESPN);
    const k = keyOf(dateET, matchup);
    if (!keyToRowNum.has(k)) appendBatch.push(rowVals);
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: appendBatch },
    });
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
    // Refresh map
    const re = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const v2 = re.data.values || []; const hdr2 = v2[0] || header; const h2 = mapHeadersToIndex(hdr2);
    (v2.slice(1)).forEach((r, i) => { const key = keyOf(r[h2["date"]], r[h2["matchup"]]); keyToRowNum.set(key, i + 2); });
  }

  /** ===== Halftime + Finals (batched finals) ===== */
  const nowET = toET(new Date());
  const batch = new BatchWriter(TAB_NAME);

  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = toET(ev.date).toLocaleDateString("en-US", { timeZone: "America/New_York" });
    const rowNum = keyToRowNum.get(keyOf(dateET, matchup));
    if (!rowNum) continue;

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const scorePairFinal = `${away?.score ?? ""}-${home?.score ?? ""}`;

    // Final → queue batched writes
    if (statusName.includes("FINAL")) {
      if (hmap["final score"] !== undefined) batch.addCell(rowNum, hmap["final score"], scorePairFinal);
      if (hmap["status"]      !== undefined) batch.addCell(rowNum, hmap["status"], ev.status?.type?.shortDetail || "Final");
      continue;
    }

    // Halftime one-time (guard using existing row)
    const snapshotRow = (await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A${rowNum}:Z${rowNum}`,
    })).data.values?.[0] || [];
    const halfAlready = (snapshotRow[hmap["half score"]] || "").toString().trim();
    const liveTotalAlready = (snapshotRow[hmap["live total"]] || "").toString().trim();
    if (halfAlready || liveTotalAlready) continue;

    if (isHalftimeLike(ev)) {
      log(`HALFTIME (scoreboard) for ${matchup} → writing now.`);
      await writeHalftime(sheets, rowNum, ev.id, hmap, matchup);
      continue;
    }

    // Watch window (55–95 mins after scheduled kickoff)
    const kickET = toET(ev.date);
    const minsSinceKick = (nowET - kickET) / 60000;
    if (minsSinceKick >= 55 && minsSinceKick <= 95) {
      log(`⏳ Watch window for ${matchup}`);
      const attempts = 7; const waitMs = 90*1000;
      for (let i=0;i<attempts;i++){
        log(`  • Poll ${i+1}/${attempts}`);
        const snap = await getEventSnapshot(LEAGUE, ev.id);
        const isHalf = snap && isHalftimeLike(snap);
        log(`    status=${snap?.status?.type?.name || "?"} short=${snap?.status?.type?.shortDetail || "?"} isHalf=${!!isHalf}`);
        if (isHalf) { await writeHalftime(sheets, rowNum, ev.id, hmap, matchup, snap?.scores?.half); break; }
        await sleep(waitMs);
      }
    }
  }

  await batch.flush(sheets); // flush finals
  log("✅ Run complete.");
})().catch(err => { console.error("❌ Error:", err); process.exit(1); });

/** ===== Halftime write (status->Half) — batched in one call ===== */
async function writeHalftime(sheets, rowNum, eventId, hmap, matchup, halfScoreFromSnap="") {
  const payload = [];

  const add = (colName, value) => {
    const idx = hmap[colName];
    if (idx === undefined) return;
    const col = String.fromCharCode(65 + idx);
    payload.push({ range: `${TAB_NAME}!${col}${rowNum}:${col}${rowNum}`, values: [[value]] });
  };

  add("status", "Half");
  if (halfScoreFromSnap) add("half score", halfScoreFromSnap);

  // Odds with short retry — halftime DOM can be late
  let live = null;
  for (let i=0; i<4; i++) {
    live = await scrapeLiveOddsOnce(LEAGUE, eventId);
    const gotAny = !!(live && (live.liveTotal || live.liveAwaySpread || live.liveHomeSpread || live.liveAwayML || live.liveHomeML));
    if (gotAny) break;
    await sleep(7500);
  }
  if (!live) live = { liveAwaySpread:"", liveHomeSpread:"", liveAwayML:"", liveHomeML:"", liveTotal:"" };

  add("live away spread", live.liveAwaySpread || "-");
  add("live home spread", live.liveHomeSpread || "-");
  add("live away ml",     live.liveAwayML     || "-");
  add("live home ml",     live.liveHomeML     || "-");
  add("live total",       live.liveTotal      || "-");

  if (payload.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: payload }
    });
  }
  log(`🕐 Halftime LIVE written for ${matchup}`);
}
