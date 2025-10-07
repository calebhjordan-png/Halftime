import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"

/** Column names (Half Score header) */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ====== Helpers ====== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

/* --- ET formatters (no string re-parsing!) --- */
const ET_TZ = "America/New_York";

function fmtETTime(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true
  }).format(new Date(dateLike));
}

function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dateLike));
}

function yyyymmddInET(d=new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(d));
  const get = k => parts.find(p=>p.type===k)?.value || "";
  return `${get("year")}${get("month")}${get("day")}`;
}

async function fetchJson(url) {
  log("GET", url);
  const res = await fetch(url, { headers: { "User-Agent": "halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, dates) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
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
    oddsArr.find(o => /espn bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}
function mapHeadersToIndex(headerRow) {
  const map = {};
  headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }

/** Normalize numeric-ish value (preserve +) */
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** ===== Status formatting (fixed to ET) =====
 * Pre: show scheduled New York time like '8:15 PM'
 * Live: show shortDetail (e.g., 'Q2 03:12')
 * Half: 'Half'
 * Final: 'Final'
 */
function tidyStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const tName = (evt.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || comp.status?.type?.shortDetail || "").trim();
  if (tName.includes("FINAL")) return "Final";
  if (tName.includes("HALFTIME")) return "Half";
  if (tName.includes("IN_PROGRESS") || tName.includes("LIVE")) {
    return short || "In Progress";
  }
  // scheduled → format directly in ET (no TZ suffix)
  return fmtETTime(evt.date);
}

/** NFL Week label via calendar if number missing */
function resolveWeekLabelNFL(sb, eventDateISO) {
  const wnum = sb?.week?.number;
  if (Number.isFinite(wnum)) return `Week ${wnum}`;
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(eventDateISO).getTime();
  for (const item of cal) {
    const entries = Array.isArray(item?.entries) ? item.entries : [item];
    for (const e of entries) {
      const label = e?.label || e?.detail || e?.text || "";
      const start = e?.startDate || e?.start;
      const end   = e?.endDate   || e?.end;
      if (!start || !end) continue;
      const s = new Date(start).getTime();
      const ed = new Date(end).getTime();
      if (Number.isFinite(s) && Number.isFinite(ed) && t >= s && t <= ed && /Week\s*\d+/i.test(label)) {
        return label;
      }
    }
  }
  return "Regular Season";
}

/** ===== ML extractor (includes PickCenter shapes) ===== */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";

  // PickCenter: awayTeamOdds/homeTeamOdds.moneyLine
  if (o && (o.awayTeamOdds || o.homeTeamOdds)) {
    const aObj = o.awayTeamOdds || {};
    const hObj = o.homeTeamOdds || {};
    awayML = awayML || numOrBlank(aObj.moneyLine ?? aObj.moneyline ?? aObj.money_line);
    homeML = homeML || numOrBlank(hObj.moneyLine ?? hObj.moneyline ?? hObj.money_line);
    if (awayML || homeML) return { awayML, homeML };
  }

  // PickCenter: moneyline.away/home.close/open.odds
  if (o && o.moneyline && (o.moneyline.away || o.moneyline.home)) {
    const awayClose = numOrBlank(o.moneyline.away?.close?.odds ?? o.moneyline.away?.open?.odds);
    const homeClose = numOrBlank(o.moneyline.home?.close?.odds ?? o.moneyline.home?.open?.odds);
    awayML = awayML || awayClose;
    homeML = homeML || homeClose;
    if (awayML || homeML) return { awayML, homeML };
  }

  // teamOdds[]
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid === String(awayId)) awayML = awayML || ml;
      if (tid === String(homeId)) homeML = homeML || ml;
    }
    if (awayML || homeML) return { awayML, homeML };
  }

  // nested competitors odds
  if (Array.isArray(o?.competitors)) {
    const findML = c => numOrBlank(c?.moneyLine ?? c?.moneyline ?? c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const aML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(awayId)));
    const hML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(homeId)));
    awayML = awayML || aML; homeML = homeML || hML;
    if (awayML || homeML) return { awayML, homeML };
  }

  // direct away/home fields
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);
  if (awayML || homeML) return { awayML, homeML };

  // favorite/underdog mapping
  const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
  const favML = numOrBlank(o?.favoriteMoneyLine);
  const dogML = numOrBlank(o?.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) { awayML = awayML || favML; homeML = homeML || dogML; return { awayML, homeML }; }
    if (String(homeId) === favId) { homeML = homeML || favML; awayML = awayML || dogML; return { awayML, homeML }; }
  }

  // competitors[] odds.moneyLine variant
  if (Array.isArray(competitors)) {
    for (const c of competitors) {
      const ml = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!ml) continue;
      if (c.homeAway === "away") awayML = awayML || ml;
      if (c.homeAway === "home") homeML = homeML || ml;
    }
  }

  return { awayML, homeML };
}

/** ML fallback: /summary */
async function extractMLWithFallback(event, baseOdds, away, home) {
  const base = extractMoneylines(baseOdds || {}, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
  if (base.awayML && base.homeML) return base;

  try {
    const sum = await fetchJson(summaryUrl(LEAGUE, event.id));
    const candidates = [];
    if (Array.isArray(sum?.header?.competitions?.[0]?.odds)) candidates.push(...sum.header.competitions[0].odds);
    if (Array.isArray(sum?.odds)) candidates.push(...sum.odds);
    if (Array.isArray(sum?.pickcenter)) candidates.push(...sum.pickcenter);

    for (const cand of candidates) {
      const ml = extractMoneylines(cand, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
      if (ml.awayML || ml.homeML) return ml;
    }
  } catch (e) {
    warn("Summary fallback failed:", e?.message || e);
  }

  return base;
}

/** Build pregame row */
function pregameRowFactory(sbForDay) {
  return async function pregameRow(event) {
    const comp = event.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    const isFinal = /final/i.test(event.status?.type?.name || comp.status?.type?.name || "");
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";

    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";
      const favId = String(o0.favorite || o0.favoriteTeamId || "");
      const spread = Number.isFinite(o0.spread) ? o0.spread :
                     (typeof o0.spread === "string" ? parseFloat(o0.spread) : NaN);
      if (!Number.isNaN(spread) && favId) {
        if (String(away?.team?.id||"") === favId) {
          awaySpread = `-${Math.abs(spread)}`;
          homeSpread = `+${Math.abs(spread)}`;
        } else if (String(home?.team?.id||"") === favId) {
          homeSpread = `-${Math.abs(spread)}`;
          awaySpread = `+${Math.abs(spread)}`;
        }
      } else if (o0.details) {
        const m = o0.details.match(/([+-]?\d+(\.\d+)?)/);
        if (m) {
          const line = parseFloat(m[1]);
          awaySpread = line > 0 ? `+${Math.abs(line)}` : `${line}`;
          homeSpread = line > 0 ? `-${Math.abs(line)}` : `+${Math.abs(line)}`;
        }
      }
      const ml = await extractMLWithFallback(event, o0, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    } else {
      const ml = await extractMLWithFallback(event, {}, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    }

    // Week label
    const weekText = (normLeague(LEAGUE) === "nfl")
      ? resolveWeekLabelNFL(sbForDay, event.date)
      : (sbForDay?.week?.text || "Regular Season");

    // Status & Date (ET)
    const statusClean = tidyStatus(event);
    const dateET = fmtETDate(event.date);

    return {
      values: [
        dateET,                 // Date
        weekText || "",         // Week
        statusClean,            // Status
        matchup,                // Matchup
        finalScore,             // Final Score
        awaySpread || "",       // Away Spread
        String(awayML || ""),   // Away ML
        homeSpread || "",       // Home Spread
        String(homeML || ""),   // Home ML
        String(total || ""),    // Total
        "", "", "", "", "", ""  // live cols (later)
      ],
      dateET,
      matchup
    };
  };
}

/** Halftime-ish? */
function isHalftimeLike(evt) {
  const t = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /Q2.*0:0?0/i.test(short) || /HALF/.test(short);
}

/** Playwright DOM scrape for LIVE odds at halftime (one-time) */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(()=>{});
    await page.waitForTimeout(500);

    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });
    const txt = (await section.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

    const spreadMatches = txt.match(/([+-]\d+(\.\d+)?)/g) || [];
    const totalOver = txt.match(/o\s?(\d+(\.\d+)?)/i);
    const totalUnder = txt.match(/u\s?(\d+(\.\d+)?)/i);
    const mlMatches = txt.match(/\s[+-]\d{2,4}\b/g) || [];

    const liveAwaySpread = spreadMatches[0] || "";
    const liveHomeSpread = spreadMatches[1] || "";
    const liveTotal = (totalOver && totalOver[1]) || (totalUnder && totalUnder[1]) || "";
    const liveAwayML = (mlMatches[0]||"").trim();
    const liveHomeML = (mlMatches[1]||"").trim();

    let halfScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (sc) halfScore = `${sc[1]}-${sc[2]}`;
    } catch {}

    return {
      liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore
    };
  } catch (err) {
    warn("Live DOM scrape failed:", err.message, url);
    return null;
  } finally {
    await browser.close();
  }
}

/** Batch writer */
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0) + i); }
class BatchWriter {
  constructor(tab){ this.tab = tab; this.acc = []; }
  add(row, colIdx, value){
    if (colIdx==null || colIdx<0) return;
    const range = `${this.tab}!${colLetter(colIdx)}${row}:${colLetter(colIdx)}${row}`;
    this.acc.push({ range, values: [[value]] });
  }
  async flush(sheets){
    if (!this.acc.length) return;
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: this.acc }
    });
    log(`🟩 Batched ${this.acc.length} cell update(s).`);
    this.acc = [];
  }
}

/** Center-align all columns A–P (idempotent) */
async function applyCenterFormatting(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME)?.properties?.sheetId;
  if (sheetId == null) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 0, startColumnIndex: 0, endColumnIndex: 16 },
            cell: { userEnteredFormat: { horizontalAlignment: "CENTER" } },
            fields: "userEnteredFormat.horizontalAlignment"
          }
        }
      ]
    }
  });
}

/** ====== MAIN ====== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing secrets.");
    process.exit(1);
  }
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
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:Z`,
  });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [COLS] }
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

  // Apply centering (A..P)
  await applyCenterFormatting(sheets);

  // Pull events (today or week)
  const datesList = RUN_SCOPE === "week"
    ? (()=>{ // Tue→Mon window (computed in ET for the yyyymmdd param)
        const today = new Date();
        const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ, weekday:"short"}).formatToParts(today);
        // We'll just start from today's ET date and go 7 days; scoreboard accepts each ET date fine.
        const start = new Date(today);
        return Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(start.getTime()+i*86400000)));
      })()
    : [ yyyymmddInET(new Date()) ];

  let firstDaySB = null;
  let events = [];
  for (const d of datesList) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!firstDaySB) firstDaySB = sb;
    events = events.concat(sb?.events || []);
  }
  const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  const buildPregame = pregameRowFactory(firstDaySB);
  let appendBatch = [];

  // Pregame append
  for (const ev of events) {
    const { values: rowVals, dateET, matchup } = await buildPregame(ev);
    const k = keyOf(dateET, matchup);
    if (!keyToRowNum.has(k)) appendBatch.push(rowVals);
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: appendBatch },
    });
    // Refresh index
    const re = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const v2 = re.data.values || [];
    const hdr2 = v2[0] || header;
    const h2 = mapHeadersToIndex(hdr2);
    (v2.slice(1)).forEach((r, i) => {
      const key = keyOf(r[h2["date"]], r[h2["matchup"]]);
      keyToRowNum.set(key, i + 2);
    });
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // Halftime / Final updates (batched finals)
  const batch = new BatchWriter(TAB_NAME);

  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = fmtETDate(ev.date);
    const rowNum = keyToRowNum.get(keyOf(dateET, matchup));
    if (!rowNum) continue;

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;

    // Final score
    if (statusName.includes("FINAL")) {
      if (hmap["final score"] !== undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if (hmap["status"] !== undefined)      batch.add(rowNum, hmap["status"], "Final");
      continue;
    }

    // Halftime (one-time) live odds
    const currentRow = (values[rowNum-1] || []);
    const halfAlready = (currentRow[hmap["half score"]] || "").toString().trim();
    const liveTotalVal = (currentRow[hmap["live total"]] || "").toString().trim();
    if (isHalftimeLike(ev) && !liveTotalVal && !halfAlready) {
      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (live) {
        const { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore } = live;

        const payload = [];
        const add = (name,val) => {
          const idx = hmap[name]; if (idx===undefined || !val) return;
          const range = `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
          payload.push({ range, values: [[val]] });
        };
        add("status", "Half");
        if (halfScore) add("half score", halfScore);
        add("live away spread", liveAwaySpread);
        add("live home spread", liveHomeSpread);
        add("live away ml", liveAwayML);
        add("live home ml", liveHomeML);
        add("live total", liveTotal);

        if (payload.length) {
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID,
            requestBody: { valueInputOption: "RAW", data: payload }
          });
        }
        log(`🕐 Halftime LIVE written for ${matchup}`);
      } else {
        log(`(Halftime) live odds not found for ${matchup}`);
      }
    }
  }

  await batch.flush(sheets);
  log("✅ Run complete.");
})().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
