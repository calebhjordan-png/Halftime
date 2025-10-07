import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const WEEK_OVERRIDE = process.env.WEEK_OVERRIDE ? Number(process.env.WEEK_OVERRIDE) : null;

/** Column names we expect in the sheet */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ====== Helpers ====== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw);            // raw JSON
  const json = Buffer.from(raw, "base64").toString("utf8");   // Base64
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

/** Normalize numeric-ish value */
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** Try to pull moneylines for each team from many ESPN shapes */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";

  const trySetByIds = (teamOddsArr) => {
    if (!Array.isArray(teamOddsArr)) return false;
    for (const t of teamOddsArr) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid && tid === String(awayId)) awayML = awayML || ml;
      if (tid && tid === String(homeId)) homeML = homeML || ml;
    }
    return !!(awayML || homeML);
  };

  // 1) teamOdds: [{ teamId, moneyLine }]
  if (trySetByIds(o.teamOdds)) return { awayML, homeML };

  // 2) nested competitors odds
  if (Array.isArray(o.competitors)) {
    const a = o.competitors.find(c => String(c?.id) === String(awayId) || String(c?.teamId) === String(awayId));
    const h = o.competitors.find(c => String(c?.id) === String(homeId) || String(c?.teamId) === String(homeId));
    if (a) awayML = awayML || numOrBlank(a.moneyLine ?? a.moneyline ?? a?.odds?.moneyLine);
    if (h) homeML = homeML || numOrBlank(h.moneyLine ?? h.moneyline ?? h?.odds?.moneyLine);
    if (awayML || homeML) return { awayML, homeML };
  }

  // 3) direct fields
  awayML = awayML || numOrBlank(o.moneyLineAway ?? o.awayTeamMoneyLine ?? o.awayMoneyLine ?? o.awayMl);
  homeML = homeML || numOrBlank(o.moneyLineHome ?? o.homeTeamMoneyLine ?? o.homeMoneyLine ?? o.homeMl);
  if (awayML || homeML) return { awayML, homeML };

  // 4) favorite/underdog mapping
  const favId = String(o.favorite || o.favoriteId || "");
  const favML = numOrBlank(o.favoriteMoneyLine);
  const dogML = numOrBlank(o.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) { awayML = favML || awayML; homeML = dogML || homeML; return { awayML, homeML }; }
    if (String(homeId) === favId) { homeML = favML || homeML; awayML = dogML || awayML; return { awayML, homeML }; }
  }

  // 5) competitors[] odds object (another variant)
  if (Array.isArray(competitors)) {
    for (const c of competitors) {
      const ml = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!ml) continue;
      if (c.homeAway === "away") awayML = awayML || ml;
      if (c.homeAway === "home") homeML = homeML || ml;
    }
    if (awayML || homeML) return { awayML, homeML };
  }

  return { awayML, homeML };
}

/** ===== ML fallback: if scoreboard lacks MLs, pull from summary ===== */
async function extractMLWithFallback(event, baseOdds, away, home) {
  const base = extractMoneylines(baseOdds || {}, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
  if (base.awayML && base.homeML) return base;

  // Fallback to summary endpoints (multiple shapes)
  try {
    const sum = await fetchJson(summaryUrl(LEAGUE, event.id));
    const candidates = [];

    // Common places MLs live:
    if (Array.isArray(sum?.header?.competitions?.[0]?.odds)) candidates.push(...sum.header.competitions[0].odds);
    if (Array.isArray(sum?.odds)) candidates.push(...sum.odds);
    if (Array.isArray(sum?.pickcenter)) candidates.push(...sum.pickcenter);

    for (const cand of candidates) {
      const ml = extractMoneylines(cand, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
      if (ml.awayML || ml.homeML) {
        console.log(`(ML fallback via summary) ${away?.team?.shortDisplayName} vs ${home?.team?.shortDisplayName}:`, ml);
        return ml;
      }
    }
  } catch (e) {
    warn("Summary fallback failed:", e?.message || e);
  }

  return base; // may be blanks
}

/** Build pregame row */
function computeWeekLabelNFL(espnWeekText, number) {
  if (/Week\s*\d+/i.test(espnWeekText || "")) return espnWeekText;
  if (Number.isFinite(number)) return `Week ${number}`;
  return espnWeekText || "Regular Season";
}

function pregameRowFactory(weekTextFromESPN, weekNumFromESPN) {
  return async function pregameRow(event) {
    const comp = event.competitions?.[0] || {};
    const status = event.status?.type?.name || comp.status?.type?.name || "";
    const shortStatus = event.status?.type?.shortDetail || comp.status?.type?.shortDetail || "";
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    // Final?
    const finalScore = /final/i.test(status)
      ? `${away?.score ?? ""}-${home?.score ?? ""}`
      : "";

    // Odds (spread/total from scoreboard if present)
    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";

    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";

      // spreads
      const favId = String(o0.favorite || "");
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

      // moneylines (robust + summary fallback)
      const ml = await extractMLWithFallback(event, o0, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    } else {
      // No odds block at all -> pure summary fallback
      const ml = await extractMLWithFallback(event, {}, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    }

    // Week label (NFL sometimes gives "Regular Season" but has week number)
    const weekText = normLeague(LEAGUE) === "nfl"
      ? computeWeekLabelNFL(weekTextFromESPN, weekNumFromESPN)
      : (weekTextFromESPN || "Regular Season");

    const dateET = toET(event.date).toLocaleDateString("en-US", { timeZone: "America/New_York" });

    return {
      values: [
        dateET,                 // Date
        weekText || "",         // Week
        shortStatus || status,  // Status
        matchup,                // Matchup
        finalScore,             // Final Score
        awaySpread || "",       // Away Spread
        String(awayML || ""),   // Away ML
        homeSpread || "",       // Home Spread
        String(homeML || ""),   // Home ML
        String(total || ""),    // Total
        "", "", "", "", "", ""  // live cols
      ],
      dateET,
      matchup
    };
  }
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

/** Batch writer (avoid 429s) */
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

  // Pull events (today or week)
  const datesList = RUN_SCOPE === "week"
    ? (()=>{ // Tue→Mon
        const et = toET(new Date());
        const dow = et.getDay(); // 0 Sun..6 Sat
        const offsetToTue = ((dow - 2) + 7) % 7;
        const start = new Date(et); start.setDate(et.getDate()-offsetToTue); start.setHours(0,0,0,0);
        return Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(start.getTime()+i*86400000)));
      })()
    : [ yyyymmddInET(new Date()) ];

  let events = [], weekText = "Regular Season", weekNum = null;
  for (const d of datesList) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!weekNum && Number.isFinite(sb?.week?.number)) weekNum = sb.week.number;
    if (weekText === "Regular Season") weekText = sb?.week?.text || "Regular Season";
    events = events.concat(sb?.events || []);
  }
  // de-dup by id
  const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}, Week label: ${weekText}, Week num: ${weekNum}`);

  const buildPregame = pregameRowFactory(weekText, weekNum);
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

  // Halftime / Final updates (with batched finals)
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
    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;

    // Final score
    if (statusName.includes("FINAL")) {
      if (hmap["final score"] !== undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if (hmap["status"] !== undefined)      batch.add(rowNum, hmap["status"], ev.status?.type?.shortDetail || "Final");
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
