// halftime-checker.mjs
import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== ENV ======
 * GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT (raw JSON or base64)
 * LEAGUE: "nfl" | "college-football"
 * TAB_NAME: e.g., "NFL" | "CFB"
 * RUN_SCOPE: "today" (default) — only today's games
 * MAX_RUNTIME_MIN: hard stop for this run (default 120)
 */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();
const MAX_RUNTIME_MIN = Number(process.env.MAX_RUNTIME_MIN ?? 120);

const ET_TZ = "America/New_York";
const MIN_RECHECK_MIN = 2;   // never less than 2 minutes
const MAX_RECHECK_MIN = 20;  // your requested cap

/* ---------- Small utils ---------- */
const log  = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

function parseServiceAccount(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);     // raw JSON
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
function normLeague(lg) {
  return (lg === "ncaaf" || lg === "college-football") ? "college-football" : "nfl";
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
function scoreboardUrl(league, dates) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function gameUrl(league, gameId) {
  const lg = normLeague(league);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}
async function fetchJson(url) {
  log("GET", url);
  const r = await fetch(url, { headers: { "User-Agent":"halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!r.ok) throw new Error(`Fetch failed ${r.status} ${url}`);
  return r.json();
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }
function mapHeadersToIndex(headerRow) {
  const map = {}; (headerRow||[]).forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i); return map;
}
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0) + i); }

/* ---------- Halftime detection helpers ---------- */
function parseShortDetailClock(shortDetail="") {
  const s = String(shortDetail).trim().toUpperCase();
  if (/HALF|HALFTIME/.test(s)) return { quarter: 2, min: 0, sec: 0, halftime: true };
  if (/FINAL/.test(s)) return { final: true };
  const m = s.match(/Q?(\d)\D+(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return { quarter: Number(m[1]), min: Number(m[2]), sec: Number(m[3]) };
}
function isHalftimeLike(evt) {
  const t = (evt.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /HALF/.test(short) || /Q2.*0:0?0/.test(short);
}
function clampRecheck(mins) {
  return Math.max(MIN_RECHECK_MIN, Math.min(MAX_RECHECK_MIN, Math.ceil(mins)));
}
function minutesAfterKickoff(evt) {
  const kickoff = new Date(evt.date).getTime();
  return (Date.now() - kickoff) / 60000;
}
/* Kickoff + ~65 min, only if Half Score empty */
function kickoff65CandidateMinutes(evt, rowValues, hmap) {
  const halfScore = (rowValues?.[hmap["half score"]] || "").toString().trim();
  if (halfScore) return null;
  const mins = minutesAfterKickoff(evt);
  if (mins < 60 || mins > 80) return null; // only consider near the window
  const remaining = 65 - mins;
  return clampRecheck(remaining <= 0 ? MIN_RECHECK_MIN : remaining);
}
/* Q2 < 10:00 → wait = 2 × minutes_left */
function q2AdaptiveCandidateMinutes(evt) {
  const pd = parseShortDetailClock(evt.status?.type?.shortDetail || "");
  if (!pd || pd.final || pd.halftime) return null;
  if (pd.quarter !== 2) return null;
  const minutesLeft = pd.min + pd.sec/60;
  if (minutesLeft >= 10) return null;
  return clampRecheck(2 * minutesLeft);
}

/* ---------- One-time LIVE ODDS scrape at halftime ---------- */
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
    const totalOver  = txt.match(/o\s?(\d+(\.\d+)?)/i);
    const totalUnder = txt.match(/u\s?(\d+(\.\d+)?)/i);
    const mlMatches  = txt.match(/\s[+-]\d{2,4}\b/g) || [];

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

    return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore };
  } catch (err) {
    warn("Live DOM scrape failed:", err.message, url);
    return null;
  } finally {
    await browser.close();
  }
}

/* ---------- Center alignment (idempotent) ---------- */
async function centerAlignAll(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME)?.properties?.sheetId;
  if (sheetId == null) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [{
      repeatCell: {
        range: { sheetId, startRowIndex: 0, startColumnIndex: 0, endColumnIndex: 16 },
        cell: { userEnteredFormat: { horizontalAlignment: "CENTER" } },
        fields: "userEnteredFormat.horizontalAlignment"
      }
    }] }
  });
}

/* ---------- MAIN LOOP (master-delay only) ---------- */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing secrets."); process.exit(1);
  }
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Ensure header map + centering
  const read0 = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const header = (read0.data.values || [])[0] || [];
  if (header.length === 0) throw new Error(`Sheet tab "${TAB_NAME}" missing header row.`);
  const hmap = mapHeadersToIndex(header);
  await centerAlignAll(sheets);

  const startWall = Date.now();
  const hardStopMs = MAX_RUNTIME_MIN * 60 * 1000;

  while (true) {
    // 1) Load today's events
    const days = RUN_SCOPE === "week"
      ? Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(Date.now()+i*86400000)))
      : [yyyymmddInET(new Date())];

    let events = [];
    for (const d of days) {
      const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
      events = events.concat(sb?.events || []);
    }
    const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));

    // 2) Snapshot current sheet rows
    const grid = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const rows = (grid.data.values || []).slice(1); // without header

    // Build a date+matchup → rowNumber index (Date ET + "Away @ Home")
    const indexMap = new Map();
    for (let i=0;i<rows.length;i++) {
      const r = rows[i];
      const k = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
      indexMap.set(k, i+2);
    }

    // 3) Try to write halftime for any eligible games; collect candidate delays
    let masterDelayMin = null;
    let halftimeWrites = 0;

    for (const ev of events) {
      const comp = ev.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");
      const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
      const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
      const matchup  = `${awayName} @ ${homeName}`;
      const dateET   = fmtETDate(ev.date);
      const rowNum   = indexMap.get(keyOf(dateET, matchup));
      if (!rowNum) continue;

      const row = rows[rowNum-2] || [];
      const halfAlready = (row[hmap["half score"]] || "").toString().trim();
      const liveTotalVal = (row[hmap["live total"]] || "").toString().trim();

      // If halftime now & not yet written → write once
      if (!halfAlready && !liveTotalVal && isHalftimeLike(ev)) {
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
            halftimeWrites++;
            log(`🕐 Halftime LIVE written for ${matchup}`);
          }
        }
        continue;
      }

      // Otherwise: propose adaptive candidates (master-delay uses the MIN of all)
      // kickoff + ~65 (only if Half Score empty)
      const kCand = kickoff65CandidateMinutes(ev, row, hmap);
      if (kCand != null) masterDelayMin = masterDelayMin == null ? kCand : Math.min(masterDelayMin, kCand);

      // Q2 < 10:00 → wait = 2×minutesLeft
      const q2Cand = q2AdaptiveCandidateMinutes(ev);
      if (q2Cand != null) masterDelayMin = masterDelayMin == null ? q2Cand : Math.min(masterDelayMin, q2Cand);
    }

    if (halftimeWrites > 0) log(`✅ Wrote ${halftimeWrites} halftime row(s).`);

    // 4) Decide whether to sleep again (one master wait)
    const elapsed = Date.now() - startWall;
    if (elapsed > hardStopMs) {
      log("⏹ Max runtime reached — stopping.");
      break;
    }

    if (masterDelayMin == null) {
      log("ℹ️ No adaptive candidates — end of this run.");
      break; // let cron/next run handle the rest
    }

    const waitMs = Math.ceil(masterDelayMin * 60 * 1000);
    const remainingBudget = hardStopMs - elapsed;
    if (waitMs > remainingBudget) {
      log("⏹ Next wait exceeds runtime budget — stopping.");
      break;
    }

    log(`⏳ Master wait: ${masterDelayMin} minute(s). Rechecking...`);
    await new Promise(r => setTimeout(r, waitMs));
    // loop continues: refetch, reevaluate, write, repeat (until no candidates or time budget ends)
  }

  log("✅ Halftime checker run complete.");
})().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
