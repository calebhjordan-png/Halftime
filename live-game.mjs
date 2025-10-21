// live-game.mjs
// Status + halftime live odds updater.
// - Status uses "MM/DD – h:mm AM/PM" for scheduled (no EDT/EST/ET).
// - Strips ET token from other ESPN status strings (e.g., Q1 7:12).
// - Writes halftime odds and score once (frozen after Q3 begins).

import axios from "axios";
import { google } from "googleapis";
import * as playwright from "playwright";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",          // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  TARGET_GAME_ID = "",     // optional: force one ID
  ONESHOT = "",            // if "true", do a single pass
  DEBUG_ODDS = "",         // if "true", print odds parse info
  FORCE_STATUS_WRITE = "", // if "true", write Status even in oneshot
} = process.env;

const DEBUG = String(DEBUG_ODDS || "").toLowerCase() === "true";
const log = (...a) => console.log(...a);

/* ───────────────────── Google Sheets bootstrap ───────────────────── */
if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────────────────────────── Helpers ───────────────────────────── */
function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}
function makeValue(range, val) { return { range, values: [[val]] }; }

const ET_TZ = "America/New_York";
const fmtETDateOnly = (d) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, month: "2-digit", day: "2-digit" })
    .format(new Date(d));
const fmtETTimeOnly = (d) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true })
    .format(new Date(d));
const fmtETDateTime = (d) => `${fmtETDateOnly(d)} - ${fmtETTimeOnly(d)}`;
const stripET = (s = "") => String(s).replace(/\s+E[DS]?T\b/i, "").trim();

const normLeague = (x) => (x === "ncaaf" || x === "college-football") ? "college-football" : "nfl";
const summaryUrl = (lg, id) => `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(lg)}/summary?event=${id}`;
const gameUrl = (lg, id) => `https://www.espn.com/${normLeague(lg)}/game/_/gameId/${id}`;

function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}
function isScheduled(statusObj) {
  return /SCHEDULED/i.test(String(statusObj?.type?.name || ""));
}

/* parse current score (away-home) */
function parseScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const hHome = home?.score ?? 0;
    const hAway = away?.score ?? 0;
    return `${hAway}-${hHome}`;
  } catch { return ""; }
}

/* ───────────── ESPN fetchers ───────────── */
async function espnSummary(gameId) {
  const url = summaryUrl(LEAGUE, gameId);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ───────────── ESPN BET quick scraper (LIVE ODDS box) ───────────── */
function normalizeDashes(s) {
  return s.replace(/\u2212|\u2013|\u2014/g, "-").replace(/\uFE63|\uFF0B/g, "+");
}
function scrapeLiveOddsTextBlock(textBlock) {
  // textBlock: normalized text of the "LIVE ODDS" section
  // We want: away spread, away ML, total; and home spread, home ML.
  // Strategy: tokens → pick the *live* numbers (ignore first “close” pair).
  const t = normalizeDashes(textBlock).replace(/\s+/g, " ");
  // Extract numeric tokens with their +/- and o/u labels (keep numbers only for ML/Spread/Total)
  // We detect structures like: "+3.5", "-140", "o35.5", "u35.5".
  const spreads = [...t.matchAll(/(?:^|\s)([+-]\d+(?:\.\d+)?)(?=\s)/g)].map(m => m[1]);
  const mls     = [...t.matchAll(/(?:^|\s)([+-]\d{2,4})(?=\s)/g)].map(m => m[1]);
  const totO    = t.match(/\bo\s?(\d+(?:\.\d+)?)\b/i)?.[1] || "";
  const totU    = t.match(/\bu\s?(\d+(?:\.\d+)?)\b/i)?.[1] || "";
  const liveTotal = totO || totU || "";

  // Heuristic: first spread after headers is away live, second is home live.
  const liveAwaySpread = spreads[0] || "";
  const liveHomeSpread = spreads[1] || "";
  const liveAwayML = mls[0] || "";
  const liveHomeML = mls[1] || "";

  return { liveAwaySpread, liveHomeSpread, liveAwayML, liveHomeML, liveTotal };
}

async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(() => {});
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });

    const txt = (await section.innerText()).replace(/\u00a0/g, " ").trim();
    if (DEBUG) {
      console.log("SCRAPE DEBUG: header context →", txt.slice(0, 1800));
    }

    // Try to identify the *live* set (ignore the first "CLOSE" numbers)
    const { liveAwaySpread, liveHomeSpread, liveAwayML, liveHomeML, liveTotal } = scrapeLiveOddsTextBlock(txt);

    // Also try to pull the visible score as "away-home"
    let halfScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g, " ");
      const sc = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (sc) halfScore = `${sc[1]}-${sc[2]}`;
    } catch {}

    return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore };
  } catch (err) {
    console.warn("ESPN BET scrape failed:", err?.message || err);
    return null;
  } finally {
    await browser.close();
  }
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function getValues() {
  const range = `${TAB_NAME}!A1:Q2000`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
  return res.data.values || [];
}
function mapCols(header) {
  const lower = s => (s || "").trim().toLowerCase();
  const find = (name, fb) => {
    const i = header.findIndex(h => lower(h) === lower(name));
    return i >= 0 ? i : fb;
  };
  return {
    GAME_ID: find("Game ID", 0),
    DATE: find("Date", 1),
    STATUS: find("Status", 3),
    SCORE: find("Half Score", 11), // sheet label "Half Score" holds current score snapshot
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

async function main() {
  const values = await getValues();
  if (!values.length) return;

  const header = values[0];
  const col = mapCols(header);
  const rows = values.slice(1);

  const targets = [];
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").toString().trim();
    if (!id) continue;
    if (TARGET_GAME_ID && !TARGET_GAME_ID.split(",").map(s => s.trim()).includes(id)) continue;
    targets.push({ row0: r + 1, id });
  }
  if (!targets.length) return;

  for (const t of targets) {
    try {
      const summary = await espnSummary(t.id);
      const comp = summary?.header?.competitions?.[0] || {};
      const statusObj = comp?.status || {};
      const period = statusObj?.period ?? 0;

      // Status formatting (scheduled → "MM/DD – h:mm AM/PM", else strip ET token)
      let newStatus = shortStatusFromEspn(statusObj);
      if (isScheduled(statusObj)) {
        const compDate = comp?.date || summary?.header?.date || new Date().toISOString();
        newStatus = fmtETDateTime(compDate);
      } else {
        newStatus = stripET(newStatus);
      }

      // Always write Status when FORCE_STATUS_WRITE, otherwise during normal runs
      if (newStatus && (String(FORCE_STATUS_WRITE).toLowerCase() === "true" || !ONESHOT)) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: GOOGLE_SHEET_ID,
          requestBody: {
            valueInputOption: "RAW",
            data: [makeValue(a1For(t.row0 + 1, col.STATUS), newStatus)],
          },
        });
      }

      // write live score snapshot to "Half Score" col until Q3 begins (frozen after 3rd starts)
      const scoreSnap = parseScore(summary);
      if (scoreSnap && period < 3) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: GOOGLE_SHEET_ID,
          requestBody: {
            valueInputOption: "RAW",
            data: [makeValue(a1For(t.row0 + 1, col.SCORE), scoreSnap)],
          },
        });
      }

      // If halftime and live odds not yet frozen, scrape odds and write once
      const rowNowRes = await sheets.spreadsheets.values.get({
        spreadsheetId: GOOGLE_SHEET_ID,
        range: `${TAB_NAME}!A${t.row0 + 2}:Q${t.row0 + 2}`,
      });
      const rowNow = (rowNowRes.data.values || [])[0] || [];
      const alreadyHasLive = (rowNow[col.L_TOT] || "").toString().trim() ||
                             (rowNow[col.LA_S] || "").toString().trim();

      if (period >= 3) continue; // frozen after Q3 starts

      const isHalftime = /HALF/i.test(String(statusObj?.type?.name || "")) ||
                         /HALF/i.test(String(statusObj?.type?.shortDetail || ""));

      if (isHalftime && !alreadyHasLive) {
        const live = await scrapeLiveOddsOnce(LEAGUE, t.id);
        if (live) {
          const { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore } = live;
          const payload = [];
          const add = (cIdx, v) => { if (cIdx != null && v !== "" && v != null) payload.push(makeValue(a1For(t.row0 + 1, cIdx), v)); };
          add(col.SCORE, halfScore || scoreSnap);
          add(col.LA_S, liveAwaySpread);
          add(col.LA_ML, liveAwayML);
          add(col.LH_S, liveHomeSpread);
          add(col.LH_ML, liveHomeML);
          add(col.L_TOT, liveTotal);
          if (payload.length) {
            await sheets.spreadsheets.values.batchUpdate({
              spreadsheetId: GOOGLE_SHEET_ID,
              requestBody: { valueInputOption: "RAW", data: payload },
            });
          }
        }
      }
    } catch (e) {
      console.warn(`⚠️ ${t.id} failed:`, e?.message || e);
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
