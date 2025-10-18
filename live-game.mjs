// live-game.mjs
// Updates: Status (D), Half Score (L), and Live odds (M..Q) from the ESPN BET
// widget on the game page. Pregame columns are left untouched.
//
// Tips:
// - Set GAME_ID to focus one game (handy for debugging).
// - Set DEBUG_MODE=1 to see detailed logs in Actions.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",                 // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                                // optional: only update this game
  DEBUG_MODE = "",                             // "1" to enable verbose logging
} = process.env;

const DBG = String(DEBUG_MODE).trim() === "1";
const log = (...a) => { if (DBG) console.log(...a); };

for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

/* ───────────────────── Google Sheets bootstrap ───────────────────── */
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

// Today key in **US/Eastern** to match the sheet’s date in column B
const todayKey = (() => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "2-digit", day: "2-digit", year: "2-digit",
  }).formatToParts(new Date());
  const mm = parts.find(p => p.type === "month")?.value ?? "00";
  const dd = parts.find(p => p.type === "day")?.value ?? "00";
  const yy = parts.find(p => p.type === "year")?.value ?? "00";
  return `${mm}/${dd}/${yy}`;
})();

const norm = s => (s || "").toLowerCase();
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
const isFinalCell = s => /^final$/i.test(String(s || ""));

// ESPN status helpers
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}

// Half score from first two period linescores
function sumFirstTwoPeriods(linescores) {
  if (!Array.isArray(linescores) || linescores.length === 0) return null;
  const take = linescores.slice(0, 2);
  let tot = 0;
  for (const p of take) {
    const v = Number(p?.value ?? p?.score ?? 0);
    if (!Number.isFinite(v)) return null;
    tot += v;
  }
  return tot;
}
function parseHalfScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const hHome = sumFirstTwoPeriods(home?.linescores);
    const hAway = sumFirstTwoPeriods(away?.linescores);
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`; // away-first
  } catch {}
  return "";
}

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

// Realistic browser headers to avoid ESPN returning a skeletal/blocked payload
const BROWSER_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Cache-Control": "no-cache",
  "Pragma": "no-cache",
  "Upgrade-Insecure-Requests": "1",
};

// Fetch the public game page HTML (desktop site). The ESPN BET widget renders server-side
async function fetchGameHtml(gameId) {
  // Example: https://www.espn.com/college-football/game/_/gameId/401762842
  const url = `https://www.espn.com/${LEAGUE.replace("college-football","college-football")}/game/_/gameId/${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000, headers: BROWSER_HEADERS });
  return String(data || "");
}

/**
 * Parse the ESPN BET widget (LIVE ODDS block).
 * We only take the live values (not CLOSE). Returns:
 *   { spreadAway, spreadHome, mlAway, mlHome, total } or undefined
 *
 * Assumptions from ESPN widget (top row = away team, bottom row = home team):
 *   - Spread buttons show like: '+21.5' and '-21.5'
 *   - Total buttons show like:  'o52.5' and 'u52.5'
 *   - Moneyline buttons show like: '+900' and '-2000' or 'OFF'
 */
function parseEspnBetWidget(html) {
  if (!html) return undefined;

  // Grab the LIVE ODDS table region (keeps the parse cheap)
  const liveBlockMatch = html.match(/ESPN BET SPORTSBOOK[\s\S]{0,8000}?LIVE ODDS[\s\S]{0,5000}?Note: Odds and lines subject to change\./i);
  const block = liveBlockMatch ? liveBlockMatch[0] : "";
  log("   [scrape] block length:", block.length);

  if (!block) return undefined;

  // Extract the 4 “main buttons” we care about for away/home rows:
  // Away spread, Home spread, Over total, Under total, Away ML, Home ML.
  // We’ll look for tokens that look like +34.5 / -34.5 / o57.5 / u57.5 / +900 / -2000 / OFF.
  const spreadTokens = [...block.matchAll(/>([+−-]\d{1,2}(?:\.\d)?)<\/span>/g)].map(m => m[1].replace("−","-"));
  const totalTokens  = [...block.matchAll(/>([ou]\d{1,2}(?:\.\d)?)<\/span>/gi)].map(m => m[1]);
  const mlTokens     = [...block.matchAll(/>([+−-]\d{1,4}|OFF)<\/span>/g)].map(m => m[1].replace("−","-"));

  // Heuristic: first pair of spreads belong to away/home (top/bottom),
  // first pair of totals are o/u, first pair of ML are away/home.
  // Some pages omit ML ("OFF") — we treat OFF as empty.
  const spreadAway = spreadTokens[0] || "";
  const spreadHome = spreadTokens[1] || "";
  const totalOver  = totalTokens[0]   || "";
  const totalUnder = totalTokens[1]   || "";
  const mlAwayRaw  = mlTokens[0]      || "";
  const mlHomeRaw  = mlTokens[1]      || "";

  if (!spreadAway && !spreadHome && !totalOver && !totalUnder && !mlAwayRaw && !mlHomeRaw) {
    return undefined;
  }

  // Normalize numbers
  const N = (v) => (v === "" || v === "OFF" ? "" : Number(v));
  const spreadAwayNum = spreadAway ? Number(spreadAway) : "";
  const spreadHomeNum = spreadHome ? Number(spreadHome) : "";
  const totalNum = (totalOver || totalUnder)
    ? Number((totalOver || totalUnder).replace(/[ou]/i,""))
    : "";

  const mlAway = N(mlAwayRaw);
  const mlHome = N(mlHomeRaw);

  // Basic sanity: a live block should have at least a spread or a total
  const any = [spreadAwayNum, spreadHomeNum, totalNum, mlAway, mlHome].some(v => v !== "");
  if (!any) return undefined;

  return {
    spreadAway: spreadAwayNum,
    spreadHome: spreadHomeNum,
    mlAway,
    mlHome,
    total: totalNum,
  };
}

/* ─────────────────────── values/A1 helpers ───────────────────────── */
function makeValue(range, val) { return { range, values: [[val]] }; }
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}
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
    HALF: find("Half Score", 11),
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

// Only row selection we want: GAME_ID, already-live rows, or rows with today’s date (ET) and not Final
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();   // MM/DD/YY
    const status   = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;

    if (GAME_ID && id === GAME_ID) {
      targets.push({ r, id, reason: "GAME_ID" });
      continue;
    }
    if (looksLiveStatus(status)) {
      targets.push({ r, id, reason: "live-like status" });
      continue;
    }
    if (dateCell === todayKey) {
      targets.push({ r, id, reason: "today" });
    }
  }
  return targets;
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const values = await getValues();
    if (values.length === 0) { console.log(`Sheet empty—nothing to do.`); return; }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) { console.log(`Nothing to update.`); return; }

    if (DBG) {
      console.log(`[${new Date().toISOString()}] Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);
    }

    const data = [];

    for (const t of targets) {
      console.log(`\n=== 🏈 GAME ${t.id} ===`);

      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // 1) STATUS + HALF
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);

        log("   status:", JSON.stringify(newStatus));
        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
        if (nowFinal) {
          log("   is final -> skip live odds this run");
          continue; // no live odds if Final
        }
      } catch (e) {
        console.log(`   summary warn ${t.id}:`, e?.message || e);
      }

      // 2) ESPN BET live odds via page scrape
      let live = undefined;
      try {
        const html = await fetchGameHtml(t.id);
        live = parseEspnBetWidget(html);
        if (DBG) {
          console.log("   chosen source:", live ? "SCRAPE =>" : "NONE =>", live || "undefined");
        }
      } catch (e) {
        console.log(`   scrape warn ${t.id}:`, e?.message || e);
      }

      if (live) {
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else {
        console.log("   ❌ no live odds found");
      }
    }

    if (!data.length) {
      console.log(`Built 0 precise cell updates across ${targets.length} target(s).`);
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });

    console.log(`✅ Updated ${data.length} cell(s).`);
  } catch (err) {
    const code = err?.response?.status || err?.code || err?.message || err;
    console.error("Live updater fatal:", "*** code:", code, "***");
    process.exit(1);
  }
}

main();
