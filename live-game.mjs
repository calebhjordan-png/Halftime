// live-game.mjs
// Watches ONE game. When it reaches halftime, writes:
//   L: Half Score     (e.g., "17-10")
//   M: Live Away Spread
//   N: Live Away ML
//   O: Live Home Spread
//   P: Live Home ML
//   Q: Live Total
//
// Env required:
//   GOOGLE_SERVICE_ACCOUNT  -> JSON of service account (with Sheets scope)
//   GOOGLE_SHEET_ID         -> your spreadsheet id
//   TAB_NAME                -> "CFB" or "NFL" (exact tab name)
//   TARGET_GAME_ID          -> ESPN event id (e.g., 401756924)
//   LEAGUE                  -> "nfl" or "college-football"
//
// Optional env:
//   POLL_SECONDS            -> default 20
//   MAX_TOTAL_MIN           -> hard stop, default 200 (3h20m)
//   DEBUG_MODE              -> "true" for extra logs

import axios from "axios";
import { google } from "googleapis";

// ---------- config ----------
const {
  GOOGLE_SERVICE_ACCOUNT,
  GOOGLE_SHEET_ID,
  TAB_NAME,
  TARGET_GAME_ID,
  LEAGUE,
  POLL_SECONDS = "20",
  MAX_TOTAL_MIN = "200",
  DEBUG_MODE = "false",
} = process.env;

if (!GOOGLE_SERVICE_ACCOUNT || !GOOGLE_SHEET_ID || !TAB_NAME || !TARGET_GAME_ID || !LEAGUE) {
  console.error("Missing one or more required env vars.");
  process.exit(1);
}
const debug = (...args) => { if (DEBUG_MODE === "true") console.log("[debug]", ...args); };

// ESPN endpoints
const SUMMARY_URL = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${TARGET_GAME_ID}`;

// Google Sheets auth
const sa = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  sa.client_email,
  null,
  sa.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function getSummary() {
  const res = await axios.get(SUMMARY_URL, { timeout: 15000 });
  return res.data;
}

// ESPN “game status” helpers
function parseStatus(summary) {
  // Status is in competitions[0].status
  const comp = summary?.header?.competitions?.[0] || summary?.competitions?.[0];
  const status = comp?.status;
  const type = status?.type || {};
  const period = status?.period ?? comp?.status?.period ?? null;
  const displayClock = status?.displayClock ?? "";
  const detail = type?.detail ?? status?.type?.detail ?? "";
  const state = type?.state ?? ""; // pre, in, post

  // Treat "Halftime" as halftime; or Q2 and clock 0:00
  const isHalftime =
    /halftime/i.test(detail || "") ||
    (/in/i.test(state) && (period === 2) && (displayClock === "0:00"));

  // If game is over we’ll still allow a halftime write if we missed it
  const isFinal = /final/i.test(detail || "") || state === "post";

  // Score at this instant
  const comp0 = summary?.header?.competitions?.[0] || summary?.competitions?.[0];
  const [home, away] = (comp0?.competitors || []).sort((a,b)=> (a.homeAway==="home")?1:-1);
  // Note: above sort puts 'home' second; let’s rebuild clearly:
  const homeTeam = (comp0?.competitors || []).find(c=>c.homeAway==="home");
  const awayTeam = (comp0?.competitors || []).find(c=>c.homeAway==="away");

  return {
    isHalftime,
    isFinal,
    display: detail || `${displayClock} P${period || ""}`,
    homeScore: Number(homeTeam?.score ?? 0),
    awayScore: Number(awayTeam?.score ?? 0),
    comp: comp0,
  };
}

// Odds extractor: prefer ESPN BET; fall back to first provider with live/current lines
function pickProvider(oddsArray) {
  if (!Array.isArray(oddsArray) || oddsArray.length === 0) return null;
  // Prefer ESPN BET
  const espnBet = oddsArray.find(o => /espn bet/i.test(o?.provider?.name || ""));
  if (espnBet) return espnBet;
  // Otherwise pick any provider with awayTeamOdds/homeTeamOdds
  return oddsArray.find(o => o?.awayTeamOdds || o?.homeTeamOdds) || oddsArray[0];
}

function extractLiveOdds(summary) {
  // Some feeds put odds at summary.pickcenter, some at summary.odds (array)
  // We’ll check:
  //   summary.pickcenter[] -> objects with provider & ...,
  //   summary.odds[]       -> similar shape
  const rawOdds = summary?.pickcenter || summary?.odds || [];
  if (!Array.isArray(rawOdds) || rawOdds.length === 0) return null;

  const prov = pickProvider(rawOdds);
  if (!prov) return null;

  // Normalize fields
  // Typical shape:
  // prov.overUnder
  // prov.awayTeamOdds: { spread, moneyLine }
  // prov.homeTeamOdds: { spread, moneyLine }
  // Sometimes spread might be undefined, sometimes 0, sometimes a string
  const toNum = v => (v === null || v === undefined || v === "") ? "" : Number(v);

  let awaySpread = toNum(prov?.awayTeamOdds?.spread);
  let awayML     = toNum(prov?.awayTeamOdds?.moneyLine);
  let homeSpread = toNum(prov?.homeTeamOdds?.spread);
  let homeML     = toNum(prov?.homeTeamOdds?.moneyLine);
  let total      = toNum(prov?.overUnder);

  // If spreads missing but one "spread" exists (single value), build symmetric:
  if ((awaySpread === "" || isNaN(awaySpread)) && (homeSpread !== "" && !isNaN(homeSpread))) {
    awaySpread = -Number(homeSpread);
  }
  if ((homeSpread === "" || isNaN(homeSpread)) && (awaySpread !== "" && !isNaN(awaySpread))) {
    homeSpread = -Number(awaySpread);
  }

  // Clean NaNs to blank strings for Sheets
  const clean = v => (v === "" || Number.isNaN(v)) ? "" : v;

  return {
    awaySpread: clean(awaySpread),
    awayML:     clean(awayML),
    homeSpread: clean(homeSpread),
    homeML:     clean(homeML),
    total:      clean(total),
  };
}

// Locate the row by Game ID in column A
async function findRowByGameId(gameId) {
  // Read col A only (fast)
  const get = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!A:A`,
    majorDimension: "COLUMNS",
  });
  const colA = get.data?.values?.[0] || [];
  // Header in row 1 -> data starts row 2
  const idx = colA.findIndex(v => String(v).trim() === String(gameId).trim());
  if (idx === -1) return null;
  const row = idx + 1; // 1-based
  if (row === 1) return null; // header row
  return row;
}

// Write halftime score + live odds to L..Q
async function writeHalftimeRow(row, halfScore, live) {
  const values = [
    [
      halfScore,
      live?.awaySpread ?? "",
      live?.awayML ?? "",
      live?.homeSpread ?? "",
      live?.homeML ?? "",
      live?.total ?? "",
    ],
  ];
  await sheets.spreadsheets.values.update({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!L${row}:Q${row}`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values },
  });
}

// Write Status cell (column D) with a minimal update (no styles)
async function writeStatus(row, statusText) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!D${row}:D${row}`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [[statusText]] },
  });
}

// If Status already "Half" or "Final", don't keep scribbling over it
async function readStatus(row) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!D${row}:D${row}`,
  });
  return res.data?.values?.[0]?.[0] ?? "";
}

(async function main() {
  console.log(`Watching ${LEAGUE} game ${TARGET_GAME_ID} for halftime → ${TAB_NAME}`);

  const row = await findRowByGameId(TARGET_GAME_ID);
  if (!row) {
    console.error(`Game ID ${TARGET_GAME_ID} not found in column A of tab "${TAB_NAME}".`);
    process.exit(0);
  }

  const startedAt = Date.now();
  const maxMs = Number(MAX_TOTAL_MIN) * 60_000;
  const pollMs = Number(POLL_SECONDS) * 1000;

  while (true) {
    if (Date.now() - startedAt > maxMs) {
      console.log("Max watch time reached. Exiting.");
      break;
    }

    let summary;
    try {
      summary = await getSummary();
    } catch (e) {
      console.warn("Fetch summary failed (will retry):", e?.message || e);
      await sleep(pollMs);
      continue;
    }

    const { isHalftime, isFinal, display, homeScore, awayScore } = parseStatus(summary);

    const currentStatus = (await readStatus(row)) || "";
    // Keep Status updated DURING the half only; if already Half/Final, leave it.
    if (!/^Half$|^Final$/i.test(currentStatus)) {
      await writeStatus(row, display || "");
    }

    if (isHalftime || isFinal) {
      // At halftime (or if we reached final before we got to it), lock in the halftime line write.
      const halfScore = `${awayScore}-${homeScore}`;

      let live = null;
      try {
        live = extractLiveOdds(summary);
      } catch (e) {
        console.warn("extractLiveOdds error:", e?.message || e);
      }

      debug("Halftime write:", { halfScore, live });

      await writeHalftimeRow(row, halfScore, live || {});
      // Also set Status explicitly to "Half" if not Final
      if (!isFinal) {
        await writeStatus(row, "Half");
      }
      console.log("Halftime data written. Exiting.");
      break;
    }

    await sleep(pollMs);
  }
})().catch(err => {
  console.error("Watcher fatal:", err?.stack || err);
  process.exit(1);
});
