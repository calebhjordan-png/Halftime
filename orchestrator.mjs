// orchestrator.mjs
// Final-time cosmetics only: bold winner in the Matchup cell;
// background-fill Away/Home spreads & Total based on the Final Score.
// Does NOT alter live odds or any other logic.

import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  TAB_NAME = "CFB",            // change to "NFL" when running for NFL
  DEBUG_MODE = "",
} = process.env;

const DEBUG = String(DEBUG_MODE || "").trim() === "1";
const log = (...a) => DEBUG && console.log(...a);

if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");

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
const COLOR = {
  GREEN_BG: { red: 0.8, green: 1, blue: 0.8 },  // win / over
  RED_BG:   { red: 1, green: 0.8, blue: 0.8 },  // loss / under
  GRAY_BG:  { red: 0.9, green: 0.9, blue: 0.9 },// push
};

function startsFinal(s) { return /^final/i.test(String(s || "").trim()); }

function parseFinalScoreCell(s) {
  // expects "away-home" like "13-38"
  const m = String(s || "").trim().match(/^\s*(-?\d+)\s*[-–]\s*(-?\d+)\s*$/);
  if (!m) return null;
  const away = Number(m[1]), home = Number(m[2]);
  if (!Number.isFinite(away) || !Number.isFinite(home)) return null;
  return { away, home };
}

function spreadOutcomeAway(awayScore, homeScore, awaySpread) {
  const adj = awayScore + awaySpread;
  if (adj > homeScore) return "win";
  if (adj < homeScore) return "loss";
  return "push";
}
function spreadOutcomeHome(awayScore, homeScore, homeSpread) {
  const adj = homeScore + homeSpread;
  if (adj > awayScore) return "win";
  if (adj < awayScore) return "loss";
  return "push";
}
function totalOutcome(awayScore, homeScore, closingTotal) {
  const tot = awayScore + homeScore;
  if (tot > closingTotal) return "over";
  if (tot < closingTotal) return "under";
  return "push";
}

function bgFromOutcome(tag) {
  if (tag === "win" || tag === "over") return COLOR.GREEN_BG;
  if (tag === "loss" || tag === "under") return COLOR.RED_BG;
  return COLOR.GRAY_BG;
}

function colorCellReq(sheetId, row0, col0, rgb) {
  return {
    repeatCell: {
      range: {
        sheetId,
        startRowIndex: row0,
        endRowIndex: row0 + 1,
        startColumnIndex: col0,
        endColumnIndex: col0 + 1,
      },
      cell: { userEnteredFormat: { backgroundColor: rgb } },
      fields: "userEnteredFormat.backgroundColor",
    },
  };
}

function boldWinnerReq(sheetId, row0, col0, matchupText, winnerIsAway) {
  const text = String(matchupText || "");
  const atIdx = text.indexOf(" @ ");
  if (atIdx < 0) return null;

  const awayName = text.slice(0, atIdx);
  const homeName = text.slice(atIdx + 3);
  const start = winnerIsAway ? 0 : awayName.length + 3;
  const winner = winnerIsAway ? awayName : homeName;
  const end = start + winner.length;

  return {
    updateCells: {
      range: {
        sheetId,
        startRowIndex: row0,
        endRowIndex: row0 + 1,
        startColumnIndex: col0,
        endColumnIndex: col0 + 1,
      },
      rows: [{
        values: [{
          userEnteredValue: { stringValue: text },
          textFormatRuns: [
            { startIndex: 0, format: { bold: false } },
            { startIndex: start, format: { bold: true } },
            { startIndex: end, format: { bold: false } },
          ],
        }],
      }],
      fields: "userEnteredValue,textFormatRuns",
    },
  };
}

/* column mapping with fallbacks (supports A/H aliases too) */
function mapCols(header) {
  const L = header.map(h => String(h || "").trim().toLowerCase());
  const idx = (names, fb = -1) => {
    for (const n of names) {
      const i = L.indexOf(n);
      if (i >= 0) return i;
    }
    return fb;
  };

  return {
    GAME_ID:   idx(["game id","gameid"]),
    DATE:      idx(["date"]),
    STATUS:    idx(["status"]),
    MATCHUP:   idx(["matchup"]),
    FINAL:     idx(["final score","final","score final"]),

    // spreads / totals
    A_SPREAD:  idx(["away spread","a spread","a_spread"]),
    H_SPREAD:  idx(["home spread","h spread","h_spread"]),
    TOTAL:     idx(["total","closing total","o/u","ou"]),
  };
}

async function getSheetMeta() {
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    includeGridData: false,
  });
  const sheet = meta.data.sheets.find(s => s.properties.title === TAB_NAME);
  if (!sheet) throw new Error(`Tab "${TAB_NAME}" not found`);
  return { sheetId: sheet.properties.sheetId };
}

async function getValues() {
  const range = `${TAB_NAME}!A1:Z2000`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
  return res.data.values || [];
}

async function run() {
  const { sheetId } = await getSheetMeta();
  const rows = await getValues();
  if (!rows.length) { console.log("Sheet empty."); return; }

  const header = rows[0];
  const col = mapCols(header);

  const requests = [];

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const status = row[col.STATUS] || "";
    if (!startsFinal(status)) continue;

    // final score
    const fs = parseFinalScoreCell(row[col.FINAL]);
    if (!fs) continue;

    const matchup = row[col.MATCHUP] || "";
    const winnerIsAway = fs.away > fs.home;
    const boldReq = boldWinnerReq(sheetId, r, col.MATCHUP, matchup, winnerIsAway);
    if (boldReq) requests.push(boldReq);

    // spreads/totals (background fill only; text color remains default)
    const aSpread = Number(row[col.A_SPREAD] ?? NaN);
    const hSpread = Number(row[col.H_SPREAD] ?? NaN);
    const total   = Number(row[col.TOTAL] ?? NaN);

    if (Number.isFinite(aSpread)) {
      const tag = spreadOutcomeAway(fs.away, fs.home, aSpread);
      requests.push(colorCellReq(sheetId, r, col.A_SPREAD, bgFromOutcome(tag)));
    }
    if (Number.isFinite(hSpread)) {
      const tag = spreadOutcomeHome(fs.away, fs.home, hSpread);
      requests.push(colorCellReq(sheetId, r, col.H_SPREAD, bgFromOutcome(tag)));
    }
    if (Number.isFinite(total)) {
      const tag = totalOutcome(fs.away, fs.home, total);
      requests.push(colorCellReq(sheetId, r, col.TOTAL, bgFromOutcome(tag)));
    }
  }

  if (!requests.length) {
    console.log("Orchestrator: nothing to format.");
    return;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: GOOGLE_SHEET_ID,
    requestBody: { requests },
  });

  console.log(`Orchestrator: formatted ${requests.length} cell operations.`);
}

run().catch(e => {
  console.error("Orchestrator fatal:", e?.response?.data || e?.message || e);
  process.exit(1);
});
