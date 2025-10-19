// orchestrator.mjs
// Final sweeper: bold winning team in Matchup, background-color spreads & total.
// Preserves existing hyperlinks in Matchup by only updating textFormatRuns.

import { google } from "googleapis";

/* ─────────────────────────── ENV ─────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  TAB_NAME = "CFB",
} = process.env;

if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");

const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────────────────────── Helpers ───────────────────────── */
const BG_GREEN = { red: 0.85, green: 1.0, blue: 0.85 };
const BG_RED   = { red: 1.0,  green: 0.85, blue: 0.85 };
const BG_NONE  = null; // interpreted below

function toNum(x) {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(String(x).replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function parseFinalScore(s) {
  const m = String(s || "").trim().match(/^(\d+)\s*-\s*(\d+)$/);
  if (!m) return null;
  return { away: Number(m[1]), home: Number(m[2]) };
}

// Safe text run builder (only if we can find "Team A @ Team B")
function buildWinnerRuns(formattedText, winner) {
  // winner: "away" | "home"
  const text = String(formattedText || "");
  const sep = text.indexOf(" @ ");
  if (sep <= 0) return null; // can't safely split
  const awayName = text.slice(0, sep);
  const homeName = text.slice(sep + 3);
  const n = text.length;

  // Compute [start,end) of the winner span in the *display* string
  let start = 0, end = 0;
  if (winner === "away") {
    start = 0;
    end   = awayName.length;
  } else {
    start = sep + 3;
    end   = sep + 3 + homeName.length;
  }
  // Clamp indices inside string length (avoid INVALID_ARGUMENT)
  start = Math.max(0, Math.min(start, n));
  end   = Math.max(start, Math.min(end, n));

  // If indices are degenerate, skip rich text
  if (start >= end) return null;

  const runs = [];
  // default (not bold) from 0
  runs.push({ startIndex: 0, format: { bold: false } });
  // bold winner
  runs.push({ startIndex: start, format: { bold: true } });
  // turn bold off after winner (if not at end)
  if (end < n) runs.push({ startIndex: end, format: { bold: false } });
  return runs;
}

function coverAway(awayScore, homeScore, awaySpread) {
  if (awaySpread === null) return null;
  return awayScore + awaySpread > homeScore;
}
function coverHome(awayScore, homeScore, homeSpread) {
  if (homeSpread === null) return null;
  return homeScore + homeSpread > awayScore;
}
function totalOver(awayScore, homeScore, total) {
  if (total === null) return null;
  const sum = awayScore + homeScore;
  if (sum > total) return true;
  if (sum < total) return false;
  return "push";
}

function a1ColToIndex(a) {
  // A -> 0, B -> 1 ...
  let res = 0;
  for (let i = 0; i < a.length; i++) {
    res = res * 26 + (a.charCodeAt(i) - 64);
  }
  return res - 1;
}

/* ───────────────────── Sheet bootstrap ───────────────────── */
async function getSheetMeta(spreadsheetId) {
  const { data } = await sheets.spreadsheets.get({
    spreadsheetId,
    includeGridData: false,
  });
  return data;
}

function findSheetIdByTitle(meta, title) {
  const sheet = meta.sheets.find(s => s.properties.title === title);
  if (!sheet) throw new Error(`Tab "${title}" not found`);
  return sheet.properties.sheetId;
}

async function getGrid(spreadsheetId, title, rangeA1) {
  const { data } = await sheets.spreadsheets.get({
    spreadsheetId,
    ranges: [`${title}!${rangeA1}`],
    includeGridData: true,
  });
  const grid = data.sheets?.[0]?.data?.[0]?.rowData || [];
  return grid;
}

/* ───────────────────────── MAIN ─────────────────────────── */
async function main() {
  // 1) Read header + all row grid data for Matchup text
  const meta = await getSheetMeta(GOOGLE_SHEET_ID);
  const sheetId = findSheetIdByTitle(meta, TAB_NAME);

  // We’ll fetch a broad range; adjust if your sheet grows
  const headerRange = "A1:K1";
  const rowsRange   = "A2:K2000";

  const headerRes = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!${headerRange}`,
  });
  const headers = headerRes.data.values?.[0] || [];

  const colIdx = (name, fb = -1) => {
    const i = headers.findIndex(h => String(h).trim().toLowerCase() === name.toLowerCase());
    return i >= 0 ? i : fb;
  };

  const COL = {
    STATUS:     colIdx("Status", 3),
    MATCHUP:    colIdx("Matchup", 4),
    FINAL:      colIdx("Final Score", 5),
    A_SPREAD:   colIdx("Away Spread", 6),
    H_SPREAD:   colIdx("Home Spread", 8),
    TOTAL:      colIdx("Total", 10),
  };

  // Pull grid data so we can read formatted text from Matchup (to keep links).
  const grid = await getGrid(GOOGLE_SHEET_ID, TAB_NAME, rowsRange);

  const updates = {
    requests: []
  };

  // Iterate visible rows
  for (let r = 0; r < grid.length; r++) {
    const row = grid[r]?.values || [];
    // Raw values for numbers
    const valuesRes = row.map(c => c?.effectiveValue?.stringValue ?? c?.formattedValue ?? "");

    const status = valuesRes[COL.STATUS] || "";
    if (!/^final/i.test(status)) continue;

    const finalScore = valuesRes[COL.FINAL] || "";
    const parsed = parseFinalScore(finalScore);
    if (!parsed) continue;

    const { away: sA, home: sH } = parsed;

    // 1) Bold winner in Matchup (preserve value, set textFormatRuns only)
    const matchupCell = row[COL.MATCHUP] || {};
    const textDisplay = matchupCell.formattedValue || "";
    const winner = sA > sH ? "away" : "home";
    const runs = buildWinnerRuns(textDisplay, winner);
    if (runs) {
      updates.requests.push({
        updateCells: {
          range: {
            sheetId,
            startRowIndex: r + 1, // rowsRange starts at A2
            endRowIndex:   r + 2,
            startColumnIndex: COL.MATCHUP,
            endColumnIndex:   COL.MATCHUP + 1,
          },
          rows: [{
            values: [{
              textFormatRuns: runs,
            }]
          }],
          fields: "textFormatRuns"
        }
      });
    }

    // 2) Background colors for spreads & total
    const aSpread = toNum(valuesRes[COL.A_SPREAD]);
    const hSpread = toNum(valuesRes[COL.H_SPREAD]);
    const total   = toNum(valuesRes[COL.TOTAL]);

    const covA = coverAway(sA, sH, aSpread); // true/false/null
    const covH = coverHome(sA, sH, hSpread);
    const totR = totalOver(sA, sH, total);   // true/false/"push"/null

    const colorFor = (flag) => {
      if (flag === true) return BG_GREEN;
      if (flag === false) return BG_RED;
      return BG_NONE;
    };

    const cellColorUpdates = [
      { col: COL.A_SPREAD, color: colorFor(covA) },
      { col: COL.H_SPREAD, color: colorFor(covH) },
      { col: COL.TOTAL,    color: (totR === "push" || totR === null) ? BG_NONE : colorFor(totR === true) }
    ];

    for (const cu of cellColorUpdates) {
      if (cu.color === null) continue; // leave as-is
      updates.requests.push({
        repeatCell: {
          range: {
            sheetId,
            startRowIndex: r + 1,
            endRowIndex:   r + 2,
            startColumnIndex: cu.col,
            endColumnIndex:   cu.col + 1,
          },
          cell: {
            userEnteredFormat: {
              backgroundColor: cu.color
            }
          },
          fields: "userEnteredFormat.backgroundColor"
        }
      });
    }
  }

  // Batch apply
  if (updates.requests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: updates
    });
  }

  console.log(`Orchestrator: applied ${updates.requests.length} formatting request(s).`);
}

main().catch(err => {
  console.error("Orchestrator fatal:", JSON.stringify(err, null, 2));
  process.exit(1);
});
