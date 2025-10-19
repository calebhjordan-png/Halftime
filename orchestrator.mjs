// orchestrator.mjs
// Finals sweeper: bold winning team in Matchup (preserve underline/links),
// and background-color A/H Spreads & Total using the SAME shades as ML columns.

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

/**
 * Build winner range (start,end) in "Away @ Home" display string.
 * Returns null if we can't safely split.
 */
function winnerRangeFromDisplay(display, winner) {
  const text = String(display || "");
  const sep = text.indexOf(" @ ");
  if (sep <= 0) return null;
  const n = text.length;
  let start = 0, end = 0;
  if (winner === "away") {
    start = 0;
    end = sep; // up to (not including) space before '@'
  } else {
    start = sep + 3; // after " @ "
    end = n;
  }
  // clamp
  start = Math.max(0, Math.min(start, n));
  end   = Math.max(start, Math.min(end, n));
  if (start >= end) return null;
  return { start, end, length: n };
}

/**
 * Merge bold into existing textFormatRuns while preserving all other styling (underline, etc.).
 * existingRuns: array of {startIndex, format?}
 * win: {start, end, length}
 */
function mergeBoldIntoRuns(existingRuns = [], win) {
  const length = win.length;
  // Normalize runs sorted with implicit default at 0
  const runs = [...(existingRuns || [])]
    .map(r => ({ startIndex: Math.max(0, Math.min(r.startIndex ?? 0, length)), format: r.format || {} }))
    .sort((a, b) => a.startIndex - b.startIndex);

  if (!runs.length || runs[0].startIndex !== 0) {
    runs.unshift({ startIndex: 0, format: {} });
  }

  // Build segment boundaries
  const points = new Set([0, length, win.start, win.end]);
  for (const r of runs) points.add(r.startIndex);
  const cuts = Array.from(points).sort((a, b) => a - b);

  // Helper to get base format at an index
  function baseFmtAt(idx) {
    let fmt = {};
    for (const r of runs) {
      if (r.startIndex <= idx) fmt = r.format || {};
      else break;
    }
    return fmt;
  }

  // Walk segments and assign formats (preserve all props, just tweak bold)
  const merged = [];
  for (let i = 0; i < cuts.length - 1; i++) {
    const segStart = cuts[i];
    const segEnd = cuts[i + 1];
    if (segStart === segEnd) continue;
    const base = { ...(baseFmtAt(segStart) || {}) };

    const overlaps =
      !(segEnd <= win.start || segStart >= win.end); // any overlap with winner span
    const nextFmt = { ...base, bold: overlaps ? true : (base.bold ?? false) };

    // push if first or format changed
    if (
      !merged.length ||
      JSON.stringify(merged[merged.length - 1].format || {}) !== JSON.stringify(nextFmt)
    ) {
      merged.push({ startIndex: segStart, format: nextFmt });
    }
  }

  // Ensure last startIndex < length (Sheets requires this)
  if (merged.length && merged[merged.length - 1].startIndex >= length) {
    merged.pop();
  }
  // Guarantee a run beginning at 0
  if (!merged.length || merged[0].startIndex !== 0) {
    merged.unshift({ startIndex: 0, format: {} });
  }
  return merged;
}

/**
 * Sample the sheet's ML background colors so we can match shades exactly.
 * Returns { green, red } with {red,green,blue} floats (0..1).
 */
function sampleMlShades(grid, colAwayML, colHomeML) {
  let green = null, red = null;

  function pick(c) {
    const bg =
      c?.effectiveFormat?.backgroundColor ??
      c?.userEnteredFormat?.backgroundColor ??
      null;
    if (!bg) return;
    const { red: r = 1, green: g = 1, blue: b = 1 } = bg;
    // very naive classifier
    if (g >= r + 0.05 && !green) green = { red: r, green: g, blue: b };
    if (r >= g + 0.05 && !red)   red   = { red: r, green: g, blue: b };
  }

  for (const row of grid) {
    const cells = row?.values || [];
    if (cells[colAwayML]) pick(cells[colAwayML]);
    if (cells[colHomeML]) pick(cells[colHomeML]);
    if (green && red) break;
  }
  // Fallback gentle pastels if sheet has no fills yet
  if (!green) green = { red: 0.85, green: 1.0, blue: 0.85 };
  if (!red)   red   = { red: 1.0,  green: 0.85, blue: 0.85 };
  return { green, red };
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
  return data.sheets?.[0]?.data?.[0]?.rowData || [];
}

/* ───────────────────────── MAIN ─────────────────────────── */
async function main() {
  const meta = await getSheetMeta(GOOGLE_SHEET_ID);
  const sheetId = findSheetIdByTitle(meta, TAB_NAME);

  const headerRange = "A1:Q1";
  const rowsRange   = "A2:Q2000";

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
    A_ML:       colIdx("Away ML", 7),
    H_SPREAD:   colIdx("Home Spread", 8),
    H_ML:       colIdx("Home ML", 9),
    TOTAL:      colIdx("Total", 10),
  };

  // Grid with formats for Matchup runs + ML background sampling
  const grid = await getGrid(GOOGLE_SHEET_ID, TAB_NAME, rowsRange);

  // Shade sampling from ML columns
  const shades = sampleMlShades(grid, COL.A_ML, COL.H_ML);

  const requests = [];

  for (let r = 0; r < grid.length; r++) {
    const row = grid[r]?.values || [];

    const cellVal = idx => {
      const v = row[idx];
      return v?.effectiveValue?.stringValue ?? v?.formattedValue ?? "";
    };

    const status = cellVal(COL.STATUS);
    if (!/^final/i.test(status)) continue;

    const finalScore = cellVal(COL.FINAL);
    const ps = parseFinalScore(finalScore);
    if (!ps) continue;
    const { away: sA, home: sH } = ps;

    // 1) Bold the winner (preserving underline/links via merge)
    const mCell = row[COL.MATCHUP] || {};
    const display = mCell.formattedValue || "";
    const existingRuns = mCell.textFormatRuns || [];
    const winner = sA > sH ? "away" : "home";
    const win = winnerRangeFromDisplay(display, winner);

    if (win) {
      const mergedRuns = mergeBoldIntoRuns(existingRuns, win);

      requests.push({
        updateCells: {
          range: {
            sheetId,
            startRowIndex: r + 1, // A2 is row index 1
            endRowIndex:   r + 2,
            startColumnIndex: COL.MATCHUP,
            endColumnIndex:   COL.MATCHUP + 1,
          },
          rows: [{
            values: [{ textFormatRuns: mergedRuns }]
          }],
          fields: "textFormatRuns"
        }
      });
    }

    // 2) Background colors: spreads & total using sampled shades
    const aSpread = toNum(cellVal(COL.A_SPREAD));
    const hSpread = toNum(cellVal(COL.H_SPREAD));
    const total   = toNum(cellVal(COL.TOTAL));

    const covA = coverAway(sA, sH, aSpread); // true/false/null
    const covH = coverHome(sA, sH, hSpread);
    const totR = totalOver(sA, sH, total);   // true/false/"push"/null

    const colorFor = (flag) => {
      if (flag === true) return shades.green;
      if (flag === false) return shades.red;
      return null; // leave unchanged
    };

    const cellColorUpdates = [
      { col: COL.A_SPREAD, color: colorFor(covA) },
      { col: COL.H_SPREAD, color: colorFor(covH) },
      { col: COL.TOTAL,    color: (totR === "push" || totR === null) ? null : colorFor(totR === true) }
    ];

    for (const cu of cellColorUpdates) {
      if (!cu.color) continue;
      requests.push({
        repeatCell: {
          range: {
            sheetId,
            startRowIndex: r + 1,
            endRowIndex:   r + 2,
            startColumnIndex: cu.col,
            endColumnIndex:   cu.col + 1,
          },
          cell: {
            userEnteredFormat: { backgroundColor: cu.color }
          },
          fields: "userEnteredFormat.backgroundColor"
        }
      });
    }
  }

  if (requests.length) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { requests }
    });
  }

  console.log(`Orchestrator: applied ${requests.length} request(s).`);
}

main().catch(err => {
  console.error("Orchestrator fatal:", JSON.stringify(err, null, 2));
  process.exit(1);
});
