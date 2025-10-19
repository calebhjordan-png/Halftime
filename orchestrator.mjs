// orchestrator.mjs
// Bold winner, underline pregame favorite, and apply consistent background color grading.
// Works for both prefill and finals grading. Safe when no ranges exist.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────── ENV ─────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  TAB_NAME = "CFB",
  LEAGUE = "college-football",
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

function rangeForSide(display, side) {
  const text = String(display || "");
  const sep = text.indexOf(" @ ");
  if (sep <= 0) return null;
  const n = text.length;
  let start = 0, end = 0;
  if (side === "away") { start = 0; end = sep; }
  else { start = sep + 3; end = n; }
  start = Math.max(0, Math.min(start, n));
  end = Math.max(start, Math.min(end, n));
  if (start >= end) return null;
  return { start, end, length: n };
}

function favoriteRange(display, favorite) {
  if (!favorite) return null;
  return rangeForSide(display, favorite);
}
function winnerRangeFromDisplay(display, winner) {
  if (!winner) return null;
  return rangeForSide(display, winner);
}

/**
 * Merge + normalize text runs.
 * @param {number} textLength  length of the cell string
 * @param {Array} existingRuns effective textFormatRuns (can be undefined)
 * @param {Object|null} winRange range to bold (start,end,length)
 * @param {Object|null} underlineRange range to underline (start,end,length)
 * @returns normalized textFormatRuns
 */
function mergeTextRuns(textLength, existingRuns = [], winRange, underlineRange) {
  const length = Math.max(0, Number(textLength) || 0);
  if (length === 0) return []; // nothing to format

  const runs = [...(existingRuns || [])]
    .map((r) => ({
      startIndex: Math.max(0, Math.min(r?.startIndex ?? 0, length)),
      format: r?.format || {},
    }))
    .sort((a, b) => a.startIndex - b.startIndex);

  if (!runs.length || runs[0].startIndex !== 0) {
    runs.unshift({ startIndex: 0, format: {} });
  }

  const points = new Set([0, length]);
  if (winRange) { points.add(winRange.start); points.add(winRange.end); }
  if (underlineRange) { points.add(underlineRange.start); points.add(underlineRange.end); }
  for (const r of runs) points.add(r.startIndex);

  const cuts = Array.from(points).sort((a, b) => a - b);

  function baseFmtAt(idx) {
    let fmt = {};
    for (const r of runs) {
      if (r.startIndex <= idx) fmt = r.format || {};
      else break;
    }
    return fmt;
  }

  const merged = [];
  for (let i = 0; i < cuts.length - 1; i++) {
    const segStart = cuts[i];
    const segEnd = cuts[i + 1];
    if (segStart === segEnd) continue;

    const base = { ...(baseFmtAt(segStart) || {}) };
    const inWinner = !!(winRange && !(segEnd <= winRange.start || segStart >= winRange.end));
    const inUnderline = !!(underlineRange && !(segEnd <= underlineRange.start || segStart >= underlineRange.end));

    const nextFmt = {
      ...base,
      bold: inWinner ? true : base.bold || false,
      underline: inUnderline ? true : base.underline || false,
    };

    if (
      !merged.length ||
      JSON.stringify(merged[merged.length - 1].format) !== JSON.stringify(nextFmt)
    ) {
      merged.push({ startIndex: segStart, format: nextFmt });
    }
  }

  // cleanup
  if (merged.length && merged[merged.length - 1].startIndex >= length) merged.pop();
  if (!merged.length || merged[0].startIndex !== 0) merged.unshift({ startIndex: 0, format: {} });

  return merged;
}

/* ───────────────────────── ESPN Summary ───────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  const comp = data?.header?.competitions?.[0];
  const home = comp?.competitors?.find((c) => c.homeAway === "home");
  const away = comp?.competitors?.find((c) => c.homeAway === "away");
  const homeScore = Number(home?.score ?? 0);
  const awayScore = Number(away?.score ?? 0);
  return `${awayScore}-${homeScore}`;
}

/* ───────────────────── Sheet bootstrap ───────────────────── */
async function getMeta(spreadsheetId) {
  const { data } = await sheets.spreadsheets.get({
    spreadsheetId,
    includeGridData: false,
  });
  return data;
}
function sheetId(meta, title) {
  const s = meta.sheets.find((x) => x.properties.title === title);
  if (!s) throw new Error(`No tab ${title}`);
  return s.properties.sheetId;
}
async function getGrid(spreadsheetId, title, rangeA1) {
  const { data } = await sheets.spreadsheets.get({
    spreadsheetId,
    ranges: [`${title}!${rangeA1}`],
    includeGridData: true,
  });
  return data.sheets?.[0]?.data?.[0]?.rowData || [];
}

/* ───────────────────────── MAIN ───────────────────────── */
async function main() {
  const meta = await getMeta(GOOGLE_SHEET_ID);
  const sid = sheetId(meta, TAB_NAME);

  const headerRes = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!A1:Q1`,
  });
  const headers = headerRes.data.values?.[0] || [];
  const col = (n) => headers.findIndex((h) => h.toLowerCase().trim() === n.toLowerCase());

  const C = {
    GAME_ID: col("Game ID"),
    STATUS: col("Status"),
    MATCHUP: col("Matchup"),
    FINAL: col("Final Score"),
    A_SPREAD: col("Away Spread"),
    A_ML: col("Away ML"),
    H_SPREAD: col("Home Spread"),
    H_ML: col("Home ML"),
    TOTAL: col("Total"),
  };

  const grid = await getGrid(GOOGLE_SHEET_ID, TAB_NAME, "A2:Q2000");

  // default shades; try to learn from ML columns if present
  let red = { red: 1, green: 0.85, blue: 0.85 };
  let green = { red: 0.85, green: 1, blue: 0.85 };
  for (const row of grid) {
    for (const c of [C.A_ML, C.H_ML]) {
      const cell = row.values?.[c];
      const bg = cell?.effectiveFormat?.backgroundColor ?? cell?.userEnteredFormat?.backgroundColor ?? null;
      if (!bg) continue;
      const r = bg.red ?? 1, g = bg.green ?? 1;
      if (g > r) green = bg;
      if (r > g) red = bg;
    }
  }

  const req = [];

  for (let i = 0; i < grid.length; i++) {
    const row = grid[i]?.values || [];
    const v = (idx) =>
      row[idx]?.effectiveValue?.stringValue ??
      row[idx]?.formattedValue ??
      "";

    const gid = v(C.GAME_ID);
    const status = v(C.STATUS);
    const matchup = v(C.MATCHUP);
    const textLen = String(matchup || "").length;

    const aSpread = toNum(v(C.A_SPREAD));
    const hSpread = toNum(v(C.H_SPREAD));
    const total = toNum(v(C.TOTAL));
    let finalScore = v(C.FINAL);
    const parsed = parseFinalScore(finalScore);
    const isFinal = /^final/i.test(status);

    // backfill missing final score when status says final
    if (isFinal && !parsed && gid) {
      try { finalScore = await espnSummary(gid); } catch {}
    }

    // underline pregame favorite (persist)
    let favorite = null;
    if (aSpread !== null && hSpread !== null) {
      favorite = Math.abs(aSpread) < Math.abs(hSpread) ? "home" : "away";
    }
    const favRange = favoriteRange(matchup, favorite);

    // bold winner on final (preserve underline + links)
    let winRange = null;
    const parsedFinal = parseFinalScore(finalScore);
    if (isFinal && parsedFinal) {
      const { away, home } = parsedFinal;
      const winner = away > home ? "away" : "home";
      winRange = winnerRangeFromDisplay(matchup, winner);
    }

    // text runs update (only if there is text)
    if (textLen > 0) {
      const mCell = row[C.MATCHUP] || {};
      const currentRuns = Array.isArray(mCell.textFormatRuns) ? mCell.textFormatRuns : [];
      const newRuns = mergeTextRuns(textLen, currentRuns, winRange, favRange);

      // only enqueue if different
      const currentJSON = JSON.stringify(currentRuns || []);
      const nextJSON = JSON.stringify(newRuns || []);
      if (currentJSON !== nextJSON) {
        req.push({
          updateCells: {
            range: {
              sheetId: sid,
              startRowIndex: i + 1,
              endRowIndex: i + 2,
              startColumnIndex: C.MATCHUP,
              endColumnIndex: C.MATCHUP + 1,
            },
            rows: [{ values: [{ textFormatRuns: newRuns }] }],
            fields: "textFormatRuns",
          },
        });
      }
    }

    // grading colors when final and score present
    if (isFinal && parsedFinal) {
      const { away, home } = parsedFinal;
      const covA = coverAway(away, home, aSpread);
      const covH = coverHome(away, home, hSpread);
      const totR = totalOver(away, home, total);

      const colorFor = (flag) => {
        if (flag === true) return green;
        if (flag === false) return red;
        return null;
      };

      const upd = [
        { c: C.A_ML, color: colorFor(away > home) },
        { c: C.H_ML, color: colorFor(home > away) },
        { c: C.A_SPREAD, color: colorFor(covA) },
        { c: C.H_SPREAD, color: colorFor(covH) },
        { c: C.TOTAL, color: totR === "push" ? null : colorFor(totR === true) },
      ];
      for (const u of upd) {
        if (!u.color) continue;
        req.push({
          repeatCell: {
            range: {
              sheetId: sid,
              startRowIndex: i + 1,
              endRowIndex: i + 2,
              startColumnIndex: u.c,
              endColumnIndex: u.c + 1,
            },
            cell: { userEnteredFormat: { backgroundColor: u.color } },
            fields: "userEnteredFormat.backgroundColor",
          },
        });
      }

      // write Final Score if missing
      if (!v(C.FINAL) && finalScore) {
        req.push({
          updateCells: {
            range: {
              sheetId: sid,
              startRowIndex: i + 1,
              endRowIndex: i + 2,
              startColumnIndex: C.FINAL,
              endColumnIndex: C.FINAL + 1,
            },
            rows: [{ values: [{ userEnteredValue: { stringValue: finalScore } }] }],
            fields: "userEnteredValue.stringValue",
          },
        });
      }
    }
  }

  if (req.length) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { requests: req },
    });
  }
  console.log(`✅ Applied ${req.length} updates.`);
}

main().catch((e) => {
  console.error("❌ Orchestrator fatal:", e?.message || e);
  process.exit(1);
});
