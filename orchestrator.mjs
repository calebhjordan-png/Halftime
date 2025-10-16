// orchestrator.mjs
// Unified orchestrator for NFL + CFB prefill/live/finals updates.
// Writes to Google Sheets tabs with columns:
// [A] Game ID | [B] Date | [C] Week | [D] Status | [E] Matchup | [F] Final Score
// [G] Away Spread | [H] Away ML | [I] Home Spread | [J] Home ML | [K] Total

import fs from "node:fs/promises";
import { google } from "googleapis";
import axios from "axios";

/* ----------------------------- config ----------------------------- */

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const TAB_NAME = process.env.TAB_NAME || "CFB";              // "CFB" or "NFL"
const LEAGUE = process.env.LEAGUE || "college-football";     // "college-football" | "nfl"
const RUN_SCOPE = process.env.RUN_SCOPE || "week";           // "week" | "today" | "finals"
const TZ_LABEL = "ET";

/** Which sportsbook odds we prefer (in order). */
const PROVIDER_PREFERENCE = [
  "ESPN BET", "ESPN BET SPORTSBOOK",
  "Caesars",
  "DraftKings",
  "consensus" // ESPN sometimes labels an aggregate this way
];

/* --------------------------- auth / sheets ------------------------ */

function getSheetsClient() {
  const svc = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  const auth = new google.auth.JWT(
    svc.client_email,
    null,
    svc.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  return google.sheets({ version: "v4", auth });
}

/* ------------------------------- utils ---------------------------- */

const pad2 = (n) => String(n).padStart(2, "0");

function yyyymmddFromDate(d) {
  return `${d.getUTCFullYear()}${pad2(d.getUTCMonth() + 1)}${pad2(d.getUTCDate())}`;
}

function startOfWeekUTC(d) {
  // ESPN CFB weeks start Monday; we’ll pull Sat–Sun by scanning the whole week.
  const copy = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dow = copy.getUTCDay(); // 0 Sun..6 Sat
  // force Monday
  const delta = (dow + 6) % 7;
  copy.setUTCDate(copy.getUTCDate() - delta);
  return copy;
}

function signStr(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "";
  return n > 0 ? `+${n}` : `${n}`;
}

function parseNumeric(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === "number") return x;
  const m = String(x).replace(/[^\d.-]/g, "");
  if (m === "" || m === "-" || m === ".") return null;
  const val = Number(m);
  return Number.isFinite(val) ? val : null;
}

function pickPreferredOdds(oddsArray) {
  if (!Array.isArray(oddsArray) || oddsArray.length === 0) return null;

  // Try preferred providers first
  for (const provider of PROVIDER_PREFERENCE) {
    const hit = oddsArray.find(o => (o?.provider?.name || "").toUpperCase() === provider.toUpperCase());
    if (hit) return hit;
  }
  // Otherwise take the first that has any ML values
  const withML = oddsArray.find(o => (o?.awayTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyLine) != null);
  if (withML) return withML;

  // Or just the first
  return oddsArray[0];
}

function extractOdds(competition) {
  // ESPN structure: competition.odds[] with provider + {spread, overUnder, homeTeamOdds, awayTeamOdds}
  const o = pickPreferredOdds(competition?.odds);
  if (!o) return { spreadAway: null, spreadHome: null, total: null, awayML: null, homeML: null };

  // Spreads: ESPN gives a single point (home favorite is negative).
  // We want "away spread" and "home spread".
  // If o.details like "MIA -13.5" exists, ESPN also provides o.spread and home/away team odds objects.
  const spread = parseNumeric(o.spread);
  let spreadAway = null;
  let spreadHome = null;
  if (spread != null) {
    // negative spread => home favorite; that means away spread is +abs
    // positive spread => away favorite; that means home spread is +abs
    const isHomeFav = spread < 0;
    const mag = Math.abs(spread);
    if (isHomeFav) {
      spreadHome = -mag;
      spreadAway = +mag;
    } else {
      spreadAway = -mag;
      spreadHome = +mag;
    }
  }

  // Totals (overUnder)
  const total = parseNumeric(o.overUnder);

  // Moneylines (numbers already signed in feed)
  // They may be strings; normalize to integers with sign.
  const awayML = parseNumeric(o?.awayTeamOdds?.moneyLine);
  const homeML = parseNumeric(o?.homeTeamOdds?.moneyLine);

  return {
    spreadAway, spreadHome, total,
    awayML, homeML,
    _provider: o?.provider?.name || "unknown"
  };
}

function teamString(away, home, favoriteFavKey) {
  // Build "Away @ Home" with underline on the pregame favorite team.
  const underline = (name) => `_${name}_`; // we’ll convert to textFormatRuns when we write
  const a = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.displayName || away?.team?.name || "Away";
  const h = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.displayName || home?.team?.name || "Home";

  if (favoriteFavKey === "away") return { text: `${a} @ ${h}`, uStart: 0, uEnd: a.length };         // underline away
  if (favoriteFavKey === "home") return { text: `${a} @ ${h}`, uStart: (a.length + 3), uEnd: (a.length + 3 + h.length) }; // underline home
  return { text: `${a} @ ${h}`, uStart: null, uEnd: null };
}

/* --------------------------- ESPN fetch --------------------------- */

async function fetchScoreboardDates(startDateUTC, numDays = 7) {
  const dates = [];
  for (let i = 0; i < numDays; i++) {
    const d = new Date(startDateUTC);
    d.setUTCDate(d.getUTCDate() + i);
    dates.push(yyyymmddFromDate(d));
  }

  const sportPath = LEAGUE === "nfl" ? "football/nfl" : "football/college-football";

  const all = [];
  await Promise.allSettled(
    dates.map(async (dateStr) => {
      const url = `https://site.api.espn.com/apis/site/v2/sports/${sportPath}/scoreboard?dates=${dateStr}`;
      const { data } = await axios.get(url, { timeout: 15000 });
      if (Array.isArray(data?.events)) {
        all.push(...data.events);
      }
    })
  );

  return all;
}

/* --------------------------- build rows --------------------------- */

function toSheetDate(iso) {
  // Expecting local display MM/DD/YY; we’ll let the sheet keep text value preformatted.
  const d = new Date(iso);
  const mm = pad2(d.getUTCMonth() + 1);
  const dd = pad2(d.getUTCDate());
  const yy = String(d.getUTCFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}

function toKickoffLocal(competition) {
  // Put the display kickoff with "ET" but we do NOT write ET to the cell (as requested earlier).
  const date = competition?.date ? new Date(competition.date) : null;
  if (!date) return "";
  // ESPN dates are ISO; we'll use HH:MM with "ET" removed (consumer knows column = ET).
  const hh = date.getUTCHours();
  const mm = pad2(date.getUTCMinutes());
  // Convert to US/Eastern (approx: ESPN’s time already respects timezone in their UI; we stick with ET label)
  // Keep it simple: user prefers no "ET" suffix; just the time string.
  const hoursET = ((hh + 24) - 4) % 24; // UTC-4 (approx EDT). Good enough for our display use here.
  const h12 = ((hoursET + 11) % 12) + 1;
  const ampm = hoursET < 12 ? "AM" : "PM";
  return `${h12}:${mm} ${ampm}`;
}

function inferWeekCN(league, weekObj) {
  // Prefer provided "week text" when available; otherwise produce `Week N`.
  const n = weekObj?.number || weekObj?.$ref || null;
  if (typeof n === "number" && n > 0) return `Week ${n}`;
  // fallback: ESPN’s CFB sometimes has type: "regular"
  const txt = weekObj?.text || "Week";
  return txt.startsWith("Week") ? txt : `Week ${txt}`;
}

function computeFavoriteKey(odds, teams) {
  if (!odds) return null;
  // If spreadAway is negative, away is favorite; if spreadHome is negative, home is favorite.
  if (odds.spreadAway != null && odds.spreadAway < 0) return "away";
  if (odds.spreadHome != null && odds.spreadHome < 0) return "home";
  // fallback to ML if spreads absent
  if (odds.awayML != null && odds.homeML != null) {
    if (odds.awayML < odds.homeML) return "away";
    if (odds.homeML < odds.awayML) return "home";
  }
  return null;
}

/* ---------------------------- write sheet ------------------------- */

function buildUnderlineRuns(text, start, end) {
  if (start == null || end == null || start >= end) return [];
  return [
    { startIndex: start, format: { underline: true } },
    { startIndex: end }
  ];
}

async function writePrefillRows(sheets, sheetId, startRow, rows) {
  if (!rows.length) return 0;

  // Build batchUpdate with both values and textFormatRuns for the Matchup column.
  const reqs = [];
  let r = startRow;

  for (const row of rows) {
    const values = [
      { userEnteredValue: { stringValue: row.gameId || "" } },          // A
      { userEnteredValue: { stringValue: row.date || "" } },            // B
      { userEnteredValue: { stringValue: row.week || "" } },            // C
      { userEnteredValue: { stringValue: row.status || "" } },          // D
      { userEnteredValue: { stringValue: row.matchupText || "" } },     // E
      { userEnteredValue: { stringValue: row.finalScore || "" } },      // F
      { userEnteredValue: { numberValue: row.awaySpread ?? null } },    // G
      { userEnteredValue: { numberValue: row.awayML ?? null } },        // H
      { userEnteredValue: { numberValue: row.homeSpread ?? null } },    // I
      { userEnteredValue: { numberValue: row.homeML ?? null } },        // J
      { userEnteredValue: { numberValue: row.total ?? null } }          // K
    ];

    // Add underline runs for the favorite team in Matchup (column E)
    const eCol = 4; // zero-based index of column E
    const textRuns = buildUnderlineRuns(row.matchupText, row.uStart, row.uEnd);

    reqs.push({
      updateCells: {
        range: {
          sheetId,
          startRowIndex: r,
          endRowIndex: r + 1,
          startColumnIndex: 0,
          endColumnIndex: 11
        },
        rows: [{ values }],
        fields: "userEnteredValue"
      }
    });

    if (textRuns.length) {
      reqs.push({
        updateCells: {
          range: {
            sheetId,
            startRowIndex: r,
            endRowIndex: r + 1,
            startColumnIndex: eCol,
            endColumnIndex: eCol + 1
          },
          rows: [{
            values: [{
              userEnteredValue: { stringValue: row.matchupText },
              textFormatRuns: textRuns
            }]
          }],
          fields: "userEnteredValue,textFormatRuns"
        }
      });
    }

    r += 1;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: reqs }
  });

  return rows.length;
}

async function getSheetInfo(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties.title === TAB_NAME);
  if (!sheet) throw new Error(`Tab "${TAB_NAME}" not found`);
  return {
    sheetId: sheet.properties.sheetId,
    rowCount: sheet.properties.gridProperties.rowCount
  };
}

/* ------------------------------ main ------------------------------ */

async function main() {
  const sheets = getSheetsClient();
  const { sheetId } = await getSheetInfo(sheets);

  // Determine date window
  let events = [];
  if (RUN_SCOPE === "today") {
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    events = await fetchScoreboardDates(start, 1);
  } else {
    const start = startOfWeekUTC(new Date());
    events = await fetchScoreboardDates(start, 7);
  }

  // Build rows we don’t already have
  // Fetch current sheet values for Game ID to avoid dupes
  const grid = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A2:A`
  });
  const existingIds = new Set((grid.data.values || []).map(v => v[0]).filter(Boolean));

  const rows = [];
  for (const ev of events) {
    const comp = ev?.competitions?.[0];
    if (!comp) continue;

    const gameId = String(comp.id || ev.id || "");
    if (!gameId) continue;
    if (existingIds.has(gameId)) continue;           // don’t duplicate

    const statusType = comp?.status?.type?.name || ev?.status?.type?.name || "";
    const isFinal = (statusType || "").toLowerCase().includes("final");

    const weekTxt = inferWeekCN(LEAGUE, ev?.week || comp?.week || {});
    const kickoff = toKickoffLocal(comp);
    const statusText = isFinal ? "Final" : kickoff;  // show kickoff time until final

    // odds
    const odds = extractOdds(comp);

    // favorite for underline
    const favKey = computeFavoriteKey(odds, comp?.competitors);
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const { text: matchupText, uStart, uEnd } = teamString(away, home, favKey);

    // final score if available
    let finalScore = "";
    if (isFinal) {
      const aScore = away?.score != null ? String(away.score) : "";
      const hScore = home?.score != null ? String(home.score) : "";
      finalScore = `${aScore}-${hScore}`;
    }

    rows.push({
      gameId,
      date: toSheetDate(comp?.date || ev?.date),
      week: weekTxt,
      status: statusText,
      matchupText, uStart, uEnd,
      finalScore,
      awaySpread: odds.spreadAway,
      homeSpread: odds.spreadHome,
      awayML: odds.awayML,
      homeML: odds.homeML,
      total: odds.total
    });
  }

  if (!rows.length) {
    console.log(`No new rows to write.`);
    return;
  }

  // Find first empty row after header by using current length of column A
  const aCol = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A:A`
  });
  const existing = (aCol.data.values || []).length; // includes header row
  const firstEmptyRow = Math.max(existing, 2);      // A1 is header; start at row 2 minimum

  const wrote = await writePrefillRows(sheets, sheetId, firstEmptyRow - 1, rows);
  console.log(`Wrote ${wrote} new row(s) to ${TAB_NAME}.`);
}

main().catch(err => {
  console.error(`Orchestrator fatal:`, err);
  process.exit(1);
});
