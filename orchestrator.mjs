// orchestrator.mjs
// ESM Node 20+
//
// ENV expected (from your workflows):
//   GOOGLE_SHEET_ID          -> target spreadsheet id
//   GOOGLE_SERVICE_ACCOUNT   -> JSON of a service account with Sheets scope
//   LEAGUE                   -> "nfl" | "college-football"
//   TAB_NAME                 -> "NFL" | "CFB"
//   RUN_SCOPE                -> "week" | "today" (prefill uses week, finals uses today)
//   DATE_FMT                 -> optional: "MM/DD/YY" (default) | "DD/MM/YY"
//
// npm deps: axios, googleapis

import axios from "axios";
import { google } from "googleapis";

// ---------- CONFIG ----------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const TAB_TITLE = process.env.TAB_NAME || (process.env.LEAGUE === "nfl" ? "NFL" : "CFB");
const LEAGUE = (process.env.LEAGUE || "nfl").toLowerCase();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const DATE_FMT = (process.env.DATE_FMT || "MM/DD/YY").toUpperCase();
const PROVIDER_PREFERENCE = ["ESPN BET", "CAESARS", "DRAFTKINGS", "FANDUEL"];

const CONCURRENCY = 5;
const GREEN = { red: 0.85, green: 0.95, blue: 0.85 };
const RED   = { red: 0.98, green: 0.88, blue: 0.88 };
const CLEAR = {};

// Column indices (0-based)
const COL = {
  gameId: 0,
  date: 1,
  week: 2,
  status: 3,
  matchup: 4,
  finalScore: 5,
  awaySpread: 6,
  awayML: 7,
  homeSpread: 8,
  homeML: 9,
  total: 10,
};

// ---------- GOOGLE AUTH ----------
function getAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT;
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");
  const creds = JSON.parse(raw);
  const jwt = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  return jwt;
}

async function getSheets() {
  const auth = getAuth();
  return google.sheets({ version: "v4", auth });
}

async function getSheetIdByTitle(sheets, spreadsheetId, title) {
  const { data } = await sheets.spreadsheets.get({ spreadsheetId });
  const sheet = (data.sheets || []).find(s => s.properties.title === title);
  if (!sheet) throw new Error(`Tab "${title}" not found`);
  return sheet.properties.sheetId;
}

async function readExistingGameIds(sheets, spreadsheetId, title) {
  const range = `'${title}'!A2:A`;
  const { data } = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const set = new Set();
  (data.values || []).forEach(row => { if (row[0]) set.add(String(row[0])); });
  return set;
}

// ---------- UTILS ----------
function pLimit(n) {
  let active = 0;
  const q = [];
  const runNext = () => {
    active--;
    if (q.length) q.shift()();
  };
  return (fn) => new Promise((resolve, reject) => {
    const run = () => {
      active++;
      Promise.resolve().then(fn).then(
        v => { resolve(v); runNext(); },
        e => { reject(e); runNext(); }
      );
    };
    active < n ? run() : q.push(run);
  });
}
const limit = pLimit(CONCURRENCY);

function parseNumeric(v) {
  if (v == null) return null;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return null;
  // ESPN ML sometimes like "+125" -> parseInt OK
  if (/^[+-]?\d+(\.\d+)?$/.test(s)) return Number(s);
  return null;
}

function pad2(n) { return n < 10 ? `0${n}` : `${n}`; }

function formatDateMMDDYY(d) {
  const dt = new Date(d);
  const mm = pad2(dt.getMonth() + 1);
  const dd = pad2(dt.getDate());
  const yy = String(dt.getFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}
function formatDateDDMMYY(d) {
  const dt = new Date(d);
  const mm = pad2(dt.getMonth() + 1);
  const dd = pad2(dt.getDate());
  const yy = String(dt.getFullYear()).slice(-2);
  return `${dd}/${mm}/${yy}`;
}
function toSheetDate(d) {
  return DATE_FMT === "DD/MM/YY" ? formatDateDDMMYY(d) : formatDateMMDDYY(d);
}

function toLocalET(iso) {
  // ESPN dates are ISO UTC. We just want a readable ET string without "ET" suffix per your request.
  try {
    const dt = new Date(iso);
    const opts = { hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" };
    return new Intl.DateTimeFormat("en-US", opts).format(dt);
  } catch { return ""; }
}

function toKickoffLocal(comp) {
  const stamp = comp?.date;
  return stamp ? toLocalET(stamp) : "";
}

function inferWeekCN(league, wk) {
  // wk may be a number or object, normalize to "Week N"
  let num = null;
  if (typeof wk === "number") num = wk;
  else if (typeof wk === "object" && wk?.number != null) num = wk.number;
  if (!num && league === "nfl") num = wk?.number;
  if (!num) return "Week";
  return `Week ${num}`;
}

function teamString(away, home, favKey) {
  const aName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const hName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  const text = `${aName} @ ${hName}`;

  // underline only favorite team (single underline; no double)
  let uStart = -1, uEnd = -1;
  if (favKey === "away") {
    uStart = 0;
    uEnd = aName.length;
  } else if (favKey === "home") {
    const start = text.indexOf("@");
    if (start >= 0) {
      uStart = start + 2; // space after '@ '
      uEnd = text.length;
    }
  }
  return { text, uStart, uEnd };
}

function computeFavoriteKey(odds) {
  // prefer spread; fall back to ML
  if (odds.spreadAway != null && odds.spreadHome != null) {
    if (odds.spreadAway < 0) return "away";
    if (odds.spreadHome < 0) return "home";
  }
  if (odds.awayML != null && odds.homeML != null) {
    // More negative ML is favorite
    const a = odds.awayML;
    const h = odds.homeML;
    if (typeof a === "number" && typeof h === "number") {
      if (a < 0 && (h >= 0 || a < h)) return "away";
      if (h < 0 && (a >= 0 || h < a)) return "home";
    }
  }
  return null;
}

function winnerKeyFromScore(aScore, hScore) {
  if (aScore > hScore) return "away";
  if (hScore > aScore) return "home";
  return null;
}

// ---------- ESPN FETCH ----------
function leaguePath() {
  return LEAGUE === "college-football" ? "football/college-football" : "football/nfl";
}

async function fetchScoreboardWeek() {
  // Week scope: entire current week board
  const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath()}/scoreboard`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

async function fetchScoreboardToday() {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath()}/scoreboard`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

function pickPreferredOdds(oddsArr) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  // try preferred provider that actually has either spread or total (ML might be absent for CFB here)
  const pickByName = (name) =>
    oddsArr.find(o => (o?.provider?.name || "").toUpperCase() === name.toUpperCase());
  for (const pName of PROVIDER_PREFERENCE) {
    const x = pickByName(pName);
    if (x) return x;
  }
  return oddsArr[0];
}

function extractOdds(competition) {
  const o = pickPreferredOdds(competition?.odds);
  let spreadAway = null, spreadHome = null, total = null, awayML = null, homeML = null;

  if (o) {
    const spread = parseNumeric(o.spread);
    if (spread != null) {
      const mag = Math.abs(spread);
      const isHomeFav = spread < 0;
      spreadAway = isHomeFav ? +mag : -mag;
      spreadHome = isHomeFav ? -mag : +mag;
    }
    total = parseNumeric(o.overUnder);
    awayML = parseNumeric(o?.awayTeamOdds?.moneyLine);
    homeML = parseNumeric(o?.homeTeamOdds?.moneyLine);
  }
  return { spreadAway, spreadHome, total, awayML, homeML };
}

async function fetchMlFallback(compId) {
  try {
    const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath()}/summary?event=${compId}`;
    const { data } = await axios.get(url, { timeout: 12000 });

    const pools = [];
    if (Array.isArray(data?.pickcenter)) pools.push(...data.pickcenter);
    if (Array.isArray(data?.odds)) pools.push(...data.odds);

    const upper = (s) => (s || "").toUpperCase();
    const byProvider = (prov) =>
      pools.find(p => upper(p?.provider?.name) === upper(prov) &&
        (p?.awayTeamOdds?.moneyLine != null || p?.homeTeamOdds?.moneyLine != null));

    let pick = PROVIDER_PREFERENCE.map(byProvider).find(Boolean);
    if (!pick) pick = pools.find(p => (p?.awayTeamOdds?.moneyLine != null || p?.homeTeamOdds?.moneyLine != null));
    if (!pick) return { awayML: null, homeML: null };

    return {
      awayML: parseNumeric(pick?.awayTeamOdds?.moneyLine),
      homeML: parseNumeric(pick?.homeTeamOdds?.moneyLine),
    };
  } catch {
    return { awayML: null, homeML: null };
  }
}

// ---------- WRITE HELPERS ----------
function value(v) {
  if (v == null || v === "") return { userEnteredValue: { stringValue: "" } };
  if (typeof v === "number") return { userEnteredValue: { numberValue: v } };
  return { userEnteredValue: { stringValue: String(v) } };
}

function rowValues(r) {
  return [
    value(r.gameId),
    value(r.date),
    value(r.week),
    value(r.status),
    value(r.matchupText),
    value(r.finalScore),
    value(r.awaySpread),
    value(r.awayML),
    value(r.homeSpread),
    value(r.homeML),
    value(r.total),
  ];
}

function textRunsForFavorite(text, uStart, uEnd) {
  // Google textFormatRuns rules:
  //   runs are ordered by startIndex asc; last run goes to end-of-string
  // We'll emit:
  //   [ {0, underline:false}, {uStart, underline:true}, {uEnd, underline:false} ]
  // Only include runs with startIndex < text.length
  const runs = [];
  const L = text.length;
  runs.push({ startIndex: 0, format: { underline: false } });
  if (uStart >= 0 && uStart < L) {
    runs.push({ startIndex: uStart, format: { underline: true } });
    if (uEnd > uStart && uEnd < L) {
      runs.push({ startIndex: uEnd, format: { underline: false } });
    }
  }
  return runs;
}

function colourCell(bg) {
  return { userEnteredFormat: { backgroundColor: bg } };
}

function noColour() {
  return { userEnteredFormat: { backgroundColor: CLEAR } };
}

function parseScoreString(s) {
  if (!s || typeof s !== "string") return { a: null, h: null };
  const m = s.match(/^\s*(\d+)\s*-\s*(\d+)\s*$/);
  if (!m) return { a: null, h: null };
  return { a: Number(m[1]), h: Number(m[2]) };
}

function gradeCells(finalScore, odds) {
  // Returns background colors for: awayML, homeML, awaySpread, homeSpread, total
  // Only for finals; otherwise leave undefined to not override.
  const { a, h } = parseScoreString(finalScore || "");
  if (a == null || h == null) return {};

  const fav = computeFavoriteKey(odds);
  const winner = winnerKeyFromScore(a, h);

  // ML
  let awayMLbg, homeMLbg;
  if (winner) {
    awayMLbg = winner === "away" ? GREEN : RED;
    homeMLbg = winner === "home" ? GREEN : RED;
  }

  // SPREAD
  let awaySprBg, homeSprBg;
  if (typeof odds.spreadAway === "number" && typeof odds.spreadHome === "number") {
    const awayCovers = (a + odds.spreadAway) > h;
    const homeCovers = (h + odds.spreadHome) > a;
    awaySprBg = awayCovers ? GREEN : RED;
    homeSprBg = homeCovers ? GREEN : RED;
  }

  // TOTAL
  let totalBg;
  if (typeof odds.total === "number") {
    const sum = a + h;
    if (sum > odds.total) totalBg = GREEN;   // Over
    else if (sum < odds.total) totalBg = RED; // Under
    else totalBg = CLEAR; // push
  }

  return {
    awayMLbg, homeMLbg, awaySprBg, homeSprBg, totalBg
  };
}

// ---------- MAIN ----------
async function main() {
  if (!SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");

  const sheets = await getSheets();
  const sheetId = await getSheetIdByTitle(sheets, SHEET_ID, TAB_TITLE);
  const existingIds = await readExistingGameIds(sheets, SHEET_ID, TAB_TITLE);

  const board = RUN_SCOPE === "today" ? await fetchScoreboardToday() : await fetchScoreboardWeek();
  const events = Array.isArray(board?.events) ? board.events : [];

  const rows = [];
  for (const ev of events) {
    const comp = ev?.competitions?.[0];
    if (!comp) continue;

    const gameId = String(comp.id || ev.id || "");
    if (!gameId) continue;
    if (existingIds.has(gameId)) continue; // skip already present

    const statusType = comp?.status?.type?.name || ev?.status?.type?.name || "";
    const isFinal = (statusType || "").toLowerCase().includes("final");
    const weekTxt = inferWeekCN(LEAGUE, ev?.week || comp?.week || {});
    const kickoff = toKickoffLocal(comp);
    const statusText = isFinal ? "Final" : kickoff;

    let odds = extractOdds(comp);

    // Fallback for missing ML (esp. CFB scoreboard often omits ML)
    if ((odds.awayML == null || odds.homeML == null) && LEAGUE === "college-football") {
      const ml = await limit(() => fetchMlFallback(comp.id));
      if (odds.awayML == null) odds.awayML = ml.awayML;
      if (odds.homeML == null) odds.homeML = ml.homeML;
    }

    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const favKey = computeFavoriteKey(odds);
    const { text: matchupText, uStart, uEnd } = teamString(away, home, favKey);

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
      awayML: odds.awayML,
      homeSpread: odds.spreadHome,
      homeML: odds.homeML,
      total: odds.total
    });
  }

  if (rows.length === 0) {
    console.log("No new rows to write.");
    return;
  }

  // Find next empty row: read column A length, then append
  const aRange = `'${TAB_TITLE}'!A2:A`;
  const aVals = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: aRange });
  const existingLen = (aVals.data.values || []).length;
  let startRowIndex = 1 + existingLen + 1 - 1; // header is row 1; API uses 0-based indices
  // Explanation: header row is at index 0; first data row index=1.
  // existingLen data rows => next row index = 1 + existingLen.

  const requests = [];

  rows.forEach((r, i) => {
    const rowIndex = startRowIndex + i; // 0-based
    // 1) Write raw values
    requests.push({
      updateCells: {
        range: {
          sheetId,
          startRowIndex: rowIndex,
          endRowIndex: rowIndex + 1,
          startColumnIndex: 0,
          endColumnIndex: 11
        },
        rows: [{
          values: rowValues(r)
        }],
        fields: "userEnteredValue"
      }
    });

    // 2) Underline only the favorite team inside matchup (safe text runs)
    const runs = textRunsForFavorite(r.matchupText, r.uStart, r.uEnd);
    if (runs.length) {
      requests.push({
        updateCells: {
          range: {
            sheetId,
            startRowIndex: rowIndex,
            endRowIndex: rowIndex + 1,
            startColumnIndex: COL.matchup,
            endColumnIndex: COL.matchup + 1
          },
          rows: [{
            values: [{
              textFormatRuns: runs
            }]
          }],
          fields: "textFormatRuns"
        }
      });
    }

    // 3) Finals-only colouring of ML/Spread/Total cells
    if (r.status === "Final" && r.finalScore) {
      const odds = {
        spreadAway: r.awaySpread,
        spreadHome: r.homeSpread,
        total: r.total,
        awayML: r.awayML,
        homeML: r.homeML,
      };
      const { awayMLbg, homeMLbg, awaySprBg, homeSprBg, totalBg } = gradeCells(r.finalScore, odds);

      const colourCellAt = (col, bg) => {
        if (!bg) return;
        requests.push({
          updateCells: {
            range: { sheetId, startRowIndex: rowIndex, endRowIndex: rowIndex + 1, startColumnIndex: col, endColumnIndex: col + 1 },
            rows: [{ values: [colourCell(bg)] }],
            fields: "userEnteredFormat.backgroundColor"
          }
        });
      };

      colourCellAt(COL.awayML, awayMLbg);
      colourCellAt(COL.homeML, homeMLbg);
      colourCellAt(COL.awaySpread, awaySprBg);
      colourCellAt(COL.homeSpread, homeSprBg);
      colourCellAt(COL.total, totalBg);
    } else {
      // Ensure non-finals have no residual colours (in case a row got reused)
      [COL.awayML, COL.homeML, COL.awaySpread, COL.homeSpread, COL.total].forEach(col => {
        requests.push({
          updateCells: {
            range: { sheetId, startRowIndex: rowIndex, endRowIndex: rowIndex + 1, startColumnIndex: col, endColumnIndex: col + 1 },
            rows: [{ values: [noColour()] }],
            fields: "userEnteredFormat.backgroundColor"
          }
        });
      });
    }
  });

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests }
  });

  console.log(`Rows written: ${rows.length}`);
}

// ---------- RUN ----------
main().catch(e => {
  console.error("Orchestrator fatal:", e);
  process.exit(1);
});
