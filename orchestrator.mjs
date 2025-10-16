// orchestrator.mjs
import fs from "node:fs/promises";
import { google } from "googleapis";
import axios from "axios";

/* ----------------------------- config ----------------------------- */
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const TAB_NAME = process.env.TAB_NAME || "CFB";
const LEAGUE = process.env.LEAGUE || "college-football";
const RUN_SCOPE = process.env.RUN_SCOPE || "week";
const TZ_LABEL = "ET";
const PROVIDER_PREFERENCE = [
  "ESPN BET", "ESPN BET SPORTSBOOK",
  "Caesars",
  "DraftKings",
  "consensus"
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
  const copy = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dow = copy.getUTCDay();
  const delta = (dow + 6) % 7; // to Monday
  copy.setUTCDate(copy.getUTCDate() - delta);
  return copy;
}
function parseNumeric(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === "number") return x;
  const m = String(x).replace(/[^\d.-]/g, "");
  if (m === "" || m === "-" || m === ".") return null;
  const val = Number(m);
  return Number.isFinite(val) ? val : null;
}

/* --------------------------- odds helpers ------------------------- */
function pickPreferredOdds(oddsArray) {
  if (!Array.isArray(oddsArray) || oddsArray.length === 0) return null;
  for (const provider of PROVIDER_PREFERENCE) {
    const hit = oddsArray.find(o => (o?.provider?.name || "").toUpperCase() === provider.toUpperCase());
    if (hit) return hit;
  }
  const withML = oddsArray.find(o => (o?.awayTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyLine) != null);
  if (withML) return withML;
  return oddsArray[0];
}
function extractOdds(competition) {
  const o = pickPreferredOdds(competition?.odds);
  if (!o) return { spreadAway: null, spreadHome: null, total: null, awayML: null, homeML: null };
  const spread = parseNumeric(o.spread);
  let spreadAway = null, spreadHome = null;
  if (spread != null) {
    const isHomeFav = spread < 0;
    const mag = Math.abs(spread);
    if (isHomeFav) { spreadHome = -mag; spreadAway = +mag; }
    else { spreadAway = -mag; spreadHome = +mag; }
  }
  const total = parseNumeric(o.overUnder);
  const awayML = parseNumeric(o?.awayTeamOdds?.moneyLine);
  const homeML = parseNumeric(o?.homeTeamOdds?.moneyLine);
  return { spreadAway, spreadHome, total, awayML, homeML, _provider: o?.provider?.name || "unknown" };
}

/* --------------------------- text helpers ------------------------- */
function teamString(away, home, favoriteFavKey) {
  const a = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.displayName || away?.team?.name || "Away";
  const h = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.displayName || home?.team?.name || "Home";
  if (favoriteFavKey === "away") return { text: `${a} @ ${h}`, uStart: 0, uEnd: a.length };
  if (favoriteFavKey === "home") return { text: `${a} @ ${h}`, uStart: (a.length + 3), uEnd: (a.length + 3 + h.length) };
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
      if (Array.isArray(data?.events)) all.push(...data.events);
    })
  );
  return all;
}

/* --------------------------- build rows --------------------------- */
function toSheetDate(iso) {
  const d = new Date(iso);
  const mm = pad2(d.getUTCMonth() + 1);
  const dd = pad2(d.getUTCDate());
  const yy = String(d.getUTCFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}
function toKickoffLocal(competition) {
  const date = competition?.date ? new Date(competition.date) : null;
  if (!date) return "";
  const hh = date.getUTCHours(), mm = pad2(date.getUTCMinutes());
  const hoursET = ((hh + 24) - 4) % 24; // simple UTC→ET display
  const h12 = ((hoursET + 11) % 12) + 1;
  const ampm = hoursET < 12 ? "AM" : "PM";
  return `${h12}:${mm} ${ampm}`;
}
function inferWeekCN(_league, weekObj) {
  const n = weekObj?.number || null;
  if (typeof n === "number" && n > 0) return `Week ${n}`;
  const txt = weekObj?.text || "Week";
  return txt.startsWith("Week") ? txt : `Week ${txt}`;
}
function computeFavoriteKey(odds) {
  if (!odds) return null;
  if (odds.spreadAway != null && odds.spreadAway < 0) return "away";
  if (odds.spreadHome != null && odds.spreadHome < 0) return "home";
  if (odds.awayML != null && odds.homeML != null) {
    if (odds.awayML < odds.homeML) return "away";
    if (odds.homeML < odds.awayML) return "home";
  }
  return null;
}

/* ---------------------------- write sheet ------------------------- */
// SAFER: never set a run that starts at or past the text length.
function buildUnderlineRuns(text, start, end) {
  if (start == null || end == null) return [];
  const len = (text || "").length;
  if (!len) return [];
  const s = Math.max(0, Math.min(start, len - 1));
  const e = Math.max(0, Math.min(end, len));
  if (s >= e) return [];
  const runs = [{ startIndex: s, format: { underline: true } }];
  // Only add a trailing run if it starts strictly before len
  if (e < len) runs.push({ startIndex: e });
  return runs;
}

async function writePrefillRows(sheets, sheetId, startRow, rows) {
  if (!rows.length) return 0;
  const reqs = [];
  let r = startRow;
  for (const row of rows) {
    const values = [
      { userEnteredValue: { stringValue: row.gameId || "" } },
      { userEnteredValue: { stringValue: row.date || "" } },
      { userEnteredValue: { stringValue: row.week || "" } },
      { userEnteredValue: { stringValue: row.status || "" } },
      { userEnteredValue: { stringValue: row.matchupText || "" } },
      { userEnteredValue: { stringValue: row.finalScore || "" } },
      { userEnteredValue: { numberValue: row.awaySpread ?? null } },
      { userEnteredValue: { numberValue: row.awayML ?? null } },
      { userEnteredValue: { numberValue: row.homeSpread ?? null } },
      { userEnteredValue: { numberValue: row.homeML ?? null } },
      { userEnteredValue: { numberValue: row.total ?? null } }
    ];
    reqs.push({
      updateCells: {
        range: { sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 0, endColumnIndex: 11 },
        rows: [{ values }],
        fields: "userEnteredValue"
      }
    });
    const textRuns = buildUnderlineRuns(row.matchupText, row.uStart, row.uEnd);
    if (textRuns.length) {
      reqs.push({
        updateCells: {
          range: { sheetId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 4, endColumnIndex: 5 },
          rows: [{ values: [{ userEnteredValue: { stringValue: row.matchupText }, textFormatRuns: textRuns }] }],
          fields: "userEnteredValue,textFormatRuns"
        }
      });
    }
    r += 1;
  }
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests: reqs } });
  return rows.length;
}

async function getSheetInfo(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = meta.data.sheets.find(s => s.properties.title === TAB_NAME);
  if (!sheet) throw new Error(`Tab "${TAB_NAME}" not found`);
  return { sheetId: sheet.properties.sheetId, rowCount: sheet.properties.gridProperties.rowCount };
}

/* ------------------------------ main ------------------------------ */
async function main() {
  const sheets = getSheetsClient();
  const { sheetId } = await getSheetInfo(sheets);

  let events = [];
  if (RUN_SCOPE === "today") {
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    events = await fetchScoreboardDates(start, 1);
  } else {
    const start = startOfWeekUTC(new Date());
    events = await fetchScoreboardDates(start, 7);
  }

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
    if (existingIds.has(gameId)) continue;

    const statusType = comp?.status?.type?.name || ev?.status?.type?.name || "";
    const isFinal = (statusType || "").toLowerCase().includes("final");
    const weekTxt = inferWeekCN(LEAGUE, ev?.week || comp?.week || {});
    const kickoff = toKickoffLocal(comp);
    const statusText = isFinal ? "Final" : kickoff;

    const odds = extractOdds(comp);
    const favKey = computeFavoriteKey(odds);
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const home = comp?.competitors?.find(c => c.homeAway === "home");
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
      homeSpread: odds.spreadHome,
      awayML: odds.awayML,
      homeML: odds.homeML,
      total: odds.total
    });
  }

  if (!rows.length) { console.log(`No new rows to write.`); return; }

  const aCol = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A:A`
  });
  const existing = (aCol.data.values || []).length;
  const firstEmptyRow = Math.max(existing, 2);
  const wrote = await writePrefillRows(sheets, sheetId, firstEmptyRow - 1, rows);
  console.log(`Wrote ${wrote} new row(s) to ${TAB_NAME}.`);
}

main().catch(err => {
  console.error(`Orchestrator fatal:`, err);
  process.exit(1);
});
