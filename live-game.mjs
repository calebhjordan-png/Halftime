// live-game.mjs — fixed target filtering + halftime lock
import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
  DEBUG_MODE = "",
} = process.env;

const DEBUG = String(DEBUG_MODE || "").trim() === "1";
const log = (...a) => DEBUG && console.log(...a);

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

const todayET = new Date().toLocaleDateString("en-US", { timeZone: "America/New_York" });
const yesterdayET = new Date(Date.now() - 86400000).toLocaleDateString("en-US", { timeZone: "America/New_York" });

const norm = s => (s || "").trim();
const isFinalCell = s => /^final$/i.test(norm(s));
function looksLiveStatus(s) {
  const t = norm(s).toLowerCase();
  return /\b(q[1-4]|1st|2nd|3rd|4th|half|live|progress)\b/.test(t);
}

/* ───────────────────────── ESPN helpers ───────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
function parseStatus(summary) {
  return summary?.header?.competitions?.[0]?.status?.type?.shortDetail || "";
}

/* ───────────────────────────── Sheets ───────────────────────────── */
async function getValues() {
  const range = `${TAB_NAME}!A1:Q2000`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
  return res.data.values || [];
}
function a1For(row, col, tab = TAB_NAME) {
  return `${tab}!${idxToA1(col)}${row + 1}:${idxToA1(col)}${row + 1}`;
}

/* ────────────────────── Target filter (patched) ────────────────────── */
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = norm(row[col.GAME_ID]);
    const date = norm(row[col.DATE]);
    const status = norm(row[col.STATUS]);

    if (!id || isFinalCell(status)) continue;

    const isToday = date === todayET;
    const isYesterday = date === yesterdayET;
    const isLive = looksLiveStatus(status);

    if (GAME_ID && id === GAME_ID) {
      targets.push({ r, id, reason: "GAME_ID" });
    } else if (isLive || isToday || isYesterday) {
      targets.push({ r, id, reason: "active" });
    }
  }
  return targets;
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  const values = await getValues();
  if (!values.length) return console.log("No sheet data.");

  const header = values[0].map(h => h.trim().toLowerCase());
  const col = {
    GAME_ID: header.indexOf("game id"),
    DATE: header.indexOf("date"),
    STATUS: header.indexOf("status"),
  };

  const targets = chooseTargets(values, col);
  if (!targets.length) return console.log("Nothing to update.");

  console.log(`[${new Date().toISOString()}] Found ${targets.length} game(s) to update: ${targets.map(t => t.id).join(", ")}`);

  const data = [];
  for (const t of targets) {
    try {
      const summary = await espnSummary(t.id);
      const status = parseStatus(summary);
      if (status) data.push({ range: a1For(t.r, col.STATUS), values: [[status]] });
    } catch (err) {
      console.log(`❌ ${t.id}: ${err.message}`);
    }
  }

  if (data.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });
    console.log(`✅ Updated ${data.length} cell(s).`);
  } else {
    console.log("No changes detected.");
  }
}

main();
