// live-game.mjs
// Robust live updater for NFL and CFB Google Sheets tabs.
// Fetches live odds (spreads, MLs, totals) and halftime scores during active games.

// ===== Imports =====
import axios from "axios";
import { google } from "googleapis";

// ===== Env Vars =====
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE = process.env.LEAGUE;              // "nfl" | "college-football"
const TAB = process.env.TAB_NAME;               // "NFL" | "CFB"

// ===== Validation =====
function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return v;
}

function now() {
  return new Date().toISOString();
}

// ===== ESPN Odds API URLs =====
const ODDS_URL = (league, gameId) =>
  `https://sports.core.api.espn.com/v2/sports/football/leagues/${league}/events/${gameId}/competitions/${gameId}/odds`;

// ===== Axios Helper =====
async function safeGet(url) {
  try {
    const { data } = await axios.get(url, { timeout: 12000 });
    return data;
  } catch (err) {
    const code = err?.response?.status;
    if (code === 404 || code === 204) {
      console.log(`[${now()}] No odds yet (HTTP ${code}): ${url}`);
      return null;
    }
    console.warn(`[${now()}] Request failed (${code ?? err.code ?? "ERR"}) for ${url}`);
    throw err;
  }
}

// ===== ESPN Odds Parser =====
function pickEspnBook(bookmakers = []) {
  const espn = bookmakers.find(b => (b?.name || b?.displayName || "").toUpperCase().includes("ESPN"));
  return espn || bookmakers[0] || null;
}

function parseOddsPayload(payload) {
  if (!payload?.items?.length) return null;

  const first = payload.items[0];
  const book = pickEspnBook(first?.bookmakers);
  if (!book?.markets?.length) return null;

  let awayML = null, homeML = null, awaySpread = null, homeSpread = null, total = null;

  for (const m of book.markets) {
    const t = (m?.type || m?.displayName || "").toLowerCase();
    const outcomes = m?.outcomes || [];

    // Moneyline
    if (t.includes("moneyline")) {
      for (const o of outcomes) {
        const side = o?.homeAway?.toLowerCase?.();
        if (side === "away") awayML = o?.price ?? awayML;
        if (side === "home") homeML = o?.price ?? homeML;
      }
    }

    // Spread
    if (t.includes("spread")) {
      for (const o of outcomes) {
        const side = o?.homeAway?.toLowerCase?.();
        if (side === "away") awaySpread = o?.point ?? awaySpread;
        if (side === "home") homeSpread = o?.point ?? homeSpread;
      }
    }

    // Total
    if (t.includes("total") || t.includes("over/under")) {
      const over = outcomes.find(o => (o?.type || o?.name || "").toLowerCase().includes("over"));
      const under = outcomes.find(o => (o?.type || o?.name || "").toLowerCase().includes("under"));
      total = over?.point ?? under?.point ?? total;
    }
  }

  return { awayML, homeML, awaySpread, homeSpread, total };
}

// ===== Google Sheets Setup =====
async function getSheets() {
  requireEnv("GOOGLE_SHEET_ID");
  requireEnv("GOOGLE_SERVICE_ACCOUNT");

  let creds;
  try {
    creds = JSON.parse(SA_JSON);
  } catch {
    try {
      creds = JSON.parse(Buffer.from(SA_JSON, "base64").toString("utf8"));
    } catch {
      throw new Error("GOOGLE_SERVICE_ACCOUNT must be valid JSON or base64-encoded JSON.");
    }
  }

  const auth = new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return google.sheets({ version: "v4", auth });
}

// ===== A1 Helpers =====
const COL = { L: 12, M: 13, N: 14, O: 15, P: 16, Q: 17 };
function a1(row, colIndex) {
  let x = colIndex, s = "";
  while (x) {
    x--;
    s = String.fromCharCode(65 + (x % 26)) + s;
    x = Math.floor(x / 26);
  }
  return `${s}${row}`;
}

// ===== Determine if a game is live =====
function isLiveStatus(s) {
  const t = (s || "").toLowerCase().trim();
  // Expanded detection: halftime, half, ht, mid, in-progress, etc.
  return /(live|in[-\s]?progress|1st|2nd|3rd|4th|q1|q2|q3|q4|half|halftime|ht|mid|ot|overtime)/.test(t);
}

// ===== Helper for numeric data =====
function numOrBlank(x) {
  if (x === null || x === undefined || x === "" || Number.isNaN(Number(x))) return "";
  return Number(x);
}

// ===== Main =====
(async function main() {
  try {
    requireEnv("LEAGUE");
    requireEnv("TAB_NAME");

    const sheets = await getSheets();

    const readRange = `${TAB}!A2:Q`;
    const readRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: readRange,
      valueRenderOption: "UNFORMATTED_VALUE",
    });

    const rows = readRes.data.values || [];
    if (!rows.length) {
      console.log(`[${now()}] No rows found in ${TAB}.`);
      return;
    }

    const updates = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = 2 + i;
      const gameId = row[0];
      const status = row[3];

      if (!gameId || !isLiveStatus(status)) continue;

      const url = ODDS_URL(LEAGUE, gameId);
      let payload = null;
      try {
        payload = await safeGet(url);
      } catch (err) {
        console.warn(`[${now()}] Skipping row ${rowNum} (${gameId}): ${err.message}`);
        continue;
      }

      if (!payload) continue;

      const parsed = parseOddsPayload(payload);
      if (!parsed) continue;

      const { awaySpread, awayML, homeSpread, homeML, total } = parsed;

      const writeRange = `${TAB}!${a1(rowNum, COL.M)}:${a1(rowNum, COL.Q)}`;
      const values = [[
        numOrBlank(awaySpread),
        numOrBlank(awayML),
        numOrBlank(homeSpread),
        numOrBlank(homeML),
        numOrBlank(total),
      ]];

      updates.push({ range: writeRange, values });
    }

    if (!updates.length) {
      console.log(`[${now()}] Nothing to update (no live rows or no markets).`);
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: "RAW",
        data: updates,
      },
    });

    console.log(`[${now()}] Updated ${updates.length} live rows on tab "${TAB}".`);
  } catch (err) {
    console.error(`Live updater fatal: ${err?.message || err}`, err?.response?.status ? `*** code: ${err.response.status} ***` : "");
    process.exitCode = 1;
  }
})();
