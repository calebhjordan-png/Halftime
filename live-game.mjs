// live-game.mjs
// Robust live updater: ignores 404/empty markets, continues processing.
// Env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT (JSON), LEAGUE, TAB_NAME

import axios from "axios";
import { google } from "googleapis";

// ====== Config you already use ======
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE = process.env.LEAGUE;               // "nfl" | "college-football"
const TAB = process.env.TAB_NAME;                // "NFL" | "CFB"

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

// ESPN core odds endpoints (same shape for NFL / CFB)
const ODDS_URL = (league, gameId) =>
  `https://sports.core.api.espn.com/v2/sports/football/leagues/${league}/events/${gameId}/competitions/${gameId}/odds`;

// ----- Axios helper that never throws on 404/204 -----
async function safeGet(url) {
  try {
    const { data } = await axios.get(url, { timeout: 12000 });
    return data;
  } catch (err) {
    const code = err?.response?.status;
    if (code === 404 || code === 204) {
      console.log(`[${now()}] odds not available yet: ${url} (HTTP ${code})`);
      return null;
    }
    // Network hiccup? Let’s surface once, but not explode the whole job.
    console.warn(`[${now()}] fetch failed (${code ?? err.code ?? "ERR"}) for ${url}`);
    throw err;
  }
}

// ----- Minimal ESPN odds parsing (Moneyline/Spread/Total) -----
function pickEspnBet(bookmakers = []) {
  // Prefer ESPN BET if present; otherwise first available
  const espn = bookmakers.find(b => (b?.name || b?.displayName || "").toUpperCase().includes("ESPN"));
  return espn || bookmakers[0] || null;
}

function parseOddsPayload(payload) {
  // payload is ESPN odds resource; we expect { items: [ { bookmakers: [ ... ] } ] }
  if (!payload?.items?.length) return null;

  const first = payload.items[0];
  const book = pickEspnBet(first?.bookmakers);
  if (!book?.markets?.length) return null;

  let awayML = null, homeML = null, awaySpread = null, homeSpread = null, total = null;

  for (const m of book.markets) {
    const t = (m?.type || m?.displayName || "").toLowerCase();
    const outcomes = m?.outcomes || [];

    // Moneyline
    if (t.includes("moneyline")) {
      for (const o of outcomes) {
        const team = (o?.team?.abbreviation || o?.team?.displayName || o?.name || "").toLowerCase();
        if (team.includes("away")) awayML = o?.price ?? awayML;
        if (team.includes("home")) homeML = o?.price ?? homeML;
        // Sometimes team objects aren't labeled away/home; use homeAway if present
        if (o?.homeAway === "away") awayML = o?.price ?? awayML;
        if (o?.homeAway === "home") homeML = o?.price ?? homeML;
      }
    }

    // Spread
    if (t.includes("spread")) {
      // Often two outcomes with same abs line, one positive one negative
      for (const o of outcomes) {
        const val = o?.point ?? o?.line ?? null;
        if (o?.homeAway === "away") awaySpread = val ?? awaySpread;
        if (o?.homeAway === "home") homeSpread = val ?? homeSpread;
      }
    }

    // Total
    if (t.includes("total") || t.includes("over/under")) {
      // ESPN usually includes a single number; if multiple, they share the same 'point'
      const ov = outcomes.find(x => (x?.type || x?.name || "").toLowerCase().includes("over"));
      const und = outcomes.find(x => (x?.type || x?.name || "").toLowerCase().includes("under"));
      total = (ov?.point ?? und?.point ?? total);
    }
  }

  return { awayML, homeML, awaySpread, homeSpread, total };
}

// ----- Google Sheets auth -----
async function getSheets() {
  requireEnv("GOOGLE_SHEET_ID");
  requireEnv("GOOGLE_SERVICE_ACCOUNT");

  let creds;
  try {
    creds = JSON.parse(SA_JSON);
  } catch {
    // Allow the secret to be provided base64 or raw; try base64 as a fallback.
    try { creds = JSON.parse(Buffer.from(SA_JSON, "base64").toString("utf8")); }
    catch { throw new Error("GOOGLE_SERVICE_ACCOUNT must be JSON (or base64-encoded JSON)."); }
  }

  const auth = new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

// ----- Utilities for A1 notation -----
const COL = {
  L: 12, M: 13, N: 14, O: 15, P: 16, Q: 17,
};
function a1(row, colIndex) {
  // 1-based column index -> letters
  let x = colIndex, s = "";
  while (x) { x--; s = String.fromCharCode(65 + (x % 26)) + s; x = Math.floor(x / 26); }
  return `${s}${row}`;
}

// ----- Decide which rows are "live" (in-progress or halftime) -----
function isLiveStatus(s) {
  const t = (s || "").toLowerCase();
  // keep flexible: live, 1st/2nd/3rd/4th, q1..q4, half, halftime, ot
  return /(live|1st|2nd|3rd|4th|q1|q2|q3|q4|half|halftime|ot)/.test(t);
}

// ====== MAIN ======
(async function main() {
  try {
    requireEnv("LEAGUE");
    requireEnv("TAB_NAME");

    const sheets = await getSheets();

    // Read enough columns to get ID, Status, and live columns (A..Q)
    const readRange = `${TAB}!A2:Q`;
    const readRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: readRange,
      valueRenderOption: "UNFORMATTED_VALUE",
    });

    const rows = readRes.data.values || [];
    if (!rows.length) {
      console.log(`[${now()}] No rows to process.`);
      return;
    }

    // Collect value updates (use Values API for simplicity/atomicity per row)
    const updates = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = 2 + i; // since we started at A2
      const gameId = row[0];  // column A
      const status = row[3];  // column D "Status"

      if (!gameId || !isLiveStatus(status)) continue;

      const url = ODDS_URL(LEAGUE, gameId);
      let payload = null;
      try {
        payload = await safeGet(url);
      } catch (err) {
        // Non-404 failure — log and skip this row
        console.warn(`[${now()}] skip row ${rowNum} gameId ${gameId}: ${err?.message || err}`);
        continue;
      }

      if (!payload) {
        // No market yet — leave cells untouched
        continue;
      }

      const parsed = parseOddsPayload(payload);
      if (!parsed) continue;

      const { awaySpread, awayML, homeSpread, homeML, total } = parsed;

      // Prepare update for L..Q (Half Score, Live Away Spread, Live Away ML, Live Home Spread, Live Home ML, Live Total)
      // We won't touch Half Score (L) here; keep your own logic if you already set it elsewhere.
      const writeRange = `${TAB}!${a1(rowNum, COL.M)}:${a1(rowNum, COL.Q)}`;
      const values = [[
        // M          N          O            P          Q
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

    // Batch the updates
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

function numOrBlank(x) {
  if (x === null || x === undefined || x === "" || Number.isNaN(Number(x))) return "";
  return Number(x);
}
