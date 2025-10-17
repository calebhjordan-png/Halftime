// live-game.mjs
// Purpose: Fill columns L–Q with halftime + live odds for games that are in-progress / halftime.
// Columns: L=Half Score, M=Live Away Spread, N=Live Away ML, O=Live Home Spread, P=Live Home ML, Q=Live Total

import axios from "axios";
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SERVICE_ACCOUNT_RAW = process.env.GOOGLE_SERVICE_ACCOUNT; // raw JSON or base64 JSON
const LEAGUE = (process.env.LEAGUE || "").trim();               // "nfl" or "college-football"
const TAB_NAME = (process.env.TAB_NAME || "").trim();           // "NFL" or "CFB"

if (!SHEET_ID || !SERVICE_ACCOUNT_RAW || !LEAGUE || !TAB_NAME) {
  console.error("Missing one or more required env vars.");
  process.exit(1);
}

// Accept raw JSON or base64 JSON for the service account
function parseServiceAccount(raw) {
  const txt = /^[A-Za-z0-9+/=]+$/.test(raw.trim())
    ? Buffer.from(raw, "base64").toString("utf8")
    : raw;
  return JSON.parse(txt);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// --- Google Sheets client (VALUES API only—no textFormatRuns to avoid 400s) ---
async function getSheetsClient() {
  const sa = parseServiceAccount(SERVICE_ACCOUNT_RAW);
  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

// A1 helpers
const COL_L = 12, COL_Q = 17;
function a1(col, row) {
  const letters = [];
  let c = col;
  while (c > 0) {
    const m = (c - 1) % 26;
    letters.unshift(String.fromCharCode(65 + m));
    c = Math.floor((c - 1) / 26);
  }
  return `${letters.join("")}${row}`;
}
function rangeLQ(row) {
  return `${a1(COL_L, row)}:${a1(COL_Q, row)}`; // L..Q for single row
}

// --- ESPN endpoints ---
// We use two endpoints:
// 1) scoreboard: current status + scores (for halftime score)
// 2) competition odds: live odds providers (prefer "ESPN BET", fallback first)

const SCOREBOARD_URL = (league) =>
  `https://site.web.api.espn.com/apis/v2/sports/football/${league}/scoreboard`;

const COMP_ODDS_URL = (league, gameId) =>
  `https://sports.core.api.espn.com/v2/sports/football/${league}/competitions/${gameId}/odds`;

function isHalftimeOrLive(espnStatus) {
  // status examples: "STATUS_IN_PROGRESS", "STATUS_HALFTIME", "STATUS_END_PERIOD", "STATUS_SCHEDULED"
  const s = (espnStatus || "").toUpperCase();
  return s.includes("IN_PROGRESS") || s.includes("HALFTIME") || s.includes("END_PERIOD");
}

// Safely get live odds from the competition odds resource.
async function fetchLiveOddsForGame(gameId) {
  try {
    const { data } = await axios.get(COMP_ODDS_URL(LEAGUE, gameId), { timeout: 10000 });
    // `data.items` is an array of provider entries. Each provider can have markets.
    // We try "ESPN BET" first, then fallback to the first provider with usable numbers.
    const providers = (data?.items || []).map(x => x) || [];

    // Helper to expand provider into concrete lines
    const pickBestLine = (prov) => {
      // Some odds docs embed the line inside a single object; others require another GET.
      // ESPN Core often returns a direct resource with fields: overUnder, spread, awayTeamOdds, homeTeamOdds.
      const fields = ["overUnder", "spread", "awayTeamOdds", "homeTeamOdds"];
      const looksDirect = fields.every(f => f in prov);

      const toLine = (o) => {
        if (!o) return null;
        const ao = o.awayTeamOdds || {};
        const ho = o.homeTeamOdds || {};
        return {
          overUnder: +o.overUnder || null,
          awaySpread: Number.isFinite(+o.spread) ? +o.spread : (Number.isFinite(+ao.spread) ? +ao.spread : null),
          homeSpread: Number.isFinite(+o.spread) ? -(+o.spread) : (Number.isFinite(+ho.spread) ? +ho.spread : (Number.isFinite(+o.homeSpread) ? +o.homeSpread : null)),
          awayMoneyline: Number.isFinite(+ao.moneyLine) ? +ao.moneyLine : (Number.isFinite(+o.awayMoneyLine) ? +o.awayMoneyLine : null),
          homeMoneyline: Number.isFinite(+ho.moneyLine) ? +ho.moneyLine : (Number.isFinite(+o.homeMoneyLine) ? +o.homeMoneyLine : null),
        };
      };

      if (looksDirect) return toLine(prov);

      // Some items are URLs—follow once.
      if (prov.$ref) return axios.get(prov.$ref, { timeout: 8000 }).then(r => toLine(r.data)).catch(() => null);
      return null;
    };

    // Try ESPN BET first
    let candidates = providers;
    const espnBet = providers.find(p =>
      (p?.provider?.name || "").toUpperCase().includes("ESPN BET")
    );
    if (espnBet) candidates = [espnBet, ...providers.filter(p => p !== espnBet)];

    for (const p of candidates) {
      const line = await pickBestLine(p);
      if (line && (
        line.overUnder !== null ||
        line.awaySpread !== null || line.homeSpread !== null ||
        line.awayMoneyline !== null || line.homeMoneyline !== null
      )) {
        return line;
      }
    }
  } catch (e) {
    // swallow; we'll return null and leave cells blank
  }
  return null;
}

// Fetch the whole scoreboard to know per-game status + halftime score
async function fetchScoreboardMap() {
  const out = new Map(); // gameId -> { status, halfScore }
  try {
    const { data } = await axios.get(SCOREBOARD_URL(LEAGUE), { timeout: 12000 });
    const events = data?.events || [];
    for (const ev of events) {
      const id = String(ev?.id || "");
      if (!id) continue;
      const comp = ev?.competitions?.[0];
      const status = comp?.status?.type?.id || comp?.status?.type?.state || "";
      const competitors = comp?.competitors || [];
      // halftime/current score "A-B"
      const scores = competitors
        .sort((a, b) => (a.homeAway === "away" ? -1 : 1))
        .map(t => t?.score ?? "")
        .filter(s => s !== "");
      const halfScore = scores.length === 2 ? `${scores[0]}-${scores[1]}` : "";

      out.set(id, { status, halfScore });
    }
  } catch (e) {
    // If scoreboard fails, we still try odds; but half score might be blank
  }
  return out;
}

async function main() {
  const sheets = await getSheetsClient();

  // Pull the sheet values—we only need A (Game ID), D (Status), and L–Q row indices to write back
  // But we read A:Q once to find target rows and preserve indexing. Header is row 1.
  const readRange = `${TAB_NAME}!A:Q`;
  const readResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: readRange,
  });

  const rows = readResp.data.values || [];
  if (rows.length <= 1) {
    console.log("Nothing to process (no data rows).");
    return;
  }

  // Build scoreboard map once
  const sbMap = await fetchScoreboardMap();

  const updates = []; // each -> { range, values }
  // Iterate rows starting at rowIndex=2 (1-based)
  for (let i = 2; i <= rows.length; i++) {
    const r = rows[i - 1] || [];
    const gameId = (r[0] || "").trim(); // col A
    const status = (r[3] || "").trim(); // col D "Status"
    if (!gameId) continue;

    // We only fill when game is in-progress/halftime
    const sb = sbMap.get(gameId) || {};
    const espnLive = isHalftimeOrLive(sb.status);
    const statusLooksLive = /half|q\d|in-?progress|2nd|live/i.test(status);

    if (!espnLive && !statusLooksLive) continue;

    // Build write row: [L, M, N, O, P, Q]
    let L_halfScore = sb.halfScore || ""; // we prefer scoreboard half score
    let M_awaySpread = "";
    let N_awayML = "";
    let O_homeSpread = "";
    let P_homeML = "";
    let Q_total = "";

    // Fetch live odds for this game
    const line = await fetchLiveOddsForGame(gameId);
    if (line) {
      if (Number.isFinite(line.awaySpread)) M_awaySpread = line.awaySpread;
      if (Number.isFinite(line.homeSpread)) O_homeSpread = line.homeSpread;
      if (Number.isFinite(line.awayMoneyline)) N_awayML = line.awayMoneyline;
      if (Number.isFinite(line.homeMoneyline)) P_homeML = line.homeMoneyline;
      if (Number.isFinite(line.overUnder)) Q_total = line.overUnder;
    }

    // Queue the row update (USER_ENTERED so numbers render as numbers)
    updates.push({
      range: `${TAB_NAME}!${rangeLQ(i)}`,
      values: [[
        L_halfScore, M_awaySpread, N_awayML, O_homeSpread, P_homeML, Q_total
      ]],
    });

    // throttle a bit so we don't hammer ESPN / Sheets
    await sleep(150);
  }

  if (!updates.length) {
    console.log("No live/halftime rows to update.");
    return;
  }

  // Batch write
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: updates,
    },
  });

  console.log(`Updated ${updates.length} row(s) on ${TAB_NAME}.`);
}

main().catch(err => {
  console.error("Live updater fatal:", err?.message || err);
  process.exit(1);
});
