// live-game.mjs
// Halftime & live-odds updater for NFL / CFB tabs.
// Columns written: L (Half Score), M (Live Away Spread), N (Live Away ML),
// O (Live Home Spread), P (Live Home ML), Q (Live Total)

import axios from "axios";
import { google } from "googleapis";

// ---------- Config / Env ----------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE = (process.env.LEAGUE || "").toLowerCase(); // "nfl" | "college-football"
const TAB_NAME =
  process.env.TAB_NAME ||
  (LEAGUE === "nfl" ? "NFL" : LEAGUE === "college-football" ? "CFB" : "");
const GAME_ID_FILTER = process.env.GAME_ID?.trim() || "";

if (!SHEET_ID || !SA_JSON || !LEAGUE || !TAB_NAME) {
  console.error(
    "Missing one or more required env vars. Need GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, LEAGUE, TAB_NAME."
  );
  process.exit(1);
}

// ---------- Google Sheets Auth ----------
function getJWT() {
  const creds = JSON.parse(SA_JSON);
  return new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"],
    null
  );
}

const sheets = google.sheets({ version: "v4", auth: getJWT() });

// ---------- Helpers ----------
const A1 = {
  // Zero-based column indexes in the sheet for this project
  gameId: 0, // A
  date: 1, // B
  week: 2, // C
  status: 3, // D
  matchup: 4, // E
  finalScore: 5, // F

  // pregame odds (F..K were used previously)
  awaySpread: 6, // G
  awayML: 7, // H
  homeSpread: 8, // I
  homeML: 9, // J
  total: 10, // K

  // live/halftime (these are what we write)
  halfScore: 11, // L
  liveAwaySpread: 12, // M
  liveAwayML: 13, // N
  liveHomeSpread: 14, // O
  liveHomeML: 15, // P
  liveTotal: 16, // Q
};

// Pull all rows from the tab
async function readAllRows(tab) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${tab}!A:Q`,
    valueRenderOption: "UNFORMATTED_VALUE",
  });
  return res.data.values || [];
}

// Batch write specific cells (row-major)
async function batchWriteCells(tab, updates) {
  // updates: [{ rowIndex, colIndex, value }]
  if (!updates.length) return;

  const rowsMap = new Map(); // rowIndex -> array( up to Q ) to send

  for (const u of updates) {
    const key = u.rowIndex;
    if (!rowsMap.has(key)) rowsMap.set(key, []);
  }

  // Build row payloads only for the changed columns (L..Q live area)
  const dataPayload = [];
  for (const [rowIndex, _] of rowsMap) {
    // We'll compute the full L..Q set for this row from the updates
    const L_to_Q = new Array(6).fill(""); // L..Q are 6 columns
    const rowUpdates = updates.filter((x) => x.rowIndex === rowIndex);

    for (const u of rowUpdates) {
      const within = u.colIndex - A1.halfScore; // 11..16 -> 0..5
      if (within >= 0 && within < 6) {
        L_to_Q[within] =
          typeof u.value === "number" || typeof u.value === "string"
            ? u.value
            : "";
      }
    }

    dataPayload.push({
      range: `${tab}!L${rowIndex + 1}:Q${rowIndex + 1}`,
      values: [L_to_Q],
    });
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: dataPayload,
    },
  });
}

// ESPN odds fetchers
// For live odds we read the per-event markets endpoint. This is the same
// endpoint you had been using; we keep it flexible and tolerant to shape changes.
function eventOddsUrl(eventId) {
  // ESPN core markets (bookmaker filter helps stability)
  // NOTE: Do not change the path structure; this matched what you had working.
  return `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${eventId}/competitions/${eventId}/odds?region=us&lang=en&bookmakers=espn`;
}

// Some events expose “live” odds at the competition-level ‘markets’ too.
// Keep a second pass URL as a fallback to reduce 404s on some college games.
function fallbackMarketsUrl(eventId) {
  return `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${eventId}/competitions/${eventId}/markets?region=us&lang=en&bookmakers=espn`;
}

async function fetchWithFallback(urls) {
  for (const u of urls) {
    try {
      const res = await axios.get(u, { timeout: 8000 });
      if (res?.status === 200 && res?.data) return res.data;
    } catch (e) {
      if (e?.response?.status === 404) continue; // try next
      throw e; // real failure (network, 5xx), bubble up
    }
  }
  // if we get here, all were 404
  const err = new Error("All odds endpoints returned 404");
  err.code = 404;
  throw err;
}

// Normalize odds payloads that have a few different shapes depending on event / book
function pickFirstMarketNode(data) {
  // commonly data.items[] or data.market[] or direct object with teamOdds/current
  if (!data) return null;
  if (Array.isArray(data.items) && data.items.length) return data.items[0];
  if (Array.isArray(data.markets) && data.markets.length) return data.markets[0];
  // sometimes direct:
  return data;
}

function parseLiveOddsFromMarket(mk) {
  if (!mk) return null;

  // Different shapes seen across book feeds. We try a best-effort mapping.
  // The important part: FIX – avoid mixed || / ?? chains; use ?? only.
  const spreadAway = Number(
    mk.awayTeamOdds?.spread ??
      mk.awayTeamOdds?.current?.spread ??
      mk.outcomes?.find?.((o) => o?.type === "HANDICAP" && o?.away)?.price ??
      ""
  );

  const spreadHome = Number(
    mk.homeTeamOdds?.spread ??
      mk.homeTeamOdds?.current?.spread ??
      mk.outcomes?.find?.((o) => o?.type === "HANDICAP" && o?.home)?.price ??
      ""
  );

  const mlAway = Number(
    mk.awayTeamOdds?.moneyLine ??
      mk.awayTeamOdds?.current?.moneyLine ??
      mk.outcomes?.find?.((o) => o?.type === "MONEYLINE" && o?.away)?.price ??
      ""
  );

  const mlHome = Number(
    mk.homeTeamOdds?.moneyLine ??
      mk.homeTeamOdds?.current?.moneyLine ??
      mk.outcomes?.find?.((o) => o?.type === "MONEYLINE" && o?.home)?.price ??
      ""
  );

  const total = Number(
    mk.overUnder ??
      mk.current?.overUnder ??
      mk.outcomes?.find?.((o) => o?.type === "TOTAL")?.total ??
      ""
  );

  const hasAny = [spreadAway, spreadHome, mlAway, mlHome, total].some((v) =>
    Number.isFinite(v)
  );
  if (!hasAny) return null;

  return { spreadAway, spreadHome, mlAway, mlHome, total };
}

async function fetchLiveOdds(eventId) {
  const data = await fetchWithFallback([
    eventOddsUrl(eventId),
    fallbackMarketsUrl(eventId),
  ]);
  const mk = pickFirstMarketNode(data);
  const odds = parseLiveOddsFromMarket(mk);
  return odds || null;
}

// Fetch game scoreboard for half score & status (stable across leagues)
function eventSummaryUrl(eventId) {
  return `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${eventId}`;
}

async function fetchHalftimeScore(eventId) {
  try {
    const res = await axios.get(eventSummaryUrl(eventId), { timeout: 8000 });
    const box = res?.data?.boxscore;
    const statusType = res?.data?.header?.competitions?.[0]?.status?.type;
    const isAtHalf =
      statusType?.name?.toLowerCase() === "status_halftime" ||
      /halftime/i.test(statusType?.detail || "");

    // Half score format: "<away>-<home>" at halftime
    let halfScore = "";
    if (box?.teams && Array.isArray(box.teams) && isAtHalf) {
      const away = box.teams.find((t) => t.homeAway === "away");
      const home = box.teams.find((t) => t.homeAway === "home");
      const awayScore = away?.score ?? "";
      const homeScore = home?.score ?? "";
      if (awayScore !== "" && homeScore !== "") {
        halfScore = `${awayScore}-${homeScore}`;
      }
    }
    return { isAtHalf, halfScore };
  } catch {
    return { isAtHalf: false, halfScore: "" };
  }
}

// ---------- Main ----------
async function main() {
  const rows = await readAllRows(TAB_NAME);
  if (!rows.length) {
    console.log("No rows found.");
    return;
  }

  // figure out candidates
  const header = rows[0];
  const updates = [];

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const gameId = String(row[A1.gameId] ?? "").trim();
    const status = String(row[A1.status] ?? "").trim();

    if (!gameId) continue;

    // If GAME_ID filter set, skip others
    if (GAME_ID_FILTER && gameId !== GAME_ID_FILTER) continue;

    // Eligible rows: halftime (contains "Half"), "LIVE", or generic "In Progress"
    const isCandidate =
      /half/i.test(status) ||
      /\blive\b/i.test(status) ||
      /in\s*progress/i.test(status);

    if (!isCandidate && !GAME_ID_FILTER) continue;

    // Try halftime score
    const { isAtHalf, halfScore } = await fetchHalftimeScore(gameId);

    // Try live odds
    let live = null;
    try {
      live = await fetchLiveOdds(gameId);
    } catch (e) {
      if (e?.code === 404) {
        // no live markets; still write halftime if we have it
      } else {
        // transient issues; skip odds update but still allow halftime score
        console.warn(`Live odds fetch failed for ${gameId}:`, e?.message || e);
      }
    }

    // Populate updates for this row’s L..Q
    if (isAtHalf || GAME_ID_FILTER) {
      if (halfScore) {
        updates.push({
          rowIndex: r + 1, // Google Sheets is 1-based in API writes
          colIndex: A1.halfScore,
          value: halfScore,
        });
      }

      if (live) {
        updates.push(
          { rowIndex: r + 1, colIndex: A1.liveAwaySpread, value: live.spreadAway ?? "" },
          { rowIndex: r + 1, colIndex: A1.liveAwayML, value: live.mlAway ?? "" },
          { rowIndex: r + 1, colIndex: A1.liveHomeSpread, value: live.spreadHome ?? "" },
          { rowIndex: r + 1, colIndex: A1.liveHomeML, value: live.mlHome ?? "" },
          { rowIndex: r + 1, colIndex: A1.liveTotal, value: live.total ?? "" },
        );
      }
    }
  }

  if (!updates.length) {
    console.log(
      `[${new Date().toISOString()}] Nothing to update (no live/half rows or no markets).`
    );
    return;
  }

  await batchWriteCells(TAB_NAME, updates);
  console.log(
    `[${new Date().toISOString()}] Updated ${new Set(
      updates.map((u) => u.rowIndex)
    ).size} row(s).`
  );
}

main().catch((err) => {
  const code = err?.code || err?.response?.status || "unknown";
  console.error(`Live updater fatal: *** code: ${code} ***`);
  console.error(err?.message || err);
  process.exit(1);
});
