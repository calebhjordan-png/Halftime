// live-game.mjs
// ──────────────────────────────────────────────────────────────────────────────
// What it does (every run):
// 1) Download today's ESPN scoreboard for the league (NFL or CFB).
// 2) For each row in the Sheet tab (A: Game ID), if the game is on the board:
//    - ALWAYS update Status (col D) with ESPN short detail (e.g., "Q2 10:53", "Halftime").
//    - IF the state is IN or HALFTIME, also write Half Score + live odds to L..Q.
// 3) Leaves odds blank when there is no live market yet (but Status still updates).
//
// Env needed (all are already used everywhere else in your repo):
//   GOOGLE_SHEET_ID
//   GOOGLE_SERVICE_ACCOUNT  (full JSON of the service account; same secret as other jobs)
//   LEAGUE                  ("nfl" or "college-football")
//   TAB_NAME                ("NFL" or "CFB")
//
// Columns used in the tab:
//   A: Game ID  | B: Date | C: Week | D: Status | E: Matchup | ... | L: Half Score | M..Q live odds
//
// Dependencies: axios, googleapis
// ──────────────────────────────────────────────────────────────────────────────

import axios from "axios";
import { google } from "googleapis";

// ────────── Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function reqEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env: ${name}`);
  return v;
}

function toSafeNumber(v) {
  if (v === null || v === undefined || v === "") return "";
  const n = Number(v);
  return Number.isFinite(n) ? n : "";
}

function pickOdds(oddsArray) {
  if (!Array.isArray(oddsArray) || oddsArray.length === 0) return null;

  // Prefer live odds first
  const preferredBooks = [
    "ESPN BET",
    "Caesars",
    "DraftKings",
    "FanDuel",
    "BetMGM",
    "William Hill",
  ];

  // 1) filter anything that looks live/in play
  const live = oddsArray.filter(
    o =>
      o?.live === true ||
      o?.inPlay === true ||
      o?.details?.toLowerCase?.().includes("live")
  );

  // 2) choose by book priority
  const byBook = (list) => {
    for (const name of preferredBooks) {
      const m = list.find(o => (o?.provider?.name || o?.provider?.displayName || "").includes(name));
      if (m) return m;
    }
    return list[0];
  };

  const chosen = byBook(live.length ? live : oddsArray);

  // Normalize the common ESPN odds shape
  // Some feeds include:
  //   chosen.spread, chosen.overUnder
  //   chosen.awayTeamOdds.moneyLine, chosen.homeTeamOdds.moneyLine
  //   chosen.awayTeamOdds.spreadOdds, chosen.homeTeamOdds.spreadOdds
  const awayOdds = chosen?.awayTeamOdds || {};
  const homeOdds = chosen?.homeTeamOdds || {};

  const spread = chosen?.spread ?? chosen?.pointSpread ?? "";
  const ou = chosen?.overUnder ?? chosen?.total ?? "";

  // Moneylines
  const awayML =
    awayOdds?.moneyLine ?? awayOdds?.moneyline ?? chosen?.awayMoneyLine ?? "";
  const homeML =
    homeOdds?.moneyLine ?? homeOdds?.moneyline ?? chosen?.homeMoneyLine ?? "";

  // Spreads: if one spread is provided on the market, sign to away/home by favorite flag if available
  let awaySpread = "";
  let homeSpread = "";

  if (spread !== "" && spread !== null && spread !== undefined) {
    // If away is favorite, then awaySpread should be negative spread
    const awayFav = !!awayOdds?.favorite;
    const homeFav = !!homeOdds?.favorite;

    if (awayFav && !homeFav) {
      awaySpread = -Math.abs(Number(spread));
      homeSpread = Math.abs(Number(spread));
    } else if (homeFav && !awayFav) {
      awaySpread = Math.abs(Number(spread));
      homeSpread = -Math.abs(Number(spread));
    } else {
      // No favorite flag, try team-specific spreads if present
      awaySpread =
        awayOdds?.spread ?? awayOdds?.spreadOdds ?? awayOdds?.handicap ?? "";
      homeSpread =
        homeOdds?.spread ?? homeOdds?.spreadOdds ?? homeOdds?.handicap ?? "";

      if (awaySpread === "" && homeSpread === "") {
        // fallback: keep market spread but assign away negative by default
        awaySpread = -Math.abs(Number(spread));
        homeSpread = Math.abs(Number(spread));
      }
    }
  } else {
    // maybe team odds carry the handicaps
    awaySpread =
      awayOdds?.spread ?? awayOdds?.spreadOdds ?? awayOdds?.handicap ?? "";
    homeSpread =
      homeOdds?.spread ?? homeOdds?.spreadOdds ?? homeOdds?.handicap ?? "";
  }

  return {
    awaySpread: toSafeNumber(awaySpread),
    homeSpread: toSafeNumber(homeSpread),
    awayML: toSafeNumber(awayML),
    homeML: toSafeNumber(homeML),
    total: toSafeNumber(ou),
  };
}

function buildHalfScore(comp) {
  try {
    const c = Array.isArray(comp) ? comp[0] : comp; // competitions[0]
    const teams = c?.competitors || [];
    const away = teams.find(t => (t?.homeAway || "").toLowerCase() === "away");
    const home = teams.find(t => (t?.homeAway || "").toLowerCase() === "home");

    const sumFirstTwo = (lines) =>
      (Array.isArray(lines) ? lines.slice(0, 2) : [])
        .map(x => Number(x?.value ?? x ?? 0))
        .reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);

    const awayHalf = sumFirstTwo(away?.linescores);
    const homeHalf = sumFirstTwo(home?.linescores);

    if (Number.isFinite(awayHalf) && Number.isFinite(homeHalf)) {
      return `${awayHalf}-${homeHalf}`;
    }
    return "";
  } catch {
    return "";
  }
}

function statusString(event) {
  const c = event?.competitions?.[0];
  const s = (c?.status || event?.status);
  const st = s?.type || {};
  const state = (st?.state || "").toUpperCase(); // PRE / IN / POST
  let text = st?.shortDetail || st?.detail || s?.displayClock || "";

  // Normalize common cases
  if (state === "IN" && /HALF/i.test(text)) text = "Halftime";
  if (state === "POST") text = "Final";
  return { state, text: text || "" };
}

// ────────── Main
(async () => {
  const SHEET_ID = reqEnv("GOOGLE_SHEET_ID");
  const TAB = reqEnv("TAB_NAME");
  const LEAGUE = reqEnv("LEAGUE"); // "nfl" | "college-football"

  // Auth to Sheets
  const svc = JSON.parse(reqEnv("GOOGLE_SERVICE_ACCOUNT"));
  const jwt = new google.auth.JWT({
    email: svc.client_email,
    key: svc.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth: jwt });

  // Get today's ESPN board
  const boardUrl = `https://site.api.espn.com/apis/v2/sports/football/${LEAGUE}/scoreboard`;
  const { data: board } = await axios.get(boardUrl, { timeout: 15000 });

  const events = Array.isArray(board?.events) ? board.events : [];
  const byId = new Map(events.map(ev => [String(ev?.id || ""), ev]));

  if (byId.size === 0) {
    console.log("[live] Scoreboard empty; nothing to do.");
    return;
  }

  // Read the Sheet rows we care about: A (id) through D (status)
  const rangeRead = `${TAB}!A2:D`;
  const readRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: rangeRead,
    valueRenderOption: "UNFORMATTED_VALUE",
  });

  const rows = readRes.data.values || [];
  if (rows.length === 0) {
    console.log("[live] No rows to check.");
    return;
  }

  const valueUpdates = []; // each element: {range, values}

  // Process each sheet row
  for (let r = 0; r < rows.length; r++) {
    const excelRow = r + 2; // 1-based + header row
    const [gameIdRaw] = rows[r];
    const gameId = String(gameIdRaw || "").trim();
    if (!gameId) continue;

    const ev = byId.get(gameId);
    if (!ev) continue; // not on today's board

    const { state, text } = statusString(ev);

    // 1) ALWAYS write Status (col D)
    valueUpdates.push({
      range: `${TAB}!D${excelRow}`,
      values: [[text]],
    });

    // 2) If IN or HALFTIME, write L..Q
    if (state === "IN" || state === "HALFTIME") {
      const comp = ev?.competitions?.[0] || {};
      const odds = pickOdds(comp?.odds || ev?.odds || []);
      const half = buildHalfScore(ev?.competitions);

      const outRow = [
        half,                       // L: Half Score
        odds?.awaySpread ?? "",     // M: Live Away Spread
        odds?.awayML ?? "",         // N: Live Away ML
        odds?.homeSpread ?? "",     // O: Live Home Spread
        odds?.homeML ?? "",         // P: Live Home ML
        odds?.total ?? "",          // Q: Live Total
      ];

      valueUpdates.push({
        range: `${TAB}!L${excelRow}:Q${excelRow}`,
        values: [outRow],
      });
    }
  }

  if (valueUpdates.length === 0) {
    console.log("[live] Nothing to update (no live rows or no matches).");
    return;
  }

  // Batch write
  // Break into chunks of ~100 to stay comfy with API limits
  const chunkSize = 100;
  for (let i = 0; i < valueUpdates.length; i += chunkSize) {
    const chunk = valueUpdates.slice(i, i + chunkSize);
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: "USER_ENTERED",
        data: chunk,
      },
    });
    await sleep(250); // very small throttle
  }

  console.log(`[live] Updated ${valueUpdates.length} range(s).`);
})().catch((err) => {
  // Make the error visible in Actions logs
  const msg = (err?.response?.status)
    ? `Live updater fatal: *** code: ${err.response.status} ***`
    : `Live updater fatal: ${err?.message || err}`;
  console.error(msg);
  process.exit(1);
});
