// live-game.mjs
// Node 20+ ESM

import axios from "axios";
import { google } from "googleapis";

// ---------- ENV & CONFIG ----------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SVC_JSON = process.env.GOOGLE_SERVICE_ACCOUNT; // service account JSON (whole JSON string)
const LEAGUE = (process.env.LEAGUE || "").toLowerCase(); // "nfl" or "college-football"
const TAB_NAME = process.env.TAB_NAME || "";            // "NFL" or "CFB"

function hardFail(msg) {
  console.error(msg);
  process.exit(1);
}

if (!SHEET_ID || !SVC_JSON || !LEAGUE || !TAB_NAME) {
  hardFail("Missing one or more required env vars. Needed: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, LEAGUE, TAB_NAME");
}

if (!["nfl", "college-football"].includes(LEAGUE)) {
  hardFail(`LEAGUE must be "nfl" or "college-football" (got "${LEAGUE}")`);
}

// ESPN paths
const ESPN_PATH = LEAGUE === "nfl" ? "nfl" : "college-football";

// ---------- GOOGLE SHEETS ----------
async function getSheetsClient() {
  let svc;
  try {
    svc = JSON.parse(SVC_JSON);
  } catch (err) {
    hardFail("GOOGLE_SERVICE_ACCOUNT is not valid JSON.");
  }

  const jwt = new google.auth.JWT({
    email: svc.client_email,
    key: svc.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth: jwt });
  return sheets;
}

// Read all Game IDs (column A) so we can map ESPN id -> row index
async function getGameIdToRowMap(sheets) {
  // A2:A because row 1 is header
  const range = `${TAB_NAME}!A2:A`;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range,
    majorDimension: "ROWS",
  });

  const map = new Map();
  const rows = resp.data.values || [];
  rows.forEach((r, idx) => {
    const gameId = (r && r[0]) ? String(r[0]).trim() : "";
    if (gameId) {
      // Row index on sheet = header row (1) + starting offset (1) + idx
      //  -> A2 is index 2, so rowNumber = 2 + idx
      const rowNumber = 2 + idx;
      map.set(gameId, rowNumber);
    }
  });
  return map;
}

// ---------- ESPN FETCH ----------
function isoDateYYYYMMDD() {
  const d = new Date();
  // ESPN uses local date for scoreboard; UTC midnight can cross over.
  // Use UTC date string to be consistent with Actions runners.
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

async function fetchScoreboard() {
  const date = isoDateYYYYMMDD();
  const url = `https://site.api.espn.com/apis/v2/sports/football/${ESPN_PATH}/scoreboard?dates=${date}`;
  const { data } = await axios.get(url, { timeout: 15000 });

  const events = data?.events || [];
  return events;
}

function isHalftime(competition) {
  // Robust halftime detection
  const status = competition?.status?.type || {};
  const detail = (status?.detail || "").toLowerCase(); // e.g., "Halftime"
  const desc = (status?.description || "").toLowerCase(); // sometimes "Halftime"
  const state = (status?.state || "").toLowerCase(); // "in" / "post" / "pre"
  const period = competition?.status?.period;

  if (detail.includes("halftime") || desc.includes("halftime")) return true;

  // Occasionally ESPN marks halftime as state="in", period=2, clock empty/00:00
  if (state === "in" && (period === 2 || period === "2")) {
    return true;
  }

  return false;
}

// Pick a usable odds object. Prefer ESPN BET if present; else first odds entry.
function pickOdds(competition) {
  const oddsArr = competition?.odds || [];
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;

  let espnBet = oddsArr.find(o => (o?.provider?.name || "").toLowerCase().includes("espn bet"));
  if (!espnBet) espnBet = oddsArr.find(o => (o?.provider?.name || "").toLowerCase().includes("espnbet"));
  return espnBet || oddsArr[0];
}

// Pull moneylines/spreads from odds. ESPN structures vary slightly between leagues/providers.
function extractLines(competition) {
  const odds = pickOdds(competition);
  if (!odds) return null;

  // Many providers put spread as "spread" (home is minus if favored) and "overUnder".
  // Moneyline often in odds.detailsTeams[...] or awayTeamOdds/homeTeamOdds (older shape).
  const overUnder = odds.overUnder ?? odds.total ?? null;

  // Spread – ESPN usually exposes as "spread" from the favorite POV (negative = favorite).
  // We want both Away and Home spread values (+/-).
  // The competitor order: competition.competitors: [{homeAway:"home", score}, {homeAway:"away", score}...]
  const comp = competition?.competitors || [];
  const home = comp.find(t => t?.homeAway === "home");
  const away = comp.find(t => t?.homeAway === "away");
  const favorite = odds.details?.favorite ?? odds.favorite ?? null; // teamId or name sometimes
  let line = odds.spread ?? odds.line ?? null;

  // Moneyline can be in multiple shapes
  let awayML = null;
  let homeML = null;

  if (odds.awayTeamOdds && odds.homeTeamOdds) {
    awayML = odds.awayTeamOdds.moneyLine ?? null;
    homeML = odds.homeTeamOdds.moneyLine ?? null;
  } else if (Array.isArray(odds.detailsTeams)) {
    for (const t of odds.detailsTeams) {
      if (!t || !t.team) continue;
      if (String(t.team?.id) === String(away?.id) || t.team?.abbreviation === away?.abbreviation || t.team?.name === away?.team?.displayName) {
        awayML = t.moneyLine ?? awayML;
      }
      if (String(t.team?.id) === String(home?.id) || t.team?.abbreviation === home?.abbreviation || t.team?.name === home?.team?.displayName) {
        homeML = t.moneyLine ?? homeML;
      }
    }
  } else if (typeof odds.details === "string") {
    // Occasionally "details" is human text like "MIA -13.5 o49.5"
    // We won't parse ML from this reliably — skip.
  }

  // Build away/home spread.
  // If we have a numeric "line" (e.g., -3.5 and favorite=home), map to away/home values.
  let awaySpread = null;
  let homeSpread = null;

  if (typeof line === "number") {
    // If favorite is the home team, home spread is negative of abs(line).
    // Else, away is negative.
    const favIsHome =
      favorite &&
      (String(favorite) === String(home?.id) ||
        favorite === home?.team?.abbreviation ||
        favorite === home?.team?.displayName ||
        favorite === home?.name);

    if (favIsHome === true) {
      homeSpread = -Math.abs(line);
      awaySpread = Math.abs(line);
    } else {
      awaySpread = -Math.abs(line);
      homeSpread = Math.abs(line);
    }
  } else if (typeof odds?.homeSpread === "number" && typeof odds?.awaySpread === "number") {
    homeSpread = odds.homeSpread;
    awaySpread = odds.awaySpread;
  }

  return {
    awaySpread: isFinite(awaySpread) ? Number(awaySpread) : null,
    awayML: isFinite(awayML) ? Number(awayML) : (typeof awayML === "string" ? Number(awayML) : null),
    homeSpread: isFinite(homeSpread) ? Number(homeSpread) : null,
    homeML: isFinite(homeML) ? Number(homeML) : (typeof homeML === "string" ? Number(homeML) : null),
    total: isFinite(overUnder) ? Number(overUnder) : null,
  };
}

// Half score "A-B" using current score snapshot
function halfScore(competition) {
  const comps = competition?.competitors || [];
  const away = comps.find(t => t.homeAway === "away");
  const home = comps.find(t => t.homeAway === "home");
  const a = away?.score != null ? String(away.score) : "";
  const h = home?.score != null ? String(home.score) : "";
  if (!a || !h) return "";
  return `${a}-${h}`;
}

// ---------- MAIN ----------
async function run() {
  const sheets = await getSheetsClient();
  const idToRow = await getGameIdToRowMap(sheets);
  const events = await fetchScoreboard();

  // Build a batchUpdate request for the halftime rows only
  const requests = [];

  for (const ev of events) {
    const gameId = String(ev?.id || "").trim();
    if (!gameId || !idToRow.has(gameId)) continue;

    const comp = ev?.competitions?.[0];
    if (!comp) continue;

    // Only write when it's actually halftime
    if (!isHalftime(comp)) continue;

    const row = idToRow.get(gameId); // 1-based row number
    const L_col = 12; // "L" zero-indexed for updateCells later
    const rangeA1 = `${TAB_NAME}!L${row}:Q${row}`;

    // Extract half score + lines
    const score = halfScore(comp);
    const lines = extractLines(comp) || {};

    // Arrange values in order: L..Q
    const values = [
      [
        score || "",
        isFinite(lines.awaySpread) ? lines.awaySpread : "",
        isFinite(lines.awayML) ? lines.awayML : "",
        isFinite(lines.homeSpread) ? lines.homeSpread : "",
        isFinite(lines.homeML) ? lines.homeML : "",
        isFinite(lines.total) ? lines.total : "",
      ],
    ];

    requests.push({
      updateCells: {
        range: {
          sheetId: undefined, // will use A1 via values.update
        },
        rows: [], // left empty deliberately; we’ll use values.update for simplicity
        fields: "userEnteredValue",
      },
    });

    // Use values.update (easier than updateCells for row values)
    requests.pop(); // remove the placeholder
    requests.push({
      updateCells: {
        range: {}, // placeholder to keep shapes similar when batching with other ops
        rows: [],
        fields: "userEnteredValue",
      },
    });

    // But better: just push a values.update request via the separate API call block we’ll construct below.
    // We'll collect them and do one sheets.values.batchUpdate instead of batchUpdate for these row writes.
    // To keep this simple inside one call, we’ll collect `data` and call values.batchUpdate after the loop.
    // (See below.)
  }

  // If nothing to write, bail gracefully
  if (requests.length === 0) {
    console.log("No halftime rows to update.");
    return;
  }

  // Instead of batchUpdate (updateCells), use values.batchUpdate with A1 ranges — safer & simpler.
  // Re-scan to build a concise data array.
  const data = [];
  for (const ev of events) {
    const gameId = String(ev?.id || "").trim();
    if (!gameId || !idToRow.has(gameId)) continue;
    const comp = ev?.competitions?.[0];
    if (!comp) continue;
    if (!isHalftime(comp)) continue;

    const row = idToRow.get(gameId);
    const score = halfScore(comp);
    const lines = extractLines(comp) || {};

    data.push({
      range: `${TAB_NAME}!L${row}:Q${row}`,
      values: [[
        score || "",
        isFinite(lines.awaySpread) ? lines.awaySpread : "",
        isFinite(lines.awayML) ? lines.awayML : "",
        isFinite(lines.homeSpread) ? lines.homeSpread : "",
        isFinite(lines.homeML) ? lines.homeML : "",
        isFinite(lines.total) ? lines.total : "",
      ]],
    });
  }

  if (data.length === 0) {
    console.log("No halftime rows to update.");
    return;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data,
    },
  });

  console.log(`Updated ${data.length} halftime row(s) on tab "${TAB_NAME}".`);
}

run().catch((err) => {
  console.error("Live updater fatal:", err?.response?.data || err);
  process.exit(1);
});
