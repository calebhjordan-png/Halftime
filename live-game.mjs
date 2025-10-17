// live-game.mjs
import axios from "axios";
import { google } from "googleapis";

/* ----------------------- env & google auth ----------------------- */
function reqEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing one or more required env vars. (${name})`);
    process.exit(1);
  }
  return v;
}

const SHEET_ID = reqEnv("GOOGLE_SHEET_ID");
const SA_JSON = JSON.parse(reqEnv("GOOGLE_SERVICE_ACCOUNT"));
const LEAGUE = reqEnv("LEAGUE");               // "nfl" or "college-football"
const TAB_NAME = reqEnv("TAB_NAME");           // "NFL" or "CFB"
const MANUAL_GAME_ID = (process.env.GAME_ID || "").trim();  // optional tiny patch

const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
const jwt = new google.auth.JWT(
  SA_JSON.client_email,
  null,
  SA_JSON.private_key,
  scopes
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ----------------------- helpers ----------------------- */
const COLS = {
  GAME_ID: 0,
  DATE: 1,
  WEEK: 2,
  STATUS: 3,
  MATCHUP: 4,
  FINAL_SCORE: 5,
  AWAY_SPREAD: 6,
  AWAY_ML: 7,
  HOME_SPREAD: 8,
  HOME_ML: 9,
  TOTAL: 10,

  HALF_SCORE: 11,  // L
  LIVE_AWAY_SPREAD: 12, // M
  LIVE_AWAY_ML: 13,     // N
  LIVE_HOME_SPREAD: 14, // O
  LIVE_HOME_ML: 15,     // P
  LIVE_TOTAL: 16        // Q
};

const ESPN_SPORT = LEAGUE === "nfl" ? "nfl" : "college-football";

/**
 * ESPN game summary endpoint
 * ex: https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=401326625
 */
async function fetchSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${ESPN_SPORT}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  return data;
}

/**
 * Best-effort odds fetch.
 * Try competition odds endpoint (may 404), then fall back to scoreboard item odds if needed.
 * Returns { spreadAway, spreadHome, mlAway, mlHome, total } or null if not available.
 */
async function fetchLiveOdds(gameId) {
  // v2 odds (may 404 if market not up yet)
  const oddsUrl = `https://sports.core.api.espn.com/v2/sports/football/${ESPN_SPORT}/events/${gameId}/competitions/${gameId}/odds`;
  try {
    const { data } = await axios.get(oddsUrl, { timeout: 10000, validateStatus: () => true });
    if (data && data.items && data.items.length > 0) {
      // pick first active “line” in items
      const first = data.items[0];
      // Safeguard value extraction
      const mk = await axios.get(first.$ref || first.href || oddsUrl, { timeout: 10000, validateStatus: () => true }).then(r => r.data).catch(() => null);
      if (mk) {
        // different shapes exist; try to map robustly
        const spreadAway = Number(mk.awayTeamOdds?.spread || mk.awayTeamOdds?.current?.spread ?? "");
        const spreadHome = Number(mk.homeTeamOdds?.spread || mk.homeTeamOdds?.current?.spread ?? "");
        const mlAway = Number(mk.awayTeamOdds?.moneyLine || mk.awayTeamOdds?.current?.moneyLine ?? "");
        const mlHome = Number(mk.homeTeamOdds?.moneyLine || mk.homeTeamOdds?.current?.moneyLine ?? "");
        const total = Number(mk.overUnder || mk.current?.overUnder ?? "");

        const hasAny =
          [spreadAway, spreadHome, mlAway, mlHome, total].some(v => Number.isFinite(v));
        if (hasAny) {
          return { spreadAway, spreadHome, mlAway, mlHome, total };
        }
      }
    }
  } catch (err) {
    if (axios.isAxiosError(err) && err.response?.status === 404) {
      // graceful skip
      return null;
    }
    throw err; // other errors bubble up
  }

  // If we got here: no usable odds found
  return null;
}

function parseHalftimeFromSummary(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const st = comp?.status?.type || {};
    const desc = String(st?.description || st?.detail || "").toLowerCase();
    return desc.includes("halftime") || desc === "half";
  } catch {
    return false;
  }
}

function currentScoreFromSummary(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const a = comp?.competitors?.find(c => c?.homeAway === "away");
    const h = comp?.competitors?.find(c => c?.homeAway === "home");
    const as = Number(a?.score ?? 0);
    const hs = Number(h?.score ?? 0);
    if (Number.isFinite(as) && Number.isFinite(hs)) {
      return `${as}-${hs}`;
    }
  } catch {}
  return "";
}

/* ----------------------- sheets I/O ----------------------- */
async function readRows() {
  // Read everything we need up through column Q
  const range = `${TAB_NAME}!A2:Q`;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });
  return resp.data.values || [];
}

function makeRowUpdate(rowIndex0, columns) {
  // rowIndex0 is zero-based from the A2 origin
  // Build a single row update starting at column L (HALF_SCORE) through Q
  const startCol = COLS.HALF_SCORE; // 11
  const values = new Array(6).fill(""); // L..Q (6 cells)
  values[0] = columns.halfScore ?? "";
  values[1] = isFiniteNumber(columns.liveAwaySpread) ? Number(columns.liveAwaySpread) : "";
  values[2] = isFiniteNumber(columns.liveAwayML) ? Number(columns.liveAwayML) : "";
  values[3] = isFiniteNumber(columns.liveHomeSpread) ? Number(columns.liveHomeSpread) : "";
  values[4] = isFiniteNumber(columns.liveHomeML) ? Number(columns.liveHomeML) : "";
  values[5] = isFiniteNumber(columns.liveTotal) ? Number(columns.liveTotal) : "";

  return {
    range: `${TAB_NAME}!L${rowIndex0 + 2}:Q${rowIndex0 + 2}`,
    values: [values]
  };
}
const isFiniteNumber = (v) => typeof v === "number" && Number.isFinite(v);

async function writeBatch(updates, statusPatches) {
  const data = [];

  // Value updates L..Q
  for (const u of updates) {
    data.push({ range: u.range, values: u.values });
  }

  // Status patches (column D)
  for (const s of statusPatches) {
    data.push({
      range: `${TAB_NAME}!D${s.row + 2}:D${s.row + 2}`,
      values: [[s.value]]
    });
  }

  if (data.length === 0) return;

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data
    }
  });
}

/* ----------------------- main ----------------------- */
async function main() {
  try {
    const rows = await readRows();
    if (!rows.length) {
      console.log(`[${new Date().toISOString()}] Nothing to update (no rows).`);
      return;
    }

    const updates = [];
    const statusPatches = [];

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || [];
      const gameId = String(r[COLS.GAME_ID] || "").trim();
      if (!gameId) continue;

      // tiny patch: when a manual GAME_ID is provided, only work that row
      if (MANUAL_GAME_ID && MANUAL_GAME_ID !== gameId) continue;

      // Only consider games that are likely live / halftime candidates
      const status = String(r[COLS.STATUS] || "").toLowerCase();

      // We allow:
      // - explicitly "half", "halftime"
      // - “in progress” strings (script will check actual halftime via API)
      const looksLive =
        status.includes("half") ||
        status.includes("in") ||
        status.includes("q") ||
        status.includes("pm") || // prefilled schedule might still be the kickoff time
        status.includes("live");

      if (!looksLive && !MANUAL_GAME_ID) continue;

      // Pull ESPN summary (scores & halftime)
      let summary;
      try {
        summary = await fetchSummary(gameId);
      } catch (err) {
        const code = axios.isAxiosError(err) ? err.response?.status : undefined;
        console.log(`Summary fetch failed for ${gameId} (code=${code ?? "?"}). Skipping row ${i + 2}.`);
        continue;
      }

      const isHalf = parseHalftimeFromSummary(summary);
      const halfScore = currentScoreFromSummary(summary);

      // Pull live odds (404-safe)
      let odds = null;
      try {
        odds = await fetchLiveOdds(gameId);
      } catch (err) {
        const code = axios.isAxiosError(err) ? err.response?.status : undefined;
        console.log(`Odds fetch error for ${gameId} (code=${code ?? "?"}). Treating as no market.`);
      }

      // If we have neither halftime nor odds and it wasn't a manual override, skip
      if (!isHalf && !odds && !MANUAL_GAME_ID) continue;

      // prepare values for L..Q
      const patch = {
        halfScore: halfScore || "",
        liveAwaySpread: odds?.spreadAway,
        liveAwayML: odds?.mlAway,
        liveHomeSpread: odds?.spreadHome,
        liveHomeML: odds?.mlHome,
        liveTotal: odds?.total
      };
      updates.push(makeRowUpdate(i, patch));

      // also patch status to "Half" if detected
      if (isHalf && status !== "half" && status !== "halftime") {
        statusPatches.push({ row: i, value: "Half" });
      }
    }

    if (!updates.length && !statusPatches.length) {
      console.log(`[${new Date().toISOString()}] Nothing to update (no halftime/odds found).`);
      return;
    }

    await writeBatch(updates, statusPatches);
    console.log(`[${new Date().toISOString()}] Updated ${updates.length} row(s)${statusPatches.length ? `, set ${statusPatches.length} status(es) to Half` : ""}.`);
  } catch (err) {
    const code = axios.isAxiosError(err) ? err.response?.status : undefined;
    console.error(`Live updater fatal: *** code: ${code ?? "n/a"} ***`);
    console.error(err?.stack || err?.message || String(err));
    process.exit(1);
  }
}

main();
