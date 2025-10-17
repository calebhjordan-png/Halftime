// live-game.mjs
// Updates only: Status (D), Half Score (L), Live odds (M..Q).
// Leaves pregame columns untouched. Supports optional GAME_ID focus.
// Prefers halftime markets (2H / Second Half) with fallback to generic "live" markets.

import axios from "axios";
import { google } from "googleapis";

// ===== ENV =====
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",              // optional: force a single game
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live", // order we try
} = process.env;

const REQUIRED = ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"];
for (const k of REQUIRED) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

// ===== Google Sheets =====
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

// ===== Helpers =====
function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
const todayKey = (() => {
  // Sheet uses MM/DD/YY; build UTC today in that format
  const d = new Date();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const yy = String(d.getUTCFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
})();

function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}

function sumFirstTwoPeriods(linescores) {
  if (!Array.isArray(linescores) || linescores.length === 0) return null;
  const take = linescores.slice(0, 2);
  let tot = 0;
  for (const p of take) {
    const v = Number(p?.value ?? p?.score ?? 0);
    if (!Number.isFinite(v)) return null;
    tot += v;
  }
  return tot;
}

function parseHalfScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const hHome = sumFirstTwoPeriods(home?.linescores);
    const hAway = sumFirstTwoPeriods(away?.linescores);
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) {
      return `${hAway}-${hHome}`; // away-first
    }
  } catch {}
  return "";
}

async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

async function espnOdds(gameId) {
  const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${gameId}/competitions/${gameId}/odds`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

function pickMarket(markets, preferenceList) {
  if (!Array.isArray(markets) || markets.length === 0) return null;

  const norm = s => (s || "").toLowerCase();
  const wants = preferenceList
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);

  // Try preferred phrases against name/displayName/period text
  for (const want of wants) {
    const m = markets.find(mk => {
      const a = norm(mk?.name);
      const b = norm(mk?.displayName);
      const c = norm(mk?.period?.displayName || mk?.period?.abbreviation || "");
      return a.includes(want) || b.includes(want) || c.includes(want);
    });
    if (m) return m;
  }

  // Fallback: any spread/total-ish market
  const fallback = markets.find(mk =>
    norm(mk?.name).includes("spread") ||
    norm(mk?.displayName).includes("spread") ||
    norm(mk?.name).includes("total") ||
    norm(mk?.displayName).includes("total") ||
    norm(mk?.name).includes("line") ||
    norm(mk?.displayName).includes("line")
  );
  return fallback || markets[0];
}

function extractLiveNumbers(oddsPayload, preference = MARKET_PREFERENCE) {
  try {
    const markets = oddsPayload?.items || [];
    if (markets.length === 0) return undefined;

    // We look for two buckets: spread and total. Each via preferred market first.
    const prefList = preference || "2H,Second Half,Halftime,Live";

    const choose = (typeWords) => {
      // Try to pick a market that includes the type word (spread/total)
      const typed = markets.filter(mk => {
        const s = (mk?.name || mk?.displayName || "").toLowerCase();
        return typeWords.some(w => s.includes(w));
      });
      return pickMarket(typed.length ? typed : markets, prefList);
    };

    const mSpread = choose(["spread", "line"]);
    const mTotal  = choose(["total", "over", "under"]);

    const firstBook = (m) => m?.books?.[0] || {};

    const sB = firstBook(mSpread);
    const tB = firstBook(mTotal);

    const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));
    const aw = sB?.awayTeamOdds || {};
    const hm = sB?.homeTeamOdds || {};

    const spreadAway = n(aw?.current?.spread ?? aw?.spread ?? sB?.current?.spread);
    const spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
    const mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
    const mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
    const total      = n(tB?.current?.total ?? tB?.total);

    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch {
    return undefined;
  }
}

// A1 helpers for values.batchUpdate
function makeValue(range, val) { return { range, values: [[val]] }; }
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}

async function getValues() {
  const range = `${TAB_NAME}!A1:Q2000`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
  return res.data.values || [];
}

// header column mapping
function mapCols(header) {
  const lower = s => (s || "").trim().toLowerCase();
  const find = (name, fb) => {
    const i = header.findIndex(h => lower(h) === lower(name));
    return i >= 0 ? i : fb;
  };
  return {
    GAME_ID: find("Game ID", 0),
    DATE: find("Date", 1),
    STATUS: find("Status", 3),
    HALF: find("Half Score", 11),
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

function looksLiveStatus(s) {
  if (!s) return false;
  const x = s.toLowerCase();
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}

function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();        // MM/DD/YY
    const status = (row[col.STATUS] || "").trim();

    if (GAME_ID && id === GAME_ID) {
      targets.push({ r, id, reason: "GAME_ID" });
      continue;
    }

    if (looksLiveStatus(status)) {
      targets.push({ r, id, reason: "live-like status" });
      continue;
    }

    // Also include rows scheduled for today (helps catch rows where status never moved off kickoff time)
    if (dateCell === todayKey && status.toLowerCase() !== "final") {
      targets.push({ r, id, reason: "today" });
    }
  }
  return targets;
}

async function main() {
  try {
    const ts = new Date().toISOString();

    const values = await getValues();
    if (values.length === 0) {
      console.log(`[${ts}] Sheet empty—nothing to do.`);
      return;
    }
    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);

    if (targets.length === 0) {
      console.log(`[${ts}] Nothing to update (no targets${GAME_ID ? " for GAME_ID" : ""}).`);
      return;
    }

    const data = [];

    for (const t of targets) {
      // 1) STATUS + HALF
      let statusText = values[t.r]?.[col.STATUS] || "";
      let halfScore = "";

      try {
        const sum = await espnSummary(t.id);
        statusText = shortStatusFromEspn(sum?.header?.competitions?.[0]?.status) || statusText || "In Progress";
        halfScore = parseHalfScore(sum) || "";
      } catch (e) {
        if (e?.response?.status !== 404) {
          console.log(`Summary warn ${t.id}:`, e?.message || e);
        }
      }

      data.push(makeValue(a1For(t.r, col.STATUS), statusText));
      if (halfScore) data.push(makeValue(a1For(t.r, col.HALF), halfScore));

      // 2) LIVE ODDS (2H preferred)
      try {
        const odds = await espnOdds(t.id);
        const live = extractLiveNumbers(odds, MARKET_PREFERENCE);
        if (live) {
          const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
          w(col.LA_S,  live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S,  live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        } else {
          console.log(`No live market found ${t.id} (pref="${MARKET_PREFERENCE}") — left M..Q as-is.`);
        }
      } catch (e) {
        if (e?.response?.status === 404) {
          console.log(`Odds 404 ${t.id} — markets unavailable, skipping M..Q.`);
        } else {
          console.log(`Odds warn ${t.id}:`, e?.message || e);
        }
      }
    }

    if (data.length === 0) {
      console.log(`[${ts}] Built 0 cell updates across ${targets.length} target(s).`);
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });

    console.log(
      `[${ts}] Updated ${targets.length} row(s). Wrote ${data.length} precise cell update(s). ` +
      `Targets: ${targets.map(t => `${t.id}(${t.reason})`).join(", ")}`
    );
  } catch (err) {
    const code = err?.response?.status || err?.code || err?.message || err;
    console.error("Live updater fatal:", "*** code:", code, "***");
    process.exit(1);
  }
}

main();
