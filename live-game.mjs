// live-game.mjs
// Updates only: Status (D), Half Score (L), Live odds (M..Q).
// Leaves pregame columns untouched. Supports optional GAME_ID focus.
// We try (in order): ESPN odds REST (prefer ESPN BET “live-ish” markets),
// then any REST live market, then summary pools fallback.

import axios from "axios";
import { google } from "googleapis";

/* ───────────── ENV ───────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                                 // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                                   // optional: only update this game
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
} = process.env;

for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

/* ───────── Google Sheets bootstrap ───────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────── Helpers ───────── */
const norm = (s) => (s || "").toLowerCase();

function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}

// Today key in **US/Eastern** to match the sheet’s date in column B (MM/DD/YY)
const todayKey = (() => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "2-digit", day: "2-digit", year: "2-digit",
  }).formatToParts(new Date());
  const mm = parts.find(p => p.type === "month")?.value ?? "00";
  const dd = parts.find(p => p.type === "day")?.value ?? "00";
  const yy = parts.find(p => p.type === "year")?.value ?? "00";
  return `${mm}/${dd}/${yy}`;
})();

function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
const isFinalCell = (s) => /^final$/i.test(String(s || ""));

// ESPN status helpers
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}

// Half score from first two period linescores
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
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`; // away-first
  } catch {}
  return "";
}

// Preferred tokens list (order matters)
function prefTokens(list = MARKET_PREFERENCE) {
  return (list || "")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
}
function textMatchesAny(text, tokens) {
  const t = norm(text);
  return tokens.some(tok => t.includes(tok));
}

/* ───────── ESPN fetchers ───────── */
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

// Fetch a single book detail via its $ref (common in ESPN odds REST)
async function fetchBookByRef(ref) {
  try {
    if (!ref) return null;
    const { data } = await axios.get(ref, { timeout: 12000 });
    return data;
  } catch {
    return null;
  }
}

// From a market object, load only books that look like ESPN BET (by provider name)
async function fetchEspnBetBooks(market) {
  try {
    const books = Array.isArray(market?.books) ? market.books : [];
    const out = [];
    for (const b of books) {
      // some odds payloads give full objects, many give { $ref: "..." }
      const book = b?.$ref ? await fetchBookByRef(b.$ref) : b;
      const providerName = (book?.provider?.name || "").toLowerCase();
      if (providerName.includes("espn bet") || providerName.includes("espnbet")) {
        out.push(book);
      }
    }
    return out;
  } catch {
    return [];
  }
}

// Parse a single book object into numbers (spread/ML/total)
function extractFromBook(b) {
  const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = b?.awayTeamOdds || {};
  const hm = b?.homeTeamOdds || {};
  const current = b?.current || {};

  const spreadAway = n(aw?.current?.spread ?? aw?.spread ?? current?.spread);
  const spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
  const mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
  const total      = n(b?.current?.total ?? b?.total);

  return { spreadAway, spreadHome, mlAway, mlHome, total };
}

/* ───────── LIVE selection/parse from REST ───────── */

// ASYNC (bug fix): we await book fetches inside
async function chooseEspnBetLiveBook(markets, prefTokensArr) {
  // market label indicates “live-ish” (2H / Halftime / Live / etc.)
  const looksLiveMarket = (mk) => {
    const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""} ${mk?.state || ""}`;
    return prefTokensArr.some((tok) => norm(label).includes(tok)) || norm(mk?.state) === "live";
  };

  const preferred = markets.filter(looksLiveMarket);
  const pool = preferred.length ? preferred : markets;

  // Within pool, require ESPN BET books and pick the first with usable numbers
  for (const mk of pool) {
    const books = await fetchEspnBetBooks(mk);
    for (const b of books) {
      const parsed = extractFromBook(b);
      if ([parsed.spreadAway, parsed.spreadHome, parsed.mlAway, parsed.mlHome, parsed.total].some((v) => v !== "")) {
        return parsed;
      }
    }
  }
  return undefined;
}

// Fallback: any REST market that is “live-ish”, any provider
function liveFromOddsRESTAny(markets, prefTokensArr) {
  const looksLiveMarket = (mk) => {
    const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""} ${mk?.state || ""}`;
    return prefTokensArr.some((tok) => norm(label).includes(tok)) || norm(mk?.state) === "live";
  };
  const candidates = markets.filter(looksLiveMarket);
  const pool = candidates.length ? candidates : markets;
  for (const mk of pool) {
    const b = (mk?.books && mk.books[0]) || null;
    const book = b?.$ref ? null : b; // if $ref, we’d need to fetch; keep it simple for this fallback
    if (book) {
      const parsed = extractFromBook(book);
      if ([parsed.spreadAway, parsed.spreadHome, parsed.mlAway, parsed.mlHome, parsed.total].some((v) => v !== "")) {
        return parsed;
      }
    }
  }
  return undefined;
}

/* ───────── Fallback from summary pools ───────── */
function liveFromSummaryPools(summary, tokens) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  const labelOf = (p) => `${p?.details || ""} ${p?.name || ""} ${p?.period || ""}`; // e.g., "2nd Half Line"
  // Try to find any pool that matches our tokens
  const match = pools.find((p) => textMatchesAny(labelOf(p), tokens));
  if (!match) return undefined;

  const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = match?.awayTeamOdds || {};
  const hm = match?.homeTeamOdds || {};
  const spreadAway = n(aw?.spread);
  const spreadHome = n(hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.moneyLine);
  const mlHome     = n(hm?.moneyLine);
  const total      = n(match?.overUnder ?? match?.total);

  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some((v) => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}

/* ───────── values/A1 helpers ───────── */
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

// Row selection: GAME_ID, already-live rows, or rows dated today (ET), ignoring Finals
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();   // MM/DD/YY
    const status   = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;

    if (GAME_ID && id === GAME_ID) {
      targets.push({ r, id, reason: "GAME_ID" });
      continue;
    }
    if (looksLiveStatus(status)) {
      targets.push({ r, id, reason: "live-like status" });
      continue;
    }
    if (dateCell === todayKey) {
      targets.push({ r, id, reason: "today" });
    }
  }
  return targets;
}

/* ───────── MAIN ───────── */
async function main() {
  try {
    const ts = new Date().toISOString();
    const values = await getValues();
    if (values.length === 0) { console.log(`[${ts}] Sheet empty—nothing to do.`); return; }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) { console.log(`[${ts}] Nothing to update.`); return; }

    const tokens = prefTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // 1) STATUS + HALF
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);

        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
        if (nowFinal) continue; // no live odds if Final
      } catch (e) {
        if (e?.response?.status !== 404) {
          console.log(`Summary warn ${t.id}:`, e?.message || e);
        }
      }

      // 2) LIVE ODDS
      let live = undefined;

      // Try REST odds (prefer ESPN BET and live-ish markets)
      try {
        const odds = await espnOdds(t.id);
        const markets = Array.isArray(odds?.items) ? odds.items : [];
        if (markets.length) {
          live = await chooseEspnBetLiveBook(markets, tokens);     // ESPN BET live-ish
          if (!live) live = liveFromOddsRESTAny(markets, tokens);  // any book live-ish
        }
      } catch (e) {
        if (e?.response?.status !== 404) console.log(`Odds REST warn ${t.id}:`, e?.message || e);
      }

      // Fallback to summary pools
      if (!live && summary) {
        const sLive = liveFromSummaryPools(summary, tokens);
        if (sLive) live = sLive;
      }

      if (live) {
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else {
        console.log(`No live market accepted for ${t.id} (needs live-ish token in "${MARKET_PREFERENCE}") — left M..Q as-is.`);
      }
    }

    if (!data.length) {
      console.log(`[${new Date().toISOString()}] Built 0 cell updates across ${targets.length} target(s).`);
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });

    console.log(
      `[${ts}] Updated ${targets.length} row(s). Wrote ${data.length} cell update(s). ` +
      `Targets: ${targets.map(t => `${t.id}(${t.reason})`).join(", ")}`
    );
  } catch (err) {
    const code = err?.response?.status || err?.code || err?.message || err;
    console.error("Live updater fatal:", "*** code:", code, "***");
    process.exit(1);
  }
}

main();
