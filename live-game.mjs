// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q).
// Does NOT touch pregame (G..K). Optional GAME_ID to focus one row.
// DEBUG_MODE=1 prints verbose fetch and market selection info.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                           // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  DEBUG_MODE = "0",
} = process.env;

if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");
const DEBUG = String(DEBUG_MODE) === "1";

/* ───────────────────── Google Sheets bootstrap ───────────────────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────────────────────────── Helpers ───────────────────────────── */
const norm = (s) => (s || "").toLowerCase();
const isFinalCell = (s) => /^final$/i.test(String(s || ""));
const looksLiveStatus = (s) =>
  /\bhalf\b/i.test(s) ||
  /\bin\s*progress\b/i.test(s) ||
  /\bq[1-4]\b/i.test(s) ||
  /\bot\b/i.test(s) ||
  /\blive\b/i.test(s);

function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}
function makeValue(range, val) { return { range, values: [[val]] }; }

// Today key in **US/Eastern** to match your sheet
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

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  DEBUG && console.log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

// NEW: try competitions/{id}/odds first; fall back to events/{id}/competitions/{id}/odds
async function espnOddsMarkets(gameId) {
  const tryUrls = [
    `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/competitions/${gameId}/odds`,
    `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${gameId}/competitions/${gameId}/odds`,
  ];
  for (const url of tryUrls) {
    try {
      DEBUG && console.log("🔎 odds:", url);
      const { data } = await axios.get(url, { timeout: 15000 });
      return data; // has .items (markets)
    } catch (e) {
      if (e?.response?.status === 404) {
        DEBUG && console.log("   ↪️ 404 on", url);
        continue;
      }
      throw e;
    }
  }
  const err = new Error("All odds endpoints returned 404");
  err.code = 404;
  throw err;
}

/* ───────────────────────── Status / half score ───────────────────── */
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
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
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`;
  } catch {}
  return "";
}

/* ───────────────────────── Live odds parsing ─────────────────────── */
// Pull a simple number from nested props
const N = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));

// Parse one market book into live numbers; return partials
function parseBookToLive(book) {
  const aw = book?.awayTeamOdds || {};
  const hm = book?.homeTeamOdds || {};
  const out = {
    spreadAway: N(aw?.current?.spread ?? aw?.spread ?? book?.current?.spread),
    spreadHome: "", // fill below if needed
    mlAway:     N(aw?.current?.moneyLine ?? aw?.moneyLine),
    mlHome:     N(hm?.current?.moneyLine ?? hm?.moneyLine),
    total:      N(book?.current?.total ?? book?.total),
  };
  if (out.spreadAway !== "") {
    out.spreadHome = N(hm?.current?.spread ?? hm?.spread ?? -out.spreadAway);
  }
  return out;
}

// Merge two partials (e.g., one from spread, one from total)
function mergeLive(a, b) {
  const out = { ...a };
  for (const k of ["spreadAway","spreadHome","mlAway","mlHome","total"]) {
    if (out[k] === "" && b && b[k] !== "") out[k] = b[k];
  }
  return out;
}

function extractLiveFromMarkets(oddsPayload) {
  const items = oddsPayload?.items || [];
  if (!items.length) return undefined;

  // Prefer any "live-ish" label if present, otherwise take the first two relevant books.
  const looksLiveMarket = (mk) => {
    const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""}`.toLowerCase();
    return /live|in-?game|2h|second half|halftime/.test(label);
  };

  const spreadish = (mk) => /spread|line/i.test(`${mk?.name || ""} ${mk?.displayName || ""}`);
  const totalish  = (mk) => /total|over|under/i.test(`${mk?.name || ""} ${mk?.displayName || ""}`);

  const liveCandidates = items.filter(looksLiveMarket);
  const src = liveCandidates.length ? liveCandidates : items; // fall back if no live tags appear

  let live = { spreadAway: "", spreadHome: "", mlAway: "", mlHome: "", total: "" };

  // Choose a spread/line market
  const mSpread = src.find(spreadish);
  if (mSpread) {
    const b = (mSpread.books && mSpread.books[0]) || {};
    live = mergeLive(live, parseBookToLive(b));
  }

  // Choose a total market
  const mTotal = src.find(totalish);
  if (mTotal) {
    const b = (mTotal.books && mTotal.books[0]) || {};
    live = mergeLive(live, parseBookToLive(b));
  }

  // If nothing useful, bail
  const any = ["spreadAway","spreadHome","mlAway","mlHome","total"].some(k => live[k] !== "");
  return any ? live : undefined;
}

// Fallback from summary.pickcenter / summary.odds (very loose; just grab any with numbers)
function extractLiveFromSummary(summary) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  // Prefer ESPN BET if present, else first pool that has moneylines or spread/total
  const score = (p) => {
    let s = 0;
    if (p?.awayTeamOdds?.moneyLine != null || p?.homeTeamOdds?.moneyLine != null) s += 2;
    if (p?.awayTeamOdds?.spread != null || p?.homeTeamOdds?.spread != null) s += 1;
    if (p?.overUnder != null || p?.total != null) s += 1;
    // light boost if the details look live-ish
    if (/live|in-?game|2nd|second half|halftime/i.test(`${p?.details || ""} ${p?.name || ""} ${p?.period || ""}`)) s += 3;
    if ((p?.provider?.name || "").toUpperCase() === "ESPN BET") s += 1;
    return s;
  };

  const best = pools
    .slice()
    .sort((a,b) => score(b) - score(a))[0];

  if (!best) return undefined;

  const aw = best?.awayTeamOdds || {};
  const hm = best?.homeTeamOdds || {};
  const live = {
    spreadAway: N(aw?.spread),
    spreadHome: N(hm?.spread ?? (aw?.spread != null ? -aw.spread : "")),
    mlAway:     N(aw?.moneyLine),
    mlHome:     N(hm?.moneyLine),
    total:      N(best?.overUnder ?? best?.total),
  };

  const any = ["spreadAway","spreadHome","mlAway","mlHome","total"].some(k => live[k] !== "");
  return any ? live : undefined;
}

/* ─────────────────────── values / columns map ────────────────────── */
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
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();
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

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  const ts = new Date().toISOString();

  const values = await getValues();
  if (values.length === 0) {
    console.log(`[${ts}] Sheet empty—nothing to do.`);
    return;
  }
  const col = mapCols(values[0]);
  const targets = chooseTargets(values, col);
  if (!targets.length) {
    console.log(`[${ts}] Nothing to update.`);
    return;
  }
  console.log(`[${ts}] Found ${targets.length} game(s) to update: ${targets.map(t => t.id).join(", ")}`);

  const data = [];

  for (const t of targets) {
    DEBUG && console.log(`\n=== 🏈 GAME ${t.id} ===`);

    // 1) STATUS + HALF
    let summary;
    try {
      summary = await espnSummary(t.id);
      const compStatus = summary?.header?.competitions?.[0]?.status;
      const newStatus  = shortStatusFromEspn(compStatus);
      const prevStatus = values[t.r]?.[col.STATUS] || "";
      if (newStatus && newStatus !== prevStatus) {
        data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
      }
      const half = parseHalfScore(summary);
      if (half) data.push(makeValue(a1For(t.r, col.HALF), half));

      if (isFinalFromEspn(compStatus)) {
        DEBUG && console.log("   ↪️ Final detected; skipping live odds.");
        continue;
      }
    } catch (e) {
      if (e?.response?.status !== 404) {
        console.log(`Summary warn ${t.id}:`, e?.message || e);
      }
    }

    // 2) LIVE ODDS — markets first, summary fallback
    let live = undefined;

    try {
      const markets = await espnOddsMarkets(t.id);
      live = extractLiveFromMarkets(markets);
      DEBUG && console.log(live
        ? `📊 markets extracted: ${JSON.stringify(live)}`
        : "   ↪️ markets had no usable live numbers");
    } catch (e) {
      if (e?.code === 404 || e?.response?.status === 404) {
        DEBUG && console.log("   ↪️ markets 404, will try summary pools");
      } else {
        console.log(`Markets fetch failed ${t.id}:`, e?.message || e);
      }
    }

    if (!live && summary) {
      const sLive = extractLiveFromSummary(summary);
      live = sLive;
      DEBUG && console.log(live
        ? `📊 summary extracted: ${JSON.stringify(live)}`
        : "   ↪️ summary pools had no usable live numbers");
    }

    if (live) {
      const write = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
      write(col.LA_S,  live.spreadAway);
      write(col.LA_ML, live.mlAway);
      write(col.LH_S,  live.spreadHome);
      write(col.LH_ML, live.mlHome);
      write(col.L_TOT, live.total);
    } else {
      DEBUG && console.log("   ↪️ no live odds chosen for this game.");
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
    `✅ Updated ${data.length} cell(s).`
  );
}

main().catch(err => {
  const code = err?.response?.status || err?.code || err?.message || err;
  console.error("Live updater fatal:", "*** code:", code, "***");
  process.exit(1);
});
