// live-game.mjs
// Updates only: Status (D), Half Score (L), Live odds (M..Q).
// Leaves pregame columns untouched. Supports optional GAME_ID focus.
// Pulls "live-ish" odds by expanding ESPN v2 odds links ($ref).

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                                   // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                                     // optional: only update this game
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live,In-Game",
} = process.env;

for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

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
function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}

// Today key in **US/Eastern** to match the sheet’s date in column B
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

const norm = s => (s || "").toLowerCase();
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
const isFinalCell = s => /^final$/i.test(String(s || ""));

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

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
const AX = axios.create({ timeout: 15000 });

async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await AX.get(url);
  return data;
}

// IMPORTANT: v2 odds returns a collection of links. We must follow $ref to expand.
async function espnOddsRoot(gameId) {
  const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${gameId}/competitions/${gameId}/odds?region=us&lang=en`;
  const { data } = await AX.get(url);
  return data; // { items: [ { $ref }, ... ] } typically
}

// Safe JSON fetcher for $ref links with a small per-game budget
async function deref(url) {
  const { data } = await AX.get(url);
  return data;
}

/* ─────────────── Live market selection & parsing ─────────────────── */
const wantTokens = (list = MARKET_PREFERENCE) =>
  (list || "")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);

const labelOfMarket = (m) =>
  `${m?.name || ""} ${m?.displayName || ""} ${m?.period?.displayName || ""} ${m?.period?.abbreviation || ""}`;

function matchesTokens(text, tokens) {
  const t = norm(text);
  return tokens.some(tok => t.includes(tok));
}

const N_BUDGET = 10; // max follow requests per game to keep it light

async function expandOddsMarkets(oddsRoot) {
  // Returns a list of fully-expanded market objects with books -> teamOdds.
  const items = oddsRoot?.items || [];
  if (!items.length) return [];

  const markets = [];
  let budget = N_BUDGET;

  for (const it of items) {
    if (budget <= 0) break;
    const ref = it?.$ref || it?.ref || it?.href;
    if (!ref) continue;

    let market;
    try {
      market = await deref(ref); // market object
      budget--;
    } catch { continue; }

    // expand books
    let books = market?.books;
    if (books?.$ref) {
      if (budget <= 0) break;
      try {
        const coll = await deref(books.$ref); // collection
        budget--;
        const expanded = [];
        for (const bi of coll?.items || []) {
          if (budget <= 0) break;
          const bref = bi?.$ref || bi?.ref || bi?.href;
          if (!bref) continue;
          try {
            const book = await deref(bref);
            budget--;
            // expand away/home odds if they’re links
            const expandTeamOdds = async (key) => {
              const t = book?.[key];
              if (t && t.$ref) {
                if (budget <= 0) return t; // out of budget, leave as-is
                const td = await deref(t.$ref);
                budget--;
                return td;
              }
              return t || {};
            };
            const awayTeamOdds = await expandTeamOdds("awayTeamOdds");
            const homeTeamOdds = await expandTeamOdds("homeTeamOdds");
            expanded.push({ ...book, awayTeamOdds, homeTeamOdds });
          } catch { /* ignore */ }
        }
        books = expanded;
      } catch { /* ignore collection fetch errors */ }
    } else if (Array.isArray(books)) {
      // best effort: ensure sublinks (rare in this layout)
      // leave as-is
    }

    markets.push({ ...market, books: Array.isArray(books) ? books : [] });
    if (budget <= 0) break;
  }

  return markets;
}

function pickLiveFromExpanded(markets, tokens) {
  if (!markets.length) return undefined;

  // Filter to markets whose label matches tokens; otherwise fall back to anything sensible.
  const liveCands = markets.filter(m => matchesTokens(labelOfMarket(m), tokens));
  const pool = liveCands.length ? liveCands : markets;

  const looks = (m, words) => words.some(w => norm(`${m?.name || ""} ${m?.displayName || ""}`).includes(w));
  const choose = (words) => {
    const typed = pool.filter(m => looks(m, words));
    return (typed[0] || pool[0]);
  };

  const mSpread = choose(["spread", "line"]);
  const mTotal  = choose(["total", "over", "under"]);

  const firstBook = (m) => (m?.books && m.books[0]) || null;
  const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));

  let spreadAway="", spreadHome="", mlAway="", mlHome="", total="";

  if (mSpread) {
    const b = firstBook(mSpread) || {};
    const aw = b.awayTeamOdds || {};
    const hm = b.homeTeamOdds || {};
    spreadAway = n(aw?.current?.spread ?? aw?.spread ?? b?.current?.spread);
    spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
    mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
    mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
  }
  if (mTotal) {
    const b = firstBook(mTotal) || {};
    total = n(b?.current?.total ?? b?.total);
  }

  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}

// Fallback: extract from summary pools (pickcenter/odds), again with token filter if possible
function liveFromSummaryPools(summary, tokens) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  const labelOf = p => `${p?.details || ""} ${p?.name || ""} ${p?.period || ""}`;
  const liveish = pools.filter(p => matchesTokens(labelOf(p), tokens));
  const pick = (liveish[0] || pools[0]);
  if (!pick) return undefined;

  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = pick?.awayTeamOdds || {};
  const hm = pick?.homeTeamOdds || {};

  const spreadAway = n(aw?.spread);
  const spreadHome = n(hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.moneyLine);
  const mlHome     = n(hm?.moneyLine);
  const total      = n(pick?.overUnder ?? pick?.total);

  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}

/* ─────────────────────── values/A1 helpers ───────────────────────── */
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

// Only row selection we want: GAME_ID, already-live rows, or rows with today’s date (ET) and not Final
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

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const ts = new Date().toISOString();
    const values = await getValues();
    if (values.length === 0) { console.log(`[${ts}] Sheet empty—nothing to do.`); return; }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) { console.log(`[${ts}] Nothing to update.`); return; }

    const tokens = wantTokens(MARKET_PREFERENCE);
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
        if (nowFinal) continue; // stop live odds if Final
      } catch (e) {
        if (e?.response?.status !== 404) {
          console.log(`Summary warn ${t.id}:`, e?.message || e);
        }
      }

      // 2) LIVE ODDS via v2 odds (expanded)
      let live = undefined;
      try {
        const root = await espnOddsRoot(t.id);
        const expanded = await expandOddsMarkets(root);
        live = pickLiveFromExpanded(expanded, tokens);
      } catch (e) {
        if (e?.response?.status !== 404) console.log(`Odds v2 warn ${t.id}:`, e?.message || e);
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
        console.log(`No live market found/accepted for ${t.id} (tokens="${MARKET_PREFERENCE}") — left M..Q as-is.`);
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
