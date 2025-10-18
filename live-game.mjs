// live-game.mjs
// Updates only: Status (D), Half Score (L), Live odds (M..Q).
// Prefers ESPN BET "Live" markets via the Markets API (not the stale /odds feed).
// Will NOT overwrite pregame columns (F..K). Supports optional GAME_ID focus.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl", // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",   // optional: focus only one game id
  // tokens used to accept a *live-ish* market label
  MARKET_PREFERENCE = "live,in-play,inplay,2h,second half,halftime",
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
const norm = s => (s || "").toLowerCase();

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

// Summary (status + linescores)
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

// Markets (ESPN BET live prices). Root returns an object with items (objects or $ref links).
async function espnMarkets(gameId) {
  const url = `https://sports.core.api.espn.com/v2/markets?sport=football&league=${LEAGUE}&event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

// Helper: fetch JSON for $ref entries safely (returns {} on failure)
async function fetchRefMaybe(ref) {
  if (!ref || typeof ref !== "string") return {};
  try {
    const { data } = await axios.get(ref, { timeout: 12000 });
    return data || {};
  } catch { return {}; }
}

/* ─────────────── Live market selection & parsing ─────────────────── */
function prefTokens(list = MARKET_PREFERENCE) {
  return (list || "")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
}
function labelMatchesPreferred(mk, tokens) {
  const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.state || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""}`;
  const l = norm(label);
  if (/live/.test(l) || mk?.state === "LIVE" || mk?.inPlay === true) return true; // fast-path for "live"
  return tokens.some(tok => l.includes(tok));
}

// Dereference a market's first book (object or $ref) and extract odds
async function extractFromMarket(market) {
  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  try {
    const book0 = (market?.books && market.books[0]) || null;
    const book = book0?.$ref ? await fetchRefMaybe(book0.$ref) : (book0 || {});
    const aw = book?.awayTeamOdds || {};
    const hm = book?.homeTeamOdds || {};

    const spreadAway = n(aw?.current?.spread ?? aw?.spread ?? book?.current?.spread);
    const spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
    const mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
    const mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
    const total      = n(book?.current?.total ?? book?.total);

    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch {
    return undefined;
  }
}

// Given all markets, pick the best live spread/total/moneyline
async function pickLiveFromMarkets(allMarkets, tokens) {
  if (!Array.isArray(allMarkets) || !allMarkets.length) return undefined;

  // Accept only live-ish markets
  const candidates = allMarkets.filter(mk => labelMatchesPreferred(mk, tokens));
  if (!candidates.length) return undefined;

  // Prefer markets that "look" like spread/total/line
  const looks = (mk, words) => {
    const s = norm(`${mk?.name || ""} ${mk?.displayName || ""}`);
    return words.some(w => s.includes(w));
  };
  const choose = words => candidates.find(mk => looks(mk, words)) || candidates[0];

  const mkSpread = choose(["spread", "line"]);
  const mkTotal  = choose(["total", "over", "under"]);

  let spreadAway="", spreadHome="", mlAway="", mlHome="", total="";

  if (mkSpread) {
    const part = await extractFromMarket(mkSpread);
    if (part) {
      spreadAway = part.spreadAway ?? "";
      spreadHome = part.spreadHome ?? "";
      mlAway     = part.mlAway ?? mlAway;
      mlHome     = part.mlHome ?? mlHome;
    }
  }
  if (mkTotal) {
    const part = await extractFromMarket(mkTotal);
    if (part && part.total !== "") total = part.total;
  }

  // If we still don't have ML but have *a* live market, try first live market's book for ML
  if ((mlAway === "" && mlHome === "") && candidates.length) {
    const any = await extractFromMarket(candidates[0]);
    if (any) { mlAway = any.mlAway ?? ""; mlHome = any.mlHome ?? ""; }
  }

  const anyVal = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return anyVal ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
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

    if (GAME_ID && id === GAME_ID) { targets.push({ r, id, reason: "GAME_ID" }); continue; }
    if (looksLiveStatus(status))    { targets.push({ r, id, reason: "live-like status" }); continue; }
    if (dateCell === todayKey)      { targets.push({ r, id, reason: "today" }); }
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

    const tokens = prefTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // 1) STATUS + HALF from Summary
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
        if (nowFinal) continue; // don't fetch live odds if Final
      } catch (e) {
        if (e?.response?.status !== 404) {
          console.log(`Summary warn ${t.id}:`, e?.message || e);
        }
      }

      // 2) LIVE LINES from Markets API (ESPN BET)
      try {
        const marketsRoot = await espnMarkets(t.id);
        // items can be objects or $ref links; dereference *lightly* (we only need label + first book later)
        const items = Array.isArray(marketsRoot?.items) ? marketsRoot.items : [];
        const markets = [];
        for (const itm of items) {
          if (itm?.$ref) {
            try {
              const m = await fetchRefMaybe(itm.$ref);
              if (m && Object.keys(m).length) markets.push(m);
            } catch {}
          } else if (itm) {
            markets.push(itm);
          }
        }

        const live = await pickLiveFromMarkets(markets, tokens);
        if (live) {
          const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
          w(col.LA_S,  live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S,  live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        } else {
          console.log(`No live market accepted for ${t.id} (needs one of "${MARKET_PREFERENCE}") — left M..Q as-is.`);
        }
      } catch (e) {
        if (e?.response?.status === 404) {
          console.log(`Markets 404 ${t.id} — no live markets, skipping M..Q.`);
        } else {
          console.log(`Markets warn ${t.id}:`, e?.message || e);
        }
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
