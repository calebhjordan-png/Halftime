// live-game.mjs
// Updates: Status (D), Score -> written to "Half Score" (L), Live odds (M..Q).
// Leaves pregame columns untouched. Optional GAME_ID focus and verbose DEBUG_MODE.
// Live odds preference: uses ESPN BET live block scraping (primary) with safe fallback to pools.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",        // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                       // optional: focus on a single game
  DEBUG_MODE = "",                    // "1" (truthy) to enable verbose logs
} = process.env;

const DEBUG = !!String(DEBUG_MODE);

/* ───────────────────── Google Sheets bootstrap ───────────────────── */
if (!GOOGLE_SHEET_ID) throw new Error("Missing required env var: GOOGLE_SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing required env var: GOOGLE_SERVICE_ACCOUNT");

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
function makeValue(range, val) { return { range, values: [[val]] }; }
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}

// Today key in **US/Eastern** to match Date column B
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

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  DEBUG && console.log(`🔎 summary: ${url}`);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ─────────────────────── Score (current) ───────────────────────────
   Write CURRENT score (away-home) into "Half Score" column (L).
   This uses header.competitions[0].competitors[].score which is
   ESPN’s live total points. */
function parseCurrentScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const hs = String(home?.score ?? "").trim();
    const as = String(away?.score ?? "").trim();
    if (hs !== "" && as !== "" && !Number.isNaN(+hs) && !Number.isNaN(+as)) {
      return `${as}-${hs}`; // away-first
    }
  } catch {}
  return "";
}

/* ───────────────── Live odds (scrape + safe fallback) ────────────── */
/** Pulls the ESPN game page HTML (for scraping the visible ESPN BET block). */
async function fetchGameHtml(gameId) {
  const url = `https://www.espn.com/${LEAGUE.replace("college-", "college/")}/game/_/gameId/${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000, responseType: "text" });
  return String(data || "");
}

/** Very simple scraper that extracts the *first LIVE* numbers that are NOT the pregame “CLOSE” row.
    We read the ESPN BET grid in the visible “LIVE ODDS” card. */
function scrapeEspnBet(html) {
  if (!html) return undefined;

  // crude block isolate
  const startIdx = html.indexOf("ESPN BET SPORTSBOOK");
  if (startIdx < 0) return undefined;
  const chunk = html.slice(startIdx, startIdx + 20000); // enough to include the live card

  // remove CLOSE column hints: we’ll ignore the first numeric pair that follows “CLOSE”.
  // Then look for SPREAD / TOTAL / ML button texts immediately to the right.
  // Strategy: find all numbers that look like spreads/totals/ml; skip the very first pair after “CLOSE”.
  // We’ll use specific anchored buttons that ESPN renders (u/o for totals, ± for spreads).

  // Spread buttons typically look like +10.5 / -10.5
  const spreadRe = />([+\-]\d+(?:\.\d)?)<\/button>/g;
  const spreads = [];
  let m;
  while ((m = spreadRe.exec(chunk))) spreads.push(m[1]);

  // Total buttons typically look like o44.5 / u44.5 (grab the numeric)
  const totalRe = />[ou](\d+(?:\.\d)?)<\/button>/g;
  const totals = [];
  while ((m = totalRe.exec(chunk))) totals.push(m[1]);

  // ML buttons look like +500 / -900 / EVEN / OFF — we’ll grab the first two moneylines that aren’t “EVEN/OFF”
  const mlRe = />([+\-]\d+|EVEN|OFF)<\/button>/g;
  const mls = [];
  while ((m = mlRe.exec(chunk))) mls.push(m[1]);

  // Heuristic:
  // - CLOSE row appears first; we skip the very first spread pair and very first total pair encountered.
  // - Then we take the next pair as live (away first, home second).
  const pickPair = (arr) => {
    if (arr.length >= 4) {
      // [pregameAway, pregameHome, liveAway, liveHome, ...]
      return [arr[2], arr[3]];
    }
    if (arr.length >= 2) {
      // If for some reason only two are available, treat them as live
      return [arr[0], arr[1]];
    }
    return [undefined, undefined];
  };

  const [spreadAway, spreadHome] = pickPair(spreads);
  const [totalAway, totalHome]   = pickPair(totals);     // totals are symmetric; we’ll use one numeric value
  let mlAway, mlHome;
  // Get two usable MLs (ignore EVEN/OFF if there are >2)
  const usableMLs = mls.filter(x => x !== "EVEN" && x !== "OFF");
  if (usableMLs.length >= 2) {
    // Heuristic: later pair is more likely to be live
    if (usableMLs.length >= 4) {
      mlAway = usableMLs[2];
      mlHome = usableMLs[3];
    } else {
      mlAway = usableMLs[0];
      mlHome = usableMLs[1];
    }
  }

  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));

  const result = {
    spreadAway: n(spreadAway),
    spreadHome: n(spreadHome),
    mlAway: mlAway === "EVEN" || mlAway === "OFF" ? "" : n(mlAway),
    mlHome: mlHome === "EVEN" || mlHome === "OFF" ? "" : n(mlHome),
    total: totalAway ? n(totalAway) : totalHome ? n(totalHome) : "",
  };

  const any = [result.spreadAway, result.spreadHome, result.mlAway, result.mlHome, result.total]
    .some(v => v !== "" && Number.isFinite(Number(v)));

  return any ? result : undefined;
}

/** Fallback to summary pools (often pregame; used only if scraping fails AND values look live-ish). */
function poolsFallback(summary) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  // prefer entries that look “live-ish” (sometimes ESPN annotates)
  const first = pools[0];
  if (!first) return undefined;

  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = first?.awayTeamOdds || {};
  const hm = first?.homeTeamOdds || {};
  const r = {
    spreadAway: n(aw?.spread),
    spreadHome: n(hm?.spread ?? (aw?.spread !== undefined ? -aw.spread : "")),
    mlAway: n(aw?.moneyLine),
    mlHome: n(hm?.moneyLine),
    total: n(first?.overUnder ?? first?.total),
  };

  const any = [r.spreadAway, r.spreadHome, r.mlAway, r.mlHome, r.total]
    .some(v => v !== "" && Number.isFinite(Number(v)));
  return any ? r : undefined;
}

/* ─────────────────────── values/A1 helpers ───────────────────────── */
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
    HALF: find("Half Score", 11),       // we now place CURRENT SCORE here
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

// Choose target rows: GAME_ID focus, live-ish status, or today's rows (not Final)
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();
    const status   = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;

    if (GAME_ID && id === GAME_ID) { targets.push({ r, id, reason: "GAME_ID" }); continue; }
    if (looksLiveStatus(status))     { targets.push({ r, id, reason: "live-like status" }); continue; }
    if (dateCell === todayKey)       { targets.push({ r, id, reason: "today" }); }
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
    let targets = chooseTargets(values, col);
    if (GAME_ID) targets = targets.filter(t => t.id === GAME_ID);
    if (targets.length === 0) { console.log(`[${ts}] Nothing to update.`); return; }
    DEBUG && console.log(`[${ts}] Found ${targets.length} game(s) to update: ${targets.map(t => t.id).join(", ")}`);

    const data = [];

    for (const t of targets) {
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      let summary;
      try {
        summary = await espnSummary(t.id);

        // STATUS
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);
        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }

        // SCORE -> write to "Half Score" column (L)
        const scoreText = parseCurrentScore(summary); // e.g., "34-0"
        if (scoreText) {
          data.push(makeValue(a1For(t.r, col.HALF), scoreText));
        }

        if (nowFinal) {
          // do not write live odds for finals
          continue;
        }
      } catch (e) {
        console.log(`Summary warn ${t.id}:`, e?.message || e);
      }

      // LIVE ODDS
      let live = undefined;

      // 1) scrape live block from game page
      try {
        const html = await fetchGameHtml(t.id);
        const scraped = scrapeEspnBet(html);
        if (DEBUG) {
          const snippet = (html || "").slice(0, 200).replace(/\s+/g, " ");
          console.log(`   [scrape] block length: ${html?.length || 0}`);
          scraped && console.log(`   [found] live =>`, JSON.stringify(scraped));
        }
        if (scraped) live = scraped;
      } catch (e) {
        DEBUG && console.log(`   [scrape] failed ${t.id}:`, e?.message || e);
      }

      // 2) fallback to pools if scrape failed
      if (!live && summary) {
        const p = poolsFallback(summary);
        if (p) live = p;
      }

      if (live) {
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else {
        DEBUG && console.log(`   ❌ no live odds found`);
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

    console.log(`✅ Updated ${data.length} cell(s).`);
  } catch (err) {
    const code = err?.response?.status || err?.code || err?.message || err;
    console.error("Live updater fatal:", "*** code:", code, "***");
    process.exit(1);
  }
}

main();
