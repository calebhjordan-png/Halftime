// live-game.mjs
// Updates: Status (D), CURRENT Score -> "Half Score" (L), Live odds (M..Q).
// Leaves pregame columns untouched. Optional GAME_ID focus and DEBUG_MODE logging.
// Live odds are scraped from the visible ESPN BET "LIVE ODDS" card ONLY when game is in progress.

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

function looksLiveStatusText(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\d+:\d+\s*-\s*(1st|2nd|3rd|4th|ot)/i.test(s || "");
}
const isFinalCell = s => /^final$/i.test(String(s || ""));

// ESPN helpers
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}
function isInProgressFromEspn(statusObj) {
  const t = statusObj?.type || {};
  const state = String(t.state || "").toLowerCase();       // "in", "pre", "post"
  const desc = `${t.shortDetail || t.detail || t.description || ""}`;
  return state === "in" || /halftime/i.test(desc) || /\b(1st|2nd|3rd|4th|ot)\b/i.test(desc);
}

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  DEBUG && console.log(`🔎 summary: ${url}`);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ───────────────────────── Score (current) ───────────────────────── */
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

/* ─────────────── Live odds: scrape ESPN BET live block only ───────── */
async function fetchGameHtml(gameId) {
  const url = `https://www.espn.com/${LEAGUE.replace("college-", "college/")}/game/_/gameId/${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000, responseType: "text" });
  return String(data || "");
}

// Extract *live* numbers (skip the CLOSE row). Do not attempt unless game is live.
function scrapeEspnBet(html) {
  if (!html) return undefined;

  // Locate the "LIVE ODDS" card area
  const anchor = html.indexOf("ESPN BET SPORTSBOOK");
  if (anchor < 0) return undefined;
  const chunk = html.slice(anchor, anchor + 25000);

  // Collect spreads (+/-x.x)
  const spreadRe = />\s*([+\-]\d+(?:\.\d)?)\s*<\/button>/g;
  const spreads = [];
  let m;
  while ((m = spreadRe.exec(chunk))) spreads.push(m[1]);

  // Collect totals (o/uX.X -> capture the numeric X.X)
  const totalRe = />\s*[ou](\d+(?:\.\d)?)\s*<\/button>/g;
  const totals = [];
  while ((m = totalRe.exec(chunk))) totals.push(m[1]);

  // Collect ML buttons (+500/-900/EVEN/OFF)
  const mlRe = />\s*([+\-]\d+|EVEN|OFF)\s*<\/button>/g;
  const allML = [];
  while ((m = mlRe.exec(chunk))) allML.push(m[1]);

  // Heuristic: first pair is CLOSE; second pair is LIVE.
  const pickPairAfterClose = (arr) => {
    if (arr.length >= 4) return [arr[2], arr[3]];
    return [undefined, undefined];
  };

  const [spreadAway, spreadHome] = pickPairAfterClose(spreads);
  const [totalAway, totalHome]   = pickPairAfterClose(totals);

  // ML: skip EVEN/OFF and take the second pair (indexes 2,3) when available
  const usable = allML.filter(x => x !== "EVEN" && x !== "OFF");
  let mlAway, mlHome;
  if (usable.length >= 4) {
    mlAway = usable[2];
    mlHome = usable[3];
  }

  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const r = {
    spreadAway: n(spreadAway),
    spreadHome: n(spreadHome),
    mlAway: mlAway ? n(mlAway) : "",
    mlHome: mlHome ? n(mlHome) : "",
    total: totalAway ? n(totalAway) : totalHome ? n(totalHome) : "",
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
    HALF: find("Half Score", 11),
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

// Target rows: keep status/score flowing for today's rows, but we only write odds for in-progress games.
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
    if (looksLiveStatusText(status)) { targets.push({ r, id, reason: "live-like status" }); continue; }
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

      let summary, statusObj, statusText, inProgress = false;

      // STATUS + SCORE
      try {
        summary   = await espnSummary(t.id);
        statusObj = summary?.header?.competitions?.[0]?.status;
        statusText = shortStatusFromEspn(statusObj);
        inProgress = isInProgressFromEspn(statusObj);

        if (statusText && statusText !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), statusText));
        }

        const scoreText = parseCurrentScore(summary); // away-home
        if (scoreText) {
          data.push(makeValue(a1For(t.r, col.HALF), scoreText));
        }

        if (isFinalFromEspn(statusObj)) {
          // Don't touch live odds once final.
          continue;
        }
      } catch (e) {
        console.log(`Summary warn ${t.id}:`, e?.message || e);
      }

      // LIVE ODDS (only when actually live)
      if (!inProgress) {
        DEBUG && console.log(`   [odds] skipped (not in progress)`);
        continue;
      }

      try {
        const html = await fetchGameHtml(t.id);
        const live = scrapeEspnBet(html);

        if (DEBUG) {
          console.log(`   [scrape] html length: ${html?.length || 0}`);
          live ? console.log(`   [live] ${JSON.stringify(live)}`) : console.log("   [live] none");
        }

        if (live) {
          const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
          w(col.LA_S,  live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S,  live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        }
        // If scrape fails, we *do not* fall back to pools (to avoid pregame contamination).
      } catch (e) {
        DEBUG && console.log(`   [scrape] failed ${t.id}:`, e?.message || e);
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
