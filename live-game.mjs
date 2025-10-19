// live-game.mjs
// Updates: Status (D), Score (L), Half odds (M..Q).
// Locks all lines after halftime. Uses ESPN summary + ESPN BET live odds scraping.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football", // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
  DEBUG_MODE = "",
} = process.env;

const DEBUG = String(DEBUG_MODE || "").trim() === "1";
const log = (...a) => DEBUG && console.log(...a);

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

const normStr = (s) => (s || "").toString();
const isFinalCell = s => /^final$/i.test(normStr(s));
function looksLiveStatus(s) {
  const x = normStr(s).toLowerCase();
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-2]\b/.test(x);
}

// ESPN status helpers
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}

/* ─────────────── Half score (full score col now) ─────────────── */
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
function parseScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const hHome = home?.score ?? 0;
    const hAway = away?.score ?? 0;
    return `${hAway}-${hHome}`;
  } catch { return ""; }
}

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ───────────── ESPN BET text scraper ───────────── */
function normalizeDashes(s) {
  return s.replace(/\u2212|\u2013|\u2014/g, "-").replace(/\uFE63|\uFF0B/g, "+");
}
function tokenizeOdds(rowText) {
  let t = normalizeDashes(rowText).replace(/\b[ou](\d+(?:\.\d+)?)/gi, "$1");
  const parts = t.split(/\s+/).filter(Boolean);
  const NUM = /^[-+]?(\d+(\.\d+)?|\.\d+)$/;
  return parts.filter(p => NUM.test(p));
}
function scrapeRowNumbers(txt) {
  const nums = tokenizeOdds(txt).map(Number);
  const spread = nums.find(v => Math.abs(v) <= 60) ?? "";
  const total = nums.find(v => v >= 30 && v <= 100) ?? "";
  const ml = nums.find(v => Math.abs(v) >= 100) ?? "";
  return { spread, total, ml };
}
function scrapeEspnBetText(summary, html) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const awayName = comp?.competitors?.find(c=>c.homeAway==="away")?.team?.displayName || "";
    const homeName = comp?.competitors?.find(c=>c.homeAway==="home")?.team?.displayName || "";
    if (!awayName || !homeName) return undefined;

    const txt = normalizeDashes(html);
    const aIdx = txt.search(new RegExp(awayName, "i"));
    const hIdx = txt.search(new RegExp(homeName, "i"));
    if (aIdx < 0 || hIdx < 0) return undefined;

    const awayRow = txt.slice(aIdx, hIdx);
    const homeRow = txt.slice(hIdx);
    const a = scrapeRowNumbers(awayRow);
    const h = scrapeRowNumbers(homeRow);

    const total = h.total || a.total || "";
    const any = [a.spread, h.spread, a.ml, h.ml, total].some(v => v !== "");
    return any ? {
      spreadA: a.spread, spreadH: h.spread,
      mlA: a.ml, mlH: h.ml, total
    } : undefined;
  } catch { return undefined; }
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
    SCORE: find("Score", 11),
    HA_S: find("Half A Spread", 12),
    HA_ML: find("Half A ML", 13),
    HH_S: find("Half H Spread", 14),
    HH_ML: find("Half H ML", 15),
    H_TOT: find("Half Total", 16),
  };
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const values = await getValues();
    if (values.length === 0) return console.log("Sheet empty—nothing to do.");
    const col = mapCols(values[0]);

    const targets = [];
    for (let r = 1; r < values.length; r++) {
      const row = values[r] || [];
      const id = (row[col.GAME_ID] || "").trim();
      const date = (row[col.DATE] || "").trim();
      const status = (row[col.STATUS] || "").trim();
      if (!id || isFinalCell(status)) continue;
      if (GAME_ID && id !== GAME_ID) continue;
      if (looksLiveStatus(status) || date === todayKey) targets.push({ r, id });
    }
    if (!targets.length) return console.log("Nothing to update.");

    console.log(`[${new Date().toISOString()}] Found ${targets.length} game(s) to update.`);

    const data = [];
    for (const t of targets) {
      console.log(`\n=== 🏈 GAME ${t.id} ===`);
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus = shortStatusFromEspn(compStatus);
        const nowFinal = isFinalFromEspn(compStatus);
        const period = compStatus?.period ?? 0;
        console.log(`   status: "${newStatus}"`);

        if (newStatus) data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        const score = parseScore(summary);
        if (score) data.push(makeValue(a1For(t.r, col.SCORE), score));

        // lock after halftime
        if (period >= 3 || nowFinal) continue;

        // ESPN BET scrape
        const url = `https://www.espn.com/${LEAGUE === "nfl" ? "nfl" : "college-football"}/game/_/gameId/${t.id}`;
        const { data: html } = await axios.get(url, { timeout: 15000 });
        const block = html.match(/ESPN BET SPORTSBOOK[\s\S]*?Note:\s*Odds and lines subject to change\./i);
        const flat = block ? block[0].replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim() : "";
        const live = scrapeEspnBetText(summary, flat);
        if (live) {
          const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
          w(col.HA_S, live.spreadA);
          w(col.HA_ML, live.mlA);
          w(col.HH_S, live.spreadH);
          w(col.HH_ML, live.mlH);
          w(col.H_TOT, live.total);
        }
      } catch (e) {
        console.log(`   ⚠️ ${t.id} failed:`, e?.message);
      }
    }

    if (!data.length) return console.log("No updates.");
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });
    console.log(`✅ Updated ${data.length} cell(s).`);
  } catch (err) {
    console.error("Live updater fatal:", err?.message || err);
    process.exit(1);
  }
}

main();
