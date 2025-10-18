// live-game.mjs
// Status (D), Half Score (L), Live odds (M..Q) from ESPN BET widget.
// Pregame columns untouched. Supports GAME_ID focus + DEBUG_MODE logs.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",                 // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  DEBUG_MODE = "",
} = process.env;

const DBG = String(DEBUG_MODE).trim() === "1";
const log = (...a) => { if (DBG) console.log(...a); };

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

/* ───────────────────────── Helpers ───────────────────────── */
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

const norm = s => (s || "").toLowerCase();
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
const isFinalCell = s => /^final$/i.test(String(s || ""));

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

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

const BROWSER_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Cache-Control": "no-cache",
  "Pragma": "no-cache",
  "Upgrade-Insecure-Requests": "1",
};

async function fetchGameHtml(gameId) {
  const url = `https://www.espn.com/${LEAGUE.replace("college-football","college-football")}/game/_/gameId/${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000, headers: BROWSER_HEADERS });
  return String(data || "");
}

/* ───────────────────── ESPN BET widget parser ────────────────────── */
/**
 * Strategy: pull the LIVE ODDS block, strip tags → text, then carve out:
 *   SPREAD section (between "SPREAD" and "TOTAL")
 *   TOTAL section  (between "TOTAL"  and "ML")
 *   ML section     (after   "ML")
 * Extraction rules:
 *   - Spread: prefer values with a decimal (.5 etc). First two are away/home.
 *   - Total:  look for o##.# and u##.# → use the numeric part.
 *   - ML:     first two tokens like +900 / -180 / OFF → OFF => empty.
 */
function decodeEntities(txt) {
  return txt
    .replace(/&nbsp;|&#160;/g, " ")
    .replace(/&minus;|&#8722;|−/g, "-")
    .replace(/&plus;|&#43;/g, "+")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\u00A0/g, " ");
}

function toText(html) {
  // Remove scripts/styles, then tags
  let t = html.replace(/<script[\s\S]*?<\/script>/gi, "")
              .replace(/<style[\s\S]*?<\/style>/gi, "");
  t = t.replace(/<[^>]+>/g, " ");
  t = decodeEntities(t);
  // Collapse whitespace
  return t.replace(/\s+/g, " ").trim();
}

function parseEspnBetWidget(html) {
  if (!html) return undefined;
  const blockMatch = html.match(/ESPN BET SPORTSBOOK[\s\S]{0,12000}?LIVE ODDS[\s\S]{0,12000}?Note: Odds and lines subject to change\./i);
  const block = blockMatch ? blockMatch[0] : "";
  log("   [scrape] block length:", block.length);
  if (!block) return undefined;

  const text = toText(block);
  if (DBG) log("   [text] snippet:", text.slice(0, 300), "...");

  // Sections
  const spreadSec = (text.match(/SPREAD([\s\S]*?)TOTAL/) || [])[1] || "";
  const totalSec  = (text.match(/TOTAL([\s\S]*?)ML/)    || [])[1] || "";
  const mlSec     = (text.match(/ML([\s\S]*)$/)        || [])[1] || "";

  // Spreads: prefer values with a decimal to dodge -110 juice.
  const spreadVals = [...spreadSec.matchAll(/[+−-]\d{1,2}\.\d/g)].map(m => m[0].replace("−","-"));
  const spreadAway = spreadVals[0] || "";
  const spreadHome = spreadVals[1] || "";

  // Totals: capture o##.# and u##.#; use the numeric part (prefer whichever exists)
  const oMatch = totalSec.match(/o(\d{1,2}(?:\.\d)?)/i);
  const uMatch = totalSec.match(/u(\d{1,2}(?:\.\d)?)/i);
  const total = oMatch ? Number(oMatch[1]) : (uMatch ? Number(uMatch[1]) : "");

  // Moneylines: first two tokens like +900 / -2000 / OFF
  const mlTokens = [...mlSec.matchAll(/([+−-]\d{2,4}|OFF)/g)].map(m => m[1].replace("−","-"));
  const mlAway = mlTokens[0] && mlTokens[0] !== "OFF" ? Number(mlTokens[0]) : "";
  const mlHome = mlTokens[1] && mlTokens[1] !== "OFF" ? Number(mlTokens[1]) : "";

  if (DBG) {
    log("   [sections] spread:", spreadSec.slice(0,120));
    log("   [sections] total :", totalSec.slice(0,120));
    log("   [sections] ml    :", mlSec.slice(0,120));
    log("   [found] spreadAway:", spreadAway, "spreadHome:", spreadHome, "total:", total, "mlAway:", mlAway, "mlHome:", mlHome);
  }

  const toNum = v => v === "" ? "" : Number(v);
  const live = {
    spreadAway: spreadAway ? Number(spreadAway) : "",
    spreadHome: spreadHome ? Number(spreadHome) : "",
    mlAway,
    mlHome,
    total: total === "" ? "" : Number(total),
  };
  const any = Object.values(live).some(v => v !== "");
  return any ? live : undefined;
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
  try {
    const values = await getValues();
    if (values.length === 0) { console.log(`Sheet empty—nothing to do.`); return; }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) { console.log(`Nothing to update.`); return; }
    if (DBG) console.log(`[${new Date().toISOString()}] Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);

    const data = [];

    for (const t of targets) {
      console.log(`\n=== 🏈 GAME ${t.id} ===`);
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // STATUS + HALF
      try {
        const summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);

        log("   status:", JSON.stringify(newStatus));
        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
        if (nowFinal) { log("   is final -> skip odds"); continue; }
      } catch (e) {
        console.log(`   summary warn ${t.id}:`, e?.message || e);
      }

      // ESPN BET LIVE ODDS
      try {
        const html = await fetchGameHtml(t.id);
        const live = parseEspnBetWidget(html);

        if (live) {
          const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
          w(col.LA_S,  live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S,  live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        } else {
          console.log("   ❌ no live odds found");
        }
      } catch (e) {
        console.log(`   scrape warn ${t.id}:`, e?.message || e);
      }
    }

    if (!data.length) { console.log(`Built 0 precise cell updates across ${targets.length} target(s).`); return; }

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
