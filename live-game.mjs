// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q) by scraping ESPN BET widget.

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
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`; // away-first
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
  const url = `https://www.espn.com/${LEAGUE}/game/_/gameId/${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000, headers: BROWSER_HEADERS });
  return String(data || "");
}

/* ───────────────────── ESPN BET widget parser ────────────────────── */
function decodeEntities(txt) {
  return txt
    .replace(/&nbsp;|&#160;/g, " ")
    .replace(/&minus;|&#8722;|−/g, "-")
    .replace(/&plus;|&#43;/g, "+")
    .replace(/&amp;/g, "&")
    .replace(/\u00A0/g, " ");
}

function stripTags(html) {
  let t = html.replace(/<script[\s\S]*?<\/script>/gi, "")
              .replace(/<style[\s\S]*?<\/style>/gi, "")
              .replace(/<[^>]+>/g, " ");
  t = decodeEntities(t);
  return t.replace(/\s+/g, " ").trim();
}

function findLiveOddsBlock(html) {
  if (!html) return "";
  const tries = [
    /ESPN BET SPORTSBOOK[\s\S]{0,18000}?LIVE ODDS[\s\S]{0,18000}?Odds by ESPN BET/i,
    /LIVE ODDS[\s\S]{0,18000}?Odds by ESPN BET/i,
    /LIVE ODDS[\s\S]{0,18000}?All Live Odds on ESPN BET Sportsbook/i
  ];
  for (const rx of tries) {
    const m = html.match(rx);
    if (m) return m[0];
  }
  return "";
}

// Heuristic extraction that is tolerant to markup/case changes
function parseEspnBetWidget(html) {
  const block = findLiveOddsBlock(html);
  log("   [scrape] block length:", block.length);
  if (!block) return undefined;

  const text = stripTags(block);
  if (DBG) log("   [text] snippet:", text.slice(0, 220), "...");

  // Locate the "Close Spread Total ML" header case-insensitively
  const hdr = text.search(/close\s+spread\s+total\s+ml/i);
  const noteIdx = text.search(/note:\s*odds/i);
  const body = hdr >= 0
    ? text.slice(hdr)
    : (noteIdx > 0 ? text.slice(0, noteIdx) : text);

  if (DBG) log("   [body] snippet:", body.slice(0, 220), "...");

  // Totals: first oXX.X or uXX.X
  const oMatch = body.match(/\bo(\d{1,2}(?:\.\d)?)\b/i);
  const uMatch = body.match(/\bu(\d{1,2}(?:\.\d)?)\b/i);
  const total = oMatch ? Number(oMatch[1]) : (uMatch ? Number(uMatch[1]) : "");

  // Spreads: signed numbers within realistic range, prefer ones with .5
  const spreadCandidates = [...body.matchAll(/(?<!\d)([+\-]\d{1,2}(?:\.\d)?)(?!\d)/g)]
    .map(m => m[1])
    .filter(v => {
      const num = Number(v);
      if (!Number.isFinite(num)) return false;
      if (Math.abs(num) > 50) return false;
      // exclude common juice prices (no decimals, >= 3 digits)
      if (!/\./.test(v) && Math.abs(num) >= 100) return false;
      return true;
    });

  // Prefer .5 lines; if we have more than 2, keep the first two
  const spreadsPref = spreadCandidates.sort((a,b) => (/\./.test(b)?1:0) - (/\./.test(a)?1:0));
  const spreadAway = spreadsPref[0] ? Number(spreadsPref[0]) : "";
  const spreadHome = spreadsPref[1] ? Number(spreadsPref[1]) : "";

  // Moneylines: after the first total occurrence, grab tokens that look like ML
  let afterTotalStart = body.length;
  const firstTotalPos = Math.min(
    oMatch ? body.indexOf(oMatch[0]) : Infinity,
    uMatch ? body.indexOf(uMatch[0]) : Infinity
  );
  if (firstTotalPos !== Infinity) afterTotalStart = firstTotalPos;

  const mlTail = body.slice(afterTotalStart);
  const mlTokens = [...mlTail.matchAll(/\b(EVEN|OFF|[+\-]\d{3,4}|[+\-]130)\b/g)].map(m => m[1]);
  // Filter out likely juice (-110/-115/-120/-125) but keep -130 since ML can be -130
  const badJuice = new Set(["-105","-110","-115","-120","-125"]);
  const mlClean = mlTokens.filter(t => t === "EVEN" || t === "OFF" || !badJuice.has(t));

  const parseML = v => (v === "EVEN" || v === "OFF") ? "" : Number(v);
  const mlAway = mlClean[0] ? parseML(mlClean[0]) : "";
  const mlHome = mlClean[1] ? parseML(mlClean[1]) : "";

  if (DBG) {
    log("   [found] spreads:", spreadAway, spreadHome);
    log("   [found] total  :", total);
    log("   [found] ML     :", mlAway, mlHome);
  }

  const live = { spreadAway, spreadHome, mlAway, mlHome, total };
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

    // If GAME_ID provided, hard-filter to it
    if (GAME_ID) {
      if (id === GAME_ID) targets.push({ r, id, reason: "GAME_ID" });
      continue;
    }

    const dateCell = (row[col.DATE] || "").trim();
    const status   = (row[col.STATUS] || "").trim();
    if (isFinalCell(status)) continue;

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

      // ESPN BET LIVE ODDS via HTML scrape
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
