// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q) from ESPN BET LIVE ODDS.
// Strategy: scrape the game page's LIVE ODDS widget; ignore stale pregame pools.

import axios from "axios";
import { google } from "googleapis";

/* ─────────── ENV ─────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",              // "college-football" | "nfl"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                             // optional: focus a single game id
  DEBUG_MODE = "0",                         // "1" to log verbosely
} = process.env;

const DEBUG = String(DEBUG_MODE) === "1";
if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT)
  throw new Error("Missing required env vars: GOOGLE_SHEET_ID / GOOGLE_SERVICE_ACCOUNT");

/* ─────────── Google Sheets ─────────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ─────────── Helpers ─────────── */
function idxToA1(n0) { let n = n0 + 1, s = ""; while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); } return s; }
const a1For = (r, c, tab = TAB_NAME) => `${tab}!${idxToA1(c)}${r + 1}:${idxToA1(c)}${r + 1}`;
const makeValue = (range, v) => ({ range, values: [[v]] });
const norm = s => (s || "").toLowerCase();
const isFinalCell = s => /^final$/i.test(String(s || ""));
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b|\bq[1-4]\b|in\s*progress|ot|live|[0-9]+:[0-9]+\s*-\s*(1st|2nd|3rd|4th)/i.test(x);
}

/* date in US/Eastern */
const todayKey = (() => {
  const d = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "2-digit", day: "2-digit", year: "2-digit",
  }).formatToParts(d);
  const mm = parts.find(p => p.type === "month")?.value ?? "00";
  const dd = parts.find(p => p.type === "day")?.value ?? "00";
  const yy = parts.find(p => p.type === "year")?.value ?? "00";
  return `${mm}/${dd}/${yy}`;
})();

/* ─────────── ESPN APIs ─────────── */
const leaguePath = LEAGUE === "college-football" ? "football/college-football" : "football/nfl";
async function espnSummary(id) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath}/summary?event=${id}`;
  if (DEBUG) console.log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ─────────── Status & Half ─────────── */
function shortStatusFromEspn(st) {
  const t = st?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(st) {
  return /final/i.test(String(st?.type?.name || st?.type?.description || ""));
}
function sumFirstTwoPeriods(scores) {
  if (!Array.isArray(scores)) return null;
  let tot = 0;
  for (const p of scores.slice(0, 2)) {
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
    const hH = sumFirstTwoPeriods(home?.linescores);
    const hA = sumFirstTwoPeriods(away?.linescores);
    if (Number.isFinite(hH) && Number.isFinite(hA)) return `${hA}-${hH}`;
  } catch {}
  return "";
}

/* ─────────── ESPN BET LIVE ODDS (scrape) ───────────
   We parse the 'LIVE ODDS' widget from the game page.
   - Away team is shown first, then home team.
   - We capture spreads (±X.X), totals (oNN.N / uNN.N), and ML (+/-####) when not OFF. */
async function scrapeGamePageLiveOdds(id) {
  const sport = LEAGUE === "college-football" ? "college-football" : "nfl";
  const url = `https://www.espn.com/${sport}/game/_/gameId/${id}`;
  try {
    const { data: html } = await axios.get(url, { timeout: 15000 });
    // Grab the LIVE ODDS section (keep a generous window)
    const m = html.match(/LIVE ODDS[\s\S]{0,6000}?Odds by ESPN BET/i);
    const block = m ? m[0] : "";

    if (DEBUG) console.log("   [scrape] block length:", block.length);

    if (!block) return undefined;

    // Totals: prefer the first 'oNN.N' or 'uNN.N' we see in the widget
    const totalMatch = block.match(/\b[ou]\s?(\d{2,3}(?:\.\d)?)\b/i);
    const total = totalMatch ? Number(totalMatch[1]) : "";

    // Moneylines: find explicit +#### and -####. 'OFF' appears as word OFF; we skip it.
    const mlPlus = block.match(/\+(\d{3,5})\b/);
    const mlMinus = block.match(/-(\d{3,5})\b/);
    // NOTE: The widget often hides ML during blowouts -> return "" when absent/OFF
    const mlAway = mlPlus ? Number(`+${mlPlus[1]}`) : "";
    const mlHome = mlMinus ? Number(`-${mlMinus[1]}`) : "";

    // Spreads: find signed decimals in a reasonable range; pick the largest-magnitude pair of opposite signs
    const spreadNums = Array.from(block.matchAll(/([+-]\d{1,2}(?:\.\d)?)/g))
      .map(x => Number(x[1]))
      .filter(v => Math.abs(v) <= 60);

    // Choose a pair with opposite signs and maximum absolute value (looks most like the current line)
    let spreadAway = "", spreadHome = "";
    if (spreadNums.length) {
      // build all opposite-sign pairs
      let best = null;
      for (const a of spreadNums) for (const b of spreadNums) {
        if (Math.sign(a) !== Math.sign(b)) {
          const mag = Math.max(Math.abs(a), Math.abs(b));
          if (!best || mag > best.mag) best = { a, b, mag };
        }
      }
      if (best) {
        // Away appears first in widget -> assign positive (dog) to away if available else fallback
        spreadAway = best.a > 0 ? best.a : (best.b > 0 ? best.b : "");
        spreadHome = best.a < 0 ? best.a : (best.b < 0 ? best.b : "");
      }
    }

    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    if (DEBUG) console.log(`   [scrape] parsed => spread ${spreadAway}/${spreadHome}, ML ${mlAway}/${mlHome}, total ${total}`);
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch (e) {
    if (DEBUG) console.log("   [scrape] fail:", e?.message || e);
    return undefined;
  }
}

/* ─────────── Row selection ─────────── */
function mapCols(header) {
  const f = n => header.findIndex(h => (h || "").trim().toLowerCase() === n.toLowerCase());
  return {
    GAME_ID: f("game id"),
    DATE: f("date"),
    STATUS: f("status"),
    HALF: f("half score"),
    LA_S: f("live away spread"),
    LA_ML: f("live away ml"),
    LH_S: f("live home spread"),
    LH_ML: f("live home ml"),
    L_TOT: f("live total"),
  };
}
function chooseTargets(rows, col) {
  const out = [];
  for (let r = 1; r < rows.length; r++) {
    const id = (rows[r]?.[col.GAME_ID] || "").trim();
    if (!id) continue;
    const date = (rows[r]?.[col.DATE] || "").trim();
    const status = (rows[r]?.[col.STATUS] || "").trim();
    if (isFinalCell(status)) continue;
    if (GAME_ID && id === GAME_ID) { out.push({ r, id, reason: "GAME_ID" }); continue; }
    if (looksLiveStatus(status) || date === todayKey) out.push({ r, id });
  }
  return out;
}

/* ─────────── MAIN ─────────── */
async function main() {
  const grid = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range: `${TAB_NAME}!A1:Q2000` });
  const values = grid.data.values || [];
  if (!values.length) { console.log("Sheet empty."); return; }

  const col = mapCols(values[0]);
  const targets = chooseTargets(values, col);
  console.log(`Found ${targets.length} game(s) to update: ${targets.map(t => t.id).join(", ")}`);

  const data = [];

  for (const t of targets) {
    if (DEBUG) console.log(`\n=== 🏈 GAME ${t.id} ===`);
    const currentStatus = values[t.r]?.[col.STATUS] || "";
    if (isFinalCell(currentStatus)) continue;

    // 1) Status + Half
    let summary;
    try {
      summary = await espnSummary(t.id);
      const st = summary?.header?.competitions?.[0]?.status;
      const newStatus = shortStatusFromEspn(st);
      const nowFinal = isFinalFromEspn(st);

      if (DEBUG) console.log("   status:", JSON.stringify(newStatus));
      if (newStatus && newStatus !== currentStatus) data.push(makeValue(a1For(t.r, col.STATUS), newStatus));

      const half = parseHalfScore(summary);
      if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
      if (nowFinal) continue; // don’t write live odds for already-final
    } catch (e) {
      if (DEBUG) console.log("   summary warn:", e?.message || e);
    }

    // 2) Live odds from ESPN BET page scrape (preferred)
    const scraped = await scrapeGamePageLiveOdds(t.id);

    // 3) Fallback to summary pools only if scrape failed AND pool looks live-ish
    let fallback = undefined;
    try {
      const pools = []
        .concat(Array.isArray(summary?.pickcenter) ? summary.pickcenter : [])
        .concat(Array.isArray(summary?.odds) ? summary.odds : []);
      const espnBet = pools.find(p => /espn\s*bet/i.test(p?.provider?.name || ""));
      if (espnBet) {
        const a = espnBet?.awayTeamOdds || {};
        const h = espnBet?.homeTeamOdds || {};
        const tmp = {
          spreadAway: a?.spread ?? "",
          spreadHome: h?.spread ?? "",
          mlAway: a?.moneyLine ?? "",
          mlHome: h?.moneyLine ?? "",
          total: espnBet?.overUnder ?? espnBet?.total ?? "",
        };
        // Heuristic to reject stale pregame-like pools (very large ML or tiny movement vs open)
        const tooBigML = Math.abs(Number(tmp.mlAway || 0)) > 1000 || Math.abs(Number(tmp.mlHome || 0)) > 1000;
        const tooSmallSpread = Math.abs(Number(tmp.spreadAway || 0)) <= 3 && Math.abs(Number(tmp.spreadHome || 0)) <= 3;
        if (!tooBigML && !tooSmallSpread) fallback = tmp;
        if (DEBUG) console.log("   [pool] considered:", JSON.stringify(tmp), "accepted:", !!fallback);
      }
    } catch {}

    const live = scraped || fallback;
    if (DEBUG) console.log("   chosen source:", scraped ? "SCRAPE" : (fallback ? "POOL" : "NONE"), "=>", JSON.stringify(live));

    if (live) {
      const put = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
      put(col.LA_S,  live.spreadAway);
      put(col.LA_ML, live.mlAway);
      put(col.LH_S,  live.spreadHome);
      put(col.LH_ML, live.mlHome);
      put(col.L_TOT, live.total);
    } else if (DEBUG) {
      console.log("   ❌ no live odds found");
    }
  }

  if (!data.length) { console.log("No updates."); return; }
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: GOOGLE_SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data },
  });
  console.log(`✅ Updated ${data.length} cell(s).`);
}

main().catch(e => {
  console.error("Live updater fatal:", e?.message || e);
  process.exit(1);
});
