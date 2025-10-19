// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q) with halftime lock.
// Targets: GAME_ID focus OR rows with date = today/yesterday (ET) OR live-like status.
// Live odds source order: ESPN REST → ESPN summary pools → ESPN BET text scraper.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",                 // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                                // focus a single game when set
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
  DEBUG_MODE = "",                             // "1" for verbose logs
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

// ET calendar helpers (today + yesterday allowed)
const fmtET = (d) => new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  month: "2-digit", day: "2-digit", year: "2-digit",
}).format(d);
const todayKey = fmtET(new Date());
const yesterdayKey = fmtET(new Date(Date.now() - 86400000));

const normStr = (s) => (s || "").toString().trim();
const isFinalCell = s => /^final$/i.test(normStr(s));
function looksLiveStatus(s) {
  const x = normStr(s).toLowerCase();
  // covers strings like "Halftime", "9:15 - 2nd", "Q3", "In Progress", "OT"
  return /\b(1st|2nd|3rd|4th|q[1-4]|half|progress|ot|live)\b/.test(x);
}

// ESPN status helpers
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "";
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
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnOddsREST_A(gameId) {
  // legacy path
  const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/competitions/${gameId}/odds`;
  log("🔎 odds:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnOddsREST_B(gameId) {
  // events/{id}/competitions/{id}/odds path
  const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${gameId}/competitions/${gameId}/odds`;
  log("🔎 odds:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ─────────────── Live market selection & parsing (REST/POOL) ─────────────── */
function prefTokens(list = MARKET_PREFERENCE) {
  return (list || "")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
}
function textMatchesAny(text, tokens) {
  const t = normStr(text).toLowerCase();
  return tokens.some(tok => t.includes(tok));
}
function pickLiveFromREST(oddsPayload, tokens) {
  const items = oddsPayload?.items || [];
  if (!items.length) return undefined;
  const label = (mk)=>`${mk?.name||""} ${mk?.displayName||""} ${mk?.period?.displayName||""} ${mk?.period?.abbreviation||""}`;
  const isLiveish = (mk)=>textMatchesAny(label(mk), tokens);
  const typed = items.filter(isLiveish);
  const take = typed[0] || undefined;
  if (!take) return undefined;

  const b = (take?.books?.[0] || {});
  const aw = b?.awayTeamOdds || {};
  const hm = b?.homeTeamOdds || {};
  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const totalBook = b?.current?.total ?? b?.total;

  const spreadAway = n(aw?.current?.spread ?? aw?.spread);
  const spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
  const mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
  const total      = n(totalBook);

  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}
function pickLiveFromPools(summary, tokens) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  const label = p => `${p?.details || ""} ${p?.name || ""} ${p?.period || ""}`;
  const match = pools.find(p => textMatchesAny(label(p), tokens));
  if (!match) return undefined;

  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = match?.awayTeamOdds || {};
  const hm = match?.homeTeamOdds || {};

  const spreadAway = n(aw?.spread);
  const spreadHome = n(hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.moneyLine);
  const mlHome     = n(hm?.moneyLine);
  const total      = n(match?.overUnder ?? match?.total);

  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}

/* ───────── ESPN BET text scraper (robust to EVEN / missing prices) ───────── */
function regexEscape(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
function normalizeDashes(s) { return s.replace(/\u2212|\u2013|\u2014/g, "-").replace(/\uFE63|\uFF0B/g, "+"); }

function tokenizeOdds(rowText) {
  // remove o/u prefixes so numbers parse uniformly
  let t = normalizeDashes(rowText).replace(/\b[ou](\d+(?:\.\d+)?)/gi, "$1");
  const parts = t.split(/\s+/).filter(Boolean);
  const NUM = /^[-+]?(\d+(\.\d+)?|\.\d+)$/;
  return parts.filter(p => NUM.test(p)); // keep only numeric tokens
}

/**
 * Row token structure at ESPN BET (numeric-only after stripping text):
 *   [close?, closePrice?] spread spreadPrice? total totalPrice? moneyline
 * We index from the END so rows with "EVEN" (non-numeric) don't break positions.
 */
function scrapeRowNumeric(rowText) {
  const tok = tokenizeOdds(rowText);
  const L = tok.length;
  if (L < 5) return undefined; // need at least spread, total, ml
  const toNum = (v) => (v === "" ? "" : Number(v));
  const ml      = toNum(tok[L - 1]);
  const total   = toNum(tok[L - 3]);         // works for 5..7 tokens
  const spread  = toNum(tok[L - 5]);         // works for 5..7 tokens
  return { spread, total, ml };
}

function scrapeEspnBetText(summary, fullText) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const awayName = comp?.competitors?.find(c=>c.homeAway==="away")?.team?.displayName || "";
    const homeName = comp?.competitors?.find(c=>c.homeAway==="home")?.team?.displayName || "";
    if (!awayName || !homeName) return undefined;

    const txt = normalizeDashes(fullText || "");
    const aIdx = txt.search(new RegExp(regexEscape(awayName), "i"));
    const hIdx = txt.search(new RegExp(regexEscape(homeName), "i"));
    if (aIdx < 0 || hIdx < 0) return undefined;

    const aRow = txt.slice(aIdx, hIdx);
    const hRow = txt.slice(hIdx);

    const a = scrapeRowNumeric(aRow);
    const h = scrapeRowNumeric(hRow);
    if (!a || !h) return undefined;

    return {
      spreadAway: a.spread,
      spreadHome: h.spread,
      mlAway: a.ml,
      mlHome: h.ml,
      total: Number.isFinite(h.total) ? h.total : a.total
    };
  } catch {
    return undefined;
  }
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
    HALF: find("Half Score", 11),         // writing away-home like "10-7"
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

// halftime lock (don’t overwrite odds once game reaches/ passes halftime)
function isHalftimeOrLater(statusCell) {
  const t = normStr(statusCell).toLowerCase();
  return /\b(halftime|3rd|4th|end of 2nd|end of 3rd|ot)\b/.test(t);
}

// Target rows: GAME_ID match, live-like status, OR date is today/yesterday ET
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
    if (dateCell === todayKey || dateCell === yesterdayKey) {
      targets.push({ r, id, reason: "date window" });
    }
  }
  return targets;
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const values = await getValues();
    if (values.length === 0) { console.log("Sheet empty—nothing to do."); return; }
    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) { console.log("Nothing to update."); return; }
    console.log(`[${new Date().toISOString()}] Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);

    const tokens = prefTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      console.log(`\n=== 🏈 GAME ${t.id} ===`);
      const currentStatus = values[t.r]?.[col.STATUS] || "";

      // 1) SUMMARY (status + half score)
      let summary;
      let nowFinal = false;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        nowFinal         = isFinalFromEspn(compStatus);

        console.log(`   status: "${newStatus}"`);

        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
        if (nowFinal) continue; // stop here on finals
      } catch (e) {
        log("   summary warn:", e?.message || e);
      }

      // Lock odds at/after halftime
      if (isHalftimeOrLater(values[t.r]?.[col.STATUS])) {
        console.log("   ⛳ halftime lock — skipping odds update");
        continue;
      }

      // 2) LIVE ODDS – REST → pools → ESPN BET scraper
      let live = undefined;

      try {
        const oddsA = await espnOddsREST_A(t.id);
        live = pickLiveFromREST(oddsA, tokens);
      } catch (e) { log("   odds A failed:", e?.response?.status || e?.message); }

      if (!live) {
        try {
          const oddsB = await espnOddsREST_B(t.id);
          live = pickLiveFromREST(oddsB, tokens);
        } catch (e) { log("   odds B failed:", e?.response?.status || e?.message); }
      }

      if (!live && summary) {
        live = pickLiveFromPools(summary, tokens);
      }

      if (!live) {
        try {
          const url = `https://www.espn.com/${LEAGUE === "nfl" ? "nfl" : "college-football"}/game/_/gameId/${t.id}`;
          const { data: html } = await axios.get(url, { timeout: 15000 });
          const blockMatch = html.match(/ESPN BET SPORTSBOOK[\s\S]*?Note:\s*Odds and lines subject to change\./i);
          const flatText = blockMatch ? blockMatch[0].replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim() : "";
          if (DEBUG) {
            console.log("   [scrape] block length:", flatText.length);
            console.log("   [text] snippet:", flatText.slice(0, 220), "...");
          }
          if (flatText) {
            live = scrapeEspnBetText(summary, flatText);
          }
        } catch (e) {
          log("   scrape warn:", e?.message || e);
        }
      }

      if (live) {
        if (DEBUG) console.log("   → live picked:", JSON.stringify(live));
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else {
        console.log("   ❌ no live odds found");
      }
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
