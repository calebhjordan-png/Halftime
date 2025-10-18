// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q).
// Leaves pregame columns untouched. Supports optional GAME_ID focus.
// Live odds sources: ESPN odds REST -> Summary pools -> ESPN BET page HTML "LIVE ODDS".

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                           // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                             // optional: only update this game (when set, we still read the sheet to find the row)
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
  DEBUG_MODE = "0",
} = process.env;

const DEBUG = String(DEBUG_MODE).trim() === "1";

if (!GOOGLE_SHEET_ID) throw new Error("Missing GOOGLE_SHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT");

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

// Today key in **US/Eastern** to match the sheet’s date (MM/DD/YY)
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
  if (!s) return false;
  const x = s.toLowerCase();
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x) || /\d+:\d+\s*-\s*(1st|2nd|3rd|4th)/i.test(x);
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
const leaguePath = LEAGUE === "college-football" ? "football/college-football" : "football/nfl";

async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath}/summary?event=${gameId}`;
  if (DEBUG) console.log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnOddsA(gameId) {
  // competitions/{id}/odds (some sports use this)
  const url = `https://sports.core.api.espn.com/v2/sports/${leaguePath}/competitions/${gameId}/odds`;
  if (DEBUG) console.log("🔎 odds:", url);
  return axios.get(url, { timeout: 15000 }).then(r => r.data);
}
async function espnOddsB(gameId) {
  // events/{id}/competitions/{id}/odds (others use this)
  const url = `https://sports.core.api.espn.com/v2/sports/${leaguePath}/events/${gameId}/competitions/${gameId}/odds`;
  if (DEBUG) console.log("🔎 odds:", url);
  return axios.get(url, { timeout: 15000 }).then(r => r.data);
}

/* ─────────────── Live market selection & parsing (REST/pools) ────── */
function prefTokens(list = MARKET_PREFERENCE) {
  return (list || "")
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
}
function textMatchesAny(text, tokens) {
  const t = norm(text);
  return tokens.some(tok => t.includes(tok));
}

function liveFromOddsREST(oddsPayload, tokens) {
  try {
    const items = oddsPayload?.items || [];
    if (!items.length) return undefined;

    const firstBook = m => (m?.books?.[0] || {});
    const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));

    const candidates = items.filter(mk => {
      const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""}`;
      return textMatchesAny(label, tokens);
    });

    // prefer type-specific within candidates; otherwise just take the first candidate
    const pickTyped = (words) => {
      const typed = candidates.filter(mk => {
        const s = norm(`${mk?.name || ""} ${mk?.displayName || ""}`);
        return words.some(w => s.includes(w));
      });
      return (typed[0] || candidates[0]);
    };

    const mSpread = pickTyped(["spread", "line"]);
    const mTotal  = pickTyped(["total", "over", "under"]);

    let spreadAway = "", spreadHome = "", mlAway = "", mlHome = "", total = "";

    if (mSpread) {
      const b = firstBook(mSpread);
      const aw = b?.awayTeamOdds || {};
      const hm = b?.homeTeamOdds || {};
      spreadAway = n(aw?.current?.spread ?? aw?.spread ?? b?.current?.spread);
      spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
      mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
      mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
    }

    if (mTotal) {
      const b = firstBook(mTotal);
      total = n(b?.current?.total ?? b?.total);
    }

    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch {
    return undefined;
  }
}

function liveFromSummaryPools(summary, tokens) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);
  if (!pools.length) return undefined;

  // favor ESPN BET, then any matching a live-ish token; otherwise best-available
  const labelOf = p => `${p?.details || ""} ${p?.name || ""} ${p?.period || ""} ${p?.provider?.name || ""}`;
  const byEspnBet = pools.filter(p => /espn\s*bet/i.test(p?.provider?.name || ""));
  const tokenMatch = (arr) => arr.find(p => textMatchesAny(labelOf(p), tokens));

  const pick = tokenMatch(byEspnBet) || tokenMatch(pools) || byEspnBet[0] || pools[0];
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

/* ─────────── HTML fallback: ESPN BET "LIVE ODDS" scraper ─────────── */
async function scrapeGamePageLiveOdds(gameId) {
  const sportPath = (LEAGUE === "college-football") ? "college-football" : "nfl";
  const url = `https://www.espn.com/${sportPath}/game/_/gameId/${gameId}`;
  try {
    const { data: html } = await axios.get(url, { timeout: 15000 });
    if (DEBUG) console.log("🔎 page:", url);

    // Try to isolate the ESPN BET SPORTSBOOK live odds block; fall back to whole HTML if not found.
    const liveBlockMatch =
      html.match(/ESPN BET[\s\S]{0,4000}?LIVE ODDS[\s\S]{0,4000}?<\/section>/i) ||
      html.match(/LIVE ODDS[\s\S]{0,4000}?Odds by ESPN BET/i);
    const block = liveBlockMatch ? liveBlockMatch[0] : html;

    if (DEBUG) {
      console.log("   [scrape] snippet:");
      console.log(block.slice(0, 2000)); // keep logs moderate
    }

    // Totals: o51.5 / u51.5
    const totalMatches = [...block.matchAll(/\b[ou]\s?(\d{2,3}(?:\.\d)?)\b/ig)];
    const total = totalMatches.length ? Number(totalMatches[0][1]) : "";

    // Moneylines: +3000 / -7500 — ignore -110 juice
    const mlMatches = [...block.matchAll(/([+-]\d{3,5})/g)]
      .map(m => Number(m[1]))
      .filter(v => Math.abs(v) >= 300);
    let mlAway = "", mlHome = "";
    if (mlMatches.length) {
      // try to choose one positive and one negative (order on page normally away then home)
      mlAway = mlMatches.find(v => v > 0) ?? "";
      mlHome = mlMatches.find(v => v < 0) ?? "";
    }

    // Spreads: +21.5 / -21.5 (filter out moneylines we already grabbed)
    const spreadMatches = [...block.matchAll(/([+-]\d{1,2}(?:\.\d)?)/g)]
      .map(m => Number(m[1]))
      .filter(v => Math.abs(v) <= 60);
    let spreadAway = "", spreadHome = "";
    if (spreadMatches.length) {
      spreadAway = spreadMatches.find(v => v > 0) ?? "";
      spreadHome = spreadMatches.find(v => v < 0) ?? "";
    }

    if (DEBUG) {
      console.log(`   [scrape] parsed => spread ${spreadAway}/${spreadHome}, ML ${mlAway}/${mlHome}, total ${total}`);
    }
    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch (e) {
    if (DEBUG) console.log("   [scrape] failed:", e?.response?.status || e?.message || e);
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
    HALF: find("Half Score", 11),
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

// Row selection: GAME_ID focus, or “live-looking” rows, or rows with Date==today (ET). Skip finals.
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();   // MM/DD/YY
    const status   = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;

    if (GAME_ID) {
      if (id === GAME_ID) targets.push({ r, id, reason: "GAME_ID" });
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
    if (DEBUG) console.log(`[${ts}] Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);

    if (targets.length === 0) { console.log(`[${ts}] Nothing to update.`); return; }

    const tokens = prefTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      if (DEBUG) console.log(`\n=== 🏈 GAME ${t.id} ===`);

      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // 1) STATUS + HALF
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);

        if (DEBUG) console.log(`   status text: "${newStatus}"`);
        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (DEBUG && half) console.log(`   half score: ${half}`);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
        if (nowFinal) continue; // don't attempt live odds if already final
      } catch (e) {
        if (DEBUG) console.log("   summary fetch warn:", e?.response?.status || e?.message || e);
      }

      // 2) LIVE ODDS: REST -> summary pools -> HTML scrape
      let live;

      // REST A
      try {
        const oddsA = await espnOddsA(t.id);
        live = liveFromOddsREST(oddsA, tokens);
      } catch (e) {
        if (DEBUG) console.log("   ↪️ odds A failed:", e?.response?.status || e?.message || e);
      }

      // REST B (second shape)
      if (!live) {
        try {
          const oddsB = await espnOddsB(t.id);
          live = liveFromOddsREST(oddsB, tokens);
        } catch (e) {
          if (DEBUG) console.log("   ↪️ odds B failed:", e?.response?.status || e?.message || e);
        }
      }

      // Summary pools
      if (!live && summary) {
        const poolsLive = liveFromSummaryPools(summary, tokens);
        if (DEBUG && poolsLive) {
          console.log(`   pools picked =>`, JSON.stringify(poolsLive));
        }
        live = poolsLive || live;
      }

      // HTML scrape (ESPN BET page block)
      if (!live) {
        const scraped = await scrapeGamePageLiveOdds(t.id);
        if (DEBUG && scraped) console.log("   scrape picked =>", JSON.stringify(scraped));
        live = scraped || live;
      }

      if (live) {
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else {
        if (DEBUG) console.log("   no live market found after all sources.");
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
    console.error("Live updater fatal:", code);
    process.exit(1);
  }
}

main();
