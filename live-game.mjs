// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q) from ESPN BET only.
// Optional GAME_ID focus. DEBUG_MODE=1 prints detailed discovery logs.

import axios from "axios";
import { google } from "googleapis";

/* ───────────────────── ENV ───────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                             // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                               // focus a single game id if set
  MARKET_PREFERENCE = "Live,In-Game,2H,Second Half,Halftime",
  DEBUG_MODE = "",
} = process.env;

const DEBUG = !!String(DEBUG_MODE || "").trim();
for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

/* ───────────── Google Sheets bootstrap ───────────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ─────────────────── Helpers ─────────────────── */
function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
const norm = s => (s || "").toLowerCase();
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
const isFinalCell = s => /^final$/i.test(String(s || ""));

// Today key in **US/Eastern** for column B
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

/* ───────────── ESPN status + summary ───────────── */
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

async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  DEBUG && console.log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

/* ───────────── Summary pools (may be pregame) ───────────── */
const PREF_BOOKS = ["ESPN BET"]; // constrain to ESPN BET only
const liveTokens = (MARKET_PREFERENCE || "")
  .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

function isTokenHit(s) {
  const t = norm(s);
  return liveTokens.some(tok => t.includes(tok));
}

function dumpPools(summary) {
  const pools = [];
  if (Array.isArray(summary?.pickcenter)) pools.push(...summary.pickcenter);
  if (Array.isArray(summary?.odds))       pools.push(...summary.odds);

  if (DEBUG) {
    if (!pools.length) console.log("📋 pools: (none in summary)");
    else {
      console.log("📋 pools (provider | label | aSpr hSpr aML hML total | live-ish?):");
      pools.forEach(p => {
        const prov  = p?.provider?.name || p?.provider || "";
        const label = [p?.details, p?.name, p?.period].filter(Boolean).join(" | ");
        const a = p?.awayTeamOdds || {};
        const h = p?.homeTeamOdds || {};
        const tot = p?.overUnder ?? p?.total ?? "";
        console.log(`   - ${prov} | ${label} | ${a?.spread ?? ""} ${h?.spread ?? ""} ${a?.moneyLine ?? ""} ${h?.moneyLine ?? ""} ${tot} | ${isTokenHit(label)}`);
      });
    }
  }
  return pools;
}

function pickPool(pools) {
  if (!pools.length) return undefined;
  const hasNumbers = p => {
    const a = p?.awayTeamOdds || {};
    const h = p?.homeTeamOdds || {};
    return (
      a?.moneyLine != null || h?.moneyLine != null ||
      a?.spread != null || h?.spread != null ||
      p?.overUnder != null || p?.total != null
    );
  };

  // 1) ESPN BET + live-ish label
  for (const book of PREF_BOOKS) {
    const hit = pools.find(p => {
      const prov = (p?.provider?.name || p?.provider || "").toUpperCase();
      const label = [p?.details, p?.name, p?.period].filter(Boolean).join(" ");
      return prov.includes(book) && isTokenHit(label) && hasNumbers(p);
    });
    if (hit) return hit;
  }
  // 2) ESPN BET (any label) with numbers
  for (const book of PREF_BOOKS) {
    const hit = pools.find(p => {
      const prov = (p?.provider?.name || p?.provider || "").toUpperCase();
      return prov.includes(book) && hasNumbers(p);
    });
    if (hit) return hit;
  }

  // 3) live-ish label (any book) as last resort
  const anyLive = pools.find(p => isTokenHit([p?.details, p?.name, p?.period].filter(Boolean).join(" ")) && hasNumbers(p));
  return anyLive;
}

function numbersFromPool(pool) {
  if (!pool) return undefined;
  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const aw = pool?.awayTeamOdds || {};
  const hm = pool?.homeTeamOdds || {};
  const spreadAway = n(aw?.spread);
  const spreadHome = n(hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
  const mlAway     = n(aw?.moneyLine);
  const mlHome     = n(hm?.moneyLine);
  const total      = n(pool?.overUnder ?? pool?.total);
  const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
  return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
}

/* ─────────── HTML fallback: ESPN BET LIVE ODDS ─────────── */
async function scrapeGamePageLiveOdds(gameId) {
  const sportPath = (LEAGUE === "college-football") ? "college-football" : "nfl";
  const url = `https://www.espn.com/${sportPath}/game/_/gameId/${gameId}`;
  try {
    const { data: html } = await axios.get(url, { timeout: 15000 });
    if (DEBUG) console.log("🔎 page:", url);

    // Strictly isolate the ESPN BET LIVE ODDS box
    const liveBlockMatch =
      html.match(/ESPN BET SPORTSBOOK[\s\S]*?LIVE ODDS[\s\S]*?<\/section>/i) ||
      html.match(/LIVE ODDS[\s\S]*?Odds by ESPN BET/i);
    const block = liveBlockMatch ? liveBlockMatch[0] : html;

    // Totals like o47.5 / u47.5 -> take the first number after o/u
    const totalMatches = [...block.matchAll(/\b[ou]\s?(\d{2,3}(?:\.\d)?)\b/ig)];
    const total = totalMatches.length ? Number(totalMatches[0][1]) : "";

    // Moneylines like +2000 / -7500; ignore -110 juice by requiring abs >= 300
    const mlMatches = [...block.matchAll(/>([+-]\d{3,5})<\/button>/g)]
      .map(m => Number(m[1]))
      .filter(v => Math.abs(v) >= 300);
    let mlAway = "", mlHome = "";
    if (mlMatches.length >= 2) {
      // first positive is away, first negative is home (widget lists away first)
      mlAway = mlMatches.find(v => v > 0) ?? "";
      mlHome = mlMatches.find(v => v < 0) ?? "";
      if (mlAway === "" || mlHome === "") {
        // fallback: most positive as away, most negative as home
        mlAway = mlAway || Math.max(...mlMatches);
        mlHome = mlHome || Math.min(...mlMatches);
      }
    }

    // Spreads like +16.5 / -16.5; ignore values with magnitude >= 100 (juice artifacts)
    const spreadMatches = [...block.matchAll(/>([+-]\d{1,2}(?:\.\d)?)<\/button>/g)]
      .map(m => Number(m[1]))
      .filter(v => Math.abs(v) < 100);
    let spreadAway = "", spreadHome = "";
    if (spreadMatches.length >= 2) {
      // choose first positive as away, first negative as home
      spreadAway = spreadMatches.find(v => v > 0) ?? "";
      spreadHome = spreadMatches.find(v => v < 0) ?? "";
      // fallback if both are same sign (rare)
      if (spreadAway === "" && spreadHome === "" && spreadMatches.length >= 2) {
        // pick two with opposite signs if present
        spreadAway = spreadMatches[0];
        spreadHome = -Math.abs(spreadAway);
      }
    }

    const gotAny = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
    if (DEBUG) {
      console.log(
        "   [scrape] spreads:", spreadAway, "/", spreadHome,
        " ML:", mlAway, "/", mlHome,
        " total:", total
      );
    }
    return gotAny ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch (e) {
    if (DEBUG) console.log("   [scrape] failed:", e?.response?.status || e?.message || e);
    return undefined;
  }
}

/* ───────────── values/A1 helpers ───────────── */
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

/* ───────────── Row selection ───────────── */
function chooseTargets(rows, col) {
  const targets = [];
  const focus = String(GAME_ID || "").trim();

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim();
    const status   = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;

    if (focus) { if (id === focus) targets.push({ r, id, reason: "GAME_ID" }); continue; }

    if (looksLiveStatus(status)) { targets.push({ r, id, reason: "live-like status" }); continue; }
    if (dateCell === todayKey)   { targets.push({ r, id, reason: "today" }); }
  }
  return targets;
}

/* ───────────────────── MAIN ───────────────────── */
async function main() {
  try {
    const ts = new Date().toISOString();
    const values = await getValues();
    if (!values.length) { console.log(`[${ts}] Sheet empty—nothing to do.`); return; }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);

    console.log(`[${ts}] Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);
    if (!targets.length) return;

    const data = [];

    for (const t of targets) {
      if (DEBUG) console.log(`\n=== 🏈 GAME ${t.id} ===`);

      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // STATUS + HALF
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus  = shortStatusFromEspn(compStatus);
        const nowFinal   = isFinalFromEspn(compStatus);

        DEBUG && console.log("   status text:", JSON.stringify(newStatus));
        if (newStatus && newStatus !== currentStatus) data.push(makeValue(a1For(t.r, col.STATUS), newStatus));

        const half = parseHalfScore(summary);
        if (half) {
          DEBUG && console.log("   half score:", half);
          data.push(makeValue(a1For(t.r, col.HALF), half));
        }
        if (nowFinal) { DEBUG && console.log("   final detected — skipping live odds."); continue; }
      } catch (e) {
        if (e?.response?.status !== 404) console.log(`Summary warn ${t.id}:`, e?.message || e);
      }

      // Summary pools (ESPN BET if possible)
      let live;
      const pools = dumpPools(summary);
      if (pools.length) {
        const chosen = pickPool(pools);
        live = numbersFromPool(chosen);
        if (DEBUG) {
          if (chosen) {
            const prov = chosen?.provider?.name || chosen?.provider || "";
            const label = [chosen?.details, chosen?.name, chosen?.period].filter(Boolean).join(" | ");
            console.log(`   chosen pool: ${prov} | ${label}`);
            console.log(`   → numbers: ${JSON.stringify(live)}`);
          } else {
            console.log("   no suitable ESPN BET pool.");
          }
        }
      }

      // HTML scrape fallback: ESPN BET LIVE ODDS widget
      if (!live) {
        const scraped = await scrapeGamePageLiveOdds(t.id);
        if (scraped) live = scraped;
      }

      if (live) {
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.LA_S,  live.spreadAway);
        w(col.LA_ML, live.mlAway);
        w(col.LH_S,  live.spreadHome);
        w(col.LH_ML, live.mlHome);
        w(col.L_TOT, live.total);
      } else if (DEBUG) {
        console.log("   ❌ no live numbers found this run.");
      }
    }

    if (!data.length) { console.log("✅ Nothing to write this run."); return; }

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
