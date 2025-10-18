// live-game.mjs
// Updates: Status (D), Score (L), Half odds (M..Q) [A/H Spread, ML, Total].
// Leaves pregame columns untouched. Optional GAME_ID focus.
// Locks updates once game reaches halftime ("Halftime" or "End of 2nd").
// Live odds source order: ESPN REST → ESPN summary pools → ESPN BET scraper.

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

// Date key (US/Eastern)
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
  return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
}
function isHalftimeLocked(status) {
  const x = normStr(status).toLowerCase();
  return /\bhalftime\b/.test(x) || /\bend of 2nd\b/.test(x);
}

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
  log("🔎 summary:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnOddsREST_A(gameId) {
  const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/competitions/${gameId}/odds`;
  log("🔎 odds:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnOddsREST_B(gameId) {
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
  const take = items.find(isLiveish);
  if (!take) return undefined;

  const b = (take?.books?.[0] || {});
  const aw = b?.awayTeamOdds || {};
  const hm = b?.homeTeamOdds || {};
  const n = v => (v === null || v === undefined || v === "" ? "" : Number(v));
  const totalBook = b?.current?.total ?? b?.total;

  const spreadA = n(aw?.current?.spread ?? aw?.spread);
  const spreadH = n(hm?.current?.spread ?? hm?.spread ?? (spreadA !== "" ? -spreadA : ""));
  const mlA     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
  const mlH     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
  const total   = n(totalBook);

  const any = [spreadA, spreadH, mlA, mlH, total].some(v => v !== "");
  return any ? { spreadA, spreadH, mlA, mlH, total } : undefined;
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

  const spreadA = n(aw?.spread);
  const spreadH = n(hm?.spread ?? (spreadA !== "" ? -spreadA : ""));
  const mlA     = n(aw?.moneyLine);
  const mlH     = n(hm?.moneyLine);
  const total   = n(match?.overUnder ?? match?.total);

  const any = [spreadA, spreadH, mlA, mlH, total].some(v => v !== "");
  return any ? { spreadA, spreadH, mlA, mlH, total } : undefined;
}

/* ───────────── ESPN BET text scraper (ignores “Close” column) ───────────── */
function regexEscape(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
function normalizeDashes(s) { return s.replace(/\u2212|\u2013|\u2014/g, "-").replace(/\uFE63|\uFF0B/g, "+"); }

function tokenizeOdds(rowText) {
  let t = normalizeDashes(rowText).replace(/\b[ou](\d+(?:\.\d+)?)/gi, "$1");
  const parts = t.split(/\s+/).filter(Boolean);
  const NUM = /^[-+]?(\d+(\.\d+)?|\.\d+)$/;
  return parts.filter(p => NUM.test(p));
}
function scrapeEspnBetText(summary, fullText) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const aName = comp?.competitors?.find(c=>c.homeAway==="away")?.team?.displayName || "";
    const hName = comp?.competitors?.find(c=>c.homeAway==="home")?.team?.displayName || "";
    if (!aName || !hName) return undefined;

    const txt = normalizeDashes(fullText || "");
    const aIdx = txt.search(new RegExp(regexEscape(aName), "i"));
    const hIdx = txt.search(new RegExp(regexEscape(hName), "i"));
    if (aIdx < 0 || hIdx < 0) return undefined;

    const aRow = txt.slice(aIdx, hIdx);
    const hRow = txt.slice(hIdx);

    const aTok = tokenizeOdds(aRow);
    const hTok = tokenizeOdds(hRow);
    if (aTok.length < 7 || hTok.length < 7) return undefined;

    const num = v => (v === "" ? "" : Number(v));
    const spreadA = num(aTok[2]);
    const spreadH = num(hTok[2]);
    const totalA  = num(aTok[4]);
    const totalH  = num(hTok[4]);
    const total   = Number.isFinite(totalH) ? totalH : totalA;
    const mlA     = num(aTok[6]);
    const mlH     = num(hTok[6]);

    const any = [spreadA, spreadH, mlA, mlH, total].some(v => Number.isFinite(v));
    return any ? { spreadA, spreadH, mlA, mlH, total } : undefined;
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
    GAME_ID: find("game id", 0),
    DATE: find("date", 1),
    STATUS: find("status", 3),
    SCORE: find("score", 11),
    HA_S: find("half a spread", 12),
    HA_ML: find("half a ml", 13),
    HH_S: find("half h spread", 14),
    HH_ML: find("half h ml", 15),
    H_TOT: find("half total", 16),
  };
}

// Target rows
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const date = (row[col.DATE] || "").trim();
    const status = (row[col.STATUS] || "").trim();
    if (isFinalCell(status)) continue;

    if (GAME_ID && id === GAME_ID) { targets.push({ r, id }); continue; }
    if (looksLiveStatus(status)) { targets.push({ r, id }); continue; }
    if (date === todayKey) { targets.push({ r, id }); }
  }
  return targets;
}

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const values = await getValues();
    if (values.length === 0) return console.log("Sheet empty—nothing to do.");
    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) return console.log("Nothing to update.");
    console.log(`[${new Date().toISOString()}] Found ${targets.length} games to update`);

    const tokens = prefTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      console.log(`\n=== 🏈 GAME ${t.id} ===`);
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;
      if (isHalftimeLocked(currentStatus)) {
        console.log("   🔒 Locked at halftime — skipping further updates.");
        continue;
      }

      // 1️⃣ Status + Score
      let summary;
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus = shortStatusFromEspn(compStatus);
        const nowFinal = isFinalFromEspn(compStatus);

        console.log(`   status: "${newStatus}"`);
        if (newStatus && newStatus !== currentStatus)
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));

        const comp = summary?.header?.competitions?.[0];
        const home = comp?.competitors?.find(c => c.homeAway === "home");
        const away = comp?.competitors?.find(c => c.homeAway === "away");
        const hs = String(home?.score ?? "").trim();
        const as = String(away?.score ?? "").trim();
        if (hs && as && !Number.isNaN(+hs) && !Number.isNaN(+as))
          data.push(makeValue(a1For(t.r, col.SCORE), `${as}-${hs}`));

        if (nowFinal) continue;
      } catch (e) { log("   summary warn:", e?.message || e); }

      // 2️⃣ Half Odds
      let live;
      try { live = pickLiveFromREST(await espnOddsREST_A(t.id), tokens); } catch {}
      if (!live) try { live = pickLiveFromREST(await espnOddsREST_B(t.id), tokens); } catch {}
      if (!live && summary) live = pickLiveFromPools(summary, tokens);
      if (!live) {
        try {
          const url = `https://www.espn.com/${LEAGUE === "nfl" ? "nfl" : "college-football"}/game/_/gameId/${t.id}`;
          const { data: html } = await axios.get(url, { timeout: 15000 });
          const block = html.match(/ESPN BET SPORTSBOOK[\s\S]*?Note:\s*Odds and lines subject to change\./i);
          const flat = block ? block[0].replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim() : "";
          if (flat) live = scrapeEspnBetText(summary, flat);
        } catch {}
      }

      if (live) {
        if (DEBUG) console.log("   → half picked:", JSON.stringify(live));
        const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
        w(col.HA_S,  live.spreadA);
        w(col.HA_ML, live.mlA);
        w(col.HH_S,  live.spreadH);
        w(col.HH_ML, live.mlH);
        w(col.H_TOT, live.total);
      } else {
        console.log("   ❌ no half odds found");
      }
    }

    if (data.length)
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
