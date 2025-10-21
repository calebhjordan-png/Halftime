// orchestrator.mjs — drop-in replacement

import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";

/** Kickoff window and adaptive bounds */
const HALF_EARLY_MIN = Number(process.env.HALFTIME_EARLY_MIN ?? 60);
const HALF_LATE_MIN  = Number(process.env.HALFTIME_LATE_MIN  ?? 90);
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

/** Column names (Half Score header) */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ====== Helpers ====== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

/* parse service account (raw JSON or base64) */
function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw); // raw JSON
  const json = Buffer.from(raw, "base64").toString("utf8"); // Base64
  return JSON.parse(json);
}

/* --- ET formatters --- */
const ET_TZ = "America/New_York";

function fmtETTime(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true
  }).format(new Date(dateLike));
}

function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).format(new Date(dateLike)); // mm/dd/yyyy
}

function yyyymmddInET(d=new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(d));
  const get = k => parts.find(p=>p.type===k)?.value || "";
  return `${get("year")}${get("month")}${get("day")}`;
}

async function fetchJson(url) {
  log("GET", url);
  const res = await fetch(url, { headers: { "User-Agent": "halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, dates) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function summaryUrl(league, eventId) {
  const lg = normLeague(league);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${eventId}`;
}
function gameUrl(league, gameId) {
  const lg = normLeague(league);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}
function pickOdds(oddsArr=[]) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}
function mapHeadersToIndex(headerRow) {
  const map = {};
  headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }

/** Normalize numeric-ish value (preserve +) */
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** ===== Status formatting (fixed to ET) ===== */
function tidyStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const typeObj = evt.status?.type || comp.status?.type || {};
  const name = (typeObj.name || "").toUpperCase();
  const short = (typeObj.shortDetail || "").trim();
  if (name.includes("FINAL")) return "Final";
  if (name.includes("HALFTIME")) return "Half";
  if (name.includes("IN_PROGRESS") || name.includes("LIVE")) {
    return short || "In Progress";
  }
  // scheduled → format directly in ET (no TZ suffix)
  return fmtETTime(evt.date);
}

/** ===== Week label resolvers ===== */
function resolveWeekLabelFromCalendar(sb, eventDateISO) {
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(eventDateISO).getTime();
  for (const item of cal) {
    const entries = Array.isArray(item?.entries) ? item.entries : [item];
    for (const e of entries) {
      const label = (e?.label || e?.detail || e?.text || "").trim();
      const start = e?.startDate || e?.start;
      const end   = e?.endDate   || e?.end;
      if (!start || !end) continue;
      const s = new Date(start).getTime();
      const ed = new Date(end).getTime();
      if (!Number.isFinite(s) || !Number.isFinite(ed)) continue;
      if (t >= s && t <= ed) {
        if (/Week\s*\d+/i.test(label)) return label;
        if (label) return label; // Bowls/CFP too
      }
    }
  }
  return "";
}
function resolveWeekLabelNFL(sb, eventDateISO) {
  const wnum = sb?.week?.number;
  if (Number.isFinite(wnum)) return `Week ${wnum}`;
  return resolveWeekLabelFromCalendar(sb, eventDateISO) || "Regular Season";
}
function resolveWeekLabelCFB(sb, eventDateISO) {
  const text = (sb?.week?.text || "").trim();
  if (text) return text;
  return resolveWeekLabelFromCalendar(sb, eventDateISO) || "Regular Season";
}

/** ===== ML extractor (includes PickCenter shapes) ===== */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";

  // PickCenter blocks
  if (o && (o.awayTeamOdds || o.homeTeamOdds)) {
    const aObj = o.awayTeamOdds || {};
    const hObj = o.homeTeamOdds || {};
    awayML = awayML || numOrBlank(aObj.moneyLine ?? aObj.moneyline ?? aObj.money_line);
    homeML = homeML || numOrBlank(hObj.moneyLine ?? hObj.moneyline ?? hObj.money_line);
    if (awayML || homeML) return { awayML, homeML };
  }
  if (o && o.moneyline && (o.moneyline.away || o.moneyline.home)) {
    const awayClose = numOrBlank(o.moneyline.away?.close?.odds ?? o.moneyline.away?.open?.odds);
    const homeClose = numOrBlank(o.moneyline.home?.close?.odds ?? o.moneyline.home?.open?.odds);
    awayML = awayML || awayClose;
    homeML = homeML || homeClose;
    if (awayML || homeML) return { awayML, homeML };
  }

  // teamOdds[]
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid === String(awayId)) awayML = awayML || ml;
      if (tid === String(homeId)) homeML = homeML || ml;
    }
    if (awayML || homeML) return { awayML, homeML };
  }

  // nested competitors odds
  if (Array.isArray(o?.competitors)) {
    const findML = c => numOrBlank(c?.moneyLine ?? c?.moneyline ?? c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const aML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(awayId)));
    const hML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(homeId)));
    awayML = awayML || aML; homeML = homeML || hML;
    if (awayML || homeML) return { awayML, homeML };
  }

  // direct fields
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);
  if (awayML || homeML) return { awayML, homeML };

  // favorite mapping
  const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
  const favML = numOrBlank(o?.favoriteMoneyLine);
  const dogML = numOrBlank(o?.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) { awayML = awayML || favML; homeML = homeML || dogML; return { awayML, homeML }; }
    if (String(homeId) === favId) { homeML = homeML || favML; awayML = awayML || dogML; return { awayML, homeML }; }
  }

  // competitors[] variant
  if (Array.isArray(competitors)) {
    for (const c of competitors) {
      const ml = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!ml) continue;
      if (c.homeAway === "away") awayML = awayML || ml;
      if (c.homeAway === "home") homeML = homeML || ml;
    }
  }

  return { awayML, homeML };
}

/** ML fallback: /summary */
async function extractMLWithFallback(event, baseOdds, away, home) {
  const base = extractMoneylines(baseOdds || {}, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
  if (base.awayML && base.homeML) return base;

  try {
    const sum = await fetchJson(summaryUrl(LEAGUE, event.id));
    const candidates = [];
    if (Array.isArray(sum?.header?.competitions?.[0]?.odds)) candidates.push(...sum.header.competitions[0].odds);
    if (Array.isArray(sum?.odds)) candidates.push(...sum.odds);
    if (Array.isArray(sum?.pickcenter)) candidates.push(...sum.pickcenter);

    for (const cand of candidates) {
      const ml = extractMoneylines(cand, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
      if (ml.awayML || ml.homeML) return ml;
    }
  } catch (e) {
    warn("Summary fallback failed:", e?.message || e);
  }

  return base;
}

/** ===== NEW: robust spread extractor (team-specific lines) ===== */
function extractSpreads(o, awayId, homeId) {
  let away = "", home = "";

  const setFromFav = (favId, raw) => {
    const n = Number.parseFloat(raw);
    if (!Number.isFinite(n)) return;
    const s = Math.abs(n);
    if (String(favId) === String(awayId)) { away = `-${s}`; home = `+${s}`; }
    else if (String(favId) === String(homeId)) { home = `-${s}`; away = `+${s}`; }
  };

  // favorite + numeric spread
  const favId = o?.favorite ?? o?.favoriteTeamId ?? o?.favoriteId;
  const line  = o?.spread ?? o?.pointSpread ?? o?.handicap;
  if (favId != null && line != null) setFromFav(favId, line);

  // per-team lines
  if (!(away && home) && Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const sp  = t?.spread ?? t?.pointSpread ?? t?.handicap ?? t?.line;
      const n   = Number.parseFloat(sp);
      if (!Number.isFinite(n)) continue;
      if (tid === String(awayId)) away = n > 0 ? `+${Math.abs(n)}` : `${n}`;
      if (tid === String(homeId)) home = n > 0 ? `+${Math.abs(n)}` : `${n}`;
    }
  }

  // competitors[] fallback
  if (!(away && home) && Array.isArray(o?.competitors)) {
    for (const c of o.competitors) {
      const n = Number.parseFloat(c?.odds?.spread ?? c?.odds?.handicap ?? c?.odds?.pointSpread);
      if (!Number.isFinite(n)) continue;
      if (c.homeAway === "away") away = n > 0 ? `+${Math.abs(n)}` : `${n}`;
      if (c.homeAway === "home") home = n > 0 ? `+${Math.abs(n)}` : `${n}`;
    }
  }

  // text like "TeamX -3.5"
  if (!(away && home) && o?.details) {
    const m = String(o.details).match(/([+-]?\d+(\.\d+)?)/);
    if (m) {
      const n = Number.parseFloat(m[1]);
      if (favId != null) setFromFav(favId, n);
      else { away = n > 0 ? `+${Math.abs(n)}` : `${n}`; home = n > 0 ? `-${Math.abs(n)}` : `+${Math.abs(n)}`; }
    }
  }

  return { awaySpread: away || "", homeSpread: home || "" };
}

/** ===== Pregame row builder ===== */
function pregameRowFactory(sbForDay) {
  return async function pregameRow(event) {
    const comp = event.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    const isFinal = /final/i.test(event.status?.type?.name || comp.status?.type?.name || "");
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";

    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";
      const sp = extractSpreads(o0, away?.team?.id, home?.team?.id);
      awaySpread = sp.awaySpread;
      homeSpread = sp.homeSpread;

      const ml = await extractMLWithFallback(event, o0, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    } else {
      const ml = await extractMLWithFallback(event, {}, away, home);
      awayML = ml.awayML || "";
      homeML = ml.homeML || "";
    }

    const weekText = (normLeague(LEAGUE) === "nfl")
      ? resolveWeekLabelNFL(sbForDay, event.date)
      : resolveWeekLabelCFB(sbForDay, event.date);

    const statusClean = tidyStatus(event);
    const dateET = fmtETDate(event.date);

    return {
      values: [
        dateET,                 // Date
        weekText || "",         // Week
        statusClean,            // Status
        matchup,                // Matchup
        finalScore,             // Final Score
        awaySpread || "",       // Away Spread
        String(awayML || ""),   // Away ML
        homeSpread || "",       // Home Spread
        String(homeML || ""),   // Home ML
        String(total || ""),    // Total
        "", "", "", "", "", ""  // live cols (later)
      ],
      dateET,
      matchup
    };
  };
}

/** ===== Halftime-ish? ===== */
function isHalftimeLike(evt) {
  const t = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /Q2.*0:0?0/.test(short) || /HALF/.test(short);
}

/** ===== Parse clock / candidates / master delay ===== */
function parseShortDetailClock(shortDetail="") {
  const s = String(shortDetail).trim().toUpperCase();
  if (/HALF/.test(s) || /HALFTIME/.test(s)) return { quarter: 2, min: 0, sec: 0, halftime: true };
  if (/FINAL/.test(s)) return { final: true };
  const m = s.match(/Q?(\d)\D+(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return { quarter: Number(m[1]), min: Number(m[2]), sec: Number(m[3]) };
}
function minutesAfterKickoff(evt) {
  const kickoff = new Date(evt.date).getTime();
  const now = Date.now();
  return (now - kickoff) / 60000;
}
function clampRecheck(mins) {
  return Math.max(MIN_RECHECK_MIN, Math.min(MAX_RECHECK_MIN, Math.ceil(mins)));
}
function kickoff65CandidateMinutes(evt, rowValues, hmap) {
  const halfScore = (rowValues?.[hmap["half score"]] || "").toString().trim();
  if (halfScore) return null; // halftime already captured
  const mins = minutesAfterKickoff(evt);
  if (mins < 60 || mins > 80) return null;
  const remaining = 65 - mins;
  return clampRecheck(remaining <= 0 ? MIN_RECHECK_MIN : remaining);
}
function q2AdaptiveCandidateMinutes(evt) {
  const short = (evt.status?.type?.shortDetail || "").trim();
  const parsed = parseShortDetailClock(short);
  if (!parsed || parsed.final || parsed.halftime) return null;
  if (parsed.quarter !== 2) return null;
  const minutesLeft = parsed.min + parsed.sec / 60;
  if (minutesLeft >= 10) return null;
  return clampRecheck(2 * minutesLeft);
}

/** ===== Playwright DOM scrape for LIVE odds at halftime (one-time) ===== */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(()=>{});
    await page.waitForTimeout(500);

    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });
    const txt = (await section.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

    const spreadMatches = txt.match(/([+-]\d+(\.\d+)?)/g) || [];
    const totalOver = txt.match(/o\s?(\d+(\.\d+)?)/i);
    const totalUnder = txt.match(/u\s?(\d+(\.\d+)?)/i);
    const mlMatches = txt.match(/\s[+-]\d{2,4}\b/g) || [];

    const liveAwaySpread = spreadMatches[0] || "";
    const liveHomeSpread = spreadMatches[1] || "";
    const liveTotal = (totalOver && totalOver[1]) || (totalUnder && totalUnder[1]) || "";
    const liveAwayML = (mlMatches[0]||"").trim();
    const liveHomeML = (mlMatches[1]||"").trim();

    let halfScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (sc) halfScore = `${sc[1]}-${sc[2]}`;
    } catch {}

    return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore };
  } catch (err) {
    warn("Live DOM scrape failed:", err.message, url);
    return null;
  } finally {
    await browser.close();
  }
}

/** Batch writer */
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0) + i); }
class BatchWriter {
  constructor(tab){ this.tab = tab; this.acc = []; }
  add(row, colIdx, value){
    if (colIdx==null || colIdx<0) return;
    const range = `${this.tab}!${colLetter(colIdx)}${row}:${colLetter(colIdx)}${row}`;
    this.acc.push({ range, values: [[value]] });
  }
  async flush(sheets){
    if (!this.acc.length) return;
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: this.acc }
    });
    log(`🟩 Batched ${this.acc.length} cell update(s).`);
    this.acc = [];
  }
}

/** Center-align all columns A–P (idempotent) */
async function applyCenterFormatting(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME)?.properties?.sheetId;
  if (sheetId == null) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 0, startColumnIndex: 0, endColumnIndex: 16 },
            cell: { userEnteredFormat: { horizontalAlignment: "CENTER" } },
            fields: "userEnteredFormat.horizontalAlignment"
          }
        }
      ]
    }
  });
}

/** ====== MAIN ====== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing secrets.");
    process.exit(1);
  }
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Ensure tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets || []).map(s => s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:Z`,
  });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [COLS] }
    });
    header = COLS.slice();
  }
  const hmap = mapHeadersToIndex(header);
  const rows = values.slice(1);

  // Build index preferring Game ID if present
  const hGameId = hmap["game id"];
  const keyToRowNum = new Map();
  rows.forEach((r, i) => {
    const rowNum = i + 2;
    const gid = hGameId != null ? (r[hGameId] || "").toString().trim() : "";
    if (gid) {
      keyToRowNum.set(`id:${gid}`, rowNum);
    } else {
      const k = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
      keyToRowNum.set(k, rowNum);
    }
  });

  // Apply centering (A..P)
  await applyCenterFormatting(sheets);

  // Pull events (today or week) and keep a per-date scoreboard map
  const datesList = RUN_SCOPE === "week"
    ? (()=>{ const start = new Date(); return Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(start.getTime()+i*86400000))); })()
    : [ yyyymmddInET(new Date()) ];

  const sbByDate = new Map();
  let events = [];
  for (const d of datesList) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    sbByDate.set(d, sb);
    events = events.concat(sb?.events || []);
  }
  const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  // Pregame append (aligned to EXISTING header; includes Game ID if present)
  let appendBatch = [];
  for (const ev of events) {
    const eventDayKey = yyyymmddInET(new Date(ev.date));
    const sbForThisEvent = sbByDate.get(eventDayKey) || sbByDate.values().next().value || null;
    const buildPregame = pregameRowFactory(sbForThisEvent);
    const { values: rowVals, dateET, matchup } = await buildPregame(ev);

    if (keyToRowNum.has(`id:${ev.id}`) || keyToRowNum.has(keyOf(dateET, matchup))) continue;

    const aligned = new Array(header.length).fill("");
    const lowerH = Object.fromEntries(Object.entries(hmap).map(([k,v])=>[k.toLowerCase(),v]));
    const put = (name, val) => {
      const j = lowerH[name.toLowerCase()];
      if (j != null) aligned[j] = val;
    };

    const names = ["Date","Week","Status","Matchup","Final Score","Away Spread","Away ML","Home Spread","Home ML","Total","Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"];
    names.forEach((n, idx) => put(n, rowVals[idx]));

    if (hGameId != null) aligned[hGameId] = String(ev.id);

    appendBatch.push(aligned);
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: appendBatch },
    });
    // Refresh index after append
    const re = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const v2 = re.data.values || [];
    const hdr2 = v2[0] || header;
    const h2 = mapHeadersToIndex(hdr2);
    const gidIdx = h2["game id"];
    (v2.slice(1)).forEach((r, i) => {
      const rowNum = i + 2;
      const gid = gidIdx != null ? (r[gidIdx] || "").toString().trim() : "";
      if (gid) keyToRowNum.set(`id:${gid}`, rowNum);
      else keyToRowNum.set(keyOf(r[h2["date"]], r[h2["matchup"]]), rowNum);
    });
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // ===== Pass 1: finals/halftime writes + live status + collect candidate delays
  const batch = new BatchWriter(TAB_NAME);
  let masterDelayMin = null;

  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = fmtETDate(ev.date);

    const rowKey = (hGameId != null) ? `id:${ev.id}` : keyOf(dateET, matchup);
    const rowNum = keyToRowNum.get(rowKey);
    if (!rowNum) continue;

    const statusType = (ev.status?.type || comp.status?.type || {});
    const statusName = (statusType.name || "").toUpperCase();
    const statusShort = (statusType.shortDetail || "").trim();
    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;

    // Final score
    if (statusName.includes("FINAL")) {
      if (hmap["final score"] !== undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if (hmap["status"] !== undefined)      batch.add(rowNum, hmap["status"], "Final");
      continue;
    }

    // Live status clock (e.g., "Q2 5:32")
    if ((/IN_PROGRESS|LIVE/i).test(statusName) && hmap["status"] !== undefined) {
      batch.add(rowNum, hmap["status"], statusShort || "In Progress");
    }

    // Halftime write (one-time)
    const currentRow = (values[rowNum-1] || []);
    const halfAlready = (currentRow[hmap["half score"]] || "").toString().trim();
    const liveTotalVal = (currentRow[hmap["live total"]] || "").toString().trim();

    if (isHalftimeLike(ev) && !liveTotalVal && !halfAlready) {
      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (live) {
        const { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore } = live;
        const payload = [];
        const add = (name,val) => {
          const idx = hmap[name]; if (idx===undefined || !val) return;
          const range = `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
          payload.push({ range, values: [[val]] });
        };
        add("status", "Half");
        if (halfScore) add("half score", halfScore);
        add("live away spread", liveAwaySpread);
        add("live home spread", liveHomeSpread);
        add("live away ml", liveAwayML);
        add("live home ml", liveHomeML);
        add("live total", liveTotal);

        if (payload.length) {
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID,
            requestBody: { valueInputOption: "RAW", data: payload }
          });
        }
        log(`🕐 Halftime LIVE written for ${matchup}`);
        continue;
      }
    }

    // Collect adaptive candidate delays
    if (ADAPTIVE_HALFTIME) {
      const kickCand = kickoff65CandidateMinutes(ev, currentRow, hmap);
      if (kickCand != null) masterDelayMin = masterDelayMin == null ? kickCand : Math.min(masterDelayMin, kickCand);

      const q2Cand = q2AdaptiveCandidateMinutes(ev);
      if (q2Cand != null) masterDelayMin = masterDelayMin == null ? q2Cand : Math.min(masterDelayMin, q2Cand);
    }
  }

  await batch.flush(sheets);

  // ===== Single master sleep (if any) and one more halftime pass
  if (ADAPTIVE_HALFTIME && masterDelayMin != null) {
    const ms = Math.ceil(masterDelayMin * 60 * 1000);
    log(`⏳ Adaptive master wait: ${masterDelayMin} minute(s).`);
    await new Promise(r => setTimeout(r, ms));

    // Re-fetch events quickly and do one halftime pass
    let events2 = [];
    for (const d of datesList) {
      const sb2 = await fetchJson(scoreboardUrl(LEAGUE, d));
      events2 = events2.concat(sb2?.events || []);
    }
    const seen2 = new Set(); events2 = events2.filter(e => !seen2.has(e.id) && seen2.add(e.id));

    for (const ev of events2) {
      const comp = ev.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");

      const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
      const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
      const matchup = `${awayName} @ ${homeName}`;
      const dateET = fmtETDate(ev.date);

      const rowKey = (hGameId != null) ? `id:${ev.id}` : keyOf(dateET, matchup);
      const rowNum = keyToRowNum.get(rowKey);
      if (!rowNum) continue;

      const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
      if (!statusName.includes("HALFTIME")) continue;

      // read current row snapshot again to avoid double write
      const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
      const rowsNow = (snap.data.values || []).slice(1);
      const headerNow = (snap.data.values || [])[0] || header;
      const hmapNow = mapHeadersToIndex(headerNow);
      const currentRow = rowsNow[rowNum - 2] || [];
      const halfAlready = (currentRow[hmapNow["half score"]] || "").toString().trim();
      const liveTotalVal = (currentRow[hmapNow["live total"]] || "").toString().trim();
      if (halfAlready || liveTotalVal) continue;

      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (live) {
        const { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore } = live;
        const payload = [];
        const add = (name,val) => {
          const idx = hmapNow[name]; if (idx===undefined || !val) return;
          const range = `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
          payload.push({ range, values: [[val]] });
        };
        add("status", "Half");
        if (halfScore) add("half score", halfScore);
        add("live away spread", liveAwaySpread);
        add("live home spread", liveHomeSpread);
        add("live away ml", liveAwayML);
        add("live home ml", liveHomeML);
        add("live total", liveTotal);

        if (payload.length) {
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID,
            requestBody: { valueInputOption: "RAW", data: payload }
          });
        }
        log(`🕐 (Adaptive) Halftime LIVE written for ${matchup}`);
      }
    }
  }

  log("✅ Run complete.");
})().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
