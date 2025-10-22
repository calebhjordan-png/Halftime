import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG ====== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();           // nfl | college-football
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();      // today | week
const ADAPTIVE_HALFTIME = "1"; // always on

const GAME_IDS = (() => {
  const raw = (process.env.GAME_IDS || "").trim();
  if (!raw) return null;
  return raw.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
})();

/** ====== Constants ====== */
const ET_TZ = "America/New_York";
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

const DEFAULT_COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ===== Helpers ===== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}

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
function fmtStatusScheduled(dateLike) {
  const d = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, month: "2-digit", day: "2-digit"
  }).format(new Date(dateLike));
  const t = fmtETTime(dateLike);
  return `${d} - ${t}`;
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
/** Header alias resolution (handles short headers) */
function resolveHeaderIndex(hmap, name) {
  const key = name.toLowerCase();
  if (hmap[key] != null) return hmap[key];
  const aliases = {
    "away spread": ["a spread","awayspread"],
    "home spread": ["h spread","homespread"],
    "away ml": ["a ml","awayml"],
    "home ml": ["h ml","homeml"],
    "half score": ["h score","halfscore"],
    "live away spread": ["live a spread","liveawayspread"],
    "live home spread": ["live h spread","livehomespread"],
    "live away ml": ["live a ml","liveawayml"],
    "live home ml": ["live h ml","livehomeml"],
    "live total": ["live o/u","live ou","livetotal"],
  };
  for (const alt of (aliases[key]||[])) {
    if (hmap[alt] != null) return hmap[alt];
  }
  return undefined;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** Status text policy */
function tidyStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const typeObj = evt.status?.type || comp.status?.type || {};
  const name = (typeObj.name || "").toUpperCase();
  const short = (typeObj.shortDetail || "").trim();
  if (name.includes("FINAL")) return "Final";
  if (name.includes("HALFTIME")) return "Half";
  if (name.includes("IN_PROGRESS") || name.includes("LIVE")) return short || "In Progress";
  return fmtStatusScheduled(evt.date); // scheduled: MM/DD - Start Time
}

/** ===== Week label resolvers (per-date) ===== */
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
      if (Number.isFinite(s) && Number.isFinite(ed) && t >= s && t <= ed) {
        if (/Week\s*\d+/i.test(label)) return label;
        if (label) return label;
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

/** Moneylines (many ESPN shapes) */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";
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
  if (Array.isArray(o?.competitors)) {
    const findML = c => numOrBlank(c?.moneyLine ?? c?.moneyline ?? c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const aML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(awayId)));
    const hML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(homeId)));
    awayML = awayML || aML; homeML = homeML || hML;
    if (awayML || homeML) return { awayML, homeML };
  }
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);
  if (awayML || homeML) return { awayML, homeML };
  const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
  const favML = numOrBlank(o?.favoriteMoneyLine);
  const dogML = numOrBlank(o?.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) { awayML = awayML || favML; homeML = homeML || dogML; return { awayML, homeML }; }
    if (String(homeId) === favId) { homeML = homeML || favML; awayML = awayML || dogML; return { awayML, homeML }; }
  }
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
  } catch (e) { warn("Summary fallback failed:", e?.message || e); }
  return base;
}

/** ==== Spread extractor (team-specific lines) ==== */
function extractSpreads(o, awayId, homeId) {
  let away = "", home = "";
  const setFromFav = (favId, raw) => {
    const n = Number.parseFloat(raw);
    if (!Number.isFinite(n)) return;
    const s = Math.abs(n);
    if (String(favId) === String(awayId)) { away = `-${s}`; home = `+${s}`; }
    else if (String(favId) === String(homeId)) { home = `-${s}`; away = `+${s}`; }
  };
  const favId = o?.favorite ?? o?.favoriteTeamId ?? o?.favoriteId;
  const line  = o?.spread ?? o?.pointSpread ?? o?.handicap;
  if (favId != null && line != null) setFromFav(favId, line);

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
  if (!(away && home) && Array.isArray(o?.competitors)) {
    for (const c of o.competitors) {
      const n = Number.parseFloat(c?.odds?.spread ?? c?.odds?.handicap ?? c?.odds?.pointSpread);
      if (!Number.isFinite(n)) continue;
      if (c.homeAway === "away") away = n > 0 ? `+${Math.abs(n)}` : `${n}`;
      if (c.homeAway === "home") home = n > 0 ? `+${Math.abs(n)}` : `${n}`;
    }
  }
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

    const isFinal = /final/i.test(event.status?.type?.name || comp.status?.type?.name || "");
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "", favSide = null;

    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";
      const sp = extractSpreads(o0, away?.team?.id, home?.team?.id);
      awaySpread = sp.awaySpread;
      homeSpread = sp.homeSpread;
      if (awaySpread && awaySpread.startsWith("-")) favSide = "away";
      else if (homeSpread && homeSpread.startsWith("-")) favSide = "home";

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

    const matchupPlain = `${awayName} @ ${homeName}`;
    return {
      values: [
        dateET,                 // Date
        weekText || "",         // Week
        statusClean,            // Status
        matchupPlain,           // Matchup (plain; we underline via formatting)
        finalScore,             // Final Score
        awaySpread || "",       // Away Spread
        String(awayML || ""),   // Away ML
        homeSpread || "",       // Home Spread
        String(homeML || ""),   // Home ML
        String(total || ""),    // Total
        "", "", "", "", "", ""  // live cols
      ],
      dateET,
      matchupPlain,
      favSide,
      names: { awayName, homeName }
    };
  };
}

/** Halftime detection & adaptive wait */
function isHalftimeLike(evt) {
  const t = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /Q2.*0:0?0/.test(short) || /HALF/.test(short);
}
function parseShortDetailClock(shortDetail="") {
  const s = String(shortDetail).trim().toUpperCase();
  if (/HALF/.test(s) || /HALFTIME/.test(s)) return { quarter: 2, min: 0, sec: 0, halftime: true };
  if (/FINAL/.test(s)) return { final: true };
  const m = s.match(/Q?(\d)\D+(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return { quarter: Number(m[1]), min: Number(m[2]), sec: Number(m[3]) };
}
function clampRecheck(mins) {
  return Math.max(MIN_RECHECK_MIN, Math.min(MAX_RECHECK_MIN, Math.ceil(mins)));
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

/** LIVE odds scrape:
 *  1) read embedded JSON (window.__espnfitt__.page.content.gamepackage.pickcenter/odds)
 *  2) fallback to LIVE ODDS DOM text
 */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(()=>{});
    await page.waitForTimeout(400);

    // Try embedded JSON first
    const raw = await page.evaluate(() => {
      try {
        const gp = (window).__espnfitt__?.page?.content?.gamepackage;
        return gp ? JSON.stringify(gp) : null;
      } catch { return null; }
    });
    if (raw) {
      const gp = JSON.parse(raw);
      const candidates = []
        .concat(gp?.odds || [])
        .concat(gp?.pickcenter || [])
        .filter(Boolean);

      // choose the most recent/live candidate
      let best = null;
      for (const c of candidates) {
        if (!best) best = c;
        // prefer items that look live / have inGame flags
        if ((c.isLive || c.inGame || /live/i.test(c?.details || ""))) best = c;
      }

      if (best) {
        // spreads: always map to team
        const awayId = gp?.boxscore?.teams?.find?.(t=>t.homeAway==="away")?.team?.id ?? gp?.competitions?.[0]?.competitors?.find?.(x=>x.homeAway==="away")?.team?.id;
        const homeId = gp?.boxscore?.teams?.find?.(t=>t.homeAway==="home")?.team?.id ?? gp?.competitions?.[0]?.competitors?.find?.(x=>x.homeAway==="home")?.team?.id;

        let liveAwaySpread = "", liveHomeSpread = "", liveTotal = "", liveAwayML = "", liveHomeML = "";

        // total
        liveTotal = String(best.overUnder ?? best.total ?? "") || "";

        // spreads
        if (Array.isArray(best.teamOdds)) {
          for (const t of best.teamOdds) {
            const tid = String(t?.teamId ?? t?.team?.id ?? "");
            const sp  = t?.spread ?? t?.pointSpread ?? t?.handicap ?? t?.line;
            const n   = Number.parseFloat(sp);
            if (!Number.isFinite(n)) continue;
            const val = n > 0 ? `+${Math.abs(n)}` : `${n}`;
            if (tid === String(awayId)) liveAwaySpread = val;
            if (tid === String(homeId)) liveHomeSpread = val;
          }
        } else {
          // favorite + line
          const favId = best?.favorite ?? best?.favoriteTeamId ?? best?.favoriteId;
          const line  = best?.spread ?? best?.pointSpread ?? best?.handicap;
          if (favId != null && line != null) {
            const s = Math.abs(Number.parseFloat(line));
            if (String(favId) === String(awayId)) { liveAwaySpread = `-${s}`; liveHomeSpread = `+${s}`; }
            else if (String(favId) === String(homeId)) { liveHomeSpread = `-${s}`; liveAwaySpread = `+${s}`; }
          }
        }

        // moneylines
        if (Array.isArray(best.teamOdds)) {
          for (const t of best.teamOdds) {
            const tid = String(t?.teamId ?? t?.team?.id ?? "");
            const ml  = t?.moneyLine ?? t?.moneyline ?? t?.money_line;
            const val = numOrBlank(ml);
            if (tid === String(awayId) && val) liveAwayML = val;
            if (tid === String(homeId) && val) liveHomeML = val;
          }
        } else {
          const a = best?.awayTeamOdds || {};
          const h = best?.homeTeamOdds || {};
          liveAwayML = numOrBlank(a.moneyLine ?? a.moneyline ?? a.money_line) || liveAwayML;
          liveHomeML = numOrBlank(h.moneyLine ?? h.moneyline ?? h.money_line) || liveHomeML;
        }

        // half score (from gp boxscore)
        let halfScore = "";
        try {
          const aPts = Number(gp?.boxscore?.teams?.find(t=>t.homeAway==="away")?.score ?? 0);
          const hPts = Number(gp?.boxscore?.teams?.find(t=>t.homeAway==="home")?.score ?? 0);
          if (Number.isFinite(aPts) && Number.isFinite(hPts)) halfScore = `${aPts}-${hPts}`;
        } catch {}

        return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore };
      }
    }

    // Fallback to the LIVE ODDS section text
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 6000 });
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
    warn("Live scrape failed:", err.message, url);
    return null;
  } finally { await browser.close(); }
}

/** Sheets helpers */
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

/** Center cols + capture sheetId for rich text formatting */
async function getSheetIdAndCenter(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME);
  const sheetId = sheet?.properties?.sheetId;
  if (sheetId == null) return null;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        repeatCell: {
          range: { sheetId, startRowIndex: 0, startColumnIndex: 0, endColumnIndex: 16 },
          cell: { userEnteredFormat: { horizontalAlignment: "CENTER" } },
          fields: "userEnteredFormat.horizontalAlignment"
        }
      }]
    }
  });
  return sheetId;
}

/** Underline favorite in Matchup cell using rich text runs */
async function underlineFavoriteInMatchup(sheets, sheetId, rowNum, colIdx, awayName, homeName, favSide) {
  if (!sheetId || favSide == null || colIdx == null) return;
  const plain = `${awayName} @ ${homeName}`;
  const atPos = plain.indexOf("@");
  if (atPos < 0) return;

  const awayStart = 0;
  const awayEnd = awayName.length;
  const homeStart = atPos + 1 + 1; // skip "@ "
  const homeEnd = plain.length;

  let runs = [];
  if (favSide === "away") {
    runs = [
      { startIndex: awayStart, format: { underline: true } },
      { startIndex: awayEnd }, // stop underline at end of away
    ];
  } else if (favSide === "home") {
    runs = [
      { startIndex: homeStart, format: { underline: true } },
      { startIndex: homeEnd }, // stop underline at end of home
    ];
  } else return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        updateCells: {
          range: {
            sheetId,
            startRowIndex: rowNum-1,
            endRowIndex: rowNum,
            startColumnIndex: colIdx,
            endColumnIndex: colIdx+1
          },
          rows: [{
            values: [{
              userEnteredValue: { stringValue: plain },
              textFormatRuns: runs
            }]
          }],
          fields: "userEnteredValue,textFormatRuns"
        }
      }]
    }
  });
}

/** ===== MAIN ===== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) { console.error("Missing secrets."); process.exit(1); }
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
    spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z`,
  });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [DEFAULT_COLS] }
    });
    header = DEFAULT_COLS.slice();
  }
  const hmapRaw = mapHeadersToIndex(header);
  const rows = values.slice(1);

  // Build index preferring Game ID
  const idxGameId = resolveHeaderIndex(hmapRaw, "game id");
  const idxDate   = resolveHeaderIndex(hmapRaw, "date");
  const idxMatch  = resolveHeaderIndex(hmapRaw, "matchup");

  const keyToRow = new Map();
  rows.forEach((r,i)=>{
    const rowNum = i+2;
    const gid = idxGameId!=null ? (r[idxGameId]||"").toString().trim() : "";
    if (gid) keyToRow.set(`id:${gid}`, rowNum);
    else keyToRow.set(keyOf(r[idxDate]||"", r[idxMatch]||""), rowNum);
  });

  // Centering + sheetId (for underline)
  const sheetId = await getSheetIdAndCenter(sheets);

  // Pull events
  const datesList = RUN_SCOPE === "week"
    ? (()=>{ const start = new Date(); return Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(start.getTime()+i*86400000))); })()
    : [ yyyymmddInET(new Date()) ];

  // map date → scoreboard
  const sbByDate = new Map();
  let events = [];
  for (const d of datesList) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    sbByDate.set(d, sb);
    events = events.concat(sb?.events || []);
  }
  if (Array.isArray(GAME_IDS) && GAME_IDS.length) {
    events = events.filter(e => GAME_IDS.includes(String(e.id)));
  }
  const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  // Pregame append aligned to existing header (with aliases)
  let appendBatch = [];
  const hmap = new Proxy(hmapRaw, { get: (t,p)=> resolveHeaderIndex(hmapRaw, p) });

  for (const ev of events) {
    const eventDayKey = yyyymmddInET(new Date(ev.date));
    const sbForDay = sbByDate.get(eventDayKey) || [...sbByDate.values()][0] || null;
    const buildPregame = pregameRowFactory(sbForDay);
    const { values: rowVals, dateET, matchupPlain, favSide, names } = await buildPregame(ev);

    if (keyToRow.has(`id:${ev.id}`) || keyToRow.has(keyOf(dateET, matchupPlain))) continue;

    const aligned = new Array(header.length).fill("");
    const nameList = ["Date","Week","Status","Matchup","Final Score","Away Spread","Away ML","Home Spread","Home ML","Total","Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"];
    for (let i=0;i<nameList.length;i++){
      const idx = resolveHeaderIndex(hmapRaw, nameList[i]);
      if (idx != null) aligned[idx] = rowVals[i];
    }
    if (idxGameId != null) aligned[idxGameId] = String(ev.id);
    appendBatch.push({ aligned, ev, matchupPlain, favSide, names });
  }

  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: appendBatch.map(x=>x.aligned) },
    });
    // rebuild index
    const re = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const v2 = re.data.values || [];
    const hdr2 = v2[0] || header;
    const h2 = mapHeadersToIndex(hdr2);
    const id2 = resolveHeaderIndex(h2, "game id");
    const date2 = resolveHeaderIndex(h2, "date");
    const match2 = resolveHeaderIndex(h2, "matchup");

    (v2.slice(1)).forEach((r, i) => {
      const rowNum = i + 2;
      const gid = id2 != null ? (r[id2] || "").toString().trim() : "";
      if (gid) keyToRow.set(`id:${gid}`, rowNum);
      else keyToRow.set(keyOf(r[date2]||"", r[match2]||""), rowNum);
    });

    // apply underline for favorites in the rows we just wrote
    for (const item of appendBatch) {
      const rowNum = keyToRow.get(`id:${item.ev.id}`) || keyToRow.get(keyOf(fmtETDate(item.ev.date), item.matchupPlain));
      if (!rowNum) continue;
      const idxMatchCol = resolveHeaderIndex(hmapRaw, "matchup");
      await underlineFavoriteInMatchup(sheets, sheetId, rowNum, idxMatchCol, item.names.awayName, item.names.homeName, item.favSide);
    }
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // Pass 1: finals / live status / halftime candidates
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

    const rowKey = (idxGameId != null) ? `id:${ev.id}` : keyOf(dateET, matchup);
    const rowNum = keyToRow.get(rowKey);
    if (!rowNum) continue;

    const statusType = (ev.status?.type || comp.status?.type || {});
    const statusName = (statusType.name || "").toUpperCase();
    const statusShort = (statusType.shortDetail || "").trim();
    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;

    if (statusName.includes("FINAL")) {
      const iFS = resolveHeaderIndex(hmapRaw, "final score");
      const iST = resolveHeaderIndex(hmapRaw, "status");
      if (iFS!=null) batch.add(rowNum, iFS, scorePair);
      if (iST!=null) batch.add(rowNum, iST, "Final");
      continue;
    }

    if ((/IN_PROGRESS|LIVE/i).test(statusName)) {
      const iST = resolveHeaderIndex(hmapRaw, "status");
      if (iST!=null) batch.add(rowNum, iST, statusShort || "In Progress");
    }

    // halftime candidates (adaptive wait)
    const cand = q2AdaptiveCandidateMinutes(ev);
    if (cand != null) masterDelayMin = masterDelayMin==null ? cand : Math.min(masterDelayMin, cand);
  }

  await batch.flush(sheets);

  // Adaptive master wait = the single sleep to revisit games near halftime
  if (ADAPTIVE_HALFTIME === "1" && masterDelayMin != null) {
    log(`⏳ Adaptive master wait: ${masterDelayMin} minute(s).`);
    await new Promise(r => setTimeout(r, Math.ceil(masterDelayMin*60*1000)));

    // Refetch & halftime pass
    let events2 = [];
    for (const d of datesList) {
      const sb2 = await fetchJson(scoreboardUrl(LEAGUE, d));
      events2 = events2.concat(sb2?.events || []);
    }
    if (Array.isArray(GAME_IDS) && GAME_IDS.length) {
      events2 = events2.filter(e => GAME_IDS.includes(String(e.id)));
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

      const rowKey = (idxGameId != null) ? `id:${ev.id}` : keyOf(dateET, matchup);
      const rowNum = keyToRow.get(rowKey);
      if (!rowNum) continue;

      const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
      if (!statusName.includes("HALFTIME")) continue;

      // prefer embedded JSON live odds; fallback to DOM; fallback to summary for score
      let live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (!live) {
        try {
          const sum = await fetchJson(summaryUrl(LEAGUE, ev.id));
          const teams = (sum?.header?.competitions?.[0]?.competitors) || (sum?.competitions?.[0]?.competitors) || [];
          const a = teams.find(t=>t.homeAway==="away"), h = teams.find(t=>t.homeAway==="home");
          live = { halfScore: `${a?.score ?? ""}-${h?.score ?? ""}` };
        } catch {}
      }

      const payload = [];
      const add = (name,val) => {
        if (!val) return;
        const idx = resolveHeaderIndex(hmapRaw, name);
        if (idx==null) return;
        const range = `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
        payload.push({ range, values: [[val]] });
      };
      add("status", "Half");
      if (live?.halfScore) add("half score", live.halfScore);
      if (live?.liveAwaySpread) add("live away spread", live.liveAwaySpread);
      if (live?.liveHomeSpread) add("live home spread", live.liveHomeSpread);
      if (live?.liveAwayML) add("live away ml", live.liveAwayML);
      if (live?.liveHomeML) add("live home ml", live.liveHomeML);
      if (live?.liveTotal) add("live total", live.liveTotal);

      if (payload.length) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: { valueInputOption: "RAW", data: payload }
        });
      }
      log(`🕐 Halftime written for ${matchup}`);
    }
  }

  log("✅ Run complete.");
})().catch(err => { console.error("❌ Error:", err); process.exit(1); });
