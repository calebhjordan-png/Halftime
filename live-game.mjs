/**
 * live-game.mjs — Status + halftime/current score + LIVE odds (team-segmented parser)
 * Freeze rule:
 *   - "Half Score" + all Live odds (M–Q) update through Q1, Q2, and Halftime
 *   - Freeze as soon as Q3 starts (period >= 3)
 *   - "Status" always updates
 */

import { google } from "googleapis";

/* ───────────── ENV ───────────── */
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();

const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE  = String(process.env.DEBUG_MODE || "").toLowerCase() === "true";
const ONESHOT     = String(process.env.ONESHOT || "").toLowerCase() === "true";
const DEBUG_ODDS  = String(process.env.DEBUG_ODDS || "").toLowerCase() === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("❌ Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* ───────────── ESPN helpers ───────────── */
const pick = (o, p) => p.replace(/\[(\d+)\]/g, ".$1").split(".").reduce((a, k) => a?.[k], o);
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "halftime-live/2.7" } });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}
const summaryUrl = id => `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${id}`;

function parseStatus(sum) {
  const s = pick(sum, "header.competitions.0.status") || pick(sum, "competitions.0.status") || {};
  const t = s.type || {};
  return {
    shortDetail: t.shortDetail || s.shortDetail || "",
    state: (t.state || "").toUpperCase(),          // SCHEDULED / IN / FINAL
    name: (t.name || "").toUpperCase(),            // STATUS_SCHEDULED / STATUS_IN_PROGRESS / STATUS_FINAL / HALFTIME
    period: Number(s.period ?? 0),
    displayClock: s.displayClock ?? "0:00"
  };
}
function getTeams(sum) {
  const comps = pick(sum, "header.competitions.0.competitors") || pick(sum, "competitions.0.competitors") || [];
  const away = comps.find(c => c.homeAway === "away") || {};
  const home = comps.find(c => c.homeAway === "home") || {};
  return {
    awayName: away.team?.shortDisplayName || away.team?.displayName || "Away",
    homeName: home.team?.shortDisplayName || home.team?.displayName || "Home",
    awayScore: Number(away.score ?? 0),
    homeScore: Number(home.score ?? 0)
  };
}

/* ───────────── Freeze / Live gating ───────────── */
function isLiveOrHalf(st) {
  if (/FINAL/.test(st.name) || /FINAL/.test(st.state)) return false;
  if (/IN/.test(st.state) || /IN_PROGRESS/.test(st.name)) return true;
  if (/HALF/.test(st.name) || /HALF/i.test(st.shortDetail)) return true;
  return /\bQ[1-4]\b|\b\d{1,2}:\d{2}\s*-\s*(1st|2nd|3rd|4th)/i.test(st.shortDetail || "");
}
const isPreThird = st => Number(st.period || 0) < 3;   // Q1/Q2/halftime
const isFinal    = st => /FINAL/.test(st.name) || /FINAL/.test(st.state);

/* ───────────── Sheets helpers ───────────── */
async function sheetsClient() {
  const creds = JSON.parse(SA_JSON);
  const jwt = new google.auth.JWT(
    creds.client_email, null, creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}
function colMap(hdr = []) { const m = {}; hdr.forEach((h,i)=> m[(h||"").trim().toLowerCase()] = i); return m; }
function A1(i){ return String.fromCharCode("A".charCodeAt(0)+i); }
async function findRow(sheets){
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z2000` });
  const v = r.data.values || [];
  if (!v.length) return -1;
  const hdr = v[0] || [];
  const h = colMap(hdr);
  const gi = h["game id"];
  if (gi != null) {
    for (let i=1;i<v.length;i++){
      if ((v[i][gi]||"").toString().trim() === EVENT_ID) return i+1;
    }
  }
  return -1;
}
async function writeValues(sheets,row,kv){
  const header=(await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1`})).data.values?.[0]||[];
  const h=colMap(header);
  const data=[];
  for(const [key,val] of Object.entries(kv)){
    const j=h[key.toLowerCase()];
    if (j==null || val==null) continue;
    data.push({ range:`${TAB_NAME}!${A1(j)}${row}`, values:[[val]] });
  }
  if (!data.length) return 0;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ valueInputOption:"USER_ENTERED", data }
  });
  return data.length;
}

/* ───────────── ESPN BET scrape (team-segmented) ───────────── */
function normalizeTxt(s){ return String(s||"").replace(/\u00a0/g," ").replace(/[–—−]/g,"-").replace(/\s+/g," ").trim(); }
function evenToNum(s){ return /^even$/i.test(String(s)) ? 100 : Number(s); }
const tokenOK = (tok) =>
  /^[+\-]\d+(?:\.\d+)?$/.test(tok) ||   // spreads, prices
  /^[ou]\d+(?:\.\d+)?$/i.test(tok) ||  // totals o/u
  /^EVEN$/i.test(tok) ||
  /^[+\-]?\d{2,5}$/.test(tok);         // ML

function tokensFromSegment(text) {
  return text.split(" ").filter(Boolean).filter(tokenOK);
}

function parseLiveByTeams(text, awayName, homeName) {
  const t = normalizeTxt(text);
  const headIdx = t.indexOf("SPREAD TOTAL ML");
  if (headIdx < 0) return null;
  const after = t.slice(headIdx + "SPREAD TOTAL ML".length);

  const aRe = new RegExp(awayName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
  const hRe = new RegExp(homeName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
  const aIdx = after.search(aRe);
  const hIdx = after.search(hRe);
  if (aIdx < 0 || hIdx < 0) return null;

  const segAway = after.slice(aIdx, hIdx);
  const segHome = after.slice(hIdx);

  const toksA = tokensFromSegment(segAway);
  const toksH = tokensFromSegment(segHome);

  if (DEBUG_ODDS) {
    console.log("SCRAPE DEBUG: away seg tokens (last 10) →", toksA.slice(-10));
    console.log("SCRAPE DEBUG: home seg tokens (last 10) →", toksH.slice(-10));
  }

  if (toksA.length < 5 || toksH.length < 5) return null;

  const last5A = toksA.slice(-5); // spread, spreadJuice, total, totalJuice, ML
  const last5H = toksH.slice(-5);

  const awaySpread = Number(last5A[0]);
  const awayTotal  = Number(String(last5A[2]).slice(1)); // o/uN(.5) -> N(.5)
  const awayML     = evenToNum(last5A[4]);
  const homeSpread = Number(last5H[0]);
  const homeTotal  = Number(String(last5H[2]).slice(1));
  const homeML     = evenToNum(last5H[4]);

  const liveTotal = Number.isFinite(awayTotal) ? awayTotal :
                    Number.isFinite(homeTotal) ? homeTotal : null;

  if (![awaySpread,homeSpread,awayML,homeML,liveTotal].every(Number.isFinite)) return null;

  return { awaySpread, homeSpread, awayML, homeML, liveTotal };
}

async function scrapeEspnBet(gameId, league, awayName, homeName) {
  const { chromium } = await import("playwright");
  const lg = league === "college-football" ? "college-football" : "nfl";
  const url = `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    const loc = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await loc.waitFor({ timeout: 20000 }).catch(()=>{});
    const liveText = await (async () => { try { return await loc.innerText(); } catch { return ""; }})();
    const bodyText = await page.evaluate(() => document.body.innerText || "");

    const parsed =
      parseLiveByTeams(liveText, awayName, homeName) ||
      parseLiveByTeams(bodyText, awayName, homeName);

    if (!parsed) return null;
    const { awaySpread, homeSpread, awayML, homeML, liveTotal } = parsed;
    console.log("📊 ESPN BET parsed:", { awaySpread, homeSpread, awayML, homeML, liveTotal });
    return {
      liveAwaySpread: awaySpread, liveHomeSpread: homeSpread,
      liveAwayML: awayML,         liveHomeML: homeML,
      liveTotal
    };
  } catch (e) {
    console.log("⚠️ ESPN BET scrape failed:", e.message);
    return null;
  } finally {
    await page.close().catch(()=>{});
    await browser.close().catch(()=>{});
  }
}

/* ───────────── loop ───────────── */
function sleepMin(m){ return new Promise(r=>setTimeout(r, Math.max(60_000, Math.round(m*60_000)))); }

async function tickOnce(sheets){
  const row = await findRow(sheets);
  if (row < 0) return console.log(`No row for Game ID ${EVENT_ID}`);

  const sum = await fetchJson(summaryUrl(EVENT_ID));
  const st  = parseStatus(sum);
  const tm  = getTeams(sum);

  const statusTxt = st.shortDetail || st.state || "unknown";
  const scoreTxt  = `${tm.awayScore}-${tm.homeScore}`;
  console.log(`[${statusTxt}] ${tm.awayName} @ ${tm.homeName} | period=${st.period} clock=${st.displayClock}`);

  // STATUS: always
  await writeValues(sheets, row, { "Status": statusTxt });

  // HALF SCORE: update through Q1/Q2/halftime; freeze at start of Q3
  if (!isFinal(st) && isPreThird(st)) {
    await writeValues(sheets, row, { "Half Score": scoreTxt });
  } else {
    console.log("Half Score frozen (Q3+ or Final).");
  }

  // LIVE LINES: only when live/halftime AND before Q3; otherwise frozen
  if (isLiveOrHalf(st) && isPreThird(st)) {
    const scraped = await scrapeEspnBet(EVENT_ID, LEAGUE, tm.awayName, tm.homeName);
    if (scraped) {
      const payload = {
        "Live Away Spread": scraped.liveAwaySpread,
        "Live Away ML":     scraped.liveAwayML,
        "Live Home Spread": scraped.liveHomeSpread,
        "Live Home ML":     scraped.liveHomeML,
        "Live Total":       scraped.liveTotal
      };
      await writeValues(sheets, row, payload);
      console.log(`→ row ${row} | live odds written.`);
    } else {
      console.log("No live tokens parsed — leaving live columns unchanged.");
    }
  } else {
    console.log("Live odds frozen (not live/half OR Q3+).");
  }
}

(async () => {
  const sheets = await sheetsClient();
  if (ONESHOT) { await tickOnce(sheets); return; }

  let total = 0;
  for (;;) {
    await tickOnce(sheets);
    const wait = DEBUG_MODE ? 0.2 : 5; // minutes
    await sleepMin(wait);
    total += wait;
    if (total >= MAX_TOTAL_MIN) return;
  }
})().catch(e => { console.error("Fatal:", e); process.exit(1); });
