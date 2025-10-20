/**
 * live-game.mjs — Status + halftime score + LIVE odds (only when live)
 * - Never writes "Live ..." columns for scheduled games.
 * - Parses ESPN BET LIVE grid by position (skips CLOSE).
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
  const r = await fetch(url, { headers: { "User-Agent": "halftime-live/2.5" } });
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
    name: (t.name || "").toUpperCase(),            // STATUS_SCHEDULED / STATUS_IN_PROGRESS / STATUS_FINAL
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

/* ───────────── LIVE detection ───────────── */
function isLiveOrHalf(st) {
  if (/FINAL/.test(st.name) || /FINAL/.test(st.state)) return false;
  if (/IN/.test(st.state) || /IN_PROGRESS/.test(st.name)) return true;
  if (/HALF/.test(st.name) || /HALF/i.test(st.shortDetail)) return true;
  // fallback: shortDetail like "8:12 - 3rd", "Q2 6:20"
  return /\bQ[1-4]\b|\b\d{1,2}:\d{2}\s*-\s*(1st|2nd|3rd|4th)/i.test(st.shortDetail || "");
}

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
  // fallback by matchup+date if needed (not used here)
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

/* ───────────── ESPN BET scrape (positional) ───────────── */
function normalizeTxt(s){ return String(s||"").replace(/\u00a0/g," ").replace(/[–—−]/g,"-").replace(/\s+/g," ").trim(); }
function evenToNum(s){ return /^even$/i.test(String(s)) ? 100 : Number(s); }

/**
 * From the text of the LIVE ODDS block:
 *   ... SPREAD TOTAL ML <AWAY: 5 tokens> <HOME: 5 tokens> ...
 * Away tokens: spread, spreadJuice, total, totalJuice, ML
 * Home tokens: spread, spreadJuice, total, totalJuice, ML
 * We ignore all CLOSE values to the left.
 */
function parseLiveTokens(text) {
  const t = normalizeTxt(text);
  const idx = t.indexOf("SPREAD TOTAL ML");
  if (idx < 0) return null;
  const after = t.slice(idx + "SPREAD TOTAL ML".length);
  const raw = after.split(" ").filter(Boolean);
  const ok = (tok) =>
    /^[+\-]\d+(?:\.\d+)?$/.test(tok) ||   // spreads, prices
    /^[ou]\d+(?:\.\d+)?$/i.test(tok) ||  // totals o/u
    /^EVEN$/i.test(tok) ||
    /^[+\-]?\d{2,5}$/.test(tok);         // ML
  const tokens = raw.filter(ok);

  if (DEBUG_ODDS) {
    const ctx = t.slice(Math.max(0, idx - 140), Math.min(t.length, idx + 320));
    console.log("SCRAPE DEBUG: header context →", ctx);
    console.log("SCRAPE DEBUG: tokens[0..19] →", tokens.slice(0,20));
  }

  if (tokens.length < 10) return null;
  const t10 = tokens.slice(0,10);

  const awaySpread = Number(t10[0]);
  const awayTotal  = Number(String(t10[2]).slice(1));
  const awayML     = evenToNum(t10[4]);
  const homeSpread = Number(t10[5]);
  const homeTotal  = Number(String(t10[7]).slice(1));
  const homeML     = evenToNum(t10[9]);

  const liveTotal = Number.isFinite(awayTotal) ? awayTotal :
                    Number.isFinite(homeTotal) ? homeTotal : null;

  if (DEBUG_ODDS) console.log("SCRAPE DEBUG: t10 →", t10);

  return { awaySpread, homeSpread, awayML, homeML, liveTotal };
}

async function scrapeEspnBet(gameId, league) {
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
    const parsed = parseLiveTokens(liveText) || parseLiveTokens(bodyText);
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

/* ───────────── main loop ───────────── */
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

  // Always keep Status fresh (unless already Final).
  await writeValues(sheets, row, { "Status": statusTxt });

  // Half-time score
  const atHalf = /HALF/.test(st.name) || /HALF/i.test(st.shortDetail);
  if (atHalf) {
    await writeValues(sheets, row, { "Half Score": scoreTxt });
  }

  // Only write LIVE odds when the event is live/half — never for SCHEDULED.
  if (isLiveOrHalf(st)) {
    const scraped = await scrapeEspnBet(EVENT_ID, LEAGUE);
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
    console.log("Game not live — skipping live columns.");
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
