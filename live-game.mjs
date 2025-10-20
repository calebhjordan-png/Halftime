/**
 * live-game.mjs — Status + current score + LIVE odds with ESPN BET fallback
 * Writes: Status | Half Score | Live Away/ML/Spread | Live Home/ML/Spread | Live Total
 */

import { google } from "googleapis";

/* ─────────── ENV ─────────── */
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();

const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE  = String(process.env.DEBUG_MODE || "").toLowerCase() === "true";
const ONESHOT     = String(process.env.ONESHOT || "").toLowerCase() === "true";
const DEBUG_ODDS  = String(process.env.DEBUG_ODDS || "").toLowerCase() === "true";
const FORCE_STATUS_WRITE = String(process.env.FORCE_STATUS_WRITE || "").toLowerCase() === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("❌ Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* ─────────── ESPN helpers ─────────── */
const pick = (o, p) => p.replace(/\[(\d+)\]/g, ".$1").split(".").reduce((a, k) => a?.[k], o);
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "halftime-live/2.3" } });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}
const summaryUrl = id => `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${id}`;

function parseStatus(sum) {
  const s = pick(sum, "header.competitions.0.status") || pick(sum, "competitions.0.status") || {};
  const t = s.type || {};
  return {
    shortDetail: t.shortDetail || s.shortDetail || "",
    state: t.state || "",
    name: (t.name || "").toUpperCase(),
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

/* ─────────── Odds via ESPN JSON ─────────── */
const ESPNBET = /espn\s*bet/i;
function looksLive(o) {
  const s = (o?.type || o?.name || "").toLowerCase();
  return o?.live === true || /live|in[-\s]?game/.test(s);
}
function coerceNum(v) { if (v === null || v === undefined || v === "") return null; const n = Number(v); return Number.isFinite(n) ? n : null; }

function extractJsonOdds(sum) {
  const comps = pick(sum, "competitions.0.odds") || [];
  const pickcenter = pick(sum, "pickcenter") || [];
  const all = [];
  if (Array.isArray(comps)) comps.forEach((o, i) => all.push({ src: `comp[${i}]`, o, group: "comp" }));
  if (Array.isArray(pickcenter)) pickcenter.forEach((o, i) => all.push({ src: `pickcenter[${i}]`, o, group: "pc" }));

  if (DEBUG_ODDS) {
    console.log("DEBUG_ODDS candidates:", all.length);
    for (const c of all) {
      console.log(`# ${c.src} ${c.o?.provider?.name} live=${looksLive(c.o)} spread=${c.o?.spread} ou=${c.o?.overUnder}`);
    }
  }

  let chosen = all.find(c => c.group==="comp" && ESPNBET.test(c.o?.provider?.name) && looksLive(c.o));
  if (!chosen) chosen = all.find(c => c.group==="comp" && ESPNBET.test(c.o?.provider?.name));
  if (!chosen) chosen = all.find(c => c.group==="comp" && looksLive(c.o));
  if (!chosen) chosen = all.find(c => c.group==="comp") || all[0];

  const o = chosen?.o || {};
  const src = chosen?.src || "";
  const provider = o?.provider?.name || "";

  return {
    src,
    provider,
    isLive: looksLive(o) && src.startsWith("comp"), // treat only competitions.* as potentially live
    awaySpread: coerceNum(o.spread),
    homeSpread: o.spread ? -coerceNum(o.spread) : null,
    total: coerceNum(o.overUnder ?? o.total),
    awayML: coerceNum(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline),
    homeML: coerceNum(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline),
    detail: o.details || ""
  };
}

/* ─────────── Fallback via ESPN BET page scrape (strict positional parse) ─────────── */

function evenTo100Str(s){ return /^even$/i.test(String(s)) ? "+100" : s; }
function evenToNum(s){ return /^even$/i.test(String(s)) ? 100 : Number(s); }

/**
 * Parses the LIVE ODDS grid text by position:
 * After the header "SPREAD TOTAL ML", tokens appear as:
 *   Away: <spreadLine> <spreadJuice> <totalLine> <totalJuice> <moneyline>
 *   Home: <spreadLine> <spreadJuice> <totalLine> <totalJuice> <moneyline>
 */
function parseLiveGridTextByPosition(text) {
  const t = text.replace(/\u00a0/g," ").replace(/[–—−]/g,"-").replace(/\s+/g," ").trim();
  const m = t.match(/SPREAD\s+TOTAL\s+ML\s+(.+)$/i);
  if (!m) return null;

  const rawTokens = m[1].split(" ").filter(Boolean);
  // Accept tokens that can appear in the 5-tuple sequence per row
  const tokenOk = (tok) =>
    /^[+\-]\d+(?:\.\d+)?$/.test(tok) ||      // spread line (+3.5) or spread juice (-120) — both signed numbers
    /^[ou]\d+(?:\.\d+)?$/i.test(tok) ||      // totals o35.5/u35.5
    /^EVEN$/i.test(tok) ||                   // EVEN
    /^[+\-]?\d{2,5}$/.test(tok);             // ML / price

  const tokens = rawTokens.filter(tokenOk);

  // Need at least 10 tokens for the two rows (5 each)
  if (tokens.length < 10) return null;

  // Pick first 10 tokens only — ignore any extras from other widgets
  const t10 = tokens.slice(0, 10);

  const seg = (a,i) => a[i];
  const awaySpreadLine = seg(t10,0);
  const awaySpreadJuice = seg(t10,1);
  const awayTotalLine = seg(t10,2);
  const awayTotalJuice = seg(t10,3);
  const awayML = seg(t10,4);

  const homeSpreadLine = seg(t10,5);
  const homeSpreadJuice = seg(t10,6);
  const homeTotalLine = seg(t10,7);
  const homeTotalJuice = seg(t10,8);
  const homeML = seg(t10,9);

  // Convert values
  const awaySpread = Number(awaySpreadLine);
  const homeSpread = Number(homeSpreadLine);
  const liveTotalA = Number(String(awayTotalLine).slice(1));
  const liveTotalH = Number(String(homeTotalLine).slice(1));
  const liveTotal = Number.isFinite(liveTotalA) ? liveTotalA :
                    Number.isFinite(liveTotalH) ? liveTotalH : null;

  return {
    awaySpread,
    homeSpread,
    awayML: evenToNum(awayML),
    homeML: evenToNum(homeML),
    liveTotal,
    // extras if you ever want them:
    awaySpreadJuice: evenTo100Str(awaySpreadJuice),
    homeSpreadJuice: evenTo100Str(homeSpreadJuice),
    awayTotalJuice: evenTo100Str(awayTotalJuice),
    homeTotalJuice: evenTo100Str(homeTotalJuice),
  };
}

async function scrapeEspnBet(gameId, league) {
  const { chromium } = await import("playwright");
  const lg = league === "college-football" ? "college-football" : "nfl";
  const url = `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });

    // Primary: read innerText of LIVE ODDS container
    const locator = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await locator.waitFor({ timeout: 20000 }); // give ESPN a bit more time
    const raw = await locator.innerText();
    let parsed = parseLiveGridTextByPosition(raw);

    // Fallback: parse whole page text if the specific section didn't resolve well
    if (!parsed) {
      const bodyTxt = (await page.evaluate(() => document.body.innerText || "")) || "";
      parsed = parseLiveGridTextByPosition(bodyTxt);
    }

    if (!parsed) {
      console.log("⚠️ ESPN BET: could not parse live grid from text");
      return null;
    }

    const { awaySpread, homeSpread, awayML, homeML, liveTotal } = parsed;
    console.log("📊 ESPN BET parsed:", { awaySpread, homeSpread, awayML, homeML, liveTotal });

    return {
      liveAwaySpread: awaySpread,
      liveHomeSpread: homeSpread,
      liveAwayML: awayML,
      liveHomeML: homeML,
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

/* ─────────── Sheets helpers ─────────── */
async function sheetsClient() {
  const creds = JSON.parse(SA_JSON);
  const jwt = new google.auth.JWT(
    creds.client_email, null, creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}
function colMap(hdr=[]) { const m={}; hdr.forEach((h,i)=>m[(h||"").trim().toLowerCase()]=i); return m; }
function A1(i){ return String.fromCharCode("A".charCodeAt(0)+i); }
async function findRow(sheets){
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:A2000` });
  const v = r.data.values || [];
  for (let i=1;i<v.length;i++) if ((v[i][0]||"").trim()===EVENT_ID) return i+1;
  return -1;
}
async function writeValues(sheets,row,kv){
  const header=(await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0]||[];
  const h=colMap(header);
  const data=[];
  for(const [k,val] of Object.entries(kv)){
    if (val==null && val!=="") continue;
    const j=h[k.toLowerCase()];
    if (j==null) continue;
    data.push({ range:`${TAB_NAME}!${A1(j)}${row}`, values:[[val]] });
  }
  if (!data.length) return 0;
  await sheets.spreadsheets.values.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ valueInputOption:"USER_ENTERED", data }});
  return data.length;
}

/* ─────────── tick ─────────── */
async function tickOnce(sheets){
  const row = await findRow(sheets);
  if (row < 0) return console.log(`No row with Game ID ${EVENT_ID}`);

  const sum = await fetchJson(summaryUrl(EVENT_ID));
  const st  = parseStatus(sum);
  const tm  = getTeams(sum);
  const status = st.shortDetail || st.state || "unknown";
  const score  = `${tm.awayScore}-${tm.homeScore}`;

  let odds = extractJsonOdds(sum);
  console.log(`[${status}] ${tm.awayName} @ ${tm.homeName} | src=${odds.src} live=${odds.isLive}`);

  const payload = {
    Status: status,
    "Half Score": score,
    "Live Away Spread": odds.awaySpread,
    "Live Away ML": odds.awayML,
    "Live Home Spread": odds.homeSpread,
    "Live Home ML": odds.homeML,
    "Live Total": odds.total
  };

  // Trigger fallback whenever JSON is not a true live odds source
  const needFallback = !odds.isLive || odds.src.startsWith("pickcenter");
  if (needFallback) {
    const scraped = await scrapeEspnBet(EVENT_ID, LEAGUE);
    if (scraped) {
      payload["Live Away Spread"] = scraped.liveAwaySpread ?? payload["Live Away Spread"];
      payload["Live Away ML"]     = scraped.liveAwayML     ?? payload["Live Away ML"];
      payload["Live Home Spread"] = scraped.liveHomeSpread ?? payload["Live Home Spread"];
      payload["Live Home ML"]     = scraped.liveHomeML     ?? payload["Live Home ML"];
      payload["Live Total"]       = scraped.liveTotal      ?? payload["Live Total"];
    }
  }

  // Don’t clobber Final; optionally don’t clobber Half unless forced
  const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`});
  const hdr = headerRes.data.values?.[0] || [];
  const h = colMap(hdr);
  const rowRes = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A${row}:Z${row}`});
  const cells = rowRes.data.values?.[0] || [];
  const curStatus = (cells[h["status"]] || "").toString().trim();
  if (/^Final$/i.test(curStatus)) delete payload["Status"];
  if (!FORCE_STATUS_WRITE && /^Half$/i.test(curStatus)) delete payload["Status"];

  await writeValues(sheets, row, payload);
  console.log(`→ row ${row} | live odds written.`);
}

/* ─────────── run ─────────── */
(async () => {
  const sheets = await sheetsClient();
  if (ONESHOT) { await tickOnce(sheets); return; }

  let total = 0;
  for (;;) {
    await tickOnce(sheets);
    const sleepMin = DEBUG_MODE ? 0.2 : 5;
    await new Promise(r => setTimeout(r, Math.max(60_000, sleepMin*60_000)));
    total += sleepMin;
    if (total >= MAX_TOTAL_MIN) return;
  }
})().catch(e => { console.error("Fatal:", e); process.exit(1); });
