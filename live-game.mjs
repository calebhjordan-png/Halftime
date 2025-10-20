/**
 * live-game.mjs — Status + current score + LIVE odds with ESPN BET fallback
 * Columns required:
 *   Status | Half Score | Live Away Spread | Live Away ML | Live Home Spread | Live Home ML | Live Total
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
  const r = await fetch(url, { headers: { "User-Agent": "halftime-live/2.0" } });
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
function coerceNum(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function extractJsonOdds(sum) {
  const comps = pick(sum, "competitions.0.odds") || [];
  const pickcenter = pick(sum, "pickcenter") || [];
  const all = [];
  if (Array.isArray(comps)) comps.forEach((o, i) => all.push({ src: `comp[${i}]`, o }));
  if (Array.isArray(pickcenter)) pickcenter.forEach((o, i) => all.push({ src: `pickcenter[${i}]`, o }));

  if (DEBUG_ODDS) {
    console.log("DEBUG_ODDS found", all.length);
    for (const c of all)
      console.log(`# ${c.src} ${c.o.provider?.name} live=${looksLive(c.o)} spread=${c.o.spread} ou=${c.o.overUnder}`);
  }

  let chosen = all.find(c => ESPNBET.test(c.o?.provider?.name) && looksLive(c.o));
  if (!chosen) chosen = all.find(c => ESPNBET.test(c.o?.provider?.name));
  if (!chosen) chosen = all.find(c => looksLive(c.o)) || all[0];
  const o = chosen?.o || {};
  return {
    src: chosen?.src,
    awaySpread: coerceNum(o.spread),
    homeSpread: o.spread ? -coerceNum(o.spread) : null,
    total: coerceNum(o.overUnder ?? o.total),
    awayML: coerceNum(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline),
    homeML: coerceNum(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline),
    detail: o.details || ""
  };
}

/* ─────────── Fallback via ESPN BET page scrape ─────────── */
async function scrapeEspnBet(gameId, league) {
  try {
    const { chromium } = await import("playwright");
    const lg = league === "college-football" ? "college-football" : "nfl";
    const url = `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 10000 });
    const text = (await section.innerText()).replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
    await browser.close();

    const nums = text.match(/[-+]?\d+(?:\.\d+)?/g) || [];
    const values = nums.map(Number);
    const spread = values.find(v => Math.abs(v) <= 60) ?? "";
    const total = values.find(v => v >= 30 && v <= 100) ?? "";
    const ml = values.find(v => Math.abs(v) >= 100) ?? "";
    console.log("📊 Scraped ESPN BET fallback:", { spread, total, ml });
    return {
      liveAwaySpread: spread,
      liveHomeSpread: -spread,
      liveAwayML: ml,
      liveHomeML: -ml,
      liveTotal: total
    };
  } catch (e) {
    console.log("⚠️ ESPN BET scrape failed:", e.message);
    return null;
  }
}

/* ─────────── Google Sheets helpers ─────────── */
async function sheetsClient() {
  const creds = JSON.parse(SA_JSON);
  const jwt = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}
function colMap(hdr = []) {
  const m = {};
  hdr.forEach((h, i) => (m[(h || "").trim().toLowerCase()] = i));
  return m;
}
function A1(i) {
  return String.fromCharCode("A".charCodeAt(0) + i);
}
async function findRow(sheets) {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:A2000`
  });
  const vals = r.data.values || [];
  for (let i = 1; i < vals.length; i++) {
    if ((vals[i][0] || "").trim() === EVENT_ID) return i + 1;
  }
  return -1;
}
async function writeValues(sheets, row, kv) {
  const header = (await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:Z1`
  })).data.values?.[0] || [];
  const h = colMap(header);
  const data = [];
  for (const [k, v] of Object.entries(kv)) {
    if (v == null && v !== "") continue;
    const j = h[k.toLowerCase()];
    if (j == null) continue;
    data.push({ range: `${TAB_NAME}!${A1(j)}${row}`, values: [[v]] });
  }
  if (!data.length) return 0;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data }
  });
  return data.length;
}

/* ─────────── Core tick ─────────── */
async function tickOnce(sheets) {
  const row = await findRow(sheets);
  if (row < 0) return console.log(`No row with Game ID ${EVENT_ID}`);

  const sum = await fetchJson(summaryUrl(EVENT_ID));
  const st = parseStatus(sum);
  const tm = getTeams(sum);
  let odds = extractJsonOdds(sum);

  const status = st.shortDetail || st.state || "unknown";
  const score = `${tm.awayScore}-${tm.homeScore}`;
  console.log(`[${status}] ${tm.awayName} @ ${tm.homeName} Q${st.period} (${st.displayClock})`);

  const payload = {
    Status: status,
    "Half Score": score,
    "Live Away Spread": odds.awaySpread,
    "Live Away ML": odds.awayML,
    "Live Home Spread": odds.homeSpread,
    "Live Home ML": odds.homeML,
    "Live Total": odds.total
  };

  const looksPregame = !odds.awayML && !odds.homeML && !odds.total;

  if (looksPregame) {
    const scraped = await scrapeEspnBet(EVENT_ID, LEAGUE);
    if (scraped) Object.assign(payload, {
      "Live Away Spread": scraped.liveAwaySpread,
      "Live Away ML": scraped.liveAwayML,
      "Live Home Spread": scraped.liveHomeSpread,
      "Live Home ML": scraped.liveHomeML,
      "Live Total": scraped.liveTotal
    });
  }

  await writeValues(sheets, row, payload);
}

/* ─────────── Run ─────────── */
(async () => {
  const sheets = await sheetsClient();
  if (ONESHOT) {
    await tickOnce(sheets);
    return;
  }
  let total = 0;
  for (;;) {
    await tickOnce(sheets);
    await new Promise(r => setTimeout(r, 5 * 60 * 1000));
    total += 5;
    if (total >= MAX_TOTAL_MIN) return;
  }
})().catch(e => {
  console.error("Fatal:", e);
  process.exit(1);
});
