// Football.mjs — Live-only updater (safe for existing Prefill/Finals)
// Columns touched: D (Status), L (H Score), M (H A Spread), N (H A ML), O (H H Spread), P (H H ML), Q (H Total)

import { google } from "googleapis";
import axios from "axios";
import * as playwright from "playwright";

/* ================== ENV ================== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase(); // 'nfl' | 'college-football'
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();         // 'today' | 'week'
const GAME_IDS  = (process.env.GAME_IDS || "").trim();                      // optional, comma-separated
const GHA_JSON  = String(process.env.GHA_JSON || "") === "1";
const ET_TZ     = "America/New_York";

/* ================== CONSTANTS ================== */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

// helpers to get URLs
function normLeague(x) { return (x === "ncaaf" || x === "college-football") ? "college-football" : "nfl"; }
function scoreboardUrl(lg, d) {
  lg = normLeague(lg);
  const extra = (lg === "college-football") ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
}
function gameUrl(lg, gameId) {
  lg = normLeague(lg);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}

/* ================== UTILS ================== */
const fmtET = (date, parts) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, ...parts }).format(new Date(date));

function yyyymmddET(d = new Date()) {
  const s = fmtET(d, { year: "numeric", month: "2-digit", day: "2-digit" }); // mm/dd/yyyy
  const [mm, dd, yyyy] = s.split("/");
  return `${yyyy}${mm}${dd}`;
}

async function fetchJSON(url) {
  const r = await axios.get(url, { timeout: 15000, headers: { "User-Agent": "football-live" } });
  return r.data;
}

function mapHeadersToIndex(h) {
  const m = {};
  (h || []).forEach((x, i) => (m[(x || "").trim().toLowerCase()] = i));
  return m;
}

function colLetter(i) {
  return String.fromCharCode("A".charCodeAt(0) + i);
}

/* ================== GOOGLE SHEETS ================== */
class Sheets {
  constructor(auth, spreadsheetId, tab) {
    this.api = google.sheets({ version: "v4", auth });
    this.id = spreadsheetId;
    this.tab = tab;
  }
  async readAtoQ() {
    const r = await this.api.spreadsheets.values.get({
      spreadsheetId: this.id,
      range: `${this.tab}!A1:Q`,
    });
    return r.data.values || [];
  }
  async batchUpdateCells(acc) {
    if (!acc.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId: this.id,
      requestBody: { valueInputOption: "RAW", data: acc },
    });
  }
}

/* ================== LIVE ODDS SCRAPE (Playwright) ================== */
/**
 * Scrape ESPN game page -> “LIVE ODDS” block.
 * Returns: { haSpread, haML, hhSpread, hhML, hTotal }
 */
async function scrapeLiveOddsFromESPN(lg, gameId) {
  const url = gameUrl(lg, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    // The “LIVE ODDS” block is rendered server-side; wait for it if present
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });

    const txt = (await section.innerText())
      .replace(/\u00a0/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    // Cut off anything before the “SPREAD” header so we never see “CLOSE” numbers.
    const afterSpread = txt.split(/\bSPREAD\b/i)[1] || txt;

    // 1) Moneylines (two biggest +/- prices) — away first, then home.
    const mlMatches = [...afterSpread.matchAll(/\s([+-]\d{3,5})\b/g)].map(m => m[1]);
    const mlUnique = [];
    for (const v of mlMatches) {
      if (!mlUnique.includes(v)) mlUnique.push(v);
      if (mlUnique.length === 2) break;
    }

    // 2) Spreads — detect lines that are followed by a price (EVEN or +/- odds)
    const spreadPairs = [...afterSpread.matchAll(/([+-]\d{1,2}(?:\.\d)?)\s+(EVEN|[+-]\d{2,4})/g)].map(m => m[1]);
    const spreadUnique = [];
    for (const v of spreadPairs) {
      if (!spreadUnique.includes(v)) spreadUnique.push(v);
      if (spreadUnique.length === 2) break;
    }

    // 3) Total — first o/u number after SPREAD chunk
    const totMatch = afterSpread.match(/[ou]\s?(\d+(?:\.\d)?)/i);
    const total = totMatch ? totMatch[1] : "";

    // Build result (some pages may miss one side late in games)
    const haSpread = spreadUnique[0] || "";
    const hhSpread = spreadUnique[1] || "";
    const haML = mlUnique[0] || "";
    const hhML = mlUnique[1] || "";
    const hTotal = total || "";

    return { haSpread, haML, hhSpread, hhML, hTotal };
  } catch (e) {
    console.error("Live scrape failed:", e?.message || e);
    return null;
  } finally {
    await browser.close();
  }
}

/* ================== STATUS HELPERS ================== */
function liveShortStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const short = (evt.status?.type?.shortDetail || comp.status?.type?.shortDetail || "").trim();
  // “10:27 - 4th”, “Half”, “End 3rd”, etc. (no timezone suffixes here)
  return short || "Live";
}

function isLive(evt) {
  const name = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return name.includes("IN_PROGRESS") || name.includes("LIVE");
}
function isFinal(evt) {
  const name = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return name.includes("FINAL");
}

/* ================== MAIN ================== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    const msg = "Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT.";
    if (GHA_JSON) return process.stdout.write(JSON.stringify({ ok: false, error: msg }) + "\n");
    console.error(msg);
    process.exit(1);
  }

  const CREDS =
    CREDS_RAW.trim().startsWith("{")
      ? JSON.parse(CREDS_RAW)
      : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));

  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);

  // Read sheet
  const table = await sheets.readAtoQ();
  const header = table[0] || [];
  const hmap = mapHeadersToIndex(header);
  const rows = table.slice(1);

  // Index: GameID -> row number
  const rowById = new Map();
  rows.forEach((r, i) => {
    const id = (r[hmap["game id"]] || "").toString().trim();
    if (id) rowById.set(id, i + 2); // +2 (1-based and header)
  });

  // Which dates to fetch
  let days = [yyyymmddET(new Date())];
  if (RUN_SCOPE === "week") {
    const start = new Date();
    days = Array.from({ length: 7 }, (_, i) => yyyymmddET(new Date(start.getTime() + i * 86400000)));
  }

  // Seed with explicit GAME_IDS if provided (so we ALWAYS try to update those)
  const forcedIds = (GAME_IDS ? GAME_IDS.split(",").map(s => s.trim()).filter(Boolean) : []);

  // Pull events
  let events = [];
  for (const d of days) {
    const sb = await fetchJSON(scoreboardUrl(LEAGUE_IN, d));
    events = events.concat(sb?.events || []);
  }

  // de-dup
  const seen = new Set();
  events = events.filter(e => !seen.has(e.id) && seen.add(e.id));

  // If GAME_IDS provided, filter to those first; otherwise only live ones
  let targets = events.filter(ev => forcedIds.length ? forcedIds.includes(String(ev.id)) : isLive(ev));

  // Prepare batch updates
  const updates = [];

  for (const ev of targets) {
    const gid = String(ev.id);
    const row = rowById.get(gid);
    if (!row) continue;

    // 1) Update live STATUS (D)
    if (!isFinal(ev)) {
      const statusText = liveShortStatus(ev);
      const idxStatus = hmap["status"];
      if (idxStatus !== undefined && statusText) {
        updates.push({
          range: `${TAB_NAME}!${colLetter(idxStatus)}${row}:${colLetter(idxStatus)}${row}`,
          values: [[statusText]],
        });
      }
    }

    // 2) Update live SCORE into L (H Score) -> "away-home"
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const hScore = `${away?.score ?? ""}-${home?.score ?? ""}`;
    const idxHScore = hmap["h score"];
    if (idxHScore !== undefined && (away?.score != null || home?.score != null)) {
      updates.push({
        range: `${TAB_NAME}!${colLetter(idxHScore)}${row}:${colLetter(idxHScore)}${row}`,
        values: [[hScore]],
      });
    }

    // 3) Scrape LIVE ODDS (Playwright)
    const live = await scrapeLiveOddsFromESPN(LEAGUE_IN, gid);
    if (!live) continue;

    const set = (colName, val) => {
      const idx = hmap[colName];
      if (idx === undefined || val == null || val === "") return;
      updates.push({
        range: `${TAB_NAME}!${colLetter(idx)}${row}:${colLetter(idx)}${row}`,
        values: [[String(val)]],
      });
    };

    set("h a spread", live.haSpread);
    set("h a ml", live.haML);
    set("h h spread", live.hhSpread);
    set("h h ml", live.hhML);
    set("h total", live.hTotal);
  }

  // Push updates
  await sheets.batchUpdateCells(updates);

  const summary = { ok: true, league: normLeague(LEAGUE_IN), tab: TAB_NAME, touched: updates.length };
  if (GHA_JSON) process.stdout.write(JSON.stringify(summary) + "\n");
  else console.log("Live update summary:", summary);
})().catch(err => {
  if (GHA_JSON) process.stdout.write(JSON.stringify({ ok: false, error: String(err?.message || err) }) + "\n");
  else {
    console.error("Fatal:", err?.stack || err);
    process.exit(1);
  }
});
