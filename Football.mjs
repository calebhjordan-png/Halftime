// Football.mjs — Prefill + Finals + Live (safe + single pass)
// Node 20.x + googleapis + axios + playwright

import { google } from "googleapis";
import axios from "axios";
import * as playwright from "playwright";

/* ===================== ENV ===================== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase();        // "nfl" | "college-football" | "both"
const TAB_NAME  = (process.env.TAB_NAME || "").trim();                 // empty => auto: "NFL"/"CFB"
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();     // "today" | "week"
const GAME_IDS  = (process.env.GAME_IDS || "").trim();                 // "4017...,4017..."

const ET_TZ = "America/New_York";
const GHA_JSON = process.argv.includes("--gha");

/* ===================== Columns ===================== */
const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ===================== Helpers ===================== */
const fmtET = (d, o) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, ...o }).format(new Date(d));

const yyyymmddET = (d = new Date()) => {
  const p = fmtET(d, { year: "numeric", month: "2-digit", day: "2-digit" }).split("/");
  return p[2] + p[0] + p[1];
};

const weekLabelNFL = (sb, d) =>
  Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Week";

const weekLabelCFB = (sb) => (sb?.week?.text || "Week");

const scoreboardUrl = (lg, d) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${
    lg === "college-football" ? "&groups=80&limit=300" : ""
  }`;
const summaryUrl = (lg, id) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const gameUrl = (lg, id) => `https://www.espn.com/${lg}/game/_/gameId/${id}`;

const fetchJSON = async (u) =>
  (await axios.get(u, { headers: { "User-Agent": "football-orchestrator" }, timeout: 12000 })).data;

const lower = (s) => (s || "").toString().trim().toLowerCase();
const numStr = (v) => (v === 0 ? "0" : v == null ? "" : String(v));

const mapHeaders = (h) => {
  const m = {};
  h.forEach((x, i) => (m[lower(x)] = i));
  return m;
};

function tidyStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const tName = lower(evt.status?.type?.name || comp.status?.type?.name);
  const short = (evt.status?.type?.shortDetail || comp.status?.type?.shortDetail || "").trim();
  if (tName.includes("final")) return "Final";
  if (!tName || tName.includes("status")) return short;
  return short;
}

function noEDT(dt) {
  // "10/23 - 7:00 PM EDT" -> "10/23 - 7:00 PM"
  return (dt || "").replace(/\s+ET[D|]/i, "").trim();
}

/* ===================== Google Sheets mini SDK ===================== */
function letter(idx) { return String.fromCharCode(65 + idx); }

class Sheets {
  constructor(auth, id, tab) {
    this.api = google.sheets({ version: "v4", auth });
    this.id = id;
    this.tab = tab;
  }
  async ensureHeader() {
    const r = await this.api.spreadsheets.values.get({ spreadsheetId: this.id, range: `${this.tab}!A1:Q1` });
    const have = r.data.values?.[0] || [];
    if (have.length >= COLS.length) return;
    await this.api.spreadsheets.values.update({
      spreadsheetId: this.id,
      range: `${this.tab}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [COLS] }
    });
  }
  async all() {
    const r = await this.api.spreadsheets.values.get({ spreadsheetId: this.id, range: `${this.tab}!A1:Q` });
    return r.data.values || [];
  }
  async batch(updates) {
    if (!updates.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId: this.id,
      requestBody: { valueInputOption: "RAW", data: updates }
    });
  }
  async formatMatchup(row, value, underlineIndex = null, underlineLen = 0, boldWinnerIndex = null, boldLen = 0) {
    // Build safe textFormatRuns
    const runs = [];
    const len = value.length;
    // Start with base (no underline/bold)
    runs.push({ startIndex: 0, format: { underline: false, bold: false } });
    // Underline segment
    if (underlineIndex != null && underlineIndex >= 0 && underlineIndex < len) {
      runs.push({ startIndex: underlineIndex, format: { underline: true, bold: false } });
      const endU = Math.min(len, underlineIndex + Math.max(1, underlineLen));
      if (endU < len) runs.push({ startIndex: endU, format: { underline: false, bold: false } });
    }
    // Bold segment
    if (boldWinnerIndex != null && boldWinnerIndex >= 0 && boldWinnerIndex < len) {
      runs.push({ startIndex: boldWinnerIndex, format: { bold: true } });
      const endB = Math.min(len, boldWinnerIndex + Math.max(1, boldLen));
      if (endB < len) runs.push({ startIndex: endB, format: { bold: false } });
    }

    // Send updateCells request for a single cell (E column)
    const meta = await this.api.spreadsheets.get({ spreadsheetId: this.id });
    const sheet = meta.data.sheets.find(s => s.properties.title === this.tab);
    if (!sheet) return;
    const sheetId = sheet.properties.sheetId;

    await this.api.spreadsheets.batchUpdate({
      spreadsheetId: this.id,
      requestBody: {
        requests: [{
          updateCells: {
            range: {
              sheetId,
              startRowIndex: row - 1,
              endRowIndex: row,
              startColumnIndex: 4,
              endColumnIndex: 5
            },
            rows: [{
              values: [{
                userEnteredValue: { stringValue: value },
                textFormatRuns: runs
              }]
            }],
            fields: "userEnteredValue,textFormatRuns"
          }
        }]
      }
    });
  }
}

/* ===================== Odds helpers ===================== */
function pickEspnBet(arr = []) {
  if (!Array.isArray(arr)) return null;
  return (
    arr.find(o => /espn\s*bet/i.test(o.provider?.name || o.provider?.displayName || "")) ||
    arr[0] || null
  );
}
function moneylinesFrom(odds, awayId, homeId) {
  let aML = "", hML = "";
  const a = odds?.awayTeamOdds || {}, h = odds?.homeTeamOdds || {};
  if (a?.moneyLine != null) aML = String(a.moneyLine);
  if (h?.moneyLine != null) hML = String(h.moneyLine);
  // Fallbacks
  if (!aML && typeof odds?.moneyline?.away?.close?.odds !== "undefined") {
    aML = String(odds.moneyline.away.close.odds);
  }
  if (!hML && typeof odds?.moneyline?.home?.close?.odds !== "undefined") {
    hML = String(odds.moneyline.home.close.odds);
  }
  return { aML, hML };
}
function spreadsFrom(odds, awayId, homeId) {
  let aSpr = "", hSpr = "";
  const favId = String(odds?.favorite || odds?.favoriteTeamId || "");
  const spread = Number.isFinite(odds?.spread) ? Number(odds.spread) :
    (typeof odds?.spread === "string" ? parseFloat(odds.spread) : NaN);
  if (favId && !Number.isNaN(spread)) {
    if (String(awayId) === favId) {
      aSpr = `-${Math.abs(spread)}`;
      hSpr = `+${Math.abs(spread)}`;
    } else {
      hSpr = `-${Math.abs(spread)}`;
      aSpr = `+${Math.abs(spread)}`;
    }
  }
  return { aSpr, hSpr };
}

/* ===================== LIVE via Playwright ===================== */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle").catch(() => {});
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });
    const txt = (await section.innerText()).replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();

    // Extract live SPREADS (away then home) — these appear as ±number near SPREAD column
    const spreadMatches = txt.match(/([+-]\d+(?:\.\d+)?)/g) || [];
    // Extract LIVE MLs — usually long odds like -4000 +1300
    const mlMatches = txt.match(/\s[+-]\d{2,5}\b/g) || [];
    // Extract LIVE total: look for oXX.X or uXX.X near TOTAL
    const totalOver = txt.match(/o\s?(\d+(?:\.\d+)?)/i);
    const totalUnder = txt.match(/u\s?(\d+(?:\.\d+)?)/i);

    const haSpread = spreadMatches[0] || "";
    const hhSpread = spreadMatches[1] || "";
    const haML = (mlMatches[0] || "").trim();
    const hhML = (mlMatches[1] || "").trim();
    const hTotal = (totalOver && totalOver[1]) || (totalUnder && totalUnder[1]) || "";

    // Find current score (any "##-##")
    let hScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g, " ");
      const m = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (m) hScore = `${m[1]}-${m[2]}`;
    } catch {}

    return { haSpread, hhSpread, haML, hhML, hTotal, hScore };
  } catch (e) {
    return null;
  } finally {
    await browser.close();
  }
}

/* ===================== Prefill row builder ===================== */
function buildPregameRow(sbForDay, league) {
  return async function preRow(event) {
    const comp = event.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    const isFinal = /final/i.test(comp.status?.type?.name || event.status?.type?.name || "");
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    const odds = pickEspnBet(comp.odds || event.odds || []);
    let aSpr = "", hSpr = "", total = "", aML = "", hML = "";
    if (odds) {
      total = odds.overUnder ?? odds.total ?? "";
      const s = spreadsFrom(odds, away?.team?.id, home?.team?.id);
      aSpr = s.aSpr; hSpr = s.hSpr;
      const m = moneylinesFrom(odds, away?.team?.id, home?.team?.id);
      aML = m.aML; hML = m.hML;
    }

    const week =
      league === "college-football" ? weekLabelCFB(sbForDay, event.date) : weekLabelNFL(sbForDay, event.date);

    // Scheduled status without EDT
    let status = event.status?.type?.shortDetail || fmtET(event.date, { month: "2-digit", day: "2-digit" }) + " - " +
      fmtET(event.date, { hour: "numeric", minute: "2-digit", hour12: true });
    status = noEDT(status);

    return {
      id: String(event.id),
      matchup,
      awayName,
      homeName,
      values: [
        String(event.id),
        fmtET(event.date, { month: "2-digit", day: "2-digit", year: "numeric" }),
        week,
        status,
        matchup,
        finalScore,
        numStr(aSpr),
        numStr(aML),
        numStr(hSpr),
        numStr(hML),
        numStr(total),
        "", "", "", "", "", ""
      ]
    };
  };
}

/* ===================== MAIN ===================== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    const msg = "Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT";
    if (GHA_JSON) return process.stdout.write(JSON.stringify({ ok: false, error: msg }) + "\n");
    console.error(msg);
    process.exit(1);
  }

  const CREDS = CREDS_RAW.trim().startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));

  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const client = await auth.getClient();

  const leagues = LEAGUE_IN === "both" ? ["nfl", "college-football"] : [LEAGUE_IN];

  let touched = 0;

  for (const league of leagues) {
    const tab = TAB_NAME || (league === "college-football" ? "CFB" : "NFL");
    const sh = new Sheets(client, SHEET_ID, tab);
    await sh.ensureHeader();

    const allVals = await sh.all();
    const header = allVals[0] || COLS;
    const hmap = mapHeaders(header);
    const rows = allVals.slice(1);

    // Build index by Game ID
    const rowById = new Map();
    rows.forEach((r, i) => {
      const gid = (r[hmap["game id"]] || "").toString().trim();
      if (gid) rowById.set(gid, i + 2);
    });

    // Fetch scoreboard events
    const days =
      RUN_SCOPE === "today"
        ? [yyyymmddET(new Date())]
        : Array.from({ length: 7 }, (_, i) => yyyymmddET(new Date(Date.now() + i * 86400000)));

    let events = [];
    for (const d of days) {
      const sb = await fetchJSON(scoreboardUrl(league, d));
      events = events.concat(sb?.events || []);
      if (!globalThis._firstSB) globalThis._firstSB = sb;
    }
    // If specific GAME_IDS were passed, include those (and de-dupe)
    const ids = new Set(
      GAME_IDS
        .split(",")
        .map(s => s.trim())
        .filter(Boolean)
    );
    if (ids.size) {
      // Pull summaries for exact IDs to ensure we see them even if not in the 7-day window
      const more = await Promise.all(
        [...ids].map(async id => {
          try {
            const s = await fetchJSON(summaryUrl(league, id));
            return s?.header?.competitions?.[0] ? s.header.competitions[0] : null;
          } catch {
            return null;
          }
        })
      );
      for (const c of more) {
        if (!c) continue;
        events.push({ id: String(c.id || c.uid?.split(":").pop() || ""), competitions: [c], status: c.status, date: c.date });
      }
    }
    // de-dupe
    const seen = new Set();
    events = events.filter(e => !seen.has(String(e.id)) && seen.add(String(e.id)));

    /* ------- Pass 1: Prefill append for any missing rows ------- */
    const preRow = buildPregameRow(globalThis._firstSB || {}, league);
    const toAppend = [];
    for (const ev of events) {
      const id = String(ev.id);
      if (!rowById.has(id)) {
        const r = await preRow(ev);
        toAppend.push(r.values);
      }
    }
    if (toAppend.length) {
      await sh.batch([{ range: `${tab}!A1`, values: toAppend }]);
      touched += toAppend.length;

      // re-read to rebuild index
      const ref = await sh.all();
      const hdr2 = ref[0] || header;
      const hm2 = mapHeaders(hdr2);
      ref.slice(1).forEach((r, i) => {
        const gid = (r[hm2["game id"]] || "").toString().trim();
        if (gid) rowById.set(gid, i + 2);
      });
    }

    /* ------- Pass 2: Finals + Live columns ------- */
    const bat = [];
    for (const ev of events) {
      const comp = ev.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");
      const gid = String(ev.id);
      const row = rowById.get(gid);
      if (!row) continue;

      // Status update (no EDT)
      const statusStr = tidyStatus(ev);
      if (hmap["status"] != null && statusStr) {
        bat.push({ range: `${tab}!${letter(hmap["status"])}${row}:${letter(hmap["status"])}${row}`, values: [[noEDT(statusStr)]] });
      }

      // If final -> write Final Score & bold winner
      const tName = lower(ev.status?.type?.name || comp.status?.type?.name);
      const isFinal = tName.includes("final");
      if (isFinal) {
        const finalScore = `${away?.score ?? ""}-${home?.score ?? ""}`;
        if (hmap["final score"] != null) {
          bat.push({ range: `${tab}!${letter(hmap["final score"])}${row}:${letter(hmap["final score"])}${row}`, values: [[finalScore]] });
        }
        if (hmap["status"] != null) {
          bat.push({ range: `${tab}!${letter(hmap["status"])}${row}:${letter(hmap["status"])}${row}`, values: [["Final"]] });
        }

        // Bold the winning team in matchup text
        const mv = rows[row - 2]?.[hmap["matchup"]] || "";
        if (mv) {
          const [aName, hName] = mv.split("@").map(s => s.trim());
          let winName = null;
          if (+away?.score > +home?.score) winName = aName;
          else if (+home?.score > +away?.score) winName = hName;
          let bIdx = null, bLen = 0;
          if (winName) {
            const idx = mv.indexOf(winName);
            if (idx >= 0) { bIdx = idx; bLen = winName.length; }
          }
          // Keep/restore underline of pregame favorite if we can infer from pregame columns
          let uIdx = null, uLen = 0;
          try {
            const pregA = (rows[row - 2]?.[hmap["a spread"]] || "").toString();
            const pregH = (rows[row - 2]?.[hmap["h spread"]] || "").toString();
            const fav = pregA.startsWith("-") ? (aName) : (pregH.startsWith("-") ? hName : null);
            if (fav) {
              const at = mv.indexOf(fav);
              if (at >= 0) { uIdx = at; uLen = fav.length; }
            }
          } catch {}
          await sh.formatMatchup(row, mv, uIdx, uLen, bIdx, bLen);
        }
        continue;
      }

      // Live odds — Summary fast path
      let hScore = "", haSpread = "", haML = "", hhSpread = "", hhML = "", hTotal = "";
      try {
        const s = await fetchJSON(summaryUrl(league, gid));
        const box = s?.boxscore;
        const aT = box?.teams?.find(t => t.homeAway === "away");
        const hT = box?.teams?.find(t => t.homeAway === "home");
        if (aT && hT) hScore = `${aT.score ?? ""}-${hT.score ?? ""}`;

        const liveOdds = pickEspnBet(s?.header?.competitions?.[0]?.odds || s?.pickcenter || []);
        if (liveOdds?.type?.toLowerCase?.() === "live" || String(liveOdds?.displayName || "").toLowerCase().includes("live")) {
          const sp = spreadsFrom(liveOdds, away?.team?.id, home?.team?.id);
          haSpread = sp.aSpr; hhSpread = sp.hSpr;
          const ml = moneylinesFrom(liveOdds, away?.team?.id, home?.team?.id);
          haML = ml.aML; hhML = ml.hML;
          hTotal = liveOdds.overUnder ?? liveOdds.total ?? "";
        }
      } catch {}

      // If summary didn’t deliver live numbers, scrape the LIVE ODDS panel
      const needOdds = !(haSpread || hhSpread || haML || hhML || hTotal);
      if (needOdds) {
        const dom = await scrapeLiveOddsOnce(league, gid);
        if (dom) {
          haSpread = haSpread || dom.haSpread;
          hhSpread = hhSpread || dom.hhSpread;
          haML = haML || dom.haML;
          hhML = hhML || dom.hhML;
          hTotal = hTotal || dom.hTotal;
          hScore = hScore || dom.hScore;
        }
      }

      // Write live columns when we have something (always write H Score if present)
      const add = (name, val) => {
        if (val == null || val === "" || hmap[name] == null) return;
        bat.push({ range: `${tab}!${letter(hmap[name])}${row}:${letter(hmap[name])}${row}`, values: [[String(val)]] });
      };
      add("h score", hScore);
      add("h a spread", haSpread);
      add("h a ml", haML);
      add("h h spread", hhSpread);
      add("h h ml", hhML);
      add("h total", hTotal);
    }

    if (bat.length) {
      await sh.batch(bat);
      touched += bat.length;
    }
  }

  const done = { ok: true, league: LEAGUE_IN, tab: TAB_NAME || "", touched };
  if (GHA_JSON) process.stdout.write(JSON.stringify(done) + "\n");
  else console.log(done);
})().catch((err) => {
  if (GHA_JSON) process.stdout.write(JSON.stringify({ ok: false, error: String(err?.message || err) }) + "\n");
  else {
    console.error("Fatal:", err);
    process.exit(1);
  }
});
