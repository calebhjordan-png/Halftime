import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
/** NEW: scope + optional explicit week */
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const WEEK_OVERRIDE = process.env.WEEK_OVERRIDE ? Number(process.env.WEEK_OVERRIDE) : null;

/** Column names we expect in the sheet (order matters) */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Live Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ====== Helpers ====== */
function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw);            // raw JSON
  const json = Buffer.from(raw, "base64").toString("utf8");   // Base64
  return JSON.parse(json);
}
function yyyymmddInET(d=new Date()) {
  const et = new Date(d.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const y = et.getFullYear();
  const m = String(et.getMonth()+1).padStart(2,"0");
  const day = String(et.getDate()).padStart(2,"0");
  return `${y}${m}${day}`;
}
async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "halftime-bot",
      "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
      "Referer": "https://www.espn.com/"
    }
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, { dates, week }) {
  const lg = normLeague(league);
  // Include FBS group for college to avoid Top-25-only view
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  if (week != null && Number.isFinite(week)) {
    return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?week=${week}${extra}`;
  }
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function gameUrl(league, gameId) {
  const lg = normLeague(league);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}
function pickOdds(oddsArr=[]) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}
function mapHeadersToIndex(headerRow) {
  const map = {};
  headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }

/** Normalize numeric-ish value into string (keeps + sign if present) */
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** ====== Week math (ET) ====== */
/** We’ll treat the “league week” as Tue 00:00 ET → Mon 23:59 ET to align with ESPN tabs */
function startOfLeagueWeekET(d=new Date()) {
  const et = new Date(d.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const dow = et.getDay(); // 0=Sun ... 6=Sat
  // Find most recent Tuesday (2)
  const offsetToTue = ((dow - 2) + 7) % 7; // days since Tuesday
  const start = new Date(et);
  start.setDate(et.getDate() - offsetToTue);
  start.setHours(0,0,0,0);
  return start;
}
function datesForWeekET(ref=new Date()) {
  const start = startOfLeagueWeekET(ref); // Tue
  const out = [];
  for (let i=0;i<7;i++){
    const d = new Date(start);
    d.setDate(start.getDate()+i);
    out.push(yyyymmddInET(d));
  }
  return out; // Tue..Mon (ET)
}
function uniqueById(events) {
  const seen = new Set();
  const out = [];
  for (const e of events) {
    const id = String(e?.id || "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(e);
  }
  return out;
}

/** ====== Robust Moneyline extraction covering ESPN variants ====== */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";

  const byId = (tid, ml) => {
    if (!ml) return;
    if (String(tid) === String(awayId)) awayML = awayML || ml;
    if (String(tid) === String(homeId)) homeML = homeML || ml;
  };

  // 1) teamOdds[] variants
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      byId(tid, ml);
    }
  }

  // 2) direct fields on odds object
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);

  // 3) favorite/underdog paired fields
  if (!awayML || !homeML) {
    const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
    const favML = numOrBlank(o?.favoriteMoneyLine);
    const dogML = numOrBlank(o?.underdogMoneyLine);
    if (favId && (favML || dogML)) {
      if (String(awayId) === favId) {
        awayML = awayML || favML;
        homeML = homeML || dogML;
      } else if (String(homeId) === favId) {
        homeML = homeML || favML;
        awayML = awayML || dogML;
      }
    }
  }

  // 4) rare competitor-level odds
  if ((!awayML || !homeML) && Array.isArray(competitors)) {
    for (const c of competitors) {
      const cand = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!cand) continue;
      if (c.homeAway === "away") awayML = awayML || cand;
      if (c.homeAway === "home") homeML = homeML || cand;
    }
  }

  return { awayML, homeML };
}

/** Build pregame row */
function pregameRow(event, weekText) {
  const comp = event.competitions?.[0] || {};
  const status = event.status?.type?.name || comp.status?.type?.name || "";
  const shortStatus = event.status?.type?.shortDetail || comp.status?.type?.shortDetail || "";
  const competitors = comp?.competitors || [];
  const away = competitors.find(c => c.homeAway === "away");
  const home = competitors.find(c => c.homeAway === "home");

  const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  const matchup = `${awayName} @ ${homeName}`;

  // Final?
  const finalScore = /final/i.test(status)
    ? `${away?.score ?? ""}-${home?.score ?? ""}`
    : "";

  const o = pickOdds(comp.odds || event.odds || []);
  let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";

  if (o) {
    // total
    total = (o.overUnder ?? o.total) ?? "";

    // spreads
    const favId = String(o.favorite || "");
    const spread = Number.isFinite(o.spread) ? o.spread :
                   (typeof o.spread === "string" ? parseFloat(o.spread) : NaN);
    if (!Number.isNaN(spread) && favId) {
      if (String(away?.team?.id||"") === favId) {
        awaySpread = `-${Math.abs(spread)}`;
        homeSpread = `+${Math.abs(spread)}`;
      } else if (String(home?.team?.id||"") === favId) {
        homeSpread = `-${Math.abs(spread)}`;
        awaySpread = `+${Math.abs(spread)}`;
      }
    } else if (o.details) {
      // fallback parse from text like "Team X -7.5"
      const m = o.details.match(/([+-]?\d+(\.\d+)?)/);
      if (m) {
        const line = parseFloat(m[1]);
        awaySpread = line > 0 ? `+${Math.abs(line)}` : `${line}`;
        homeSpread = line > 0 ? `-${Math.abs(line)}` : `+${Math.abs(line)}`;
      }
    }

    // moneylines (robust)
    const ids = { awayId: away?.team?.id, homeId: home?.team?.id };
    const ml = extractMoneylines(o, ids.awayId, ids.homeId, competitors);
    awayML = ml.awayML || "";
    homeML = ml.homeML || "";
  }

  const dateET = new Date(event.date).toLocaleDateString("en-US", { timeZone: "America/New_York" });

  return {
    values: [
      dateET,                 // Date
      weekText || "",         // Week
      shortStatus || status,  // Status
      matchup,                // Matchup
      finalScore,             // Final Score
      awaySpread || "",       // Away Spread
      String(awayML || ""),   // Away ML
      homeSpread || "",       // Home Spread
      String(homeML || ""),   // Home ML
      String(total || ""),    // Total
      "", "", "", "", "", ""  // live cols
    ],
    dateET,
    matchup
  };
}

/** Halftime-ish? */
function isHalftimeLike(evt) {
  const t = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || "").toUpperCase();
  return t.includes("HALFTIME") || /Q2.*0:0?0/i.test(short) || /HALF\s*TIME/i.test(short);
}

/** ====== Resilient Playwright DOM scrape for LIVE odds (one-time) ====== */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 6000 }).catch(() => {});
    await page.waitForTimeout(750);

    // Anchor by stable footer/labels rather than brittle classnames
    const primaryFooter = page.getByText(/All Live Odds on ESPN BET Sportsbook/i).first();
    const altFooter = page.getByText(/^Odds by$/i).first();

    const container = await nearestOddsContainer(primaryFooter) || await nearestOddsContainer(altFooter);
    if (!container) {
      console.warn("LIVE ODDS container not found:", url);
      return null;
    }

    const raw = (await container.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

    // Total: "o/u 45.5" OR "Total 45.5" OR "o45.5 / u45.5"
    const totalMatch =
      raw.match(/\b(?:o\/?u|total)\s*([0-9]+(?:\.[0-9])?)/i) ||
      raw.match(/\bo\s*([0-9]+(?:\.[0-9])?)\b/i) ||
      raw.match(/\bu\s*([0-9]+(?:\.[0-9])?)\b/i);
    const liveTotal = totalMatch ? totalMatch[1] : "";

    // Spreads: take first two signed numbers that are likely spreads (<= 40 abs)
    const spreadNums = [...raw.matchAll(/([+-]\d+(?:\.\d+)?)(?!\s*(?:o|u)\b)/gi)]
      .map(m => Number(m[1])).filter(n => Number.isFinite(n) && Math.abs(n) <= 40);
    const liveAwaySpread = (spreadNums[0] != null) ? (spreadNums[0] > 0 ? `+${spreadNums[0]}` : `${spreadNums[0]}`) : "";
    const liveHomeSpread = (spreadNums[1] != null) ? (spreadNums[1] > 0 ? `+${spreadNums[1]}` : `${spreadNums[1]}`) : "";

    // Moneylines: prefer explicit "ML -120" tokens, fallback to bare +/- near the grid
    const mlTokens = [...raw.matchAll(/\bML\s*([+-]?\d{2,4})\b/gi)].map(m => m[1]);
    let liveAwayML = mlTokens[0] || "";
    let liveHomeML = mlTokens[1] || "";
    if (!liveAwayML || !liveHomeML) {
      const bare = [...raw.matchAll(/\s([+-]\d{2,4})\s/g)].map(m => m[1]);
      liveAwayML = liveAwayML || bare[0] || "";
      liveHomeML = liveHomeML || bare[1] || "";
    }

    // Live score: grab from global header text if present
    let liveScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (sc) liveScore = `${sc[1]}-${sc[2]}`;
    } catch {}

    // Only write if we parsed something meaningful
    const parsedAny = !!(liveTotal || liveAwaySpread || liveHomeSpread || liveAwayML || liveHomeML || liveScore);
    if (!parsedAny) return null;

    return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, liveScore };

  } catch (err) {
    console.warn("Live DOM scrape failed:", err.message, url);
    return null;
  } finally {
    await page.close().catch(()=>{});
    await browser.close().catch(()=>{});
  }

  async function nearestOddsContainer(locator) {
    try {
      await locator.wait({ timeout: 4000 });
      const el = await locator.elementHandle();
      if (!el) return null;
      return await el.evaluateHandle(node => {
        let cur = node;
        for (let i = 0; i < 6 && cur && cur.parentElement; i++) {
          cur = cur.parentElement;
          if (cur?.querySelector && (cur.querySelector('table') || cur.querySelector('[role="table"]'))) {
            return cur;
          }
        }
        return node.parentElement || node;
      });
    } catch {
      return null;
    }
  }
}

async function updateRow(sheets, rowNumber, colIndex, value) {
  if (value === "" || value == null) return; // don't overwrite with blank
  const colLetter = String.fromCharCode("A".charCodeAt(0) + colIndex);
  const range = `${TAB_NAME}!${colLetter}${rowNumber}:${colLetter}${rowNumber}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values: [[value]] },
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
  const keyToRowNum = new Map();
  rows.forEach((r, i) => {
    const k = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
    keyToRowNum.set(k, i + 2);
  });

  /** ===== Fetch events per scope ===== */
  let events = [];
  let weekText = "Regular Season";

  if (WEEK_OVERRIDE != null && Number.isFinite(WEEK_OVERRIDE)) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, { week: WEEK_OVERRIDE }));
    weekText = (sb?.week?.text) ? sb.week.text
              : (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
    events = sb?.events || [];
  } else if (RUN_SCOPE === "week") {
    const allDates = datesForWeekET(new Date());
    let agg = [];
    for (const d of allDates) {
      const sb = await fetchJson(scoreboardUrl(LEAGUE, { dates: d }));
      if (weekText === "Regular Season") {
        weekText = (sb?.week?.text) ? sb.week.text
                 : (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
      }
      agg = agg.concat(sb?.events || []);
    }
    events = uniqueById(agg);
  } else {
    const d = yyyymmddInET(new Date());
    const sb = await fetchJson(scoreboardUrl(LEAGUE, { dates: d }));
    weekText = (sb?.week?.text) ? sb.week.text
              : (Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Regular Season");
    events = sb?.events || [];
  }

  /** ===== Pregame append ===== */
  let appendBatch = [];
  for (const ev of events) {
    const { values: rowVals, dateET, matchup } = pregameRow(ev, weekText);
    const k = keyOf(dateET, matchup);
    if (!keyToRowNum.has(k)) {
      appendBatch.push(rowVals);
    }
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: appendBatch },
    });
    // refresh map after append
    const re = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1:Z`,
    });
    const v2 = re.data.values || [];
    const hdr2 = v2[0] || header;
    const h2 = mapHeadersToIndex(hdr2);
    (v2.slice(1)).forEach((r, i) => {
      const key = keyOf(r[h2["date"]], r[h2["matchup"]]);
      keyToRowNum.set(key, i + 2);
    });
    console.log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  /** ===== Halftime / Final updates ===== */
  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = new Date(ev.date).toLocaleDateString("en-US", { timeZone: "America/New_York" });
    const k = keyOf(dateET, matchup);
    const rowNum = keyToRowNum.get(k);
    if (!rowNum) continue;

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;

    // Final score
    if (statusName.includes("FINAL")) {
      if (hmap["final score"] !== undefined) {
        await updateRow(sheets, rowNum, hmap["final score"], scorePair);
      }
      if (hmap["status"] !== undefined) {
        const short = ev.status?.type?.shortDetail || "Final";
        await updateRow(sheets, rowNum, hmap["status"], short);
      }
      continue;
    }

    // Halftime (one-time) live odds: use "Live Total" as the guard
    const snapshot = (await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A${rowNum}:Z${rowNum}`,
    })).data.values?.[0] || [];
    const liveAlready = (snapshot[hmap["live total"]] || "").toString().trim();

    if (isHalftimeLike(ev) && !liveAlready) {
      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (live) {
        const {
          liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, liveScore
        } = live;

        await updateRow(sheets, rowNum, hmap["live score"],        liveScore);
        await updateRow(sheets, rowNum, hmap["live away spread"],  liveAwaySpread);
        await updateRow(sheets, rowNum, hmap["live home spread"],  liveHomeSpread);
        await updateRow(sheets, rowNum, hmap["live away ml"],      liveAwayML);
        await updateRow(sheets, rowNum, hmap["live home ml"],      liveHomeML);
        await updateRow(sheets, rowNum, hmap["live total"],        liveTotal);

        console.log(`🕐 Halftime LIVE written for ${matchup}`);
      } else {
        console.log(`(Halftime) live odds not found for ${matchup}`);
      }
    }
  }

  console.log("✅ Run complete.");
})().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
