import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || "NFL").trim();

/** Column names we expect in the sheet */
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
  const res = await fetch(url, { headers: { "User-Agent": "halftime-bot" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function scoreboardUrl(league, dates) {
  const lg = league === "ncaaf" || league === "college-football" ? "college-football" : "nfl";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}`;
}
function gameUrl(league, gameId) {
  const lg = league === "ncaaf" || league === "college-football" ? "college-football" : "nfl";
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

/** Normalize numeric-ish value */
function numOrBlank(v) {
  if (v === 0) return 0;
  if (v == null) return "";
  const n = parseFloat(String(v).replace(/[^\d.+-]/g, ""));
  return Number.isFinite(n) ? (String(v).trim().startsWith("+") ? `+${n}` : `${n}`) : "";
}

/** Try to pull moneylines for each team from many ESPN shapes */
function extractMoneylines(o, awayId, homeId) {
  let awayML = "", homeML = "";

  const trySetByIds = (teamOddsArr) => {
    if (!Array.isArray(teamOddsArr)) return false;
    for (const t of teamOddsArr) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid && tid === String(awayId)) awayML = ml;
      if (tid && tid === String(homeId)) homeML = ml;
    }
    return !!(awayML || homeML);
  };

  // 1) teamOdds: [{ teamId, moneyLine }]
  if (trySetByIds(o.teamOdds)) return { awayML, homeML };

  // 2) nested competitors-> odds info (sometimes used)
  if (Array.isArray(o.competitors)) {
    const a = o.competitors.find(c => String(c?.id) === String(awayId) || String(c?.teamId) === String(awayId));
    const h = o.competitors.find(c => String(c?.id) === String(homeId) || String(c?.teamId) === String(homeId));
    if (a) awayML = awayML || numOrBlank(a.moneyLine ?? a.moneyline);
    if (h) homeML = homeML || numOrBlank(h.moneyLine ?? h.moneyline);
    if (awayML || homeML) return { awayML, homeML };
  }

  // 3) direct fields
  awayML = awayML || numOrBlank(o.moneyLineAway ?? o.awayTeamMoneyLine ?? o.awayMoneyLine ?? o.awayMl);
  homeML = homeML || numOrBlank(o.moneyLineHome ?? o.homeTeamMoneyLine ?? o.homeMoneyLine ?? o.homeMl);
  if (awayML || homeML) return { awayML, homeML };

  // 4) favorite/underdog mapping
  const favId = String(o.favorite || "");
  const favML = numOrBlank(o.favoriteMoneyLine);
  const dogML = numOrBlank(o.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) {
      awayML = favML || awayML;
      homeML = dogML || homeML;
      return { awayML, homeML };
    }
    if (String(homeId) === favId) {
      homeML = favML || homeML;
      awayML = dogML || awayML;
      return { awayML, homeML };
    }
  }

  return { awayML, homeML };
}

/** Build pregame row */
function pregameRow(event, weekText) {
  const comp = event.competitions?.[0] || {};
  const status = event.status?.type?.name || comp.status?.type?.name || "";
  const shortStatus = event.status?.type?.shortDetail || comp.status?.type?.shortDetail || "";
  const away = comp.competitors?.find(c => c.homeAway === "away");
  const home = comp.competitors?.find(c => c.homeAway === "home");

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
      const m = o.details.match(/([+-]?\d+(\.\d+)?)/);
      if (m) {
        const line = parseFloat(m[1]);
        awaySpread = line > 0 ? `+${Math.abs(line)}` : `${line}`;
        homeSpread = line > 0 ? `-${Math.abs(line)}` : `+${Math.abs(line)}`;
      }
    }

    // moneylines (robust)
    const ids = { awayId: away?.team?.id, homeId: home?.team?.id };
    const ml = extractMoneylines(o, ids.awayId, ids.homeId);
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
  return t.includes("HALFTIME") || /Q2.*0:0?0/i.test(short);
}

/** Playwright DOM scrape for LIVE odds at halftime (one-time) */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
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

    let liveScore = "";
    try {
      const allTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc = allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if (sc) liveScore = `${sc[1]}-${sc[2]}`;
    } catch {}

    return {
      liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, liveScore
    };
  } catch (err) {
    console.warn("Live DOM scrape failed:", err.message, url);
    return null;
  } finally {
    await browser.close();
  }
}

async function updateRow(sheets, rowNumber, colIndex, value) {
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

  const dates = yyyymmddInET(new Date());
  const sb = await fetchJson(scoreboardUrl(LEAGUE, dates));
  const weekText = sb?.week?.text || sb?.leagues?.[0]?.season?.type?.name || "Regular Season";
  const events = sb?.events || [];

  let appendBatch = [];

  // Pregame append
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

  // Halftime / Final updates
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

    // Halftime (one-time) live odds
    const currentRow = (values[rowNum-1] || []);
    const liveTotalVal = currentRow[hmap["live total"]] || "";
    if (isHalftimeLike(ev) && !liveTotalVal) {
      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
      if (live) {
        const {
          liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, liveScore
        } = live;

        if (hmap["live score"] !== undefined && liveScore)       await updateRow(sheets, rowNum, hmap["live score"], liveScore);
        if (hmap["live away spread"] !== undefined && liveAwaySpread) await updateRow(sheets, rowNum, hmap["live away spread"], liveAwaySpread);
        if (hmap["live home spread"] !== undefined && liveHomeSpread) await updateRow(sheets, rowNum, hmap["live home spread"], liveHomeSpread);
        if (hmap["live away ml"] !== undefined && liveAwayML)   await updateRow(sheets, rowNum, hmap["live away ml"], liveAwayML);
        if (hmap["live home ml"] !== undefined && liveHomeML)   await updateRow(sheets, rowNum, hmap["live home ml"], liveHomeML);
        if (hmap["live total"] !== undefined && liveTotal)       await updateRow(sheets, rowNum, hmap["live total"], liveTotal);

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
