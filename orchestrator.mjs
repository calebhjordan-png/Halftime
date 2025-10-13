import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";

const HALF_EARLY_MIN = Number(process.env.HALFTIME_EARLY_MIN ?? 60);
const HALF_LATE_MIN  = Number(process.env.HALFTIME_LATE_MIN  ?? 90);
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  const json = Buffer.from(raw, "base64").toString("utf8");
  return JSON.parse(json);
}

const ET_TZ = "America/New_York";
function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dateLike));
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
function normLeague(lg){ return (lg === "ncaaf" || lg === "college-football") ? "college-football" : "nfl"; }
function scoreboardUrl(league, dates){
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function gameUrl(league, gameId){
  const lg = normLeague(league);
  return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
}

function colLetter(i){ return String.fromCharCode("A".charCodeAt(0) + i); }

/** ===== Playwright: stronger LIVE ODDS fetch ===== */
async function scrapeLiveOddsOnce(league, gameId) {
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(()=>{});
    await page.waitForSelector("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))", { timeout: 8000 }).catch(()=>{});
    await page.waitForTimeout(1500);

    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
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
      const bodyTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc = bodyTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
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

/** ===== Sheets helpers ===== */
function mapHeadersToIndex(headerRow){
  const map = {};
  headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}

/** Conditional formatting: Home Spread column (H) */
async function applyHomeSpreadFormatting(sheets){
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME);
  const sheetId = sheet?.properties?.sheetId;
  if (!sheetId) return;
  const requests = [
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [{ sheetId, startColumnIndex: 7, endColumnIndex: 8, startRowIndex: 1 }],
          booleanRule: {
            condition: { type: "NUMBER_GREATER", values: [{ userEnteredValue: "0" }] },
            format: { backgroundColor: { red: 1, green: 0.8, blue: 0.8 } }
          }
        },
        index: 0
      }
    },
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [{ sheetId, startColumnIndex: 7, endColumnIndex: 8, startRowIndex: 1 }],
          booleanRule: {
            condition: { type: "NUMBER_LESS", values: [{ userEnteredValue: "0" }] },
            format: { backgroundColor: { red: 0.8, green: 1, blue: 0.8 } }
          }
        },
        index: 0
      }
    }
  ];
  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests } });
  log("🎨 Conditional formatting applied to column H (Home Spread)");
}

/** Center alignment (A–P) */
async function applyCenterFormatting(sheets){
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME)?.properties?.sheetId;
  if (!sheetId) return;
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
}

/** ===== MAIN ===== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    console.error("Missing secrets.");
    process.exit(1);
  }
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = google.sheets({ version:"v4", auth });

  // Center + format setup
  await applyCenterFormatting(sheets);
  await applyHomeSpreadFormatting(sheets);

  const today = yyyymmddInET(new Date());
  const sb = await fetchJson(scoreboardUrl(LEAGUE, today));
  const events = sb.events || [];
  log(`Found ${events.length} ${LEAGUE.toUpperCase()} games.`);

  // Every halftime: scrape and update live lines
  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const statusName = (ev.status?.type?.name || "").toUpperCase();
    if (!statusName.includes("HALF")) continue;

    const away = comp.competitors?.find(c => c.homeAway==="away");
    const home = comp.competitors?.find(c => c.homeAway==="home");
    const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
    const dateET = fmtETDate(ev.date);

    log(`⏱ Updating LIVE odds for halftime: ${matchup}`);

    const live = await scrapeLiveOddsOnce(LEAGUE, ev.id);
    if (!live){ warn("No live data:", matchup); continue; }

    const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0]||[];
    const hmap = mapHeadersToIndex(header);
    const findRow = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P2000` });
    const rows = findRow.data.values || [];
    const iDate = hmap["date"], iMu = hmap["matchup"];
    let rowNum = 0;
    for(let i=1;i<rows.length;i++){
      if(rows[i][iDate]===dateET && rows[i][iMu]===matchup){ rowNum=i+1; break; }
    }
    if(!rowNum){ warn("No row for matchup:", matchup); continue; }

    const data = [];
    const add = (name,val)=>{
      const idx = hmap[name];
      if(idx===undefined||!val) return;
      data.push({ range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val]] });
    };
    add("status","Half");
    add("half score", live.halfScore);
    add("live away spread", live.liveAwaySpread);
    add("live home spread", live.liveHomeSpread);
    add("live away ml", live.liveAwayML);
    add("live home ml", live.liveHomeML);
    add("live total", live.liveTotal);

    if(data.length){
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId:SHEET_ID,
        requestBody:{ valueInputOption:"USER_ENTERED", data }
      });
      log(`✅ Updated ${matchup} live lines.`);
    }
  }

  log("✅ Complete. All halftime games updated with live odds + formatting.");
})();
