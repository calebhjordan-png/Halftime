import { google } from "googleapis";
import * as fs from "node:fs";
import * as playwright from "playwright";

/** ==== ENV ==== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase(); // today | week
const DEBUG_LEVEL = Number(process.env.DEBUG_LEVEL ?? 0); // 0 normal, 1 verbose, 2 +trace

const ET_TZ = "America/New_York";
const log  = (...a)=>console.log(...a);
const vlog = (...a)=>{ if (DEBUG_LEVEL >= 1) console.log("[debug]", ...a); };
const wlog = (...a)=>console.warn("[warn]", ...a);

/** ==== Helpers ==== */
function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, year: "numeric", month:"numeric", day:"numeric" })
    .format(new Date(dateLike));
}
function yyyymmddET(d=new Date()){
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, year:"numeric", month:"2-digit", day:"2-digit" })
    .formatToParts(new Date(d));
  const get = k => parts.find(p=>p.type===k)?.value || "";
  return `${get("year")}${get("month")}${get("day")}`;
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
function colLetter(i){ return String.fromCharCode(65 + i); }
function headerMap(headerRow){
  const m = {};
  headerRow.forEach((h,i)=> m[(h||"").toString().trim().toLowerCase()] = i);
  return m;
}

/** ==== Google Sheets ==== */
function googleClient() {
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  return google.sheets({ version:"v4", auth });
}

async function sheetIdByTitle(sheets, spreadsheetId, title){
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  return meta.data.sheets?.find(s=>s.properties?.title===title)?.properties?.sheetId ?? null;
}

/** Center alignment A–P once per run */
async function applyCenterFormatting(sheets){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        repeatCell: {
          range: { sheetId: sid, startRowIndex: 0, startColumnIndex: 0, endColumnIndex: 16 },
          cell: { userEnteredFormat: { horizontalAlignment: "CENTER" } },
          fields: "userEnteredFormat.horizontalAlignment"
        }
      }]
    }
  });
  vlog("Applied center alignment A–P");
}

/** Clear + add rules on column H (index 7) */
async function applyHomeSpreadFormatting(sheets){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;

  // Fetch current rules
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID, ranges: [`${TAB_NAME}!H2:H5000`], includeGridData: false });
  const sheet = meta.data.sheets?.find(s=>s.properties?.sheetId===sid);
  const rules = sheet?.conditionalFormats || []; // not always returned; safest to clear by range

  // Use deleteConditionalFormatRule by index is messy; easiest robust way is to overwrite rules for the range:
  const requests = [
    // First, delete all CF rules (Google will delete by index; if none, it silently ignores)
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    // Then add: H>0 -> red
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [{ sheetId: sid, startColumnIndex: 7, endColumnIndex: 8, startRowIndex: 1 }], // H2:H
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=H2>0" }] },
            format: { backgroundColor: { red: 1, green: 0.85, blue: 0.85 } }
          }
        },
        index: 0
      }
    },
    // H<0 -> green
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [{ sheetId: sid, startColumnIndex: 7, endColumnIndex: 8, startRowIndex: 1 }],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=H2<0" }] },
            format: { backgroundColor: { red: 0.85, green: 1, blue: 0.85 } }
          }
        },
        index: 0
      }
    }
  ];

  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { requests } });
  log("🎨 Applied conditional formatting to H (Home Spread)");
}

/** ==== ESPN fetch ==== */
async function fetchJson(url){
  vlog("GET", url);
  const res = await fetch(url, { headers: { "User-Agent":"halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}

/** ==== Playwright: robust LIVE ODDS with retries ==== */
async function scrapeLiveOdds(league, gameId){
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const context = await browser.newContext();
  if (DEBUG_LEVEL >= 2){
    await context.tracing.start({ screenshots: true, snapshots: true });
  }
  const page = await context.newPage();
  try{
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

    let attempt = 0, parsed = null, lastTxt = "";
    while(attempt < 3 && !parsed){
      attempt++;
      await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(()=>{});
      await page.waitForSelector("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))", { timeout: 8000 }).catch(()=>{});
      await page.waitForTimeout(1500);

      const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
      const txt = (await section.innerText().catch(()=>"" ))?.replace(/\u00a0/g," ").replace(/\s+/g," ").trim() || "";
      lastTxt = txt;

      const spreadMatches = txt.match(/(?:^|\s)([+-]\d+(?:\.\d+)?)(?=\s)/g) || [];
      const totalAny = txt.match(/\b(?:o|u)\s?(\d+(?:\.\d+)?)/i);
      const mlMatches = txt.match(/\s[+-]\d{2,4}\b/g) || [];

      if (spreadMatches.length >= 2 || totalAny || mlMatches.length >= 2){
        const liveAwaySpread = (spreadMatches[0]||"").trim();
        const liveHomeSpread = (spreadMatches[1]||"").trim();
        const liveTotal      = totalAny ? totalAny[1] : "";
        const liveAwayML     = (mlMatches[0]||"").trim();
        const liveHomeML     = (mlMatches[1]||"").trim();

        // crude half score fallback
        let halfScore = "";
        try {
          const bodyTxt = (await page.locator("body").innerText()).replace(/\s+/g," ");
          const sc = bodyTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
          if (sc) halfScore = `${sc[1]}-${sc[2]}`;
        } catch {}

        parsed = { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, halfScore, raw: txt };
      } else {
        await page.waitForTimeout(2500);
      }
    }

    if (!parsed && DEBUG_LEVEL >= 1){
      wlog("LIVE ODDS parse failed", url, "last block:", lastTxt.slice(0, 200));
    }
    return parsed;
  } finally {
    if (DEBUG_LEVEL >= 2){
      await context.tracing.stop({ path: "trace.zip" });
    }
    await browser.close();
  }
}

/** ==== Main ==== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
    process.exit(1);
  }
  const sheets = googleClient();

  await applyCenterFormatting(sheets);
  await applyHomeSpreadFormatting(sheets);

  const dateKey = RUN_SCOPE === "week" ? "" : yyyymmddET(new Date());
  const datesParam = RUN_SCOPE === "week" ? "" : dateKey;
  const url = scoreboardUrl(LEAGUE, datesParam || yyyymmddET(new Date()));
  const data = await fetchJson(url);

  const events = data.events || [];
  log(`Found ${events.length} ${LEAGUE.toUpperCase()} events`);

  // Pull header + rows once
  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0] || [];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P3000` })).data.values || [];
  const iDate = hmap["date"], iMu = hmap["matchup"];

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const status = (ev.status?.type?.name || "").toUpperCase();

    // we update at halftime and also if we already have live odds blank while game is in progress
    const isHalftime = status.includes("HALF");
    const isInProg = status.includes("STATUS_IN_PROGRESS") || status.includes("INPROGRESS") || status.includes("ENDOFHALF") || isHalftime;
    if (!isInProg && !isHalftime) continue;

    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
    const dateET = fmtETDate(ev.date);

    // find the sheet row
    let rowNum = 0;
    for(let r=1; r<list.length; r++){
      const row = list[r] || [];
      if ((row[iDate]||"") === dateET && (row[iMu]||"") === matchup){ rowNum = r+1; break; }
    }
    if (!rowNum){ vlog("Row not found for", dateET, matchup); continue; }

    const live = await scrapeLiveOdds(LEAGUE, ev.id);
    if (!live){ continue; }

    // drop per-game debug JSON
    try { fs.writeFileSync(`live-${ev.id}.json`, JSON.stringify(live, null, 2)); } catch {}

    const updates = [];
    const put = (name, val)=>{
      const idx = hmap[name];
      if (idx===undefined || val==="" || val==null) return;
      updates.push({
        range: `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`,
        values: [[val]]
      });
    };

    put("status", "Half");
    put("half score", live.halfScore);
    put("live away spread", live.liveAwaySpread);
    put("live away ml", live.liveAwayML);
    put("live home spread", live.liveHomeSpread);
    put("live home ml", live.liveHomeML);
    put("live total", live.liveTotal);

    if (updates.length){
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "USER_ENTERED", data: updates }
      });
      log(`✅ Updated live lines for ${matchup}`);
    } else {
      vlog("No updates for", matchup);
    }
  }

  log("✅ Orchestrator complete");
})().catch(e=>{
  console.error("FATAL", e);
  process.exit(1);
});
