// orchestrator.mjs
import { google } from "googleapis";
import * as fs from "node:fs";
import * as playwright from "playwright";

/** ===== ENV ===== */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE     = (process.env.LEAGUE || "nfl").toLowerCase();           // "nfl" | "college-football"
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "today").toLowerCase();      // "today" | "week"

const MATCHUP_FILTER = (process.env.MATCHUP_FILTER || "").trim();         // e.g. "Lions @ Chiefs"
const FORCE_LIVE     = String(process.env.FORCE_LIVE || "false").toLowerCase() === "true";

const DEBUG_LEVEL = Number(process.env.DEBUG_LEVEL ?? 0); // 0=normal, 1=verbose, 2=trace

/** ===== Log helpers ===== */
const ET_TZ = "America/New_York";
const log  = (...a)=>console.log(...a);
const vlog = (...a)=>{ if (DEBUG_LEVEL >= 1) console.log("[debug]", ...a); };
const wlog = (...a)=>console.warn("[warn]", ...a);

/** ===== Utilities ===== */
function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, year:"numeric", month:"numeric", day:"numeric" })
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

/** ===== Google Sheets ===== */
function sheetsClient() {
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

/** Center alignment A–P */
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

/** Conditional formatting for column H (Home Spread)
 *  Rules (in order of priority):
 *   1) Final + pushes -> yellow
 *   2) Final + home covers -> green
 *   3) Final + home doesn't cover -> red
 *   4) Not final -> sign tint (H<0 green, H>0 red)
 */
async function applyHomeSpreadFormatting(sheets){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;

  // wipe a few slots to avoid duplicates; harmless if none exist
  const wipe = [
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } },
    { deleteConditionalFormatRule: { index: 0, sheetId: sid } }
  ];

  const range = { sheetId: sid, startColumnIndex: 7, endColumnIndex: 8, startRowIndex: 1 }; // H2:H

  const rules = [
    // Push = 0 -> yellow
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [range],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=AND($C2=\"Final\", (VALUE(INDEX(SPLIT($E2,\"-\"),2)) - VALUE(INDEX(SPLIT($E2,\"-\"),1)) + VALUE($H2))=0)" }] },
            format: { backgroundColor: { red: 1, green: 1, blue: 0.7 } }
          }
        },
        index: 0
      }
    },
    // Final + home covers -> green
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [range],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=AND($C2=\"Final\", (VALUE(INDEX(SPLIT($E2,\"-\"),2)) - VALUE(INDEX(SPLIT($E2,\"-\"),1)) + VALUE($H2))>0)" }] },
            format: { backgroundColor: { red: 0.85, green: 1, blue: 0.85 } }
          }
        },
        index: 0
      }
    },
    // Final + home doesn't cover -> red
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [range],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=AND($C2=\"Final\", (VALUE(INDEX(SPLIT($E2,\"-\"),2)) - VALUE(INDEX(SPLIT($E2,\"-\"),1)) + VALUE($H2))<0)" }] },
            format: { backgroundColor: { red: 1, green: 0.85, blue: 0.85 } }
          }
        },
        index: 0
      }
    },
    // Not Final: sign-based tint (soft)
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [range],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=AND($C2<>\"Final\", $H2<0)" }] },
            format: { backgroundColor: { red: 0.9, green: 1, blue: 0.9 } }
          }
        },
        index: 1
      }
    },
    {
      addConditionalFormatRule: {
        rule: {
          ranges: [range],
          booleanRule: {
            condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: "=AND($C2<>\"Final\", $H2>0)" }] },
            format: { backgroundColor: { red: 1, green: 0.9, blue: 0.9 } }
          }
        },
        index: 1
      }
    }
  ];

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [...wipe, ...rules] }
  });
  log("🎨 Applied conditional formatting to H (Home Spread) with Final logic");
}

/** ===== HTTP ===== */
async function fetchJson(url){
  vlog("GET", url);
  const res = await fetch(url, { headers: { "User-Agent":"halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}

/** ===== Playwright LIVE ODDS (retry + trace) ===== */
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
      const totalAny      = txt.match(/\b(?:o|u)\s?(\d+(?:\.\d+)?)/i);
      const mlMatches     = txt.match(/\s[+-]\d{2,4}\b/g) || [];

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
      wlog("LIVE ODDS parse failed", url, "last block:", lastTxt.slice(0, 240));
    }
    return parsed;
  } finally {
    if (DEBUG_LEVEL >= 2){
      await context.tracing.stop({ path: "trace.zip" });
    }
    await browser.close();
  }
}

/** ===== MAIN ===== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
    process.exit(1);
  }
  const sheets = sheetsClient();

  // Formatting each run
  await applyCenterFormatting(sheets);
  await applyHomeSpreadFormatting(sheets);

  // Scoreboard
  const dateKey = RUN_SCOPE === "week" ? yyyymmddET(new Date()) : yyyymmddET(new Date());
  const url = scoreboardUrl(LEAGUE, dateKey);
  const data = await fetchJson(url);
  const events = data.events || [];
  log(`Found ${events.length} ${LEAGUE.toUpperCase()} events`);

  // Sheet data
  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0] || [];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P5000` })).data.values || [];
  const iDate = hmap["date"], iMu = hmap["matchup"];

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const awayC = comp.competitors?.find(c=>c.homeAway==="away");
    const homeC = comp.competitors?.find(c=>c.homeAway==="home");
    const awayTeam = awayC?.team?.shortDisplayName || "Away";
    const homeTeam = homeC?.team?.shortDisplayName || "Home";
    const matchup  = `${awayTeam} @ ${homeTeam}`;
    const statusObj = ev.status?.type || comp.status?.type || {};
    const statusName = (statusObj.name || "").toUpperCase();
    const isFinal = Boolean(statusObj.completed) || statusName.includes("STATUS_FINAL") || statusName.includes("FINAL");
    const isHalftime = statusName.includes("HALF");
    const isInProg = statusName.includes("STATUS_IN_PROGRESS") || statusName.includes("INPROGRESS") || statusName.includes("ENDOFHALF") || isHalftime;
    const dateET   = fmtETDate(ev.date);

    // filter to one matchup if asked
    if (MATCHUP_FILTER && matchup.toLowerCase() !== MATCHUP_FILTER.toLowerCase()){
      vlog(`Skip ${matchup} (filter=${MATCHUP_FILTER})`);
      continue;
    }

    // Find target row
    let rowNum = 0;
    for(let r=1; r<list.length; r++){
      const row = list[r] || [];
      if ((row[iDate]||"") === dateET && (row[iMu]||"") === matchup){ rowNum = r+1; break; }
    }
    if (!rowNum){ wlog("No sheet row for", dateET, matchup); continue; }

    const updates = [];
    const put = (name, val)=>{
      const idx = hmap[name];
      if (idx===undefined || val===undefined || val===null || val==="") return;
      updates.push({
        range: `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`,
        values: [[val]]
      });
    };

    // If final, write final status & score first
    if (isFinal){
      const awayScore = Number(awayC?.score ?? "");
      const homeScore = Number(homeC?.score ?? "");
      if (!Number.isNaN(awayScore) && !Number.isNaN(homeScore)){
        put("final score", `${awayScore}-${homeScore}`);  // E
      }
      put("status", "Final"); // C
    }

    // Live odds: write when in-progress/halftime or if forcing live in debug
    if (isInProg || isHalftime || FORCE_LIVE){
      const live = await scrapeLiveOdds(LEAGUE, ev.id);
      if (live){
        try { fs.writeFileSync(`live-${ev.id}.json`, JSON.stringify({ matchup, isFinal, ...live }, null, 2)); } catch {}
        put("half score", live.halfScore);
        put("live away spread", live.liveAwaySpread);
        put("live away ml", live.liveAwayML);
        put("live home spread", live.liveHomeSpread);
        put("live home ml", live.liveHomeML);
        put("live total", live.liveTotal);
        if (!isFinal) put("status", isHalftime ? "Half" : "Live");
      }
    }

    if (updates.length){
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "USER_ENTERED", data: updates }
      });
      log(`✅ Updated ${matchup} (row ${rowNum})`);
    } else {
      vlog("No changes for", matchup);
    }
  }

  log("✅ Orchestrator complete");
})().catch(e=>{
  console.error("FATAL", e);
  process.exit(1);
});
