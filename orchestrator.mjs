// orchestrator.mjs
import { google } from "googleapis";
import * as fs from "node:fs";
import * as playwright from "playwright";

/** ===== ENV ===== */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE     = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "today").toLowerCase();

const MATCHUP_FILTER = (process.env.MATCHUP_FILTER || "").trim();
const FORCE_LIVE     = String(process.env.FORCE_LIVE || "false").toLowerCase() === "true";

const DEBUG_LEVEL = Number(process.env.DEBUG_LEVEL ?? 0); // 0=normal, 1=verbose, 2=trace

/** ===== Helpers ===== */
const ET_TZ = "America/New_York";
const log  = (...a)=>console.log(...a);
const vlog = (...a)=>{ if (DEBUG_LEVEL >= 1) console.log("[debug]", ...a); };
const wlog = (...a)=>console.warn("[warn]", ...a);
const num  = v => Number.parseFloat(String(v).replace("+",""));

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT missing");
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
function addDays(d, n){ const dt = new Date(d); dt.setDate(dt.getDate()+n); return dt; }

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

/** ===== Formatting ===== */
async function resetConditionalFormatting(sheets) {
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;

  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SHEET_ID,
    fields: "sheets(properties(sheetId,title),conditionalFormats)"
  });
  const sheet = meta.data.sheets?.find(s => s.properties?.sheetId === sid);
  const cfCount = sheet?.conditionalFormats?.length || 0;

  const deleteReqs = [];
  for (let i = cfCount - 1; i >= 0; i--) {
    deleteReqs.push({ deleteConditionalFormatRule: { sheetId: sid, index: i } });
  }

  const color = (r,g,b)=>({ red:r, green:g, blue:b });
  const rng = (col) => ({ sheetId: sid, startColumnIndex: col-1, endColumnIndex: col, startRowIndex: 1 });

  const addRule = (col, formula, color) => ({
    addConditionalFormatRule: {
      rule: {
        ranges: [rng(col)],
        booleanRule: { condition: { type:"CUSTOM_FORMULA", values:[{ userEnteredValue:formula }] },
                       format: { backgroundColor: color } }
      },
      index: 0
    }
  });

  const requests = [
    ...deleteReqs,
    // F:G,I by sign
    addRule(6, "=$F2<0", color(0.85,1,0.85)),
    addRule(6, "=$F2>0", color(1,0.85,0.85)),
    addRule(7, "=$G2<0", color(0.85,1,0.85)),
    addRule(7, "=$G2>0", color(1,0.85,0.85)),
    addRule(9, "=$I2<0", color(0.85,1,0.85)),
    addRule(9, "=$I2>0", color(1,0.85,0.85)),
    // H: Final logic
    addRule(8, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))=0)', color(1,1,0.7)),
    addRule(8, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))>0)', color(0.85,1,0.85)),
    addRule(8, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))<0)', color(1,0.85,0.85)),
    // H: not final tint
    addRule(8, '=AND($C2<>"Final", $H2>0)', color(1,0.92,0.92)),
    addRule(8, '=AND($C2<>"Final", $H2<0)', color(0.92,1,0.92)),
  ];

  await sheets.spreadsheets.batchUpdate({ spreadsheetId: SHEET_ID, requestBody:{ requests }});
  log(`🎨 Reset and reapplied formatting for F:J`);
}

/** ===== Matchup Styling ===== */
function matchupTextRuns(matchup, fav, winner){
  const parts = matchup.split(" @ ");
  if (parts.length !== 2) return null;
  const away = parts[0], home = parts[1];
  const sep = " @ ", full = `${away}${sep}${home}`;

  const fmtAway = { underline: fav==="away", bold: winner==="away" };
  const fmtHome = { underline: fav==="home", bold: winner==="home" };
  const runs = [
    { startIndex: 0, format: fmtAway },
    { startIndex: away.length + sep.length, format: fmtHome }
  ];
  return { full, runs };
}

async function styleMatchupCell(sheets, row, matchup, fav, winner){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;
  const fmt = matchupTextRuns(matchup, fav, winner);
  if (!fmt) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody:{
      requests:[{
        updateCells:{
          range:{ sheetId:sid, startRowIndex:row-1, endRowIndex:row, startColumnIndex:3, endColumnIndex:4 },
          rows:[{ values:[{ userEnteredValue:{ stringValue:fmt.full }, textFormatRuns:fmt.runs }]}],
          fields:"userEnteredValue,textFormatRuns"
        }
      }]
    }
  });
}

/** ===== Odds / ESPN ===== */
function favoriteBySpreads(away, home){
  const a = num(away), h = num(home);
  if (Number.isNaN(a) || Number.isNaN(h)) return null;
  if (h < 0 && a >= 0) return "home";
  if (a < 0 && h >= 0) return "away";
  return Math.abs(h) < Math.abs(a) ? "home" : "away";
}
function scoreboardUrl(league, date){
  const lg = league==="college-football"?"college-football":"nfl";
  const extra = lg==="college-football"?"&groups=80&limit=300":"";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${date}${extra}`;
}
function gameUrl(league, id){
  const lg = league==="college-football"?"college-football":"nfl";
  return `https://www.espn.com/${lg}/game/_/gameId/${id}`;
}

/** ===== Live Scrape ===== */
async function scrapeLiveOdds(league, gameId){
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless:true });
  const context = await browser.newContext();
  if (DEBUG_LEVEL >= 2) await context.tracing.start({ screenshots:true, snapshots:true });
  const page = await context.newPage();
  try{
    await page.goto(url, { waitUntil:"domcontentloaded", timeout:60000 });
    await page.waitForTimeout(2000);
    const txt = (await page.content()).replace(/\s+/g," ");
    const spreadMatches = txt.match(/([+-]\d+(?:\.\d+)?)/g)||[];
    const totalAny = txt.match(/\b(?:o|u)\s?(\d+(?:\.\d+)?)/i);
    const mlMatches = txt.match(/\s[+-]\d{2,4}\b/g)||[];
    const liveAwaySpread=(spreadMatches[0]||"").trim();
    const liveHomeSpread=(spreadMatches[1]||"").trim();
    const liveTotal=totalAny?totalAny[1]:"";
    const liveAwayML=(mlMatches[0]||"").trim();
    const liveHomeML=(mlMatches[1]||"").trim();
    return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML };
  } finally {
    if (DEBUG_LEVEL>=2) await context.tracing.stop({ path:"trace.zip" });
    await browser.close();
  }
}

/** ===== Main ===== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing creds");
  const sheets = sheetsClient();

  await resetConditionalFormatting(sheets);

  const today = new Date();
  const dates = RUN_SCOPE==="week"
    ? [ yyyymmddET(addDays(today,-1)), yyyymmddET(today) ]
    : [ yyyymmddET(today) ];

  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0]||[];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P6000` })).data.values||[];

  const iDate=hmap["date"], iMu=hmap["matchup"], iAwaySp=hmap["away spread"], iHomeSp=hmap["home spread"];

  for (const date of dates){
    const data = await (await fetch(scoreboardUrl(LEAGUE,date))).json();
    for (const ev of (data.events||[])){
      const comp = ev.competitions?.[0]||{};
      const awayC=comp.competitors?.find(c=>c.homeAway==="away");
      const homeC=comp.competitors?.find(c=>c.homeAway==="home");
      const away=awayC?.team?.shortDisplayName||"Away";
      const home=homeC?.team?.shortDisplayName||"Home";
      const matchup=`${away} @ ${home}`;
      const status=(ev.status?.type?.name||"").toUpperCase();
      const isFinal=status.includes("FINAL");
      const isHalf=status.includes("HALF");
      const isInProg=status.includes("PROGRESS");
      const dateET=fmtETDate(ev.date);

      if (MATCHUP_FILTER && matchup.toLowerCase()!==MATCHUP_FILTER.toLowerCase()) continue;

      let row=0;
      for(let r=1;r<list.length;r++){
        const rowVals=list[r];
        if(!rowVals) continue;
        if(rowVals[iMu]===matchup && rowVals[iDate]===dateET){ row=r+1; break; }
      }
      if(!row) continue;

      const rowVals=list[row-1]||[];
      const fav=favoriteBySpreads(rowVals[iAwaySp],rowVals[iHomeSp]);
      const updates=[]; const put=(name,val)=>{ const i=hmap[name]; if(i===undefined)return; updates.push({range:`${TAB_NAME}!${colLetter(i)}${row}:${colLetter(i)}${row}`,values:[[val]]}); };

      let winner=null;
      if(isFinal){
        const a=Number(awayC?.score||0), h=Number(homeC?.score||0);
        put("final score",`${a}-${h}`); put("status","Final");
        winner=a>h?"away":h>a?"home":null;
      }

      if(isInProg||isHalf||FORCE_LIVE){
        const live=await scrapeLiveOdds(LEAGUE,ev.id);
        if(live){
          put("live away spread",live.liveAwaySpread);
          put("live home spread",live.liveHomeSpread);
          put("live away ml",live.liveAwayML);
          put("live home ml",live.liveHomeML);
          put("live total",live.liveTotal);
          if(!isFinal) put("status",isHalf?"Half":"Live");
        }
      }

      if(updates.length){
        await sheets.spreadsheets.values.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ valueInputOption:"USER_ENTERED", data:updates }});
      }

      await styleMatchupCell(sheets,row,matchup,fav,winner);
    }
  }

  log("✅ Complete");
})();
