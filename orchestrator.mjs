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

/** ===== Conditional formatting & number coercion ===== */
async function resetAndApplyFormatting(sheets){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;

  // Delete existing CF rules safely
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SHEET_ID,
    fields: "sheets(properties(sheetId,title),conditionalFormats)"
  });
  const sheet = (meta.data.sheets || []).find(s => s.properties?.sheetId === sid);
  const cf = sheet?.conditionalFormats || [];
  const count = cf.length || 0;

  const deleteReqs = [];
  for (let idx = count - 1; idx >= 0; idx--) {
    deleteReqs.push({ deleteConditionalFormatRule: { sheetId: sid, index: idx } });
  }

  // Force F:J to Automatic number format so CF treats them as numbers
  const numberCoerce = {
    repeatCell: {
      range: { sheetId: sid, startRowIndex: 1, startColumnIndex: 5, endColumnIndex: 10 }, // F:J
      cell: { userEnteredFormat: { numberFormat: { type: "NUMBER" } } },
      fields: "userEnteredFormat.numberFormat"
    }
  };

  const color = (r,g,b)=>({ red:r, green:g, blue:b });
  const rng = (startColIdxZeroBased) =>
    ({ sheetId: sid, startColumnIndex: startColIdxZeroBased, endColumnIndex: startColIdxZeroBased+1, startRowIndex: 1 });

  const addRule = (startColIdxZeroBased, formula, bg) => ({
    addConditionalFormatRule: {
      rule: {
        ranges: [rng(startColIdxZeroBased)],
        booleanRule: { condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: formula }] },
                       format: { backgroundColor: bg } }
      },
      index: 0
    }
  });

  // Column indices (0-based): F=5, G=6, H=7, I=8, J=9
  const green = color(0.85,1,0.85);
  const red   = color(1,0.85,0.85);

  const requests = [
    ...deleteReqs,
    numberCoerce,

    // H first: Final logic + not-final sign tint
    addRule(7, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))=0)', color(1,1,0.7)), // push
    addRule(7, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))>0)', green),        // cover
    addRule(7, '=AND($C2="Final", (VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2))<0)', red),          // no cover
    addRule(7, '=AND($C2<>"Final", $H2<0)', color(0.92,1,0.92)),
    addRule(7, '=AND($C2<>"Final", $H2>0)', color(1,0.92,0.92)),

    // F (Away Spread) by sign
    addRule(5, '=$F2<0', green),
    addRule(5, '=$F2>0', red),

    // G (Away ML) by sign
    addRule(6, '=$G2<0', green),
    addRule(6, '=$G2>0', red),

    // I (Home ML) by sign
    addRule(8, '=$I2<0', green),
    addRule(8, '=$I2>0', red),

    // J left neutral for now
  ];

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests }
  });
  log("🎨 Rebuilt conditional formatting for F:J and coerced numbers.");
}

/** ===== Matchup styling (favorite underline + winner bold) ===== */
function matchupTextRuns(matchup, fav, winner){
  const parts = matchup.split(" @ ");
  if (parts.length !== 2) return null;
  const away = parts[0], home = parts[1];
  const sep = " @ ", full = `${away}${sep}${home}`;

  const fmtAway = { underline: fav==="away", bold: winner==="away" };
  const fmtHome = { underline: fav==="home", bold: winner==="home" };

  return {
    full,
    runs: [
      { startIndex: 0, format: fmtAway },
      { startIndex: away.length + sep.length, format: fmtHome }
    ]
  };
}
async function styleMatchupCell(sheets, rowNum, matchup, fav, winner){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;
  const mr = matchupTextRuns(matchup, fav, winner);
  if (!mr) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        updateCells: {
          range: { sheetId: sid, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: 3, endColumnIndex: 4 }, // D
          rows: [{ values: [{ userEnteredValue: { stringValue: mr.full }, textFormatRuns: mr.runs }]}],
          fields: "userEnteredValue,textFormatRuns"
        }
      }]
    }
  });
}

/** ===== Favorite detection (fixed) =====
 * Choose the team with the *more negative* spread.
 * This works whether one side is negative and the other positive (normal),
 * or if both are negative/positive (fallback).
 */
function favoriteBySpreads(awaySpread, homeSpread){
  const a = num(awaySpread);
  const h = num(homeSpread);
  if (Number.isNaN(a) || Number.isNaN(h)) return null;
  if (a === h) return null;           // identical; no favorite
  return (a < h) ? "away" : "home";   // smaller (more negative) number is favorite
}

/** ===== ESPN + Live odds ===== */
function scoreboardUrl(league, date){
  const lg = (league === "college-football") ? "college-football" : "nfl";
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${date}${extra}`;
}
function gameUrl(league, id){
  const lg = (league === "college-football") ? "college-football" : "nfl";
  return `https://www.espn.com/${lg}/game/_/gameId/${id}`;
}

/** ===== Live scrape ===== */
async function scrapeLiveOdds(league, gameId){
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless:true });
  const context = await browser.newContext();
  if (DEBUG_LEVEL >= 2) await context.tracing.start({ screenshots:true, snapshots:true });
  const page = await context.newPage();
  try{
    await page.goto(url, { waitUntil:"domcontentloaded", timeout:60000 });
    await page.waitForTimeout(1500);
    const body = (await page.locator("body").innerText()).replace(/\s+/g," ");
    const spreads = body.match(/(^|\s)([+-]\d+(?:\.\d+)?)(?=\s)/g) || [];
    const totalAny = body.match(/\b(?:o|u)\s?(\d+(?:\.\d+)?)/i);
    const mls = body.match(/\s[+-]\d{2,4}\b/g) || [];
    return {
      liveAwaySpread: (spreads[0]||"").trim(),
      liveHomeSpread: (spreads[1]||"").trim(),
      liveTotal: totalAny ? totalAny[1] : "",
      liveAwayML: (mls[0]||"").trim(),
      liveHomeML: (mls[1]||"").trim()
    };
  } finally {
    if (DEBUG_LEVEL >= 2) await context.tracing.stop({ path:"trace.zip" });
    await browser.close();
  }
}

/** ===== Main ===== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  const sheets = sheetsClient();

  await resetAndApplyFormatting(sheets);

  // date set (today; +yesterday for week scope)
  const today = new Date();
  const dates = RUN_SCOPE==="week"
    ? [ yyyymmddET(addDays(today,-1)), yyyymmddET(today) ]
    : [ yyyymmddET(today) ];

  // sheet header+data
  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0] || [];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P6000` })).data.values || [];
  const iDate = hmap["date"], iMu = hmap["matchup"], iAwaySp = hmap["away spread"], iHomeSp = hmap["home spread"];

  for (const date of dates){
    const data = await (await fetch(scoreboardUrl(LEAGUE,date))).json();
    for (const ev of (data.events||[])){
      const comp = ev.competitions?.[0] || {};
      const awayC = comp.competitors?.find(c=>c.homeAway==="away");
      const homeC = comp.competitors?.find(c=>c.homeAway==="home");
      const awayTeam = awayC?.team?.shortDisplayName || "Away";
      const homeTeam = homeC?.team?.shortDisplayName || "Home";
      const matchup  = `${awayTeam} @ ${homeTeam}`;
      const statusName = (ev.status?.type?.name || "").toUpperCase();
      const isFinal   = statusName.includes("FINAL");
      const isHalf    = statusName.includes("HALF");
      const isInProg  = statusName.includes("PROGRESS");
      const dateET    = fmtETDate(ev.date);

      if (MATCHUP_FILTER && matchup.toLowerCase() !== MATCHUP_FILTER.toLowerCase()) continue;

      // locate row by Date + Matchup
      let rowNum = 0;
      for(let r=1;r<list.length;r++){
        const row = list[r] || [];
        if ((row[iMu]||"") === matchup && (row[iDate]||"") === dateET){ rowNum = r+1; break; }
      }
      if (!rowNum) continue;

      const updates = [];
      const put = (name, val)=>{
        const idx = hmap[name];
        if (idx === undefined || val === undefined || val === null || val === "") return;
        updates.push({ range: `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values: [[val]] });
      };

      // favorite from existing sheet spreads (F/H)
      const rowVals = list[rowNum-1] || [];
      const fav = favoriteBySpreads(rowVals[iAwaySp], rowVals[iHomeSp]);

      // finals always written
      let winner = null;
      if (isFinal){
        const a = Number(awayC?.score ?? "");
        const h = Number(homeC?.score ?? "");
        if (!Number.isNaN(a) && !Number.isNaN(h)){
          put("final score", `${a}-${h}`);
          winner = a>h ? "away" : (h>a ? "home" : null);
        }
        put("status", "Final");
      }

      // live odds while in play OR debug-force
      if (isInProg || isHalf || FORCE_LIVE){
        const live = await scrapeLiveOdds(LEAGUE, ev.id);
        if (live){
          put("live away spread", live.liveAwaySpread);
          put("live home spread", live.liveHomeSpread);
          put("live away ml", live.liveAwayML);
          put("live home ml", live.liveHomeML);
          put("live total", live.liveTotal);
          if (!isFinal) put("status", isHalf ? "Half" : "Live");
        }
      }

      if (updates.length){
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: { valueInputOption: "USER_ENTERED", data: updates }
        });
      }

      // style D: favorite underline + winner bold
      await styleMatchupCell(sheets, rowNum, matchup, fav, winner);
    }
  }

  log("✅ Orchestrator complete");
})();
