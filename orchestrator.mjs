// orchestrator.mjs
import { google } from "googleapis";
import * as fs from "node:fs";
import * as playwright from "playwright";

/* ========= ENV ========= */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE     = (process.env.LEAGUE || "nfl").toLowerCase();             // nfl | college-football
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "today").toLowerCase();        // today | week
const MATCHUP_FILTER = (process.env.MATCHUP_FILTER || "").trim();
const FORCE_LIVE     = String(process.env.FORCE_LIVE || "false").toLowerCase()==="true";
const DEBUG_LEVEL    = Number(process.env.DEBUG_LEVEL ?? 0);

/* ========= Utils ========= */
const log=(...a)=>console.log(...a);
const wlog=(...a)=>console.warn("[warn]",...a);
const num=v=>Number.parseFloat(String(v).replace("+",""));
const ET_TZ="America/New_York";
const fmtETDate=d=>new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(d));
function yyyymmddET(d=new Date()){
  const p=new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(d));
  const g=k=>p.find(x=>x.type===k)?.value||""; return `${g("year")}${g("month")}${g("day")}`;
}
const addDays=(d,n)=>{const t=new Date(d);t.setDate(t.getDate()+n);return t;};
const colLetter=i=>String.fromCharCode(65+i);
const headerMap=h=>{const m={}; (h||[]).forEach((v,i)=>m[(v||"").toLowerCase()]=i); return m;};

/* ========= Sheets ========= */
function sheetsClient(){
  const creds = CREDS_RAW.trim().startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials:{client_email:creds.client_email, private_key:creds.private_key},
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  return google.sheets({version:"v4", auth});
}
async function sheetIdByTitle(sheets, spreadsheetId, title){
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  return meta.data.sheets?.find(s=>s.properties?.title===title)?.properties?.sheetId ?? null;
}

/* ========= Conditional Formatting (Final-aware) =========
   F (Away Spread): Final -> cover/push/no-cover vs final score; else no color
   G (Away ML):     Final -> green if away won; red if lost
   H (Home Spread): Final -> cover/push/no-cover; Not Final -> soft sign tint
   I (Home ML):     Final -> green if home won; red if lost
   J (Total):       Final -> Over green / Under red / Push yellow
   All formulas use VALUE() so +2.5 etc. work as numbers.
*/
async function resetAndApplyFormatting(sheets){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;

  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SHEET_ID,
    fields: "sheets(properties(sheetId,title),conditionalFormats)"
  });
  const sheet = (meta.data.sheets || []).find(s => s.properties?.sheetId === sid);
  const count = sheet?.conditionalFormats?.length || 0;

  const deleteReqs = [];
  for(let i=count-1;i>=0;i--) deleteReqs.push({ deleteConditionalFormatRule:{ sheetId:sid, index:i } });

  const rng = (c0)=>({ sheetId:sid, startColumnIndex:c0, endColumnIndex:c0+1, startRowIndex:1 });
  const add = (c0, formula, bg)=>({ addConditionalFormatRule:{ rule:{ ranges:[rng(c0)], booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:formula}] }, format:{ backgroundColor:bg } } }, index:0 } });
  const color=(r,g,b)=>({red:r,green:g,blue:b});
  const green=color(0.85,1,0.85), red=color(1,0.85,0.85), yellow=color(1,1,0.7), softG=color(0.92,1,0.92), softR=color(1,0.92,0.92);

  // helpers in formulas
  // margin = home - away; sum = home + away
  const MARGIN = "(VALUE(INDEX(SPLIT($E2,\"-\"),2)) - VALUE(INDEX(SPLIT($E2,\"-\"),1)))";
  const SUM    = "(VALUE(INDEX(SPLIT($E2,\"-\"),2)) + VALUE(INDEX(SPLIT($E2,\"-\"),1)))";

  const reqs = [
    ...deleteReqs,

    // ----- H (Home Spread): Final cover/push/no-cover + not-final sign tint
    add(7, `=AND($C2="Final", ${MARGIN} + VALUE($H2)=0)`, yellow),
    add(7, `=AND($C2="Final", ${MARGIN} + VALUE($H2)>0)`, green),
    add(7, `=AND($C2="Final", ${MARGIN} + VALUE($H2)<0)`, red),
    add(7, `=AND($C2<>"Final", VALUE($H2)<0)`, softG),
    add(7, `=AND($C2<>"Final", VALUE($H2)>0)`, softR),

    // ----- F (Away Spread): Final cover/push/no-cover against away side
    // away cover test: (away - home + F) > 0  -> (-margin + F) > 0
    add(5, `=AND($C2="Final", -${MARGIN} + VALUE($F2)=0)`, yellow),
    add(5, `=AND($C2="Final", -${MARGIN} + VALUE($F2)>0)`, green),
    add(5, `=AND($C2="Final", -${MARGIN} + VALUE($F2)<0)`, red),

    // ----- G (Away ML): Final -> green if away won; red otherwise
    add(6, `=AND($C2="Final", VALUE(INDEX(SPLIT($E2,"-"),1)) > VALUE(INDEX(SPLIT($E2,"-"),2)))`, green),
    add(6, `=AND($C2="Final", VALUE(INDEX(SPLIT($E2,"-"),1)) < VALUE(INDEX(SPLIT($E2,"-"),2)))`, red),

    // ----- I (Home ML): Final -> green if home won; red otherwise
    add(8, `=AND($C2="Final", VALUE(INDEX(SPLIT($E2,"-"),2)) > VALUE(INDEX(SPLIT($E2,"-"),1)))`, green),
    add(8, `=AND($C2="Final", VALUE(INDEX(SPLIT($E2,"-"),2)) < VALUE(INDEX(SPLIT($E2,"-"),1)))`, red),

    // ----- J (Total): Final Over/Under/Push
    add(9, `=AND($C2="Final", ${SUM} - VALUE($J2)>0)`, green),
    add(9, `=AND($C2="Final", ${SUM} - VALUE($J2)<0)`, red),
    add(9, `=AND($C2="Final", ${SUM} - VALUE($J2)=0)`, yellow),
  ];

  await sheets.spreadsheets.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ requests:reqs }});
  log("🎨 Rebuilt CF: Final-aware F,G,H,I,J (and soft tints for H when not final).");
}

/* ========= Matchup styling (favorite underline + winner bold) ========= */
function favoriteBySpreads(awaySpread, homeSpread){
  const a=num(awaySpread), h=num(homeSpread);
  if (Number.isNaN(a) || Number.isNaN(h)) return null;
  if (a===h) return null;
  // team with the MORE NEGATIVE spread is favorite
  return (a < h) ? "away" : "home";
}
function matchupTextRuns(matchup, fav, winner){
  const parts = matchup.split(" @ ");
  if (parts.length!==2) return null;
  const away=parts[0], home=parts[1], sep=" @ ", full=`${away}${sep}${home}`;
  const fmtAway = { underline: fav==="away" || undefined, bold: winner==="away" || undefined };
  const fmtHome = { underline: fav==="home" || undefined, bold: winner==="home" || undefined };
  return { full, runs:[ {startIndex:0, format:fmtAway}, {startIndex:away.length+sep.length, format:fmtHome} ] };
}
async function styleMatchupCell(sheets, rowNum, matchup, fav, winner){
  const sid = await sheetIdByTitle(sheets, SHEET_ID, TAB_NAME);
  if (!sid) return;
  const mr = matchupTextRuns(matchup, fav, winner);
  if (!mr) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ requests:[{
      updateCells:{
        range:{ sheetId:sid, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:3, endColumnIndex:4 },
        rows:[{ values:[{ userEnteredValue:{ stringValue:mr.full }, textFormatRuns:mr.runs }]}],
        fields:"userEnteredValue,textFormatRuns"
      }
    }]}
  });
}

/* ========= ESPN + Live ========= */
const scoreboardUrl=(league,date)=>{
  const lg = league==="college-football" ? "college-football" : "nfl";
  const extra = lg==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${date}${extra}`;
};
const gameUrl=(league,id)=>`https://www.espn.com/${league==="college-football"?"college-football":"nfl"}/game/_/gameId/${id}`;

async function scrapeLiveOdds(league, gameId){
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless:true });
  const ctx = await browser.newContext();
  if (DEBUG_LEVEL>=2) await ctx.tracing.start({ screenshots:true, snapshots:true });
  const page = await ctx.newPage();
  try{
    await page.goto(url,{waitUntil:"domcontentloaded",timeout:60000});
    await page.waitForTimeout(1500);
    const txt=(await page.locator("body").innerText()).replace(/\s+/g," ");

    const spreadMatches = txt.match(/(^|\s)([+-]\d+(?:\.\d+)?)(?=\s)/g) || [];
    const totalAny      = txt.match(/\b(?:o|u)\s?(\d+(?:\.\d+)?)/i);
    const mlMatches     = txt.match(/\s[+-]\d{2,4}\b/g) || [];

    return {
      liveAwaySpread:(spreadMatches[0]||"").trim(),
      liveHomeSpread:(spreadMatches[1]||"").trim(),
      liveTotal: totalAny ? totalAny[1] : "",
      liveAwayML:(mlMatches[0]||"").trim(),
      liveHomeML:(mlMatches[1]||"").trim()
    };
  } finally {
    if (DEBUG_LEVEL>=2) await ctx.tracing.stop({ path:"trace.zip" });
    await browser.close();
  }
}

/* ========= MAIN ========= */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT"); process.exit(1);
  }
  const sheets = sheetsClient();

  // Rebuild formatting each run so it's consistent
  await resetAndApplyFormatting(sheets);

  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0]||[];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P6000` })).data.values||[];
  const iDate=hmap["date"], iMu=hmap["matchup"], iAwaySp=hmap["away spread"], iHomeSp=hmap["home spread"];

  const today = new Date();
  const dates = RUN_SCOPE==="week" ? [ yyyymmddET(addDays(today,-1)), yyyymmddET(today) ] : [ yyyymmddET(today) ];

  for (const dateStr of dates){
    const data = await (await fetch(scoreboardUrl(LEAGUE,dateStr))).json();
    for (const ev of (data.events||[])){
      const comp=ev.competitions?.[0]||{};
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

      // find row by Date+Matchup
      let rowNum=0;
      for(let r=1;r<list.length;r++){
        const row=list[r]||[];
        if ((row[iMu]||"")===matchup && (row[iDate]||"")===dateET){ rowNum=r+1; break; }
      }
      if (!rowNum) continue;

      const rowVals=list[rowNum-1]||[];
      const updates=[]; const put=(n,v)=>{ const i=hmap[n]; if(i===undefined||v===undefined||v===null||v==="") return; updates.push({range:`${TAB_NAME}!${colLetter(i)}${rowNum}:${colLetter(i)}${rowNum}`,values:[[v]]}); };

      // Finals: always write
      let winner=null;
      if (isFinal){
        const a=Number(awayC?.score ?? ""), h=Number(homeC?.score ?? "");
        if (!Number.isNaN(a) && !Number.isNaN(h)){ put("final score", `${a}-${h}`); winner = a>h ? "away" : (h>a ? "home" : null); }
        put("status","Final");
      }

      // Live odds if needed
      if (isInProg || isHalf || FORCE_LIVE){
        const live = await scrapeLiveOdds(LEAGUE, ev.id);
        if (live){
          put("live away spread", live.liveAwaySpread);
          put("live home spread", live.liveHomeSpread);
          put("live away ml",    live.liveAwayML);
          put("live home ml",    live.liveHomeML);
          put("live total",      live.liveTotal);
          if (!isFinal) put("status", isHalf ? "Half" : "Live");
        }
      }

      if (updates.length){
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId:SHEET_ID,
          requestBody:{ valueInputOption:"USER_ENTERED", data:updates }
        });
      }

      // Favorite from spreads; style D with underline+bold
      const fav = favoriteBySpreads(rowVals[iAwaySp], rowVals[iHomeSp]);
      await styleMatchupCell(sheets, rowNum, matchup, fav, winner);
    }
  }

  log("✅ Orchestrator complete");
})().catch(e=>{ console.error("FATAL", e); process.exit(1); });
