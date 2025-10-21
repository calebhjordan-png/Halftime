import { google } from "googleapis";
import * as playwright from "playwright";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";
/** Optional: comma-separated game IDs to force update regardless of date */
const TARGET_GAME_ID = (process.env.TARGET_GAME_ID || "").trim();

/** GitHub Actions JSON-output mode (stdout = JSON only) */
const GHA_JSON_MODE = process.argv.includes("--gha") || String(process.env.GHA_JSON || "") === "1";

/** Bounds for adaptive sleep */
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

/** Column names (target) */
const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/** ===== Helpers ===== */
const _out = (s, ...a)=>s.write(a.map(x=>typeof x==='string'?x:String(x)).join(" ")+"\n");
const log  = (...a)=> GHA_JSON_MODE ? _out(process.stderr, ...a) : console.log(...a);
const warn = (...a)=> GHA_JSON_MODE ? _out(process.stderr, ...a) : console.warn(...a);

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  const json = Buffer.from(raw, "base64").toString("utf8");
  return JSON.parse(json);
}

const ET_TZ = "America/New_York";
function fmtETTime(d){return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,hour:"numeric",minute:"2-digit",hour12:true}).format(new Date(d));}
function fmtETDate(d){return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(d));}
function yyyymmddInET(d=new Date()){
  const parts=new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(d));
  const g=k=>parts.find(p=>p.type===k)?.value||""; return `${g("year")}${g("month")}${g("day")}`;
}
async function fetchJson(url){
  log("GET",url);
  const r=await fetch(url,{headers:{"User-Agent":"halftime-bot","Referer":"https://www.espn.com/"}});
  if(!r.ok) throw new Error(`Fetch failed ${r.status} ${url}`);
  return r.json();
}
function normLeague(x){return (x==="ncaaf"||x==="college-football")?"college-football":"nfl";}
function scoreboardUrl(lg,dates){lg=normLeague(lg); const extra=lg==="college-football"?"&groups=80&limit=300":""; return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;}
function summaryUrl(lg,eventId){lg=normLeague(lg); return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${eventId}`;}
function gameUrl(lg,gameId){lg=normLeague(lg); return `https://www.espn.com/${lg}/game/_/gameId/${gameId}`;}
function pickOdds(arr=[]){ if(!Array.isArray(arr)||!arr.length) return null; const p=arr.find(o=>/espn\s*bet/i.test(o.provider?.name||""))||arr.find(o=>/espn bet/i.test(o.provider?.displayName||"")); return p||arr[0];}
function mapHeadersToIndex(h){const m={}; h.forEach((x,i)=>m[(x||"").trim().toLowerCase()]=i); return m;}
function numOrBlank(v){ if(v===0) return "0"; if(v==null) return ""; const s=String(v).trim(); const n=parseFloat(s.replace(/[^\d.+-]/g,"")); if(!Number.isFinite(n)) return ""; return s.startsWith("+")?`+${n}`:`${n}`; }

/** Key helpers — prefer Game ID if present */
const KEY_KIND = { GAME_ID: "GAME_ID", DATE_MATCHUP: "DATE_MATCHUP" };
function keyForRow(row, hmap) {
  if (hmap["game id"] !== undefined && row[hmap["game id"]]) {
    return { kind: KEY_KIND.GAME_ID, key: String(row[hmap["game id"]]).trim() };
  }
  const d = (row[hmap["date"]]||"").toString().trim();
  const m = (row[hmap["matchup"]]||"").toString().trim();
  return { kind: KEY_KIND.DATE_MATCHUP, key: `${d}__${m}` };
}
function keyForEvent(ev, dateET, matchup) {
  const id = String(ev?.id || "");
  if (id) return { kind: KEY_KIND.GAME_ID, key: id };
  return { kind: KEY_KIND.DATE_MATCHUP, key: `${dateET}__${matchup}` };
}

/** Status formatting */
function tidyStatus(evt){
  const comp=evt.competitions?.[0]||{};
  const tName=(evt.status?.type?.name||comp.status?.type?.name||"").toUpperCase();
  const short=(evt.status?.type?.shortDetail||comp.status?.type?.shortDetail||"").trim();
  if(tName.includes("FINAL")) return "Final";
  if(tName.includes("HALFTIME")) return "Half";
  if(tName.includes("IN_PROGRESS")||tName.includes("LIVE")) return short||"In Progress";
  return fmtETTime(evt.date);
}

/** Week labels */
function resolveWeekLabelFromCalendar(sb, eventDateISO){
  const cal=sb?.leagues?.[0]?.calendar||sb?.calendar||[];
  const t=new Date(eventDateISO).getTime();
  for(const item of cal){
    const entries=Array.isArray(item?.entries)?item.entries:[item];
    for(const e of entries){
      const label=(e?.label||e?.detail||e?.text||"").trim();
      const s=new Date(e?.startDate||e?.start||0).getTime();
      const ed=new Date(e?.endDate||e?.end||0).getTime();
      if(Number.isFinite(s)&&Number.isFinite(ed)&&t>=s&&t<=ed) return label||"";
    }
  } return "";
}
function resolveWeekLabelNFL(sb,d){return Number.isFinite(sb?.week?.number)?`Week ${sb.week.number}`:resolveWeekLabelFromCalendar(sb,d)||"Regular Season";}
function resolveWeekLabelCFB(sb,d){const tx=(sb?.week?.text||"").trim(); return tx||resolveWeekLabelFromCalendar(sb,d)||"Regular Season";}

/** Moneylines */
function extractMoneylines(o,awayId,homeId,competitors=[]){
  let awayML="",homeML="";
  if(o&&(o.awayTeamOdds||o.homeTeamOdds)){
    const a=o.awayTeamOdds||{},h=o.homeTeamOdds||{};
    awayML=awayML||numOrBlank(a.moneyLine??a.moneyline??a.money_line);
    homeML=homeML||numOrBlank(h.moneyLine??h.moneyline??h.money_line);
    if(awayML||homeML) return {awayML,homeML};
  }
  if(o&&o.moneyline&&(o.moneyline.away||o.moneyline.home)){
    const a=numOrBlank(o.moneyline.away?.close?.odds??o.moneyline.away?.open?.odds);
    const h=numOrBlank(o.moneyline.home?.close?.odds??o.moneyline.home?.open?.odds);
    awayML=awayML||a; homeML=homeML||h; if(awayML||homeML) return {awayML,homeML};
  }
  if(Array.isArray(o?.teamOdds)){
    for(const t of o.teamOdds){
      const tid=String(t?.teamId??t?.team?.id??"");
      const ml=numOrBlank(t?.moneyLine??t?.moneyline??t?.money_line);
      if(!ml) continue;
      if(tid===String(awayId)) awayML=awayML||ml;
      if(tid===String(homeId)) homeML=homeML||ml;
    }
    if(awayML||homeML) return {awayML,homeML};
  }
  if(Array.isArray(o?.competitors)){
    const f=c=>numOrBlank(c?.moneyLine??c?.moneyline??c?.odds?.moneyLine??c?.odds?.moneyline);
    const a=f(o.competitors.find(c=>String(c?.id??c?.teamId)===String(awayId)));
    const h=f(o.competitors.find(c=>String(c?.id??c?.teamId)===String(homeId)));
    awayML=awayML||a; homeML=homeML||h; if(awayML||homeML) return {awayML,homeML};
  }
  awayML=awayML||numOrBlank(o?.moneyLineAway??o?.awayTeamMoneyLine??o?.awayMoneyLine??o?.awayMl);
  homeML=homeML||numOrBlank(o?.moneyLineHome??o?.homeTeamMoneyLine??o?.homeMoneyLine??o?.homeMl);
  if(awayML||homeML) return {awayML,homeML};
  const favId=String(o?.favorite??o?.favoriteId??o?.favoriteTeamId??"");
  const favML=numOrBlank(o?.favoriteMoneyLine); const dogML=numOrBlank(o?.underdogMoneyLine);
  if(favId&&(favML||dogML)){
    if(String(awayId)===favId){awayML=awayML||favML; homeML=homeML||dogML; return {awayML,homeML};}
    if(String(homeId)===favId){homeML=homeML||favML; awayML=awayML||dogML; return {awayML,homeML};}
  }
  if(Array.isArray(competitors)){
    for(const c of competitors){
      const ml=numOrBlank(c?.odds?.moneyLine??c?.odds?.moneyline??c?.odds?.money_line);
      if(!ml) continue;
      if(c.homeAway==="away") awayML=awayML||ml;
      if(c.homeAway==="home") homeML=homeML||ml;
    }
  }
  return {awayML,homeML};
}
async function extractMLWithFallback(event, baseOdds, away, home){
  const base=extractMoneylines(baseOdds||{},away?.team?.id,home?.team?.id,(event.competitions?.[0]?.competitors)||[]);
  if(base.awayML&&base.homeML) return base;
  try{
    const sum=await fetchJson(summaryUrl(LEAGUE,event.id));
    const cands=[];
    if(Array.isArray(sum?.header?.competitions?.[0]?.odds)) cands.push(...sum.header.competitions[0].odds);
    if(Array.isArray(sum?.odds)) cands.push(...sum.odds);
    if(Array.isArray(sum?.pickcenter)) cands.push(...sum.pickcenter);
    for(const cand of cands){
      const ml=extractMoneylines(cand,away?.team?.id,home?.team?.id,(event.competitions?.[0]?.competitors)||[]);
      if(ml.awayML||ml.homeML) return ml;
    }
  }catch(e){ warn("Summary fallback failed:", e?.message||e); }
  return base;
}

/** Pregame row builder */
function pregameRowFactory(sbForDay){
  return async function pregameRow(event){
    const comp=event.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
    const matchup=`${awayName} @ ${homeName}`;

    const isFinal=/final/i.test(event.status?.type?.name||comp.status?.type?.name||"");
    const finalScore=isFinal?`${away?.score??""}-${home?.score??""}`:"";

    const o0=pickOdds(comp.odds||event.odds||[]); let awaySpread="",homeSpread="",total="",awayML="",homeML="";
    let favorite = null;

    if(o0){
      total=(o0.overUnder??o0.total)??"";
      const favId=String(o0.favorite||o0.favoriteTeamId||"");
      const spread=Number.isFinite(o0.spread)?o0.spread:(typeof o0.spread==="string"?parseFloat(o0.spread):NaN);
      if(!Number.isNaN(spread)&&favId){
        if(String(away?.team?.id||"")===favId){favorite="away"; awaySpread=`-${Math.abs(spread)}`; homeSpread=`+${Math.abs(spread)}`;}
        else if(String(home?.team?.id||"")===favId){favorite="home"; homeSpread=`-${Math.abs(spread)}`; awaySpread=`+${Math.abs(spread)}`;}
      } else if (o0.details){
        const m=o0.details.match(/([+-]?\d+(\.\d+)?)/);
        if(m){
          const line=parseFloat(m[1]);
          if(line<0){ favorite="away"; awaySpread=`${line}`; homeSpread=`+${Math.abs(line)}`; }
          else if(line>0){ favorite="home"; homeSpread=`-${Math.abs(line)}`; awaySpread=`+${Math.abs(line)}`; }
        }
      }
      const ml=await extractMLWithFallback(event,o0,away,home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
      if(!favorite && awayML && homeML){
        const a=parseInt(String(awayML),10), h=parseInt(String(homeML),10);
        if(Number.isFinite(a)&&Number.isFinite(h)){
          if(a<0 && h>=0) favorite="away";
          else if(h<0 && a>=0) favorite="home";
          else if(Math.abs(a)>Math.abs(h)) favorite="away";
          else if(Math.abs(h)>Math.abs(a)) favorite="home";
        }
      }
    } else {
      const ml=await extractMLWithFallback(event,{},away,home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
      if(awayML && homeML){
        const a=parseInt(String(awayML),10), h=parseInt(String(homeML),10);
        if(Number.isFinite(a)&&Number.isFinite(h)){
          if(a<0 && h>=0) favorite="away";
          else if(h<0 && a>=0) favorite="home";
          else if(Math.abs(a)>Math.abs(h)) favorite="away";
          else if(Math.abs(h)>Math.abs(a)) favorite="home";
        }
      }
    }

    const weekText=(normLeague(LEAGUE)==="nfl")?resolveWeekLabelNFL(sbForDay,event.date):resolveWeekLabelCFB(sbForDay,event.date);
    const statusClean=tidyStatus(event);
    const dateET=fmtETDate(event.date);

    return {
      gameId: String(event.id),
      values:[
        String(event.id),
        dateET,
        weekText || "",
        statusClean,
        matchup,               // plain text; rich text added later
        finalScore,
        awaySpread || "",
        String(awayML || ""),
        homeSpread || "",
        String(homeML || ""),
        String(total || ""),
        "", "", "", "", "", ""
      ],
      dateET,
      matchup,
      meta: {
        awayName, homeName, favorite, isFinal,
        awayFinal: Number(away?.score ?? NaN),
        homeFinal: Number(home?.score ?? NaN),
      }
    };
  };
}

/** Halftime helpers */
function isHalftimeLike(evt){
  const t=(evt.status?.type?.name||evt.competitions?.[0]?.status?.type?.name||"").toUpperCase();
  const short=(evt.status?.type?.shortDetail||"").toUpperCase();
  return t.includes("HALFTIME")||/Q2.*0:0?0/.test(short)||/HALF/.test(short);
}
function parseShortDetailClock(s=""){ s=String(s).trim().toUpperCase();
  if(/HALF/.test(s)||/HALFTIME/.test(s)) return {quarter:2,min:0,sec:0,halftime:true};
  if(/FINAL/.test(s)) return {final:true};
  const m=s.match(/Q?(\d)\D+(\d{1,2}):(\d{2})/); if(!m) return null;
  return {quarter:Number(m[1]),min:Number(m[2]),sec:Number(m[3])};
}
function minutesAfterKickoff(evt){ return (Date.now()-new Date(evt.date).getTime())/60000; }
function clampRecheck(mins){ return Math.max(MIN_RECHECK_MIN, Math.min(MAX_RECHECK_MIN, Math.ceil(mins))); }

/** Live odds scrape (one-time at halftime) */
async function scrapeLiveOddsOnce(league, gameId){
  const url=gameUrl(league,gameId);
  const browser=await playwright.chromium.launch({headless:true});
  const page=await browser.newPage();
  try{
    await page.goto(url,{timeout:60000,waitUntil:"domcontentloaded"});
    await page.waitForLoadState("networkidle",{timeout:6000}).catch(()=>{});
    await page.waitForTimeout(500);
    const section=page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({timeout:8000});
    const txt=(await section.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();
    const spreadMatches=txt.match(/([+-]\d+(\.\d+)?)/g)||[];
    const totalOver=txt.match(/o\s?(\d+(\.\d+)?)/i);
    const totalUnder=txt.match(/u\s?(\d+(\.\d+)?)/i);
    const mlMatches=txt.match(/\s[+-]\d{2,4}\b/g)||[];
    const liveAwaySpread=spreadMatches[0]||"";
    const liveHomeSpread=spreadMatches[1]||"";
    const liveTotal=(totalOver&&totalOver[1])||(totalUnder&&totalUnder[1])||"";
    const liveAwayML=(mlMatches[0]||"").trim();
    const liveHomeML=(mlMatches[1]||"").trim();
    let halfScore="";
    try{
      const allTxt=(await page.locator("body").innerText()).replace(/\s+/g," ");
      const sc=allTxt.match(/(\b\d{1,2}\b)\s*-\s*(\b\d{1,2}\b)/);
      if(sc) halfScore=`${sc[1]}-${sc[2]}`;
    }catch{}
    return {liveAwaySpread,liveHomeSpread,liveTotal,liveAwayML,liveHomeML,halfScore};
  }catch(err){ warn("Live DOM scrape failed:", err.message, url); return null; }
  finally{ await browser.close(); }
}

/** Batch writer */
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0)+i); }
class BatchWriter{
  constructor(tab){ this.tab=tab; this.acc=[]; }
  add(row, colIdx, value){ if(colIdx==null||colIdx<0) return; const range=`${this.tab}!${colLetter(colIdx)}${row}:${colLetter(colIdx)}${row}`; this.acc.push({range, values:[[value]]}); }
  async flush(sheets){ if(!this.acc.length) return; await sheets.spreadsheets.values.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{valueInputOption:"RAW", data:this.acc}}); log(`Batched ${this.acc.length} cell update(s).`); this.acc=[]; }
}

/** Header reconciliation (rename legacy headers → target labels) */
function reconcileHeaderRow(header){
  const map = (s)=> (s||"").toLowerCase().trim();
  const renamed = header.slice();
  const rename = (from, to)=>{
    const i = renamed.findIndex(h=>map(h)===map(from));
    if(i>=0) renamed[i]=to;
  };

  // Away/Home → A/H
  rename("Away Spread","A Spread"); rename("Away ML","A ML");
  rename("Home Spread","H Spread"); rename("Home ML","H ML");

  // Half Score → Score
  rename("Half Score","Score");

  // Live → Half
  rename("Live Away Spread","H A Spread");
  rename("Live Away ML","H A ML");
  rename("Live Home Spread","H H Spread");
  rename("Live Home ML","H H ML");
  rename("Live Total","H Total");

  // Also allow “Half …” → “H …”
  rename("Half A Spread","H A Spread");
  rename("Half A ML","H A ML");
  rename("Half H Spread","H H Spread");
  rename("Half H ML","H H ML");
  rename("Half Total","H Total");

  if(renamed.length===0) return COLS.slice();

  // Normalize ordering to COLS
  const out = COLS.slice();
  const lower = renamed.map(x=>x.toLowerCase());
  COLS.forEach((label, idx)=>{
    const j = lower.indexOf(label.toLowerCase());
    out[idx] = j>=0 ? renamed[j] : label;
  });
  return out;
}

/** Center-align + conditional format (H Spread only when Final Score present) */
async function applyCenterAndCF(sheets){
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const sheet = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME);
  const sheetId = sheet?.properties?.sheetId;
  if(sheetId==null) return null;

  const reqs = [];

  // center A..Q
  reqs.push({
    repeatCell:{
      range:{sheetId,startRowIndex:0,startColumnIndex:0,endColumnIndex:17},
      cell:{userEnteredFormat:{horizontalAlignment:"CENTER"}},
      fields:"userEnteredFormat.horizontalAlignment"
    }
  });

  // Conditional formatting for I (H Spread): only when Final Score (F) present
  const firstDataRow = 2;
  const addCF = (expr, color) => reqs.push({
    addConditionalFormatRule:{
      index:0,
      rule:{
        ranges:[{sheetId,startRowIndex:firstDataRow-1,startColumnIndex:8,endColumnIndex:9}],
        booleanRule:{
          condition:{
            type:"CUSTOM_FORMULA",
            values:[{userEnteredValue: `=AND($F2<>"", ${expr})`}]
          },
          format:{ backgroundColor: color }
        }
      }
    }
  });
  addCF("I2>0", {red:1,green:0.85,blue:0.85});
  addCF("I2<0", {red:0.85,green:1,blue:0.85});

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ requests:reqs }
  });

  return sheetId;
}

/** Rich text for Matchup — exactly two runs, clear any links */
function matchupRuns(fullText, awayName, homeName, favorite, winner){
  const awayFmt = { underline: favorite==='away'||false, bold: winner==='away'||false, link: null };
  const homeFmt = { underline: favorite==='home'||false, bold: winner==='home'||false, link: null };

  const awayStart = 0;
  const homeStart = awayName.length + 3; // " @ "
  const len = fullText.length;

  const runs = [];
  runs.push({ startIndex: awayStart, format: awayFmt });
  if (homeStart > 0 && homeStart < len) runs.push({ startIndex: homeStart, format: homeFmt });
  return runs;
}

/** ===== MAIN ===== */
(async function main(){
  if(!SHEET_ID||!CREDS_RAW){
    const msg="Missing secrets.";
    if(GHA_JSON_MODE){ console.log(JSON.stringify({ok:false,error:msg})); return; }
    console.error(msg); process.exit(1);
  }
  const CREDS=parseServiceAccount(CREDS_RAW);
  const auth=new google.auth.GoogleAuth({credentials:{client_email:CREDS.client_email, private_key:CREDS.private_key}, scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sheets=google.sheets({version:"v4",auth});

  // Ensure tab + header; reconcile names if needed
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const tabs=(meta.data.sheets||[]).map(s=>s.properties?.title);
  if(!tabs.includes(TAB_NAME)){
    await sheets.spreadsheets.batchUpdate({spreadsheetId:SHEET_ID, requestBody:{requests:[{addSheet:{properties:{title:TAB_NAME}}}]}}); 
  }
  const read=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z`});
  const values=read.data.values||[];
  let headerOrig=values[0]||[];
  let header=reconcileHeaderRow(headerOrig);

  if(headerOrig.join("|||") !== header.join("|||")){
    await sheets.spreadsheets.values.update({
      spreadsheetId:SHEET_ID,
      range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW",
      requestBody:{values:[header]}
    });
  }
  const hmap=mapHeadersToIndex(header);
  const rows=values.slice(1);

  /** Build index of existing rows — prefer Game ID */
  const keyToRowNum = new Map();
  rows.forEach((r,i)=>{
    const {key}=keyForRow(r,hmap);
    if(key) keyToRowNum.set(key, i+2);
  });

  const sheetId = await applyCenterAndCF(sheets);

  // Pull events (today/week) + forced IDs
  const datesList = RUN_SCOPE==="week"
    ? (()=>{const start=new Date(); return Array.from({length:7},(_,i)=>yyyymmddInET(new Date(start.getTime()+i*86400000)));})()
    : [yyyymmddInET(new Date())];

  let firstDaySB=null, events=[];
  for(const d of datesList){ const sb=await fetchJson(scoreboardUrl(LEAGUE,d)); if(!firstDaySB) firstDaySB=sb; events=events.concat(sb?.events||[]); }

  // Force-add specific IDs (any date)
  if (TARGET_GAME_ID) {
    const ids = TARGET_GAME_ID.split(",").map(s=>s.trim()).filter(Boolean);
    for (const id of ids) {
      try {
        const sum = await fetchJson(summaryUrl(LEAGUE, id));
        const evt = {
          id,
          date: sum?.header?.competitions?.[0]?.date || sum?.boxscore?.gameInfo?.gameClock || new Date().toISOString(),
          competitions: [{
            status: sum?.header?.competitions?.[0]?.status,
            competitors: sum?.header?.competitions?.[0]?.competitors,
            odds: sum?.header?.competitions?.[0]?.odds || sum?.odds || sum?.pickcenter || []
          }],
          status: sum?.header?.competitions?.[0]?.status
        };
        events.push(evt);
      } catch(e) {
        warn("Force-add ID failed:", id, e?.message||e);
      }
    }
  }

  const seen=new Set(); events=events.filter(e=>!seen.has(e.id)&&seen.add(e.id));
  log(`Events found: ${events.length}`);

  const buildPregame=pregameRowFactory(firstDaySB);
  let appendBatch=[];
  const formatRequests = [];

  // Append pregame rows for new events
  for(const ev of events){
    const pr=await buildPregame(ev);
    const {values:rowVals, dateET, matchup} = pr;
    const {key}=keyForEvent(ev, dateET, matchup);
    if(!keyToRowNum.has(key)){
      appendBatch.push(rowVals);
    }
  }
  if(appendBatch.length){
    await sheets.spreadsheets.values.append({spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1`, valueInputOption:"RAW", requestBody:{values:appendBatch}});
    // Refresh index
    const re=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z`});
    const v2=re.data.values||[], hdr2=v2[0]||header, h2=mapHeadersToIndex(hdr2);
    (v2.slice(1)).forEach((r,i)=>{ const {key}=keyForRow(r,h2); if(key) keyToRowNum.set(key, i+2); });
    log(`Appended ${appendBatch.length} pregame row(s).`);
  }

  // Finals/format pass
  const batch=new BatchWriter(TAB_NAME);
  let masterDelayMin=null;

  for(const ev of events){
    const comp=ev.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
    const matchup=`${awayName} @ ${homeName}`;
    const dateET=fmtETDate(ev.date);

    const {key}=keyForEvent(ev, dateET, matchup);
    const rowNum=keyToRowNum.get(key);
    if(!rowNum) continue;

    // favorite (for underline)
    let favorite=null;
    const o0=pickOdds(comp.odds||ev.odds||[]);
    const favId=String(o0?.favorite||o0?.favoriteTeamId||"");
    if(favId){
      if(String(away?.team?.id||"")===favId) favorite="away";
      else if(String(home?.team?.id||"")===favId) favorite="home";
    } else {
      const ml=await extractMLWithFallback(ev, o0, away, home);
      const a=parseInt(String(ml.awayML||""),10), h=parseInt(String(ml.homeML||""),10);
      if(Number.isFinite(a)&&Number.isFinite(h)){
        if(a<0 && h>=0) favorite="away";
        else if(h<0 && a>=0) favorite="home";
        else if(Math.abs(a)>Math.abs(h)) favorite="away";
        else if(Math.abs(h)>Math.abs(a)) favorite="home";
      }
    }

    const statusName=(ev.status?.type?.name||comp.status?.type?.name||"").toUpperCase();
    const isFinalGame = statusName.includes("FINAL");
    const scorePair=`${away?.score??""}-${home?.score??""}`;

    // rich text: underline favorite, bold winner if final (and clear any links)
    if(sheetId!=null){
      const winner = isFinalGame
        ? (Number(away?.score)>Number(home?.score) ? "away"
          : Number(home?.score)>Number(away?.score) ? "home" : null)
        : null;
      const fullText = `${awayName} @ ${homeName}`;
      const runs = matchupRuns(fullText, awayName, homeName, favorite, winner);
      formatRequests.push({
        updateCells: {
          range: { sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: 4, endColumnIndex: 5 },
          rows: [{ values: [{ userEnteredValue: { stringValue: fullText }, textFormatRuns: runs }] }],
          fields: "userEnteredValue,textFormatRuns"
        }
      });
    }

    if(isFinalGame){
      if(hmap["final score"]!==undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if(hmap["status"]!==undefined)      batch.add(rowNum, hmap["status"], "Final");
      continue;
    }

    // (Adaptive halftime scheduling left as-is)
    if(ADAPTIVE_HALFTIME){
      const short=(ev.status?.type?.shortDetail||"").trim();
      const parsed=parseShortDetailClock(short);
      const mins=minutesAfterKickoff(ev);
      const currentRow=(values[rowNum-1]||[]);
      const halfAlready=(currentRow[hmap["score"]]||"").toString().trim();

      const kickCand = (()=>{ if(halfAlready) return null; if(mins<60||mins>80) return null; const rem=65-mins; return clampRecheck(rem<=0?MIN_RECHECK_MIN:rem);})();
      const q2Cand   = (()=>{ if(!parsed||parsed.final||parsed.halftime||parsed.quarter!==2) return null; const left=parsed.min+parsed.sec/60; if(left>=10) return null; return clampRecheck(2*left); })();

      if(kickCand!=null) masterDelayMin = masterDelayMin==null?kickCand:Math.min(masterDelayMin,kickCand);
      if(q2Cand  !=null) masterDelayMin = masterDelayMin==null?q2Cand  :Math.min(masterDelayMin,q2Cand);
    }
  }
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ valueInputOption:"RAW", data: batch.acc }
  });
  batch.acc = [];

  if(formatRequests.length){
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: formatRequests }
    });
  }

  log("Run complete.");

  if(GHA_JSON_MODE){
    const payload={ ok:true, league:normLeague(LEAGUE), scope:RUN_SCOPE, tab:TAB_NAME };
    process.stdout.write(JSON.stringify(payload)+"\n");
  }
})().catch(err=>{
  if(GHA_JSON_MODE){ process.stdout.write(JSON.stringify({ok:false,error:String(err?.message||err)})+"\n"); }
  else { console.error("Error:", err); process.exit(1); }
});
