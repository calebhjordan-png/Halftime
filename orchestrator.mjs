import { google } from "googleapis";
import * as playwright from "playwright"; // kept for parity if you call others
import axios from "axios";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID      = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW     = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE        = (process.env.LEAGUE || "nfl").toLowerCase();          // "nfl" | "college-football"
const TAB_NAME      = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE     = (process.env.RUN_SCOPE || "today").toLowerCase();     // "today" | "week"
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";

// Accept single or multiple IDs, comma-separated
const TARGET_GAME_ID = (process.env.TARGET_GAME_ID || "").trim();

/** GitHub Actions JSON-output mode (stdout = JSON only) */
const GHA_JSON_MODE = process.argv.includes("--gha") || String(process.env.GHA_JSON || "") === "1";

/** Bounds for adaptive sleep */
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

/** Column names — includes Game ID in col A */
const COLS = [
  "Game ID", "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
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
const fmtETDateOnly = (d) =>
  new Intl.DateTimeFormat("en-US",{ timeZone: ET_TZ, month:"2-digit", day:"2-digit" }).format(new Date(d));
const fmtETTimeOnly = (d) =>
  new Intl.DateTimeFormat("en-US",{ timeZone: ET_TZ, hour:"numeric", minute:"2-digit", hour12:true }).format(new Date(d));
const fmtETDateTime = (d) => `${fmtETDateOnly(d)} - ${fmtETTimeOnly(d)}`;
const stripET = (s="") => String(s).replace(/\s+E[DS]?T\b/i,"").trim();

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
  const comp = evt.competitions?.[0] || {};
  const tName = (evt.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || comp.status?.type?.shortDetail || "").trim();

  if (tName.includes("FINAL")) return "Final";
  if (tName.includes("HALFTIME")) return "Half";
  if (tName.includes("IN_PROGRESS") || tName.includes("LIVE")) return stripET(short || "In Progress");
  if (tName.includes("SCHEDULED")) return fmtETDateTime(evt.date);

  return fmtETDateTime(evt.date);
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
    if(o0){
      total=(o0.overUnder??o0.total)??"";
      const favId=String(o0.favorite||o0.favoriteTeamId||"");
      const spread=Number.isFinite(o0.spread)?o0.spread:(typeof o0.spread==="string"?parseFloat(o0.spread):NaN);
      if(!Number.isNaN(spread)&&favId){
        if(String(away?.team?.id||"")===favId){awaySpread=`-${Math.abs(spread)}`; homeSpread=`+${Math.abs(spread)}`;}
        else if(String(home?.team?.id||"")===favId){homeSpread=`-${Math.abs(spread)}`; awaySpread=`+${Math.abs(spread)}`;}
      } else if (o0.details){
        const m=o0.details.match(/([+-]?\d+(\.\d+)?)/);
        if(m){const line=parseFloat(m[1]); awaySpread=line>0?`+${Math.abs(line)}`:`${line}`; homeSpread=line>0?`-${Math.abs(line)}`:`+${Math.abs(line)}`;}
      }
      const ml=await extractMLWithFallback(event,o0,away,home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
    } else {
      const ml=await extractMLWithFallback(event,{},away,home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
    }

    const weekText=(normLeague(LEAGUE)==="nfl")?resolveWeekLabelNFL(sbForDay,event.date):resolveWeekLabelCFB(sbForDay,event.date);
    const statusClean=tidyStatus(event);
    const dateET=fmtETDate(event.date);

    return {
      gameId: String(event.id),
      values:[
        String(event.id),      // Game ID (A)
        dateET,                // Date
        weekText || "",        // Week
        statusClean,           // Status
        matchup,               // Matchup
        finalScore,            // Final Score
        awaySpread || "",      // Away Spread
        String(awayML || ""),  // Away ML
        homeSpread || "",      // Home Spread
        String(homeML || ""),  // Home ML
        String(total || ""),   // Total
        "", "", "", "", "", "" // live cols
      ],
      dateET,
      matchup
    };
  };
}

/** Halftime helpers */
function isHalftimeLike(evt){
  const t=(evt.status?.type?.name||evt.competitions?.[0]?.status?.type?.name||"").toUpperCase();
  const short=(evt.status?.type?.shortDetail||"").toUpperCase();
  return t.includes("HALFTIME")||/HALF/.test(short);
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
  // We only need the odds during halftime; sheets write happens in the live workflow.
  const url=gameUrl(league,gameId);
  const browser=await playwright.chromium.launch({headless:true});
  const page=await browser.newPage();
  try{
    await page.goto(url,{timeout:60000,waitUntil:"domcontentloaded"});
    await page.waitForLoadState("networkidle",{timeout:6000}).catch(()=>{});
    await page.waitForTimeout(500);
    const section=page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({timeout:8000});
    const txt=(await section.innerText()).replace(/\u00a0/g, " ").replace(/\s+/g," ").trim();

    // Lightweight parser
    const spreads = [...txt.matchAll(/(?:^|\s)([+-]\d+(?:\.\d+)?)(?=\s)/g)].map(m => m[1]);
    const mls     = [...txt.matchAll(/(?:^|\s)([+-]\d{2,4})(?=\s)/g)].map(m => m[1]);
    const totO    = txt.match(/\bo\s?(\d+(?:\.\d+)?)\b/i)?.[1] || "";
    const totU    = txt.match(/\bu\s?(\d+(?:\.\d+)?)\b/i)?.[1] || "";

    return {
      liveAwaySpread: spreads[0] || "",
      liveHomeSpread: spreads[1] || "",
      liveTotal: totO || totU || "",
      liveAwayML: mls[0] || "",
      liveHomeML: mls[1] || "",
      halfScore: ""
    };
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

/** Center-align A..Q (17 cols) */
async function applyCenterFormatting(sheets){
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const sheetId=(meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;
  if(sheetId==null) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{requests:[{repeatCell:{range:{sheetId,startRowIndex:0,startColumnIndex:0,endColumnIndex:17},cell:{userEnteredFormat:{horizontalAlignment:"CENTER"}},fields:"userEnteredFormat.horizontalAlignment"}}]}
  });
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

  // Ensure tab + header
  const meta=await sheets.spreadsheets.get({spreadsheetId:SHEET_ID});
  const tabs=(meta.data.sheets||[]).map(s=>s.properties?.title);
  if(!tabs.includes(TAB_NAME)){
    await sheets.spreadsheets.batchUpdate({spreadsheetId:SHEET_ID, requestBody:{requests:[{addSheet:{properties:{title:TAB_NAME}}}]}}); 
  }
  const read=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z`});
  const values=read.data.values||[];
  let header=values[0]||[];
  if(header.length===0){
    await sheets.spreadsheets.values.update({spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1`, valueInputOption:"RAW", requestBody:{values:[COLS]}});
    header=COLS.slice();
  }
  const hmap=mapHeadersToIndex(header);
  const rows=values.slice(1);

  /** Build index of existing rows — prefer Game ID */
  const keyToRowNum = new Map();
  rows.forEach((r,i)=>{
    const {key}=keyForRow(r,hmap);
    if(key) keyToRowNum.set(key, i+2);
  });

  await applyCenterFormatting(sheets);

  // Pull events (today or week)
  const datesList = RUN_SCOPE==="week"
    ? (()=>{const start=new Date(); return Array.from({length:7},(_,i)=>yyyymmddInET(new Date(start.getTime()+i*86400000)));})()
    : [yyyymmddInET(new Date())];

  // If specific IDs are provided, we’ll fetch summaries directly for those,
  // but we still fetch scoreboards to resolve week labels / fill-ins.
  let firstDaySB=null, events=[];
  for(const d of datesList){ const sb=await fetchJson(scoreboardUrl(LEAGUE,d)); if(!firstDaySB) firstDaySB=sb; events=events.concat(sb?.events||[]); }
  const seen=new Set(); events=events.filter(e=>!seen.has(e.id)&&seen.add(e.id));
  log(`Events found: ${events.length}`);

  const buildPregame=pregameRowFactory(firstDaySB);
  let appendBatch=[];

  // Append pregame rows only for events not present by **Game ID**
  for(const ev of events){
    const {values:rowVals, dateET, matchup}=await buildPregame(ev);
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

  // Narrow to explicit IDs if provided
  const targetIds = TARGET_GAME_ID
    ? TARGET_GAME_ID.split(",").map(s => s.trim()).filter(Boolean)
    : null;

  // Pass 1: finals/halftime + collect adaptive delay
  const batch=new BatchWriter(TAB_NAME);
  let masterDelayMin=null;

  // Merge scoreboard events + explicit ID summaries
  let workEvents = [...events];

  if (targetIds && targetIds.length) {
    // pull summaries for each target id and push synthetic events
    for (const id of targetIds) {
      try {
        const sum = await fetchJson(summaryUrl(LEAGUE, id));
        const evt = {
          id,
          date: sum?.header?.competitions?.[0]?.date || sum?.header?.date || new Date().toISOString(),
          competitions: sum?.header?.competitions || [],
          status: sum?.header?.competitions?.[0]?.status || {},
        };
        workEvents.push(evt);
      } catch (e) {
        warn("Failed to fetch target ID", id, e?.message || e);
      }
    }
    // dedupe
    const s2 = new Set(); workEvents = workEvents.filter(e => !s2.has(e.id) && s2.add(e.id));
  }

  for(const ev of workEvents){
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

    const statusName=(ev.status?.type?.name||comp.status?.type?.name||"").toUpperCase();
    const scorePair=`${away?.score??""}-${home?.score??""}`;

    if(statusName.includes("FINAL")){
      if(hmap["final score"]!==undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if(hmap["status"]!==undefined)      batch.add(rowNum, hmap["status"], "Final");
      continue;
    }

    // Always format scheduled as MM/DD – h:mm AM/PM; strip ET token elsewhere
    const statusValue = tidyStatus(ev);
    if (hmap["status"] !== undefined && statusValue) {
      batch.add(rowNum, hmap["status"], statusValue);
    }

    const currentRow=(values[rowNum-1]||[]);
    const halfAlready=(currentRow[hmap["half score"]]||"").toString().trim();
    const liveTotalVal=(currentRow[hmap["live total"]]||"").toString().trim();

    // Adaptive delay candidates (unchanged)
    if(ADAPTIVE_HALFTIME){
      const short=(ev.status?.type?.shortDetail||"").trim();
      const parsed=parseShortDetailClock(short);
      const mins=minutesAfterKickoff(ev);

      const kickCand = (()=>{ if(halfAlready) return null; if(mins<60||mins>80) return null; const rem=65-mins; return clampRecheck(rem<=0?MIN_RECHECK_MIN:rem);})();
      const q2Cand   = (()=>{ if(!parsed||parsed.final||parsed.halftime||parsed.quarter!==2) return null; const left=parsed.min+parsed.sec/60; if(left>=10) return null; return clampRecheck(2*left); })();

      if(kickCand!=null) masterDelayMin = masterDelayMin==null?kickCand:Math.min(masterDelayMin,kickCand);
      if(q2Cand  !=null) masterDelayMin = masterDelayMin==null?q2Cand  :Math.min(masterDelayMin,q2Cand);
    }
  }
  await batch.flush(sheets);

  // (Adaptive halftime second pass could be here if you want; left as-is)

  log("Run complete.");

  if(GHA_JSON_MODE){
    const payload={ ok:true, league:normLeague(LEAGUE), scope:RUN_SCOPE, tab:TAB_NAME };
    process.stdout.write(JSON.stringify(payload)+"\n");
  }
})().catch(err=>{
  if(GHA_JSON_MODE){ process.stdout.write(JSON.stringify({ok:false,error:String(err?.message||err)})+"\n"); }
  else { console.error("Error:", err); process.exit(1); }
});
