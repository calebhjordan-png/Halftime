// orchestrator.mjs – Prefill + Finals unified (NFL / CFB)
// Fixed all ?? / || grouping issues to eliminate SyntaxError.

import { google } from "googleapis";
import axios from "axios";

/* ────────────── ENV ────────────── */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();          
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();      
const TARGET_IDS = (process.env.TARGET_GAME_ID || "").split(",").map(s=>s.trim()).filter(Boolean);
const GHA_JSON  = (process.env.GHA_JSON || "") === "1";

const DONT_TOUCH_STATUS_IF_LIVE = true;

const HEADER = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

const ET_TZ = "America/New_York";

/* ────────────── helpers ────────────── */
const log = (...a)=> GHA_JSON ? process.stderr.write(a.join(" ")+"\n") : console.log(...a);

function leagueKey(x){ return (/college|ncaaf/i.test(x) ? "college-football" : "nfl"); }
function sbUrl(lg,dates){
  const l = leagueKey(lg); const extra = l==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${l}/scoreboard?dates=${dates}${extra}`;
}

function fmtDate(d){ return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).format(new Date(d)); }
function fmtStatusPregameNoYear(d){
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,month:"2-digit",day:"2-digit",hour:"numeric",minute:"2-digit",hour12:true}).formatToParts(new Date(d));
  const g = k => p.find(x=>x.type===k)?.value||"";
  return `${g("month")}/${g("day")} - ${g("hour")}:${g("minute")} ${g("dayPeriod")}`.replace(/\s+/g," ").trim();
}
function yyyymmddET(d=new Date()){
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const g = k => p.find(x=>x.type===k)?.value||"";
  return `${g("year")}${g("month")}${g("day")}`;
}
async function getJSON(url){ log("GET",url); const {data}=await axios.get(url,{timeout:15000}); return data; }

function isFinal(evt){
  const t=(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return t.includes("FINAL");
}
function isLive(evt){
  const t=(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const s=(evt?.status?.type?.shortDetail||"").toUpperCase();
  return t.includes("IN_PROGRESS") || t.includes("LIVE") || s.includes("HALF") || /Q[1-4]/.test(s);
}

/* Sheets helpers */
function A1col(i){ let n=i+1,s=""; while(n>0){n--;s=String.fromCharCode(65+(n%26))+s;n=Math.floor(n/26);} return s; }
function cellA1(row1, col, tab){ return `${tab}!${A1col(col)}${row1}:${A1col(col)}${row1}`; }
function mapHeaderIdx(h){ const m={}; h.forEach((x,i)=>m[String(x||"").trim().toLowerCase()]=i); return m; }

async function sheetsClient(){
  const svc = CREDS_RAW.startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({ credentials:{client_email:svc.client_email, private_key:svc.private_key}, scopes:["https://www.googleapis.com/auth/spreadsheets"] });
  return google.sheets({version:"v4",auth});
}
class Batch {
  constructor(tab){ this.tab=tab; this.acc=[]; }
  set(r,c,v){ if(c==null||c<0) return; this.acc.push({range:cellA1(r,c,this.tab),values:[[v]]}); }
  async flush(s){ if(!this.acc.length) return; await s.spreadsheets.values.batchUpdate({spreadsheetId:SHEET_ID, requestBody:{valueInputOption:"USER_ENTERED", data:this.acc}}); this.acc.length=0; }
}

/* text runs (underline favorite / bold winner) */
function span(matchup, team){ const i=matchup.indexOf(team); return i<0?null:{start:i,end:i+team.length}; }
function runsFor(matchup, underlineSpan, boldSpan){
  const L = matchup.length;
  const rs = [{ startIndex:0, format:{ underline:false, bold:false }}];
  const add = (sp, key) => {
    if(!sp) return; const s=Math.max(0,Math.min(sp.start,L)); const e=Math.max(0,Math.min(sp.end,L));
    if(e<=s) return; rs.push({ startIndex:s, format:{ [key]:true }}); if(e<L) rs.push({ startIndex:e, format:{ [key]:false }});
  };
  add(underlineSpan,"underline"); add(boldSpan,"bold");
  rs.sort((a,b)=>a.startIndex-b.startIndex);
  return rs;
}
async function writeMatchupWithRuns(sheets, sheetId, row0, matchup, textRuns){
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests:[{
      updateCells:{
        range:{sheetId,startRowIndex:row0,endRowIndex:row0+1,startColumnIndex:4,endColumnIndex:5},
        rows:[{ values:[{ userEnteredValue:{stringValue:matchup}, textFormatRuns:textRuns }] }],
        fields:"userEnteredValue,textFormatRuns"
      }
    }]}
  });
}

/* odds extraction – strict away/home first */
function toNumStr(n){
  if(n==null || n==="") return "";
  const x = Number(n);
  if (!Number.isFinite(x)) return String(n);
  return x>=0 ? `+${x}` : `${x}`;
}
function parseOddsStrict(comp, oddsObj){
  let aSpread="", hSpread="", aML="", hML="", total="";

  const aTO = oddsObj?.awayTeamOdds || {};
  const hTO = oddsObj?.homeTeamOdds || {};
  if (aTO || hTO){
    if (aTO?.spread!=null) aSpread = String(aTO.spread);
    if (hTO?.spread!=null) hSpread = String(hTO.spread);
    if (aTO?.moneyLine!=null || aTO?.moneyline!=null || aTO?.money_line!=null)
      aML = toNumStr(aTO.moneyLine ?? aTO.moneyline ?? aTO.money_line);
    if (hTO?.moneyLine!=null || hTO?.moneyline!=null || hTO?.money_line!=null)
      hML = toNumStr(hTO.moneyLine ?? hTO.moneyline ?? hTO.money_line);
  }

  if ((!aSpread || !hSpread || !aML || !hML) && Array.isArray(oddsObj?.teamOdds)){
    for (const t of oddsObj.teamOdds){
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      if (tid === String(comp.competitors?.find(c=>c.homeAway==="away")?.team?.id)){
        if (!aSpread && t?.spread!=null) aSpread = String(t.spread);
        if (!aML && (t?.moneyLine!=null || t?.moneyline!=null || t?.money_line!=null))
          aML = toNumStr(t.moneyLine ?? t.moneyline ?? t.money_line);
      }
      if (tid === String(comp.competitors?.find(c=>c.homeAway==="home")?.team?.id)){
        if (!hSpread && t?.spread!=null) hSpread = String(t.spread);
        if (!hML && (t?.moneyLine!=null || t?.moneyline!=null || t?.money_line!=null))
          hML = toNumStr(t.moneyLine ?? t.moneyline ?? t.money_line);
      }
    }
  }

  if ((!aML || !hML) && Array.isArray(comp?.competitors)){
    for (const c of comp.competitors){
      const ml = c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line;
      if (ml==null) continue;
      if (c.homeAway==="away" && !aML) aML = toNumStr(ml);
      if (c.homeAway==="home" && !hML) hML = toNumStr(ml);
    }
  }

  if ((!aSpread || !hSpread) && oddsObj){
    const favId = String(oddsObj.favorite ?? oddsObj.favoriteTeamId ?? "");
    const sp = Number.isFinite(oddsObj.spread) ? Number(oddsObj.spread)
               : (typeof oddsObj.spread === "string" ? parseFloat(oddsObj.spread) : NaN);
    if (favId && !Number.isNaN(sp)){
      const q = Math.abs(sp).toString();
      const awayId = String(comp.competitors?.find(c=>c.homeAway==="away")?.team?.id||"");
      const homeId = String(comp.competitors?.find(c=>c.homeAway==="home")?.team?.id||"");
      if (favId === awayId){ aSpread = aSpread || `-${q}`; hSpread = hSpread || `+${q}`; }
      else if (favId === homeId){ hSpread = hSpread || `-${q}`; aSpread = aSpread || `+${q}`; }
    }
  }

  total = String((oddsObj?.overUnder ?? oddsObj?.total ?? total) || "");

  return { aSpread, hSpread, aML, hML, total };
}
function pickOdds(oddsArr=[]){
  if (!Array.isArray(oddsArr)) return null;
  return oddsArr.find(o=>/espn\s*bet/i.test(o?.provider?.name || o?.provider?.displayName || "")) || oddsArr[0] || null;
}

/* ────────────── MAIN ────────────── */
(async function(){
  if (!SHEET_ID || !CREDS_RAW){
    const msg="Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT.";
    if (GHA_JSON) return process.stdout.write(JSON.stringify({ok:false,error:msg})+"\n");
    console.error(msg); process.exit(1);
  }
  const sheets = await sheetsClient();

  // ensure tab + header
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  let sheetId = meta.data.sheets?.find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;
  if (sheetId==null){
    const add = await sheets.spreadsheets.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title:TAB_NAME } } }] } });
    sheetId = add.data.replies?.[0]?.addSheet?.properties?.sheetId;
  }
  const snap0 = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Q` });
  let table = snap0.data.values || [];
  if ((table[0]||[]).join("|") !== HEADER.join("|")){
    await sheets.spreadsheets.values.update({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1`, valueInputOption:"RAW", requestBody:{ values:[HEADER] }});
    const re = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Q` });
    table = re.data.values || [];
  }
  const header = table[0] || HEADER;
  const H = mapHeaderIdx(header);
  const rows = table.slice(1);

  const rowById = new Map();
  rows.forEach((r,i)=>{ const id=String(r[0]||"").trim(); if(id) rowById.set(id,i+2); });

  const dates = (RUN_SCOPE==="today") ? [yyyymmddET(new Date())]
    : Array.from({length:7},(_,i)=>yyyymmddET(new Date(Date.now()+i*86400000)));

  let events = [];
  for (const d of dates){
    const sb = await getJSON(sbUrl(LEAGUE,d));
    events.push(...(sb?.events||[]));
  }
  const seen=new Set();
  events = events.filter(e=>e?.id && !seen.has(e.id) && seen.add(e.id));
  if (TARGET_IDS.length) events = events.filter(e=>TARGET_IDS.includes(String(e.id)));

  log(`Events found: ${events.length}`);

  const toAppend = [];
  const batch = new Batch(TAB_NAME);

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    if (!away || !home) continue;

    const awayName = away.team?.shortDisplayName || away.team?.abbreviation || away.team?.name || "Away";
    const homeName = home.team?.shortDisplayName || home.team?.abbreviation || home.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const gid = String(ev.id);

    const baseOdds = pickOdds(comp.odds || ev.odds || []);
    const { aSpread, hSpread, aML, hML, total } = parseOddsStrict(comp, baseOdds||{});

    const finalScore = isFinal(ev) ? `${away.score??""}-${home.score??""}` : "";
    const preStatus  = fmtStatusPregameNoYear(ev.date);
    const weekLabel  = Number.isFinite(ev?.week?.number) ? `Week ${ev.week.number}` : (ev?.week?.text || "");

    let row1 = rowById.get(gid);
    if (!row1){
      toAppend.push([
        gid, fmtDate(ev.date), weekLabel, preStatus, matchup, finalScore,
        aSpread||"", aML||"", hSpread||"", hML||"", total||"",
        "","","","","",""
      ]);
      continue;
    }

    const is_live = isLive(ev), is_final = isFinal(ev);
    if (!is_live && !is_final){
      if (H["a spread"]!=null) batch.set(row1,H["a spread"],aSpread||"");
      if (H["h spread"]!=null) batch.set(row1,H["h spread"],hSpread||"");
      if (H["a ml"]!=null)     batch.set(row1,H["a ml"],aML||"");
      if (H["h ml"]!=null)     batch.set(row1,H["h ml"],hML||"");
      if (H["total"]!=null)    batch.set(row1,H["total"],total||"");
      if (H["status"]!=null){
        const cur=(table[row1-1]?.[H["status"]]||"").toString();
        if (!DONT_TOUCH_STATUS_IF_LIVE || !/Q\d|HALF|IN PROGRESS/i.test(cur)) batch.set(row1,H["status"],preStatus);
      }
    }
    if (is_final){
      if (H["final score"]!=null) batch.set(row1,H["final score"],finalScore);
      if (H["status"]!=null)      batch.set(row1,H["status"],"Final");
    }
    await batch.flush(await sheetsClient());

    if (!is_live && !is_final){
      let favTeam = "";
      if (aSpread && !isNaN(+aSpread) && +aSpread < 0) favTeam = awayName;
      else if (hSpread && !isNaN(+hSpread) && +hSpread < 0) favTeam = homeName;
      const underlineSpan = favTeam ? span(matchup, favTeam) : null;
      const tr = runsFor(matchup, underlineSpan, null);
      await writeMatchupWithRuns(await sheetsClient(), sheetId, row1-1, matchup, tr);
    }

    if (is_final){
      const a = Number(away.score||0), h = Number(home.score||0);
      const winTeam = a>h ? awayName : (h>a ? homeName : "");
      const boldSpan = winTeam ? span(matchup, winTeam) : null;
      const tr = runsFor(matchup, null, boldSpan);
      await writeMatchupWithRuns(await sheetsClient(), sheetId, row1-1, matchup, tr);
    }
  }

  if (toAppend.length){
    const startRow = (table.length ? table.length+1 : 2);
    await (await sheetsClient()).spreadsheets.values.update({
      spreadsheetId:SHEET_ID,
      range:`${TAB_NAME}!A${startRow}`,
      valueInputOption:"USER_ENTERED",
      requestBody:{ values: toAppend }
    });

    const snap = await (await sheetsClient()).spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Q` });
    const vals = snap.data.values || [];
    const idToRow = new Map((vals.slice(1)).map((r,i)=>[String(r[0]||"").trim(), i+2]));

    for
