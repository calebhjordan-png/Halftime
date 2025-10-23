// Football.mjs — Prefill + Finals + Live + Finals-backfill + Direct grading colors (G..K)

import { google } from "googleapis";
import axios from "axios";

/* ========= ENV ========= */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const GAME_IDS  = (process.env.GAME_IDS || "").trim();
const ET_TZ     = "America/New_York";

/* ========= HEADERS ========= */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ========= HELPERS ========= */
const fmtET = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,...opt}).format(new Date(d));
function yyyymmddET(d=new Date()){
  const p=fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return p[2]+p[0]+p[1];
}
const toNum = v => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(String(v).replace(/[^\d.+-]/g,""));
  return Number.isFinite(n) ? n : null;
};
const uniq = a => Array.from(new Set(a));
const lg = (LEAGUE_IN==="ncaaf"||LEAGUE_IN==="college-football") ? "college-football" : "nfl";
const sbUrl = (d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl= (id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON = async u => (await axios.get(u,{headers:{"User-Agent":"football-bot"},timeout:15000})).data;

function mapHeaders(h){ const m={}; (h||[]).forEach((x,i)=>m[(x||"").trim().toLowerCase()]=i); return m; }
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0)+i); }
function rc(tab, cIdx, r){ const c = colLetter(cIdx); return `${tab}!${c}${r}:${c}${r}`; }

function statusClock(evt){
  const comp = evt.competitions?.[0] || {};
  const t = comp.status?.type || evt.status?.type || {};
  const name = (t.name || "").toUpperCase();
  const short = (t.shortDetail || "").trim();
  if (name.includes("FINAL")) return "Final";
  if (name.includes("STATUS_HALFTIME") || name.includes("HALFTIME")) return "Half";
  if (name.includes("IN_PROGRESS") || name.includes("LIVE")) return short || "In Progress";
  const md = fmtET(evt.date,{month:"2-digit",day:"2-digit"});
  const hm = fmtET(evt.date,{hour:"numeric",minute:"2-digit",hour12:true});
  return `${md} - ${hm}`;
}

const weekLabelNFL = sb => Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Week";
const weekLabelCFB = (sb,dISO)=>{
  const tx=(sb?.week?.text||"").trim(); if(tx) return tx;
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(dISO).getTime();
  for (const item of cal){
    const entries = Array.isArray(item?.entries)? item.entries : [item];
    for (const e of entries){
      const s = new Date(e?.startDate || e?.start || 0).getTime();
      const ed= new Date(e?.endDate   || e?.end   || 0).getTime();
      if (Number.isFinite(s)&&Number.isFinite(ed)&&t>=s&&t<=ed){
        const label = (e?.label||e?.detail||e?.text||"").trim();
        return label || "Week";
      }
    }
  }
  return "Week";
};

function favoriteFromOdds(event){
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const odds=(comp.odds||event.odds||[])[0] || {};
  const favId = String(odds.favorite||odds.favoriteTeamId||"");
  if (!favId) return "";
  if (String(away?.team?.id)===favId) return away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"";
  if (String(home?.team?.id)===favId) return home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"";
  return "";
}
function matchupText(event){
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
  const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
  return `${awayName} @ ${homeName}`;
}
function winnerName(finalScore,event){
  const [aStr,hStr]=String(finalScore||"").split("-");
  const a=toNum(aStr), h=toNum(hStr);
  if(a==null||h==null) return "";
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"";
  const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"";
  return (a>h)?awayName:(h>a?homeName:"");
}
function textRunsForMatchup(full, underlineTeam, boldTeam){
  const L = full.length;
  const runs = [{ startIndex:0, format:{underline:false,bold:false} }];
  const add = (team,fmt)=>{
    if(!team) return;
    const i = full.indexOf(team);
    if(i<0) return;
    runs.push({startIndex:i, format:fmt});
    const end = Math.min(i+team.length,L);
    if(end<L) runs.push({startIndex:end, format:{underline:false,bold:false}});
  };
  add(underlineTeam,{underline:true,bold:false});
  add(boldTeam,{underline:false,bold:true});
  const sorted=runs.filter(r=>r.startIndex>=0&&r.startIndex<L).sort((a,b)=>a.startIndex-b.startIndex);
  const out=[];
  for(const r of sorted){
    if(!out.length || out[out.length-1].startIndex!==r.startIndex) out.push(r);
    else out[out.length-1]={startIndex:r.startIndex,format:{...out[out.length-1].format,...r.format}};
  }
  return out;
}

async function liveOdds(eid){
  try{
    const s=await fetchJSON(sumUrl(eid));
    const oList = s?.header?.competitions?.[0]?.odds || [];
    const o = oList.find(x=>/live/i.test(x?.details||"")) || oList[0] || {};
    const a=o?.awayTeamOdds||{}, h=o?.homeTeamOdds||{};
    const overUnder = o?.overUnder ?? o?.total ?? "";
    const aSpread = a?.spread ?? "";
    const hSpread = h?.spread ?? "";
    const aML = a?.moneyLine ?? a?.moneyline ?? "";
    const hML = h?.moneyLine ?? h?.moneyline ?? "";

    const box=s?.boxscore;
    const away=box?.teams?.find(t=>t.homeAway==="away");
    const home=box?.teams?.find(t=>t.homeAway==="home");
    const score = `${away?.score??""}-${home?.score??""}`;

    const comp=s?.header?.competitions?.[0]||{};
    const name=(comp.status?.type?.name||"").toUpperCase();

    return { score, aSpread, aML, hSpread, hML, total:overUnder, statusName:name };
  }catch{ return null; }
}

/* ========= SHEETS ========= */
class Sheets{
  constructor(auth,id,tab){ this.api=google.sheets({version:"v4",auth}); this.id=id; this.tab=tab; }
  async read(){ const r=await this.api.spreadsheets.values.get({spreadsheetId:this.id,range:`${this.tab}!A1:Q`}); return r.data.values||[]; }
  async batch(values){ if(values.length) await this.api.spreadsheets.values.batchUpdate({spreadsheetId:this.id,requestBody:{valueInputOption:"RAW",data:values}}); }
  async batchReq(reqs){ if(reqs.length) await this.api.spreadsheets.batchUpdate({spreadsheetId:this.id,requestBody:{requests:reqs}}); }
  async sheetId(){ const meta=await this.api.spreadsheets.get({spreadsheetId:this.id}); const s=meta?.data?.sheets?.find(x=>x.properties?.title===this.tab); return s?.properties?.sheetId; }
}

/* ========= MAIN ========= */
(async ()=>{
  if(!SHEET_ID || !CREDS_RAW){ console.error("Missing GOOGLE_* env"); process.exit(1); }
  const creds = CREDS_RAW.trim().startsWith("{")? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth  = new google.auth.GoogleAuth({credentials:{client_email:creds.client_email,private_key:creds.private_key},scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sh = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);
  const sheetId = await sh.sheetId();

  /* --- read & ensure headers --- */
  let grid = await sh.read();
  let header = grid[0] || [];
  if (header.length !== HEADERS.length || header[0] !== "Game ID"){
    await sh.batch([{ range:`${TAB_NAME}!A1`, values:[HEADERS]}]);
    grid = await sh.read();
    header = grid[0] || HEADERS;
  }
  let hmap = mapHeaders(header);
  let rows = grid.slice(1);

  /* --- row index by Game ID --- */
  const gi = hmap["game id"] ?? 0;
  const rowById = new Map();
  rows.forEach((r,i)=>{ const id=(r[gi]||"").toString().trim(); if(id) rowById.set(id, i+2); });

  /* --- fetch events (scope + forced ids) --- */
  const dates = RUN_SCOPE==="today" ? [yyyymmddET(new Date())] :
    Array.from({length:7},(_,i)=>yyyymmddET(new Date(Date.now()+i*86400000)));
  let events=[]; let firstSB=null;
  for (const d of dates){ const sb=await fetchJSON(sbUrl(d)); if(!firstSB) firstSB=sb; events=events.concat(sb?.events||[]); }
  const forced = (GAME_IDS? GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean) : []);
  for (const id of forced){
    const s = await fetchJSON(sumUrl(id)).catch(()=>null);
    const cmp = s?.header?.competitions?.[0];
    if (cmp && !events.find(e=>String(e.id)===String(id))){
      events.push({ id, competitions:[cmp], date: cmp?.date || new Date().toISOString(), status: cmp?.status });
    }
  }
  events = uniq(events.map(e=>e)).filter(Boolean);

  /* --- PREFILL (append missing) --- */
  const prefillRows = [];
  for (const ev of events){
    const id=String(ev.id); if (rowById.get(id)) continue;
    const comp=ev.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
    const matchup = `${awayName} @ ${homeName}`;

    const sbWeek = (lg==="nfl") ? weekLabelNFL(firstSB) : weekLabelCFB(firstSB, ev.date);
    const dateET = fmtET(ev.date,{year:"numeric",month:"numeric",day:"numeric"});
    const status = statusClock(ev);

    const odds=(comp.odds||ev.odds||[])[0] || {};
    let aSpread="",hSpread="",aML="",hML="",total="";
    if (odds){
      total = odds.overUnder ?? odds.total ?? "";
      const favId = String(odds.favorite||odds.favoriteTeamId||"");
      const spread = Number(odds.spread);
      if (Number.isFinite(spread) && favId){
        if (String(away?.team?.id)===favId){ aSpread=`-${Math.abs(spread)}`; hSpread=`+${Math.abs(spread)}`; }
        else if (String(home?.team?.id)===favId){ hSpread=`-${Math.abs(spread)}`; aSpread=`+${Math.abs(spread)}`; }
      }
      const a=odds?.awayTeamOdds||{}, h=odds?.homeTeamOdds||{};
      aML = a?.moneyLine ?? a?.moneyline ?? "";
      hML = h?.moneyLine ?? h?.moneyline ?? "";
    }
    prefillRows.push([id, dateET, sbWeek, status, matchup, "", aSpread, String(aML||""), hSpread, String(hML||""), String(total||""), "","","","","",""]);
  }
  if (prefillRows.length){
    await sh.batch([{ range:`${TAB_NAME}!A2`, values:prefillRows }]);  // start at row 2
    // refresh index
    const g2=await sh.read(); const r2=g2.slice(1); hmap = mapHeaders(g2[0]); rows=r2;
    r2.forEach((r,i)=>{ const id=(r[hmap["game id"]]||"").toString().trim(); if(id) rowById.set(id, i+2); });
  }

  /* --- Finals / Status / Favorite underline / Winner bold --- */
  const valWrites=[]; const fmtReqs=[];
  for (const ev of events){
    const id=String(ev.id); const row=rowById.get(id); if(!row||row<2) continue;
    const comp=ev.competitions?.[0]||{};
    const name=(comp.status?.type?.name||ev.status?.type?.name||"").toUpperCase();

    // always update Status
    if (hmap["status"]!==undefined) valWrites.push({ range:rc(TAB_NAME,hmap["status"],row), values:[[ statusClock(ev) ]]});

    if (name.includes("FINAL")){
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const finalScore=`${away?.score??""}-${home?.score??""}`;
      if (hmap["final score"]!==undefined) valWrites.push({ range:rc(TAB_NAME,hmap["final score"],row), values:[[finalScore]]});

      if (hmap["matchup"]!==undefined && sheetId!=null){
        const full = (rows[row-2]?.[hmap["matchup"]] ?? matchupText(ev));
        const runs = textRunsForMatchup(full, null, winnerName(finalScore,ev));
        fmtReqs.push({
          updateCells:{
            range:{ sheetId, startRowIndex:row-1, endRowIndex:row, startColumnIndex:hmap["matchup"], endColumnIndex:hmap["matchup"]+1 },
            rows:[{ values:[{ userEnteredValue:{stringValue:full}, textFormatRuns:runs }]}],
            fields:"userEnteredValue,textFormatRuns"
          }
        });
      }
    } else if (!/in_progress|live/i.test(name)){ // pregame underline favorite
      if (hmap["matchup"]!==undefined && sheetId!=null){
        const full = (rows[row-2]?.[hmap["matchup"]] ?? matchupText(ev));
        const runs = textRunsForMatchup(full, favoriteFromOdds(ev), null);
        if (runs.length){
          fmtReqs.push({
            updateCells:{
              range:{ sheetId, startRowIndex:row-1, endRowIndex:row, startColumnIndex:hmap["matchup"], endColumnIndex:hmap["matchup"]+1 },
              rows:[{ values:[{ userEnteredValue:{stringValue:full}, textFormatRuns:runs }]}],
              fields:"userEnteredValue,textFormatRuns"
            }
          });
        }
      }
    }
  }
  await sh.batch(valWrites);
  await sh.batchReq(fmtReqs);

  /* --- Finals backfill sweep (promote non-Final rows to Final if summary says so) --- */
  const needCheck = [];
  rows.forEach((r,i)=>{
    const id=(r[hmap["game id"]]||"").toString().trim();
    const status=(r[hmap["status"]]||"").toString().toLowerCase();
    if (id && !/final/.test(status)) needCheck.push({id,row:i+2});
  });
  // limit to avoid quota storms
  for (const {id,row} of needCheck.slice(0,120)){
    try{
      const s=await fetchJSON(sumUrl(id));
      const comp=s?.header?.competitions?.[0]||{};
      const nm=(comp.status?.type?.name||"").toUpperCase();
      if (nm.includes("FINAL")){
        const away=comp.competitors?.find(c=>c.homeAway==="away");
        const home=comp.competitors?.find(c=>c.homeAway==="home");
        const finalScore=`${away?.score??""}-${home?.score??""}`;
        await sh.batch([
          { range:rc(TAB_NAME,hmap["status"],row),      values:[["Final"]] },
          { range:rc(TAB_NAME,hmap["final score"],row), values:[[finalScore]] }
        ]);
      }
    }catch{ /* ignore */ }
  }

  /* --- Live odds (L–Q) & live status --- */
  const liveWrites=[];
  for (const ev of events){
    const id=String(ev.id); const row=rowById.get(id); if(!row||row<2) continue;
    const comp=ev.competitions?.[0]||{};
    const nm=(comp.status?.type?.name||"").toUpperCase();
    if (!nm.includes("IN_PROGRESS") && !nm.includes("HALF")) continue;
    const live=await liveOdds(id); if(!live) continue;

    const put=(key,val)=>{ const idx=hmap[key.toLowerCase()]; if(idx===undefined || val==null || val==="") return;
      liveWrites.push({ range:rc(TAB_NAME,idx,row), values:[[ String(val) ]]});
    };
    put("H Score", live.score);
    put("H A Spread", live.aSpread);
    put("H A ML", live.aML);
    put("H H Spread", live.hSpread);
    put("H H ML", live.hML);
    put("H Total", live.total);
    if (hmap["status"]!==undefined){
      const sText = live.statusName?.includes("HALF") ? "Half" :
                    (live.statusName?.includes("IN_PROGRESS") ? "In Progress" : null);
      if (sText) liveWrites.push({ range:rc(TAB_NAME,hmap["status"],row), values:[[sText]]});
    }
  }
  await sh.batch(liveWrites);

  /* --- Direct grading colors for G..K based on Final Score --- */
  if (sheetId!=null){
    const GREEN={red:0.85,green:0.95,blue:0.85}, RED={red:0.97,green:0.85,blue:0.85}, CLEAR={red:1,green:1,blue:1};
    const reqs=[];
    for (let r=0; r<rows.length; r++){
      const rowNum = r+2;
      const F = (rows[r][hmap["final score"]]||"").toString();
      const G = toNum(rows[r][hmap["a spread"]]);
      const H = toNum(rows[r][hmap["a ml"]]);
      const I = toNum(rows[r][hmap["h spread"]]);
      const J = toNum(rows[r][hmap["h ml"]]);
      const K = toNum(rows[r][hmap["total"]]);
      // clear first
      for (const idx of [hmap["a spread"],hmap["a ml"],hmap["h spread"],hmap["h ml"],hmap["total"]]){
        if (idx===undefined) continue;
        reqs.push({ repeatCell:{
          range:{ sheetId, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:idx, endColumnIndex:idx+1 },
          cell:{ userEnteredFormat:{ backgroundColor:CLEAR }}, fields:"userEnteredFormat.backgroundColor"
        }});
      }
      if (!F || !F.includes("-")) continue;
      const [aStr,hStr]=F.split("-");
      const a = toNum(aStr), h = toNum(hStr);
      if (a==null || h==null) continue;

      const paint = (idx,color)=>{
        if (idx===undefined) return;
        reqs.push({ repeatCell:{
          range:{ sheetId, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:idx, endColumnIndex:idx+1 },
          cell:{ userEnteredFormat:{ backgroundColor:color }}, fields:"userEnteredFormat.backgroundColor"
        }});
      };

      // A Spread: a + spread > h wins
      if (G!=null) paint(hmap["a spread"], (a + G > h) ? GREEN : RED);
      // A ML: a > h wins
      if (H!=null) paint(hmap["a ml"], (a > h) ? GREEN : RED);
      // H Spread: h + spread > a wins
      if (I!=null) paint(hmap["h spread"], (h + I > a) ? GREEN : RED);
      // H ML: h > a wins
      if (J!=null) paint(hmap["h ml"], (h > a) ? GREEN : RED);
      // Total: (a+h) > total wins
      if (K!=null) paint(hmap["total"], ((a+h) > K) ? GREEN : RED);
    }
    await sh.batchReq(reqs);
  }

  const out = { ok:true, tab:TAB_NAME, events:events.length };
  process.stdout.write(`***${JSON.stringify(out)}***\n`);
})().catch(e=>{
  console.error("Fatal:", e?.message||e);
  process.stdout.write(`***${JSON.stringify({ok:false,error:String(e?.message||e)})}***\n`);
  process.exit(1);
});
