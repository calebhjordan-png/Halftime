// Football.mjs — Prefill + Finals + Live (safe header handling)
// Node 20+, "type": "module" (package.json)

import { google } from "googleapis";
import axios from "axios";

/* ================= ENV ================= */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const GAME_IDS  = (process.env.GAME_IDS || "").trim();
const ET_TZ     = "America/New_York";

/* ================= HEADERS ================= */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ================= HELPERS ================= */
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
const cleanStr = s => (s==null?"":String(s));
const uniq = arr => Array.from(new Set(arr));

function statusClock(evt){
  const comp = evt.competitions?.[0] || {};
  const st = comp.status?.type || evt.status?.type || {};
  const name = (st.name || "").toUpperCase();
  const short = (st.shortDetail || "").trim();
  if (name.includes("FINAL")) return "Final";
  if (name.includes("STATUS_HALFTIME") || name.includes("HALFTIME")) return "Half";
  if (name.includes("IN_PROGRESS") || name.includes("LIVE")) return short || "In Progress";
  // pregame -> show "MM/DD - h:mm AM/PM" w/o TZ/year
  const md = fmtET(evt.date,{month:"2-digit",day:"2-digit"});
  const hm = fmtET(evt.date,{hour:"numeric",minute:"2-digit",hour12:true});
  return `${md} - ${hm}`;
}
const weekLabelNFL = sb => Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : "Week";
const weekLabelCFB = (sb, dISO) => {
  const tx = (sb?.week?.text || "").trim();
  if (tx) return tx;
  // fallback: from calendar window
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(dISO).getTime();
  for (const item of cal){
    const entries = Array.isArray(item?.entries) ? item.entries : [item];
    for (const e of entries){
      const s = new Date(e?.startDate || e?.start || 0).getTime();
      const ed= new Date(e?.endDate   || e?.end   || 0).getTime();
      if (Number.isFinite(s) && Number.isFinite(ed) && t>=s && t<=ed){
        const label = (e?.label || e?.detail || e?.text || "").trim();
        return label || "Week";
      }
    }
  }
  return "Week";
};
const normLeague = lg => (lg==="ncaaf"||lg==="college-football") ? "college-football" : "nfl";
const lg = normLeague(LEAGUE_IN);
const sbUrl = (d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl= (id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON = async (u) => (await axios.get(u,{headers:{"User-Agent":"football-bot"},timeout:15000})).data;

function mapHeaders(h){ const m={}; (h||[]).forEach((x,i)=>m[(x||"").trim().toLowerCase()]=i); return m; }
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0)+i); }
function rangeRC(colIdx, row){ const c = colLetter(colIdx); return `${TAB_NAME}!${c}${row}:${c}${row}`; }

/* ================= SHEETS WRAPPER ================= */
class Sheets {
  constructor(auth,id,tab){ this.api=google.sheets({version:"v4",auth}); this.id=id; this.tab=tab; }
  async readAll(){ const r=await this.api.spreadsheets.values.get({spreadsheetId:this.id,range:`${this.tab}!A1:Q`}); return r.data.values||[]; }
  async batch(values){ if(values.length) await this.api.spreadsheets.values.batchUpdate({spreadsheetId:this.id, requestBody:{valueInputOption:"RAW", data:values}}); }
  async batchReq(reqs){ if(reqs.length) await this.api.spreadsheets.batchUpdate({spreadsheetId:this.id, requestBody:{requests:reqs}}); }
  async sheetId(){ const meta = await this.api.spreadsheets.get({spreadsheetId:this.id}); const s = meta?.data?.sheets?.find(x=>x.properties?.title===this.tab); return s?.properties?.sheetId; }
}

/* ================== FAVORITE / BOLD HELPERS ================== */
function favoriteFromOdds(event){
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const odds=(comp.odds||event.odds||[])[0] || {};
  const favId = String(odds.favorite||odds.favoriteTeamId||"");
  let favName="";
  if (favId){
    if (String(away?.team?.id)===favId) favName = away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"";
    if (String(home?.team?.id)===favId) favName = home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"";
  }
  return favName;
}
function matchupText(event){
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
  const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
  return `${awayName} @ ${homeName}`;
}
function winnerName(finalScore, event){
  const [aStr,hStr] = String(finalScore||"").split("-");
  const a = toNum(aStr), h = toNum(hStr);
  if (a==null || h==null) return "";
  const comp=event.competitions?.[0]||{};
  const away=comp.competitors?.find(c=>c.homeAway==="away");
  const home=comp.competitors?.find(c=>c.homeAway==="home");
  const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"";
  const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"";
  return (a>h) ? awayName : (h>a ? homeName : "");
}
function textRunsForMatchup(full, underlineTeam, boldTeam){
  // Build textFormatRuns safely (indices always < length and increasing).
  // We set base (0) with default fmt (no underline/bold), then insert team spans.
  const L = full.length;
  const runs = [{ startIndex: 0, format: { underline:false, bold:false } }];

  const addRun = (team, fmt) => {
    if (!team) return;
    const idx = full.indexOf(team);
    if (idx < 0) return;
    // Insert a run at idx -> fmt, and another reset run after team.
    runs.push({ startIndex: idx, format: fmt });
    const end = Math.min(idx + team.length, L);
    if (end < L) runs.push({ startIndex: end, format: { underline:false, bold:false } });
  };

  // Order: underline first, then bold — if same team both, bold wins at same start index.
  addRun(underlineTeam, { underline:true, bold:false });
  addRun(boldTeam,       { underline:false, bold:true  });

  // Sort by startIndex & compress identical adjacent
  const sorted = runs
    .filter(r => r.startIndex>=0 && r.startIndex<L)
    .sort((a,b)=>a.startIndex-b.startIndex);

  const compact = [];
  for (const r of sorted){
    if (!compact.length || compact[compact.length-1].startIndex!==r.startIndex){
      compact.push(r);
    }else{
      // merge (bold overwrites underline if both on same start)
      compact[compact.length-1] = { startIndex:r.startIndex, format:{...compact[compact.length-1].format, ...r.format} };
    }
  }
  return compact;
}

/* ================== GRADING HELPERS ================== */
function gradeFromFinal(finalScore, aSpread, aML, hSpread, hML, total){
  const [aStr,hStr] = String(finalScore||"").split("-");
  const a = toNum(aStr), h = toNum(hStr);
  if (a==null || h==null) return {aS:null,hS:null,aML:null,hML:null,total:null};
  const diff = h - a;     // home - away
  const sum  = a + h;

  const as = toNum(aSpread);
  const hs = toNum(hSpread);
  const aml= toNum(aML);
  const hml= toNum(hML);
  const tot= toNum(total);

  return {
    aS   : as==null ? null : ((a + as) > h ? "win" : "loss"),
    hS   : hs==null ? null : ((h + hs) > a ? "win" : "loss"),
    aML  : aml==null? null : (a>h ? "win":"loss"),
    hML  : hml==null? null : (h>a ? "win":"loss"),
    total: tot==null? null : (sum>tot ? "over" : "under"),
  };
}

/* ================== LIVE ODDS ================== */
async function liveOdds(eventId){
  try{
    const s = await fetchJSON(sumUrl(eventId));
    const headerOdds = s?.header?.competitions?.[0]?.odds || [];
    // Prefer first record tagged as live if present, else first
    const o = headerOdds.find(x => /live/i.test(x?.details||"")) || headerOdds[0] || {};
    const a = o?.awayTeamOdds || {}, h = o?.homeTeamOdds || {};
    const overUnder = o?.overUnder ?? o?.total ?? "";
    const aSpread = a?.spread ?? "";
    const hSpread = h?.spread ?? "";
    const aML = a?.moneyLine ?? a?.moneyline ?? "";
    const hML = h?.moneyLine ?? h?.moneyline ?? "";

    // Score (from boxscore if present)
    const box = s?.boxscore;
    const away = box?.teams?.find(t=>t.homeAway==="away");
    const home = box?.teams?.find(t=>t.homeAway==="home");
    const scorePair = `${away?.score??""}-${home?.score??""}`;

    return { scorePair, aSpread, aML, hSpread, hML, total: overUnder };
  }catch{ return null; }
}

/* ================== MAIN ================== */
(async function main(){
  if(!SHEET_ID || !CREDS_RAW){ console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT"); process.exit(1); }
  const creds = CREDS_RAW.trim().startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth  = new google.auth.GoogleAuth({credentials:{client_email:creds.client_email, private_key:creds.private_key}, scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);
  const sheetId = await sheets.sheetId();

  // Read current sheet
  const grid = await sheets.readAll();
  const header = grid[0] || [];
  const hmap = mapHeaders(header);
  const rows = grid.slice(1);

  // Ensure headers exist (but NEVER overwrite if already there)
  if (header.length === 0){
    await sheets.batch([{ range:`${TAB_NAME}!A1`, values:[HEADERS] }]);
  }

  // Build row index by Game ID
  const gi = (hmap["game id"] ?? 0);
  const rowById = new Map();
  rows.forEach((r,i)=>{ const id=(r[gi]||"").toString().trim(); if(id) rowById.set(id, i+2); });

  // Gather events (scope)
  const dates = RUN_SCOPE==="today"
    ? [yyyymmddET(new Date())]
    : Array.from({length:7},(_,i)=>yyyymmddET(new Date(Date.now()+i*86400000)));

  let events = [];
  let firstSB = null;
  for (const d of dates){
    const sb = await fetchJSON(sbUrl(d));
    if(!firstSB) firstSB = sb;
    events = events.concat(sb?.events||[]);
  }
  // Force-list by GAME_IDS
  const forceIds = GAME_IDS ? GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean) : [];
  if (forceIds.length){
    // merge summaries if not already present
    for (const id of forceIds){
      const sum = await fetchJSON(sumUrl(id)).catch(()=>null);
      if (sum?.header?.competitions?.[0]){
        const e = { id, competitions:[sum.header.competitions[0]], date: sum.header?.competitions?.[0]?.date || sum?.header?.season?.startDate || new Date().toISOString(), status: sum?.header?.competitions?.[0]?.status };
        // If not in list, add
        if (!events.find(x=>String(x.id)===String(id))) events.push(e);
      }
    }
  }
  // uniquify
  events = uniq(events.map(e=>e)).filter(Boolean);

  /* ====== PREFILL (only append missing) ====== */
  const prefillWrites = [];
  for (const ev of events){
    const id = String(ev.id);
    const row = rowById.get(id);
    if (row) continue; // already on sheet
    const comp=ev.competitions?.[0]||{};
    const away=comp.competitors?.find(c=>c.homeAway==="away");
    const home=comp.competitors?.find(c=>c.homeAway==="home");
    const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
    const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
    const matchup = `${awayName} @ ${homeName}`;

    const sbWeek = (lg==="nfl") ? weekLabelNFL(firstSB) : weekLabelCFB(firstSB, ev.date);
    const dateET = fmtET(ev.date,{year:"numeric",month:"numeric",day:"numeric"});
    const status  = statusClock(ev);

    // odds (pregame)
    const o = (comp.odds||ev.odds||[])[0] || {};
    let aSpread="",hSpread="",aML="",hML="",total="";
    if (o){
      total = o.overUnder ?? o.total ?? "";
      const fav = String(o.favorite||o.favoriteTeamId||"");
      const spread = (typeof o.spread==="number") ? o.spread : (typeof o.spread==="string" ? parseFloat(o.spread) : NaN);
      if (!Number.isNaN(spread) && fav){
        if (String(away?.team?.id)===fav){ aSpread = `-${Math.abs(spread)}`; hSpread = `+${Math.abs(spread)}`; }
        else if (String(home?.team?.id)===fav){ hSpread = `-${Math.abs(spread)}`; aSpread = `+${Math.abs(spread)}`; }
      }
      // moneylines (best-effort)
      const a=o?.awayTeamOdds||{}, h=o?.homeTeamOdds||{};
      aML = a?.moneyLine ?? a?.moneyline ?? "";
      hML = h?.moneyLine ?? h?.moneyline ?? "";
    }

    prefillWrites.push([id, dateET, sbWeek, status, matchup, "", aSpread, String(aML||""), hSpread, String(hML||""), String(total||""), "","","","","",""]);
  }
  if (prefillWrites.length){
    await sheets.batch([{ range:`${TAB_NAME}!A1`, values:prefillWrites, }]);
    // refresh index
    const grid2 = await sheets.readAll();
    const header2 = grid2[0]||header;
    const h2 = mapHeaders(header2);
    const r2 = grid2.slice(1);
    r2.forEach((r,i)=>{ const id=(r[h2["game id"]]||"").toString().trim(); if(id) rowById.set(id, i+2); });
  }

  /* ====== FINALS + STATUS + MATCHUP FORMATTING ====== */
  const valueWrites = [];
  const formatReqs  = [];

  for (const ev of events){
    const id = String(ev.id);
    const row = rowById.get(id);
    if (!row || row < 2) continue; // never touch header

    const comp=ev.competitions?.[0]||{};
    const st = (comp.status?.type?.name || ev.status?.type?.name || "").toUpperCase();
    const statusStr = statusClock(ev);

    // Update status (always)
    if (hmap["status"] !== undefined){
      valueWrites.push({ range: rangeRC(hmap["status"], row), values:[[ statusStr ]] });
    }

    // If Final: write final score + bold winner, and lock pregame grading
    if (st.includes("FINAL")){
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const finalScore = `${away?.score??""}-${home?.score??""}`;

      // Final score cell
      if (hmap["final score"]!==undefined){
        valueWrites.push({ range: rangeRC(hmap["final score"], row), values:[[finalScore]] });
      }

      // Bold the winner
      if (hmap["matchup"]!==undefined && sheetId!=null){
        const full = grid[row-1]?.[hmap["matchup"]] || matchupText(ev);
        const winner = winnerName(finalScore, ev);
        const runs = textRunsForMatchup(full, null, winner);
        formatReqs.push({
          updateCells:{
            range:{ sheetId, startRowIndex:row-1, endRowIndex:row, startColumnIndex:hmap["matchup"], endColumnIndex:hmap["matchup"]+1 },
            rows:[{ values:[{ userEnteredValue:{stringValue:full}, textFormatRuns:runs }]}],
            fields:"userEnteredValue,textFormatRuns"
          }
        });
      }
    } else {
      // Pregame underline favorite (only if not live/final)
      if (!/in_progress|live|final/i.test(st) && hmap["matchup"]!==undefined && sheetId!=null){
        const full = grid[row-1]?.[hmap["matchup"]] || matchupText(ev);
        const favTeam = favoriteFromOdds(ev);
        const runs = textRunsForMatchup(full, favTeam, null);
        if (runs.length){
          formatReqs.push({
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
  await sheets.batch(valueWrites);
  await sheets.batchReq(formatReqs);

  /* ====== LIVE (L–Q) and live Status ====== */
  const liveWrites = [];
  for (const ev of events){
    const id = String(ev.id);
    const row = rowById.get(id);
    if (!row || row<2) continue;

    const comp=ev.competitions?.[0]||{};
    const name=(comp.status?.type?.name||"").toUpperCase();
    const isLive = name.includes("IN_PROGRESS") || name.includes("LIVE") || name.includes("HALF");

    if (!isLive) continue;

    const live = await liveOdds(id);
    if (!live) continue;

    const put = (key, val) => {
      const idx = hmap[key.toLowerCase()];
      if (idx === undefined || val==="" || val==null) return;
      liveWrites.push({ range: rangeRC(idx, row), values:[[ String(val) ]]});
    };
    put("H Score",  live.scorePair || "");
    put("H A Spread", live.aSpread || "");
    put("H A ML",    live.aML     || "");
    put("H H Spread",live.hSpread || "");
    put("H H ML",    live.hML     || "");
    put("H Total",   live.total   || "");
  }
  await sheets.batch(liveWrites);

  /* ====== GRADING (only G–J) ====== */
  const gridNow = await sheets.readAll();
  const rowsNow = gridNow.slice(1);
  const gradeReqs = [];

  // Clear any previous conditional formatting and re-apply only for G..J
  if (sheetId!=null){
    gradeReqs.push({ deleteConditionalFormatRule:{ sheetId, index:0 } }); // harmless if none
  }

  // Apply four simple CF rules per column (win green / loss red), and Total (over green/under red)
  // Using custom formulas that reference row-relative cells.
  function cf(colIndex, type /*"win"/"loss"/"over"/"under"*/, color){
    const col = colLetter(colIndex);
    let formula = "";
    if (type==="win"){
      // e.g. for G (A Spread): and($F2<>"", $G2<>"", VALUE(LEFT($F2, FIND("-", $F2)-1))+VALUE(RIGHT($F2,LEN($F2)-FIND("-",$F2))) + VALUE($G2) > VALUE(RIGHT($F2,LEN($F2)-FIND("-",$F2)))
      // Simpler: parse with SPLIT by "-" (safe in Sheets)
      formula = `=AND($F2<>"",$${col}2<>"",INDEX(SPLIT($F2,"-"),1)+$${col}2>INDEX(SPLIT($F2,"-"),2))`;
    }else if (type==="loss"){
      formula = `=AND($F2<>"",$${col}2<>"",INDEX(SPLIT($F2,"-"),1)+$${col}2<=INDEX(SPLIT($F2,"-"),2))`;
    }else if (type==="over"){
      formula = `=AND($F2<>"",$${col}2<>"",(INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2))>$${col}2)`;
    }else if (type==="under"){
      formula = `=AND($F2<>"",$${col}2<>"",(INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2))<=$${col}2)`;
    }
    return {
      addConditionalFormatRule:{
        rule:{
          ranges:[{ sheetId, startRowIndex:1, startColumnIndex:colIndex, endColumnIndex:colIndex+1 }],
          booleanRule:{
            condition:{ type:"CUSTOM_FORMULA", values:[{ userEnteredValue: formula }]},
            format:{ backgroundColor: color }
          }
        },
        index:0
      }
    };
  }

  if (sheetId!=null){
    // G (A Spread)
    gradeReqs.push(cf(hmap["a spread"], "win",  {red:0.85,green:0.95,blue:0.85}));
    gradeReqs.push(cf(hmap["a spread"], "loss", {red:0.97,green:0.85,blue:0.85}));
    // H (A ML)
    gradeReqs.push(cf(hmap["a ml"], "win",  {red:0.85,green:0.95,blue:0.85}));
    gradeReqs.push(cf(hmap["a ml"], "loss", {red:0.97,green:0.85,blue:0.85}));
    // I (H Spread)
    gradeReqs.push(cf(hmap["h spread"], "win",  {red:0.85,green:0.95,blue:0.85}));
    gradeReqs.push(cf(hmap["h spread"], "loss", {red:0.97,green:0.85,blue:0.85}));
    // J (H ML)
    gradeReqs.push(cf(hmap["h ml"], "win",  {red:0.85,green:0.95,blue:0.85}));
    gradeReqs.push(cf(hmap["h ml"], "loss", {red:0.97,green:0.85,blue:0.85}));
    // K (Total) — over/under
    gradeReqs.push(cf(hmap["total"], "over",  {red:0.85,green:0.95,blue:0.85}));
    gradeReqs.push(cf(hmap["total"], "under", {red:0.97,green:0.85,blue:0.85}));
    await sheets.batchReq(gradeReqs);
  }

  // Done
  const out = { ok:true, league:lg, tab:TAB_NAME, events:events.length };
  if (process.argv.includes("--gha")) {
    process.stdout.write(`***${JSON.stringify(out)}***\n`);
  } else {
    console.log(out);
  }
})().catch(err=>{
  console.error("Fatal:", err?.message||err);
  if (process.argv.includes("--gha")) process.stdout.write(`***${JSON.stringify({ok:false,error:String(err?.message||err)})}***\n`);
  process.exit(1);
});
