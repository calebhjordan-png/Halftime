// Football.mjs — Prefill + Finals + Live (quota-safe batching & gated formatting)

import { google } from "googleapis";
import axios from "axios";

/* ---------- ENV ---------- */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase();               // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();          // "today" | "week"
const GAME_IDS  = (process.env.GAME_IDS || "").trim();
const TZ        = "America/New_York";

// set FORMAT_VERSION to bump rules forcibly
const FORMAT_VERSION = "cf:v2";

/* ---------- CONSTANTS ---------- */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];
const COLS = {}; // filled after reading header

/* ---------- UTIL ---------- */
const fmtET = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:TZ,...opt}).format(new Date(d));
const yyyymmddET = (d=new Date())=>{
  const [mm,dd,yyyy] = fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return `${yyyy}${mm}${dd}`;
};
const sbUrl = (lg,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl=(lg,id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON=async u=>(await axios.get(u,{headers:{"User-Agent":"football-suite"},timeout:15000})).data;

function a1FromIndex(idx){ let n=idx+1,out=""; while(n>0){const r=(n-1)%26; out=String.fromCharCode(65+r)+out; n=Math.floor((n-1)/26);} return out; }
function fmtListDate(dateStr){ return fmtET(dateStr,{year:"numeric",month:"2-digit",day:"2-digit"}); }
function fmtStatus(dateStr){ const md=fmtET(dateStr,{month:"2-digit",day:"2-digit"}); const hm=fmtET(dateStr,{hour:"numeric",minute:"2-digit",hour12:true}); return `${md} - ${hm}`; }
function weekLabel(ev){ const wk=ev?.week?.number; return wk?`Week ${wk}`:""; }

function normalizeMoneyLine(v){ if(v==null) return ""; const s=String(v).trim().toUpperCase(); if(s==="EVEN") return "+100"; if(s==="OFF") return ""; return String(v); }
function normalizeSpread(v){ if(v==null) return ""; const s=String(v).trim().toUpperCase(); if(s==="OFF") return ""; return String(v); }
function normalizeTotal(v){ if(v==null) return ""; const s=String(v).trim().toUpperCase(); if(s==="OFF") return ""; return String(v); }

function pickOddsEntry(oddsArr=[]){
  if(!Array.isArray(oddsArr)||!oddsArr.length) return null;
  const pref = oddsArr.find(o=>/espn bet/i.test(o?.provider?.name||""));
  if (pref && (pref.homeTeamOdds||pref.awayTeamOdds||pref.overUnder||pref.total)) return pref;
  return oddsArr.find(o=>{
    const a=o?.awayTeamOdds||{}, h=o?.homeTeamOdds||{};
    return a.spread!=null || h.spread!=null || a.moneyLine!=null || h.moneyLine!=null || o.overUnder!=null || o.total!=null;
  }) || oddsArr[0];
}

/* ---------- SHEETS ---------- */
class Sheets {
  constructor(auth,id,tab){ this.api=google.sheets({version:"v4",auth}); this.id=id; this.tab=tab; }
  async readAll(){ const r=await this.api.spreadsheets.values.get({spreadsheetId:this.id,range:`${this.tab}!A1:Q`}); return r.data.values||[]; }
  async metadata(){ return (await this.api.spreadsheets.get({spreadsheetId:this.id})).data; }
  async batchValues(data){ if(!data.length) return; await this.api.spreadsheets.values.batchUpdate({spreadsheetId:this.id,requestBody:{valueInputOption:"RAW",data}}); }
  async batchRequests(reqs){ if(!reqs.length) return; await this.api.spreadsheets.batchUpdate({spreadsheetId:this.id,requestBody:{requests:reqs}}); }
}

/* ---------- BATCH ACCUMULATORS ---------- */
const valueOps = [];       // [{range, values}]
const requestOps = [];     // sheet requests (formatting)
function queueCell(tab, row1, colIdx, v){
  if(v==null || v==="") return;
  const col = a1FromIndex(colIdx);
  valueOps.push({ range:`${tab}!${col}${row1}:${col}${row1}`, values:[[String(v)]] });
}

/* ---------- LIVE/PREFILL HELPERS ---------- */
async function getOddsFromSummary(league, gameId){
  try{
    const s=await fetchJSON(sumUrl(league, gameId));
    const comp = s?.header?.competitions?.[0] || {};
    const odds = pickOddsEntry(comp.odds || []);
    if(!odds) return null;
    const a=odds.awayTeamOdds||{}, h=odds.homeTeamOdds||{};
    const total = odds.overUnder ?? odds.total ?? "";
    return {
      aSpread: normalizeSpread(a.spread),
      aML:     normalizeMoneyLine(a.moneyLine),
      hSpread: normalizeSpread(h.spread),
      hML:     normalizeMoneyLine(h.moneyLine),
      total:   normalizeTotal(total)
    };
  }catch{ return null; }
}

async function getLiveBoard(league, gameId){
  try{
    const s=await fetchJSON(sumUrl(league, gameId));
    const box=s?.boxscore;
    const away=box?.teams?.find(t=>t.homeAway==="away");
    const home=box?.teams?.find(t=>t.homeAway==="home");
    const a=away?.score, h=home?.score;

    const comp=s?.header?.competitions?.[0] || {};
    const odds=pickOddsEntry(comp.odds||[]);
    const A=odds?.awayTeamOdds||{}, H=odds?.homeTeamOdds||{};
    const total=odds?.overUnder ?? odds?.total ?? "";

    return {
      hScore: (a!=null && h!=null) ? `${a}-${h}` : "",
      haSpread: normalizeSpread(A.spread),
      haML:     normalizeMoneyLine(A.moneyLine),
      hhSpread: normalizeSpread(H.spread),
      hhML:     normalizeMoneyLine(H.moneyLine),
      hTotal:   normalizeTotal(total),
      status:   comp?.status?.type?.shortDetail || comp?.status?.type?.name || "Live"
    };
  }catch{ return null; }
}

/* ---------- FORMATTING (only G–K; gated) ---------- */
function addCFRule(rangeIdx, formula, color){
  requestOps.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[rangeIdx],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:formula}] },
          format:{ backgroundColor:color }
        }
      },
      index:0
    }
  });
}

async function ensureFormatting(sh, sheetId, sheetVals){
  // Use cell R1 as a tiny marker (outside A–Q). If it equals FORMAT_VERSION, skip.
  const markerCell = sheetVals?.[0]?.[17] || ""; // R is index 17
  if (markerCell === FORMAT_VERSION) return;

  // 1) wipe existing CF rules (do minimal: try a few deletes; if none present it’s fine)
  for (let i=0;i<10;i++){
    try{ await sh.batchRequests([{ deleteConditionalFormatRule:{ sheetId, index:0 } }]); }
    catch { break; }
  }
  // 2) reset text format (remove accidental bold/underline) below header
  requestOps.push({
    repeatCell:{
      range:{ sheetId, startRowIndex:1 },
      cell:{ userEnteredFormat:{ textFormat:{ bold:false, underline:false } } },
      fields:"userEnteredFormat.textFormat"
    }
  });

  const col = (c)=>({ sheetId, startRowIndex:1, startColumnIndex:c, endColumnIndex:c+1 });
  const green = { red:0.85, green:0.94, blue:0.88 };
  const red   = { red:0.98, green:0.85, blue:0.86 };

  // G (A Spread)
  addCFRule(col(6), '=AND($F2<>"",$G2<>"", INDEX(SPLIT($F2,"-"),1)+VALUE($G2) >  INDEX(SPLIT($F2,"-"),2))', green);
  addCFRule(col(6), '=AND($F2<>"",$G2<>"", INDEX(SPLIT($F2,"-"),1)+VALUE($G2) <= INDEX(SPLIT($F2,"-"),2))', red);
  // I (H Spread)
  addCFRule(col(8), '=AND($F2<>"",$I2<>"", INDEX(SPLIT($F2,"-"),2)+VALUE($I2) >  INDEX(SPLIT($F2,"-"),1))', green);
  addCFRule(col(8), '=AND($F2<>"",$I2<>"", INDEX(SPLIT($F2,"-"),2)+VALUE($I2) <= INDEX(SPLIT($F2,"-"),1))', red);
  // H (A ML)
  addCFRule(col(7), '=AND($F2<>"",$H2<>"", VALUE($H2)<>0, INDEX(SPLIT($F2,"-"),1) >  INDEX(SPLIT($F2,"-"),2))', green);
  addCFRule(col(7), '=AND($F2<>"",$H2<>"", VALUE($H2)<>0, INDEX(SPLIT($F2,"-"),1) <= INDEX(SPLIT($F2,"-"),2))', red);
  // J (H ML)
  addCFRule(col(9), '=AND($F2<>"",$J2<>"", VALUE($J2)<>0, INDEX(SPLIT($F2,"-"),2) >  INDEX(SPLIT($F2,"-"),1))', green);
  addCFRule(col(9), '=AND($F2<>"",$J2<>"", VALUE($J2)<>0, INDEX(SPLIT($F2,"-"),2) <= INDEX(SPLIT($F2,"-"),1))', red);
  // K (Total)  Over green / Under red
  addCFRule(col(10),'=AND($F2<>"",$K2<>"", (INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2)) >  VALUE($K2))', green);
  addCFRule(col(10),'=AND($F2<>"",$K2<>"", (INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2)) <= VALUE($K2))', red);

  // 3) queue marker write R1
  valueOps.push({ range:`${TAB_NAME}!R1:R1`, values:[[FORMAT_VERSION]] });
}

/* ---------- FLOW HELPERS ---------- */
function mapHeaderRow(hdr){ hdr.forEach((h,i)=>COLS[h.trim().toLowerCase()]=i); }
function buildRowFromEvent(ev){
  const comp = ev.competitions?.[0] || {};
  const a = comp.competitors?.find(t=>t.homeAway==="away");
  const h = comp.competitors?.find(t=>t.homeAway==="home");
  const date = comp.date || ev.date || new Date().toISOString();
  const aTeam = a?.team?.abbreviation || a?.team?.shortDisplayName || a?.team?.name || "";
  const hTeam = h?.team?.abbreviation || h?.team?.shortDisplayName || h?.team?.name || "";
  return {
    id: String(ev.id),
    date: fmtListDate(date),
    wk: weekLabel(ev),
    status: fmtStatus(date),
    matchup: `${aTeam} @ ${hTeam}`
  };
}
function collectDates(){
  if (GAME_IDS) return [];
  const span = RUN_SCOPE==="today" ? 1 : 7;
  const out=[]; for(let i=0;i<span;i++) out.push(yyyymmddET(new Date(Date.now()+i*86400000)));
  return out;
}

/* ---------- MAIN ---------- */
(async ()=>{
  if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");

  // Auth
  const creds = CREDS_RAW.trim().startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials:{ client_email:creds.client_email, private_key:creds.private_key },
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  const client = await auth.getClient();
  const sh = new Sheets(client, SHEET_ID, TAB_NAME);

  // Sheet data + header map
  const meta = await sh.metadata();
  const sheet = meta.sheets.find(s=>s.properties.title===TAB_NAME);
  if (!sheet) throw new Error(`Tab ${TAB_NAME} not found`);
  const sheetId = sheet.properties.sheetId;

  const vals = await sh.readAll();
  const header = vals[0] || HEADERS;
  mapHeaderRow(header);

  // Build current row index by Game ID
  const rows = vals.slice(1);
  const rowById = new Map();
  rows.forEach((r,i)=>{ const id=r[COLS["game id"]]||""; if(id) rowById.set(String(id), i+2); });

  // Gather events
  const dates = collectDates();
  let events = [];
  if (GAME_IDS) {
    GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean).forEach(id=> events.push({ id, force:true }));
  } else {
    for (const d of dates) {
      const sb = await fetchJSON(sbUrl(LEAGUE_IN, d));
      events.push(...(sb?.events || []));
    }
  }

  // Append staging
  let createdRows = 0;
  async function ensureRow(ev){
    const id = String(ev.id);
    let row = rowById.get(id);
    if (row) return row;

    const base = ev.force ? { id, date:"", wk:"", status:"", matchup:"" } : buildRowFromEvent(ev);
    const nextRow = rows.length + 2 + createdRows; // header + existing + previously staged
    const blank = new Array(HEADERS.length).fill("");

    blank[COLS["game id"]] = base.id;
    blank[COLS["date"]]    = base.date;
    blank[COLS["week"]]    = base.wk;
    blank[COLS["status"]]  = base.status;
    blank[COLS["matchup"]] = base.matchup;

    valueOps.push({ range:`${TAB_NAME}!A${nextRow}:Q${nextRow}`, values:[blank] });
    rowById.set(id, nextRow);
    createdRows++;
    return nextRow;
  }

  // Process all events (queue writes only)
  let prefilled=0, finals=0, lives=0;
  for (const ev of events) {
    const id = String(ev.id);
    const comp = ev.competitions?.[0] || {};
    const state = (comp.status?.type?.state || "").toLowerCase();
    const row = await ensureRow(ev);

    // prefill
    if (!state || state==="pre") {
      const odds = await getOddsFromSummary(LEAGUE_IN, id);
      if (odds) {
        queueCell(TAB_NAME, row, COLS["a spread"], odds.aSpread);
        queueCell(TAB_NAME, row, COLS["a ml"],     odds.aML);
        queueCell(TAB_NAME, row, COLS["h spread"], odds.hSpread);
        queueCell(TAB_NAME, row, COLS["h ml"],     odds.hML);
        queueCell(TAB_NAME, row, COLS["total"],    odds.total);
        prefilled++;
      }
      if (!ev.force && comp.date) {
        const b = buildRowFromEvent(ev);
        queueCell(TAB_NAME, row, COLS["date"],    b.date);
        queueCell(TAB_NAME, row, COLS["week"],    b.wk);
        queueCell(TAB_NAME, row, COLS["status"],  b.status);
        queueCell(TAB_NAME, row, COLS["matchup"], b.matchup);
      }
      continue;
    }

    // live
    if (state==="in") {
      const live = await getLiveBoard(LEAGUE_IN, id);
      if (live) {
        queueCell(TAB_NAME, row, COLS["h score"],   live.hScore);
        queueCell(TAB_NAME, row, COLS["h a spread"], live.haSpread);
        queueCell(TAB_NAME, row, COLS["h a ml"],     live.haML);
        queueCell(TAB_NAME, row, COLS["h h spread"], live.hhSpread);
        queueCell(TAB_NAME, row, COLS["h h ml"],     live.hhML);
        queueCell(TAB_NAME, row, COLS["h total"],    live.hTotal);
        queueCell(TAB_NAME, row, COLS["status"],     live.status || "Live");
        lives++;
      }
      continue;
    }

    // finals
    if (state==="post") {
      const a = comp.competitors?.find(t=>t.homeAway==="away");
      const h = comp.competitors?.find(t=>t.homeAway==="home");
      const aS=a?.score, hS=h?.score;
      const final = (aS!=null && hS!=null) ? `${aS}-${hS}` : "Final";
      queueCell(TAB_NAME, row, COLS["final score"], final);
      queueCell(TAB_NAME, row, COLS["status"], "Final");
      finals++;
      continue;
    }
  }

  // Conditional formatting once if needed
  await ensureFormatting(sh, sheetId, vals);

  // Flush VALUE WRITES in a small number of requests (chunk by 400 ranges to be safe)
  const CHUNK = 400;
  for (let i=0;i<valueOps.length;i+=CHUNK) {
    await sh.batchValues(valueOps.slice(i, i+CHUNK));
  }
  // Flush formatting requests (if any)
  if (requestOps.length) await sh.batchRequests(requestOps);

  console.log(JSON.stringify({ ok:true, tab:TAB_NAME, league:LEAGUE_IN, events:events.length, createdRows, prefilled, finals, lives, valueRequests: Math.ceil(valueOps.length/CHUNK), formatRequests: requestOps.length }));
})().catch(e=>{ console.error("Fatal:", e?.message || e); process.exit(1); });
