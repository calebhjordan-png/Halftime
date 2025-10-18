// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q) from ESPN BET.
// Prefers scraped LIVE ODDS when available; skips stale pregame pools.

import axios from "axios";
import { google } from "googleapis";

/* ─────────── ENV ─────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  DEBUG_MODE = "0",
} = process.env;
const DEBUG = String(DEBUG_MODE) === "1";

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT)
  throw new Error("Missing required environment variables");

/* ─────────── Google Sheets Auth ─────────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ─────────── Helpers ─────────── */
function idxToA1(n0) {
  let n = n0 + 1, s = "";
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
const norm = s => (s || "").toLowerCase();
const isFinalCell = s => /^final$/i.test(String(s || ""));
function looksLiveStatus(s) {
  const x = norm(s);
  return /\bhalf\b|\bq[1-4]\b|in\s*progress|ot|live|[0-9]+:[0-9]+\s*-\s*(1st|2nd|3rd|4th)/i.test(x);
}

/* date in US/Eastern */
const todayKey = (() => {
  const d = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "2-digit", day: "2-digit", year: "2-digit",
  }).formatToParts(d);
  return `${parts.find(p=>p.type==="month").value}/${parts.find(p=>p.type==="day").value}/${parts.find(p=>p.type==="year").value}`;
})();

/* ─────────── ESPN APIs ─────────── */
const leaguePath = LEAGUE === "college-football" ? "football/college-football" : "football/nfl";
async function espnSummary(id) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${leaguePath}/summary?event=${id}`;
  if (DEBUG) console.log("🔎 summary:", url);
  return axios.get(url, { timeout: 15000 }).then(r => r.data);
}

/* ─────────── Parse Helpers ─────────── */
function shortStatusFromEspn(st) {
  const t = st?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(st) {
  return /final/i.test(st?.type?.name || st?.type?.description || "");
}
function sumFirstTwoPeriods(scores) {
  if (!scores) return null;
  return scores.slice(0,2).reduce((a,b)=>a+Number(b?.value??b?.score??0),0);
}
function parseHalfScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find(c=>c.homeAway==="home");
    const away = comp?.competitors?.find(c=>c.homeAway==="away");
    const hh=sumFirstTwoPeriods(home?.linescores);
    const ha=sumFirstTwoPeriods(away?.linescores);
    if (isFinite(hh)&&isFinite(ha)) return `${ha}-${hh}`;
  } catch {}
  return "";
}

/* ─────────── HTML scrape ESPN BET LIVE ODDS ─────────── */
async function scrapeGamePageLiveOdds(id) {
  const sport = LEAGUE==="college-football"?"college-football":"nfl";
  const url = `https://www.espn.com/${sport}/game/_/gameId/${id}`;
  try {
    const { data: html } = await axios.get(url, { timeout: 15000 });
    const liveBlockMatch =
      html.match(/ESPN BET[\s\S]{0,4000}?LIVE ODDS[\s\S]{0,4000}?<\/section>/i) ||
      html.match(/LIVE ODDS[\s\S]{0,4000}?Odds by ESPN BET/i);
    const block = liveBlockMatch ? liveBlockMatch[0] : html;
    if (DEBUG) console.log("   [scrape] snippet:", block.slice(0,400));

    const totalMatch = [...block.matchAll(/\b[ou]\s?(\d{2,3}(?:\.\d)?)\b/ig)];
    const total = totalMatch.length ? Number(totalMatch[0][1]) : "";

    const mlMatch = [...block.matchAll(/([+-]\d{3,5})/g)]
      .map(m=>Number(m[1])).filter(v=>Math.abs(v)>=300);
    let mlAway="",mlHome="";
    if (mlMatch.length){
      mlAway=mlMatch.find(v=>v>0)??"";
      mlHome=mlMatch.find(v=>v<0)??"";
    }

    const spreadMatch=[...block.matchAll(/([+-]\d{1,2}(?:\.\d)?)/g)]
      .map(m=>Number(m[1])).filter(v=>Math.abs(v)<=60);
    let spreadAway="",spreadHome="";
    if (spreadMatch.length){
      spreadAway=spreadMatch.find(v=>v>0)??"";
      spreadHome=spreadMatch.find(v=>v<0)??"";
    }

    if (DEBUG) console.log(`   [scrape] parsed => spread ${spreadAway}/${spreadHome}, ML ${mlAway}/${mlHome}, total ${total}`);
    if ([spreadAway,spreadHome,mlAway,mlHome,total].some(v=>v!==""))
      return { spreadAway, spreadHome, mlAway, mlHome, total };
  } catch(e){ if(DEBUG) console.log("   [scrape] failed", e?.message); }
  return undefined;
}

/* ─────────── chooseTargets ─────────── */
function chooseTargets(rows,col){
  const out=[];
  for(let r=1;r<rows.length;r++){
    const id=(rows[r][col.GAME_ID]||"").trim();
    if(!id) continue;
    const date=(rows[r][col.DATE]||"").trim();
    const status=(rows[r][col.STATUS]||"").trim();
    if(isFinalCell(status)) continue;
    if(GAME_ID && id===GAME_ID){ out.push({r,id,reason:"GAME_ID"}); continue; }
    if(looksLiveStatus(status)||date===todayKey) out.push({r,id});
  }
  return out;
}

/* ─────────── mapCols / helpers ─────────── */
function mapCols(h){
  const f=n=>h.findIndex(c=>c.trim().toLowerCase()===n.toLowerCase());
  return{
    GAME_ID:f("Game ID"),
    DATE:f("Date"),
    STATUS:f("Status"),
    HALF:f("Half Score"),
    LA_S:f("Live Away Spread"),
    LA_ML:f("Live Away ML"),
    LH_S:f("Live Home Spread"),
    LH_ML:f("Live Home ML"),
    L_TOT:f("Live Total")
  };
}
const makeValue=(r,v)=>({range:r,values:[[v]]});
const a1For=(r,c,tab=TAB_NAME)=>`${tab}!${idxToA1(c)}${r+1}:${idxToA1(c)}${r+1}`;

/* ─────────── MAIN ─────────── */
async function main(){
  const values=(await sheets.spreadsheets.values.get({spreadsheetId:GOOGLE_SHEET_ID,range:`${TAB_NAME}!A1:Q2000`})).data.values||[];
  if(!values.length) return console.log("Sheet empty.");
  const col=mapCols(values[0]);
  const targets=chooseTargets(values,col);
  console.log(`Found ${targets.length} game(s) to update: ${targets.map(t=>t.id).join(", ")}`);

  const data=[];
  for(const t of targets){
    if(DEBUG) console.log(`\n=== 🏈 GAME ${t.id} ===`);
    const currStatus=values[t.r]?.[col.STATUS]||"";
    if(isFinalCell(currStatus)) continue;

    /* STATUS + HALF */
    let summary;
    try{
      summary=await espnSummary(t.id);
      const st=summary?.header?.competitions?.[0]?.status;
      const newStatus=shortStatusFromEspn(st);
      const nowFinal=isFinalFromEspn(st);
      if(DEBUG) console.log("   status text:", newStatus);
      if(newStatus!==currStatus) data.push(makeValue(a1For(t.r,col.STATUS),newStatus));
      const half=parseHalfScore(summary);
      if(half) data.push(makeValue(a1For(t.r,col.HALF),half));
      if(nowFinal) continue;
    }catch(e){ if(DEBUG) console.log("   summary warn:", e?.message); }

    /* Try ESPN BET pools first */
    let pool;
    try{
      const pools=(summary?.pickcenter||[]).concat(summary?.odds||[]);
      pool=pools.find(p=>/espn\s*bet/i.test(p?.provider?.name||""));
      if(pool){
        const a=pool?.awayTeamOdds||{},h=pool?.homeTeamOdds||{};
        pool={spreadAway:a?.spread,spreadHome:h?.spread,mlAway:a?.moneyLine,mlHome:h?.moneyLine,total:pool?.overUnder};
      }
    }catch{}
    let live=undefined;

    /* scrape always (prefer fresh) */
    const scraped=await scrapeGamePageLiveOdds(t.id);

    /* determine which to trust */
    const looksStale=p=>!p||Math.abs(p.spreadAway||0)>30||Math.abs(p.mlAway||0)>1000;
    if(scraped && (!pool || looksStale(pool))) live=scraped;
    else live=pool||scraped;

    if(DEBUG) console.log("   chosen =>", JSON.stringify(live));

    if(live){
      const w=(c,v)=>{if(v!==""&&Number.isFinite(Number(v)))data.push(makeValue(a1For(t.r,c),Number(v)));};
      w(col.LA_S,live.spreadAway); w(col.LA_ML,live.mlAway);
      w(col.LH_S,live.spreadHome); w(col.LH_ML,live.mlHome);
      w(col.L_TOT,live.total);
    } else if(DEBUG) console.log("   ❌ no live odds found");
  }

  if(!data.length) return console.log("No updates.");
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId:GOOGLE_SHEET_ID,
    requestBody:{valueInputOption:"USER_ENTERED",data}
  });
  console.log(`✅ Updated ${data.length} cell(s).`);
}
main();
