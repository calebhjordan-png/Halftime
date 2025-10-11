// cfb-halftime-single.mjs
// Watches a single CFB game (TARGET_GAME_ID). Adaptive cadence pre-halftime, then writes halftime snapshot.

import { google } from "googleapis";
import { chromium } from "playwright";

const SHEET_ID  = need("GOOGLE_SHEET_ID");
const SA_RAW    = need("GOOGLE_SERVICE_ACCOUNT");
const TAB_NAME  = process.env.TAB_NAME || "CFB";
const GAME_ID   = need("TARGET_GAME_ID");              // <-- required
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || 200); // safety cap

function need(k){ const v=(process.env[k]||"").trim(); if(!v) throw new Error(`Missing env ${k}`); return v; }
function parseSA(raw){ return raw.startsWith("{") ? JSON.parse(raw) : JSON.parse(Buffer.from(raw,"base64").toString("utf8")); }
const ET_TZ = "America/New_York";

// ----- ESPN -----
const SPORT = "football/college-football";
const SUM_URL  = (id)=>`https://site.api.espn.com/apis/site/v2/sports/${SPORT}/summary?event=${id}`;
const GAME_URL = (id)=>`https://www.espn.com/college-football/game?gameId=${id}`;

async function fetchJson(url){
  const r = await fetch(url, { headers: { "User-Agent":"cfb-halftime-single" }});
  if(!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}

function parseClock(displayClock){
  // "10:23" -> 623; "0:30" -> 30
  if(!displayClock) return NaN;
  const m = displayClock.match(/^(\d+):(\d{2})/);
  if(!m) return NaN;
  return (+m[1])*60 + (+m[2]);
}
function statusInfo(sum){
  const comp0 = sum?.competitions?.[0];
  const st = comp0?.status || {};
  const type = st?.type || {};
  const period = st?.period ?? comp0?.status?.period ?? 0;
  const displayClock = st?.displayClock ?? comp0?.status?.displayClock ?? "";
  const short = type?.shortDetail || "";
  let status = "Scheduled";
  if (/final/i.test(type?.name) || /\bFinal\b/i.test(short)) status = "Final";
  else if (/\bHalftime\b/i.test(short)) status = "Halftime";
  else if (/inprogress|status\.in/i.test(type?.name) || /\bQ[1-4]\b/i.test(short)) status = "Live";
  const comps = comp0?.competitors || [];
  const away = comps.find(c=>c.homeAway==="away");
  const home = comps.find(c=>c.homeAway==="home");
  const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  const dateET = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(comp0?.date || sum?.header?.competitions?.[0]?.date));
  return { status, period, displayClock, awayName, homeName, dateET };
}

async function halfScoreFromSummary(gameId){
  try{
    const sum = await fetchJson(SUM_URL(gameId));
    const comp0 = sum?.competitions?.[0];
    const comps = Array.isArray(comp0?.competitors) ? comp0.competitors : [];
    const get = side => comps.find(c=>c.homeAway===side);
    const toNum = v => { const n=Number(v?.value ?? v); return Number.isFinite(n)?n:0; };
    const sumH1 = arr => Array.isArray(arr) ? toNum(arr[0]) + toNum(arr[1]) : 0;
    const aH1 = sumH1(get("away")?.linescores || get("away")?.linescore || get("away")?.scorebreakdown);
    const hH1 = sumH1(get("home")?.linescores || get("home")?.linescore || get("home")?.scorebreakdown);
    if(Number.isFinite(aH1) && Number.isFinite(hH1)) return `${aH1}-${hH1}`;
  }catch{}
  return "";
}

async function liveOddsSnapshot(gameId){
  let awaySpread="", homeSpread="", total="", awayML="", homeML="", halfScore="";
  const browser = await chromium.launch({ headless:true });
  const page = await browser.newPage();
  try{
    await page.goto(GAME_URL(gameId), { waitUntil:"domcontentloaded", timeout:60000 });
    await page.waitForLoadState("networkidle", { timeout:6000 }).catch(()=>{});
    halfScore = await halfScoreFromSummary(gameId);

    const sec = page.locator('section:has-text("Live Odds"), section:has-text("LIVE ODDS"), [data-testid*="odds"]').first();
    if(await sec.count()){
      const txt = (await sec.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();
      const spreadMatches = txt.match(/[+-]\d+(?:\.\d+)?/g) || [];
      awaySpread = spreadMatches[0] || "";
      homeSpread = spreadMatches[1] || "";
      const tO = txt.match(/\bO\s?(\d+(?:\.\d+)?)\b/i);
      const tU = txt.match(/\bU\s?(\d+(?:\.\d+)?)\b/i);
      total = (tO && tO[1]) || (tU && tU[1]) || "";
      const mls = txt.match(/[+-]\d{2,4}\b/g) || [];
      awayML = mls[0] || "";
      homeML = mls[1] || "";
    }

    // Fallbacks via summary odds/pickcenter if anything missing
    if(!(awayML&&homeML) || !total || !(awaySpread&&homeSpread)){
      try{
        const sum = await fetchJson(SUM_URL(gameId));
        const oddsBuckets = [];
        const h = sum?.header?.competitions?.[0]?.odds; if(Array.isArray(h)) oddsBuckets.push(...h);
        if(Array.isArray(sum?.odds)) oddsBuckets.push(...sum.odds);
        if(Array.isArray(sum?.pickcenter)) oddsBuckets.push(...sum.pickcenter);
        const comps = sum?.competitions?.[0]?.competitors || [];
        const awayId = String(comps.find(c=>c.homeAway==="away")?.id||"");
        const homeId = String(comps.find(c=>c.homeAway==="home")?.id||"");
        const first = oddsBuckets.find(Boolean) || {};
        const favId = String(first?.favorite ?? first?.favoriteTeamId ?? "");
        const spread = Number(first?.spread ?? (typeof first?.details==="string" ? parseFloat((first.details.match(/([+-]?\d+(?:\.\d+)?)/)||[])[1]) : NaN));
        const ou = first?.overUnder ?? first?.total;
        if(!total && ou) total = String(ou);

        const mlFields = cand=>{
          let a="",h="";
          const tryNum=v=> (v==null? "": (isNaN(+v)? "": String(v)));
          a = tryNum(cand?.moneyLineAway ?? cand?.awayTeamMoneyLine ?? cand?.awayMoneyLine);
          h = tryNum(cand?.moneyLineHome ?? cand?.homeTeamMoneyLine ?? cand?.homeMoneyLine);
          if(!(a&&h) && Array.isArray(cand?.teamOdds)){
            for(const t of cand.teamOdds){
              const id = String(t?.teamId ?? t?.team?.id ?? "");
              const ml = tryNum(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
              if(id===awayId && ml) a=a||ml;
              if(id===homeId && ml) h=h||ml;
            }
          }
          return {a,h};
        };
        if(!(awayML&&homeML)){
          const {a,h} = mlFields(first);
          if(a) awayML = awayML || a;
          if(h) homeML = homeML || h;
          if(!(awayML&&homeML)){
            for(const b of oddsBuckets){
              const {a:aa,h:hh} = mlFields(b);
              if(!awayML && aa) awayML=aa;
              if(!homeML && hh) homeML=hh;
              if(awayML && homeML) break;
            }
          }
        }
        if(!(awaySpread&&homeSpread) && favId && Number.isFinite(spread)){
          if(awayId===favId){ awaySpread = awaySpread || `-${Math.abs(spread)}`; homeSpread = homeSpread || `+${Math.abs(spread)}`; }
          else if(homeId===favId){ homeSpread = homeSpread || `-${Math.abs(spread)}`; awaySpread = awaySpread || `+${Math.abs(spread)}`; }
        }
      }catch{}
    }
  } finally {
    await page.close().catch(()=>{});
    await browser.close().catch(()=>{});
  }
  return { halfScore, awaySpread, homeSpread, awayML, homeML, total };
}

// ----- Sheets -----
function colA1(n){ let s="",x=n; while(x>0){ const m=(x-1)%26; s=String.fromCharCode(65+m)+s; x=Math.floor((x-1)/26);} return s; }
function lowerMap(arr){ const m={}; arr.forEach((v,i)=>m[String(v||"").toLowerCase()]=i); return m; }
function truthy(v){ return v!==undefined && v!==null && String(v).trim()!==""; }
function keyOf(dateET, matchup){ return `${(dateET||"").trim()}__${(matchup||"").trim()}`; }
const IDX = (map, ...names) => { for (const n of names){ const i = map[n.toLowerCase()]; if (typeof i === "number") return i; } return -1; };

async function getSheets(){
  const CREDS = parseSA(SA_RAW);
  const auth = new google.auth.JWT(CREDS.client_email, undefined, CREDS.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
  await auth.authorize();
  return google.sheets({ version:"v4", auth });
}
async function readGrid(sheets, tab){
  const grid = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${tab}!A1:ZZ` });
  return grid.data.values || [];
}
async function batchUpdate(sheets, data){
  if(!data.length) return;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "RAW", data }
  });
}

// ----- Main -----
(async function main(){
  const sheets = await getSheets();
  const started = Date.now();
  const hardStop = started + MAX_TOTAL_MIN*60*1000;

  // prefetch names & date, and then loop with adaptive cadence
  let matchKey = null, colIdx = null, rowIdx = null;

  while(true){
    const sum = await fetchJson(SUM_URL(GAME_ID));
    const { status, period, displayClock, awayName, homeName, dateET } = statusInfo(sum);
    const matchup = `${awayName} @ ${homeName}`;
    matchKey = matchKey || keyOf(dateET, matchup);

    console.log(`[${status}] ${matchup}  Q${period} ${displayClock || ""}`);

    // Halftime -> write snapshot and exit
    if (status === "Halftime") {
      const snap = await liveOddsSnapshot(GAME_ID);

      // read grid and map columns (accept "Date" or "Date (ET)")
      const grid = await readGrid(sheets, TAB_NAME);
      const head = grid[0] || [];
      const rows = grid.slice(1);
      const hm = lowerMap(head);
      const idxDate   = IDX(hm, "date (et)", "date");
      const idxMu     = IDX(hm, "matchup");
      const idxStatus = IDX(hm, "status");
      const idxHalf   = IDX(hm, "half score");
      const idxLAsp   = IDX(hm, "live away spread", "away spread");
      const idxLAml   = IDX(hm, "live away ml",     "away ml");
      const idxLHsp   = IDX(hm, "live home spread", "home spread");
      const idxLHml   = IDX(hm, "live home ml",     "home ml");
      const idxTot    = IDX(hm, "live total",       "total");

      // locate row
      const key = keyOf(dateET, matchup);
      let rowNum = -1;
      rows.forEach((r,i)=>{ const k = keyOf(r[idxDate]||"", r[idxMu]||""); if(k===key) rowNum=i+2; });
      if (rowNum < 0) {
        console.log("No matching row found; nothing to write.");
        break;
      }

      const writes = [];
      const add = (ci,val)=>{ if(ci==null||ci<0) return; if(!truthy(val)) return;
        const range = `${TAB_NAME}!${colA1(ci+1)}${rowNum}:${colA1(ci+1)}${rowNum}`;
        writes.push({ range, values: [[val]] });
      };
      add(idxStatus, "Halftime");
      add(idxHalf,   snap.halfScore);
      add(idxLAsp,   snap.awaySpread);
      add(idxLAml,   snap.awayML);
      add(idxLHsp,   snap.homeSpread);
      add(idxLHml,   snap.homeML);
      add(idxTot,    snap.total);

      await batchUpdate(sheets, writes);
      console.log(`✅ wrote ${writes.length} cell(s)`);
      break;
    }

    if (status === "Final") {
      console.log("Game is final; exiting.");
      break;
    }

    // Adaptive cadence:
    let sleepSec = 20 * 60; // default 20 minutes
    if (period === 2) {
      const rem = parseClock(displayClock);
      if (Number.isFinite(rem) && rem <= 10 * 60) {
        sleepSec = Math.max(60, Math.min(20*60, rem * 2));
      }
    }

    const untilStop = Math.floor((hardStop - Date.now())/1000);
    if (untilStop <= 0) { console.log("⏹ MAX_TOTAL_MIN reached"); break; }
    sleepSec = Math.min(sleepSec, untilStop);

    console.log(`Sleeping ${Math.round(sleepSec/60)}m (${sleepSec}s)…`);
    await new Promise(r=>setTimeout(r, sleepSec*1000));
  }
})().catch(e=>{ console.error("Fatal:", e?.stack||e); process.exit(1); });
