// CFB-halftime-check.mjs
// Watches ONE CFB game (TARGET_GAME_ID) until halftime, then writes data to Google Sheets.
// Adaptive recheck cadence: 20m normally; if 2Q <=10:00, sleep = 2x time remaining.

import { google } from "googleapis";
import { chromium } from "playwright";

// ---------- ENV ----------
const SHEET_ID  = need("GOOGLE_SHEET_ID");
const SA_RAW    = need("GOOGLE_SERVICE_ACCOUNT");
const TAB_NAME  = process.env.TAB_NAME || "CFB";
const GAME_ID   = need("TARGET_GAME_ID");
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || 200);
const ET_TZ = "America/New_York";

// ---------- UTILS ----------
function need(k){ const v=(process.env[k]||"").trim(); if(!v) throw new Error(`Missing env ${k}`); return v; }
function parseSA(raw){ return raw.startsWith("{") ? JSON.parse(raw) : JSON.parse(Buffer.from(raw,"base64").toString("utf8")); }
function parseClock(clock){ const m = clock?.match(/^(\d+):(\d{2})/); return m ? (+m[1])*60+(+m[2]) : NaN; }
function colA1(n){ let s="",x=n; while(x>0){ const m=(x-1)%26; s=String.fromCharCode(65+m)+s; x=Math.floor((x-1)/26);} return s; }
function lowerMap(arr){ const m={}; arr.forEach((v,i)=>m[String(v||"").toLowerCase()]=i); return m; }
function truthy(v){ return v!==undefined && v!==null && String(v).trim()!==""; }
function keyOf(dateET, matchup){ return `${(dateET||"").trim()}__${(matchup||"").trim()}`; }
const IDX = (map, ...names)=>{ for(const n of names){ const i=map[n.toLowerCase()]; if(typeof i==="number") return i; } return -1; };

// ---------- ESPN ----------
const SPORT="football/college-football";
const SUM_URL=id=>`https://site.api.espn.com/apis/site/v2/sports/${SPORT}/summary?event=${id}`;
const GAME_URL=id=>`https://www.espn.com/college-football/game?gameId=${id}`;
async function fetchJson(url){ const r=await fetch(url,{headers:{"User-Agent":"cfb-halftime-check"}}); if(!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }

function statusInfo(sum){
  const comp0=sum?.competitions?.[0]||{};
  const st=comp0.status||{}, type=st.type||{};
  const period=st.period??0, clock=st.displayClock||"";
  const short=type.shortDetail||"", name=type.name||"";
  let status="Scheduled";
  if(/final/i.test(name)||/\bFinal\b/i.test(short)) status="Final";
  else if(/\bHalftime\b/i.test(short)) status="Halftime";
  else if(/inprogress|status\.in/i.test(name)||/\bQ[1-4]\b/i.test(short)) status="Live";
  const comps=comp0.competitors||[];
  const away=comps.find(c=>c.homeAway==="away"), home=comps.find(c=>c.homeAway==="home");
  const awayName=away?.team?.shortDisplayName||away?.team?.abbreviation||away?.team?.name||"Away";
  const homeName=home?.team?.shortDisplayName||home?.team?.abbreviation||home?.team?.name||"Home";
  const dateET=new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(comp0.date));
  return {status,period,clock,awayName,homeName,dateET};
}

async function halfScoreFromSummary(id){
  try{
    const s=await fetchJson(SUM_URL(id));
    const comp0=s?.competitions?.[0]; const comps=comp0?.competitors||[];
    const get=side=>comps.find(c=>c.homeAway===side);
    const num=v=>{const n=Number(v?.value??v);return Number.isFinite(n)?n:0;};
    const sumH1=a=>Array.isArray(a)?num(a[0])+num(a[1]):0;
    const a=sumH1(get("away")?.linescores), h=sumH1(get("home")?.linescores);
    if(Number.isFinite(a)&&Number.isFinite(h)) return `${a}-${h}`;
  }catch{} return "";
}

async function liveOddsSnapshot(id){
  let aS="",hS="",tot="",aML="",hML="",half="";
  const browser=await chromium.launch({headless:true}); const page=await browser.newPage();
  try{
    await page.goto(GAME_URL(id),{waitUntil:"domcontentloaded",timeout:60000});
    await page.waitForLoadState("networkidle",{timeout:6000}).catch(()=>{});
    half=await halfScoreFromSummary(id);
    const sec=page.locator('section:has-text("Live Odds"), [data-testid*="odds"]').first();
    if(await sec.count()){
      const txt=(await sec.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();
      const spreads=txt.match(/[+-]\d+(?:\.\d+)?/g)||[]; aS=spreads[0]||""; hS=spreads[1]||"";
      const tO=txt.match(/\bO\s?(\d+(?:\.\d+)?)\b/i),tU=txt.match(/\bU\s?(\d+(?:\.\d+)?)\b/i);
      tot=(tO&&tO[1])||(tU&&tU[1])||""; const mls=txt.match(/[+-]\d{2,4}\b/g)||[];
      aML=mls[0]||""; hML=mls[1]||"";
    }
  }finally{await page.close().catch(()=>{});await browser.close().catch(()=>{});}
  return {halfScore:half,awaySpread:aS,homeSpread:hS,awayML:aML,homeML:hML,total:tot};
}

// ---------- Sheets ----------
async function getSheets(){
  const creds=parseSA(SA_RAW);
  const auth=new google.auth.JWT(creds.client_email,undefined,creds.private_key,["https://www.googleapis.com/auth/spreadsheets"]);
  await auth.authorize(); return google.sheets({version:"v4",auth});
}
async function readGrid(s,t){const g=await s.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${t}!A1:ZZ`});return g.data.values||[];}
async function batchUpdate(s,d){if(!d.length)return;await s.spreadsheets.values.batchUpdate({spreadsheetId:SHEET_ID,requestBody:{valueInputOption:"RAW",data:d}});}

// ---------- MAIN ----------
(async()=>{
  const sheets=await getSheets();
  const start=Date.now(), stop=start+MAX_TOTAL_MIN*60*1000;
  while(true){
    const sum=await fetchJson(SUM_URL(GAME_ID));
    const {status,period,clock,awayName,homeName,dateET}=statusInfo(sum);
    const matchup=`${awayName} @ ${homeName}`;
    console.log(`[${status}] ${matchup}  Q${period} ${clock}`);

    // ----- halftime branch -----
    if(status==="Halftime"){
      const snap=await liveOddsSnapshot(GAME_ID);
      const grid=await readGrid(sheets,TAB_NAME);
      const head=grid[0]||[],rows=grid.slice(1),hm=lowerMap(head);
      const idxDate=IDX(hm,"date (et)","date"),idxMu=IDX(hm,"matchup"),idxSt=IDX(hm,"status");
      const idxHalf=IDX(hm,"half score"),idxLAsp=IDX(hm,"live away spread","away spread");
      const idxLAml=IDX(hm,"live away ml","away ml"),idxLHsp=IDX(hm,"live home spread","home spread");
      const idxLHml=IDX(hm,"live home ml","home ml"),idxTot=IDX(hm,"live total","total");
      let row=-1;rows.forEach((r,i)=>{if(keyOf(r[idxDate]||"",r[idxMu]||"")===keyOf(dateET,matchup))row=i+2;});
      if(row<0){console.log("No row found; exiting.");break;}
      const w=[];const add=(ci,v)=>{if(ci>=0&&truthy(v))w.push({range:`${TAB_NAME}!${colA1(ci+1)}${row}:${colA1(ci+1)}${row}`,values:[[v]]});};
      add(idxSt,"Halftime");add(idxHalf,snap.halfScore);
      add(idxLAsp,snap.awaySpread);add(idxLAml,snap.awayML);
      add(idxLHsp,snap.homeSpread);add(idxLHml,snap.homeML);add(idxTot,snap.total);
      await batchUpdate(sheets,w);
      console.log(`✅ wrote ${w.length} cell(s)`);break;
    }
    if(status==="Final"){console.log("Final; exiting.");break;}

    // ----- adaptive cadence -----
    let sleepSec=20*60;
    if(period===2){const rem=parseClock(clock);
      if(Number.isFinite(rem)&&rem<=600){
        sleepSec=Math.max(60,Math.min(20*60,rem*2));
      }
    }

    const left=Math.floor((stop-Date.now())/1000);
    if(left<=0){console.log("⏹ timeout");break;}
    sleepSec=Math.min(sleepSec,left);
    console.log(`Sleeping ${Math.round(sleepSec/60)}m (${sleepSec}s)…`);
    await new Promise(r=>setTimeout(r,sleepSec*1000));
  }
})().catch(e=>{console.error("Fatal:",e);process.exit(1);});
