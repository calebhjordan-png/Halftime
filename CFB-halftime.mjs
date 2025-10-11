// CFB-halftime.mjs
// Watches today's CFB games; when a game hits HALFTIME, writes Half Score + live odds once.
// Works with either "Date" or "Date (ET)" header; doesn't require adding Date (ET).

import { google } from "googleapis";
import { chromium } from "playwright";

// --------- ENV ----------
const SHEET_ID  = need("GOOGLE_SHEET_ID");
const SA_RAW    = need("GOOGLE_SERVICE_ACCOUNT"); // raw JSON or base64 of service account
const TAB_NAME  = process.env.TAB_NAME || "CFB";
const MAX_RUNTIME_MIN = Number(process.env.MAX_RUNTIME_MIN || 90);
const AUTO_APPEND = process.env.AUTO_APPEND_MISSING_ROWS === "1";

function need(k){ const v=(process.env[k]||"").trim(); if(!v) throw new Error(`Missing env ${k}`); return v; }
function parseSA(raw){ return raw.startsWith("{") ? JSON.parse(raw) : JSON.parse(Buffer.from(raw,"base64").toString("utf8")); }
const ET_TZ = "America/New_York";

// ---------- small utils ----------
function yyyymmddET(d=new Date()){
  const p=new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const g=k=>p.find(x=>x.type===k)?.value||"";
  return `${g("year")}${g("month")}${g("day")}`;
}
function fmtETDate(dlike){
  return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(dlike));
}
function colA1(n){ let s="",x=n; while(x>0){ const m=(x-1)%26; s=String.fromCharCode(65+m)+s; x=Math.floor((x-1)/26);} return s; }
function lowerMap(arr){ const m={}; arr.forEach((v,i)=>m[String(v||"").toLowerCase()]=i); return m; }
function truthy(v){ return v!==undefined && v!==null && String(v).trim()!==""; }
function keyOf(dateET, matchup){ return `${(dateET||"").trim()}__${(matchup||"").trim()}`; }
const IDX = (map, ...names) => { for (const n of names){ const i = map[n.toLowerCase()]; if (typeof i === "number") return i; } return -1; };

// --------- ESPN ----------
const SPORT = "football/college-football";
const SB_URL  = (dates)=>`https://site.api.espn.com/apis/site/v2/sports/${SPORT}/scoreboard?dates=${dates}&groups=80&limit=300`;
const SUM_URL = (id)=>`https://site.api.espn.com/apis/site/v2/sports/${SPORT}/summary?event=${id}`;
const GAME_URL= (id)=>`https://www.espn.com/college-football/game?gameId=${id}`;

async function fetchJson(url){
  const r = await fetch(url, { headers: { "User-Agent":"cfb-halftime" }});
  if(!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
function normalizeStatus(evt){
  const name = evt?.competitions?.[0]?.status?.type?.name || evt?.status?.type?.name || "";
  const short= evt?.competitions?.[0]?.status?.type?.shortDetail || "";
  if(/final/i.test(name)||/\bFinal\b/i.test(short)) return "Final";
  if(/\bHalftime\b/i.test(short)) return "Halftime";
  if(/inprogress|status\.in/i.test(name)||/\bQ[1-4]\b/i.test(short)) return "Live";
  if(/pre|scheduled/i.test(name)||/\bScheduled\b/i.test(short)) return "Scheduled";
  return name || "Scheduled";
}
function namesFor(evt){
  const comp = evt?.competitions?.[0]||{};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  return {away,home,awayName,homeName};
}

// Half score from summary (sum Q1+Q2)
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

// Live odds snapshot (DOM + fallbacks)
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

    // Fallbacks via summary odds/pickcenter
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

// --------- Google Sheets (only ensures halftime/live columns; doesn't enforce Date ET) ----------
const REQUIRED_COLS = [
  "Week","Status","Matchup",
  "Half Score","Final Score",
  "Live Away Spread","Live Away ML",
  "Live Home Spread","Live Home ML","Live Total"
];

async function getSheets(){
  const CREDS = parseSA(SA_RAW);
  const auth = new google.auth.JWT(CREDS.client_email, undefined, CREDS.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
  await auth.authorize();
  return google.sheets({ version:"v4", auth });
}
async function ensureHeaderAndMap(sheets){
  // ensure tab exists
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tab = meta.data.sheets?.find(s=>s.properties?.title===TAB_NAME);
  if(!tab){
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  // read header
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:ZZ1` });
  let header = res.data.values?.[0] || [];
  const have = new Set(header.map(h=>String(h||"").toLowerCase()));
  for(const h of REQUIRED_COLS){ if(!have.has(h.toLowerCase())) header.push(h); }
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:${colA1(header.length)}1`,
    valueInputOption: "RAW",
    requestBody: { values: [header] }
  });
  return { header, hmap: lowerMap(header) };
}
async function readGrid(sheets){
  const grid = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:ZZ` });
  return grid.data.values || [];
}
async function batchUpdate(sheets, data){
  if(!data.length) return;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: "RAW", data }
  });
}

// --------- MAIN LOOP ----------
(async function main(){
  const sheets = await getSheets();
  const { header, hmap } = await ensureHeaderAndMap(sheets);

  const start = Date.now();
  const hardStop = start + MAX_RUNTIME_MIN*60*1000;

  while(true){
    const sb = await fetchJson(SB_URL(yyyymmddET()));
    let events = (sb?.events||[]).slice();
    const seen = new Set(); events = events.filter(e=>!seen.has(e.id)&&seen.add(e.id));

    const grid = await readGrid(sheets);
    const head = grid[0] || header;
    const rows = grid.slice(1);
    const hm = lowerMap(head);

    // Accept "Date" or "Date (ET)"
    const idxDate   = IDX(hm, "date (et)", "date");
    const idxMu     = IDX(hm, "matchup");
    const idxStatus = IDX(hm, "status");

    const idxHalf   = IDX(hm, "half score");
    const idxFinal  = IDX(hm, "final score", "final");

    const idxLAsp   = IDX(hm, "live away spread", "away spread");
    const idxLAml   = IDX(hm, "live away ml",     "away ml");
    const idxLHsp   = IDX(hm, "live home spread", "home spread");
    const idxLHml   = IDX(hm, "live home ml",     "home ml");
    const idxTot    = IDX(hm, "live total",       "total");

    const rowIndex = new Map();
    rows.forEach((r,i)=>{ const k = keyOf(r[idxDate]||"", r[idxMu]||""); if(k.trim()) rowIndex.set(k, i+2); });

    const writes = [];

    for(const ev of events){
      const status = normalizeStatus(ev);
      const { awayName, homeName } = namesFor(ev);
      const matchup = `${awayName} @ ${homeName}`;
      const dateET  = fmtETDate(ev.date);
      const key     = keyOf(dateET, matchup);
      const rowNum  = rowIndex.get(key);

      console.log(`[${status.padEnd(8)}] ${dateET}  ${matchup}  ${rowNum ? '(row found)' : '(no row)'}`);

      // Optionally create the row at halftime if missing
      if(!rowNum && status === "Halftime" && AUTO_APPEND){
        const newRowIdx = rows.length + 2; // header + 1-based
        const ins = [];
        const put = (ci,val)=>{ if(ci==null||ci<0) return; ins.push({ range: `${TAB_NAME}!${colA1(ci+1)}${newRowIdx}:${colA1(ci+1)}${newRowIdx}`, values:[[val]] }); };
        if (idxDate >= 0)  put(idxDate,  dateET);
        if (idxMu   >= 0)  put(idxMu,    matchup);
        if (idxStatus>=0)  put(idxStatus,"Halftime");
        await batchUpdate(sheets, ins);
        rowIndex.set(key, newRowIdx);
      }

      const rowId = rowIndex.get(key);
      if(!rowId) continue; // require a matching row

      const r = rows[rowId-2] || [];
      const haveHalf = truthy(r[idxHalf]);
      const haveOdds = truthy(r[idxLAsp]) || truthy(r[idxLAml]) || truthy(r[idxLHsp]) || truthy(r[idxLHml]) || truthy(r[idxTot]);

      if(status === "Halftime" && (!haveHalf || !haveOdds)){
        const snap = await liveOddsSnapshot(ev.id);
        const add = (ci,val)=>{ if(ci==null||ci<0) return; if(!truthy(val)) return;
          const range = `${TAB_NAME}!${colA1(ci+1)}${rowId}:${colA1(ci+1)}${rowId}`;
          writes.push({ range, values: [[val]] });
        };
        if (idxStatus>=0) add(idxStatus, "Halftime");
        if (idxHalf  >=0) add(idxHalf,   snap.halfScore);
        if (idxLAsp  >=0) add(idxLAsp,   snap.awaySpread);
        if (idxLAml  >=0) add(idxLAml,   snap.awayML);
        if (idxLHsp  >=0) add(idxLHsp,   snap.homeSpread);
        if (idxLHml  >=0) add(idxLHml,   snap.homeML);
        if (idxTot   >=0) add(idxTot,    snap.total);
      }
    }

    if(writes.length){
      await batchUpdate(sheets, writes);
      console.log(`✅ wrote ${writes.length} cell(s) this loop`);
    } else {
      console.log("…no halftime writes this loop");
    }

    if(Date.now() > hardStop) { console.log("⏹ MAX_RUNTIME reached"); break; }
    await new Promise(r=>setTimeout(r, 3*60*1000)); // 3 min cadence
  }
})().catch(e=>{ console.error("Fatal:", e?.stack||e); process.exit(1); });
