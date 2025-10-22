// orchestrator.mjs — Prefill + Live (halftime) + Finals & Grading in one file

import { google } from "googleapis";

/* =========================
   Environment / Settings
   ========================= */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();              // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="nfl"?"NFL":"CFB")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();         // "today" | "week"
const TARGET_GAME_ID = (process.env.TARGET_GAME_ID || "").trim();          // optional "4017...,4017..."
const GHA_JSON  = process.argv.includes("--gha") || String(process.env.GHA_JSON || "") === "1";
const DEBUG_ODDS = String(process.env.DEBUG_ODDS || "0") === "1";

/* =========================
   Column Layout (A..Q)
   ========================= */
const COLS = [
  "Game ID", "Date", "Week", "Status", "Matchup", "Final Score",
  "A Spread", "A ML", "H Spread", "H ML", "Total",
  "H Score", "Half A Spread", "Half A ML", "Half H Spread", "Half H ML", "Half Total"
];
const GREEN = { red:0.85, green:0.94, blue:0.84 };
const RED   = { red:0.99, green:0.85, blue:0.85 };
const ET = "America/New_York";

/* =========================
   Tiny helpers
   ========================= */
const _out = (s, ...a)=>s.write(a.map(x=>typeof x==='string'?x:String(x)).join(" ")+"\n");
const log  = (...a)=> GHA_JSON ? _out(process.stderr, ...a) : console.log(...a);

function parseServiceAccount(raw){
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const fmtETDate = (d)=> new Intl.DateTimeFormat("en-US",{timeZone:ET, month:"2-digit", day:"2-digit", year:"2-digit"}).format(new Date(d));
const fmtETTime = (d)=> new Intl.DateTimeFormat("en-US",{timeZone:ET, hour:"numeric", minute:"2-digit"}).format(new Date(d));
const yyyymmddInET = (d=new Date())=>{
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET, year:"numeric", month:"2-digit", day:"2-digit"}).formatToParts(d);
  const g = k => p.find(x=>x.type===k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
};
const normLeague = (x)=> (x==="ncaaf"||x==="college-football")?"college-football":"nfl";
const sbUrl = (lg, dates)=> {
  lg = normLeague(lg);
  const extra = lg==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
};
const sumUrl = (lg, id)=> `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(lg)}/summary?event=${id}`;
const gameUrl = (lg, id)=> `https://www.espn.com/${normLeague(lg)==="nfl"?"nfl":"college-football"}/game/_/gameId/${id}`;

async function fetchJson(url){
  const r = await fetch(url, { headers:{ "User-Agent":"halftime-bot" }});
  if (!r.ok) throw new Error(`Fetch failed ${r.status} ${url}`);
  return r.json();
}
function mapHeadersToIndex(h){ const m={}; h.forEach((x,i)=>m[(x||"").trim().toLowerCase()]=i); return m; }
function colLetter(i){ return String.fromCharCode("A".charCodeAt(0)+i); }

function tidyStatus(evt){
  const comp  = evt.competitions?.[0]||{};
  const t     = comp.status?.type || evt.status?.type || {};
  const name  = String(t.name || "").toUpperCase();
  const short = (t.shortDetail || t.detail || t.description || "").replace(/\s+EDT|EST/i, "").trim();
  if (name.includes("FINAL")) return "Final";
  if (name.includes("HALFTIME")) return "Half";
  if (name.includes("IN_PROGRESS")) return short || "In Progress";
  // pregame: date + time (no TZ)
  return `${fmtETDate(evt.date)} - ${fmtETTime(evt.date)}`;
}

function favoriteSideFromOdds(odds, awayId, homeId){
  const fav = String(odds?.favorite || odds?.favoriteTeamId || "");
  if (!fav) return null;
  if (String(awayId) === fav) return "away";
  if (String(homeId) === fav) return "home";
  return null;
}

function extractPregameLines(event){
  const comp = event.competitions?.[0]||{};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const o    = (comp.odds || event.odds || [])[0] || {};
  let aSpread="", hSpread="", total="";
  const spread = Number.isFinite(o.spread) ? o.spread : (typeof o.spread==="string" ? parseFloat(o.spread) : NaN);
  if (!Number.isNaN(spread)){
    const favSide = favoriteSideFromOdds(o, away?.team?.id, home?.team?.id);
    const s = Math.abs(spread).toFixed(Math.abs(spread)%1?1:0);
    if (favSide==="away"){ aSpread = `-${s}`; hSpread = `+${s}`; }
    else if (favSide==="home"){ hSpread = `-${s}`; aSpread = `+${s}`; }
  }
  if (o.details && !(aSpread||hSpread)){
    const m = o.details.match(/([+-]?\d+(?:\.\d+)?)/);
    if (m){
      const s = parseFloat(m[1]);
      const ss = Math.abs(s).toFixed(Math.abs(s)%1?1:0);
      aSpread = s>0 ? `+${ss}` : `${s}`;
      hSpread = s>0 ? `-${ss}` : `+${ss}`;
    }
  }
  total = String(o.overUnder ?? o.total ?? "");
  const aML = String(o?.awayTeamOdds?.moneyLine ?? o?.teamOdds?.find(t=>String(t?.teamId)===String(away?.team?.id))?.moneyLine ?? o?.moneyline?.away?.open?.odds ?? o?.moneyLineAway ?? "");
  const hML = String(o?.homeTeamOdds?.moneyLine ?? o?.teamOdds?.find(t=>String(t?.teamId)===String(home?.team?.id))?.moneyLine ?? o?.moneyline?.home?.open?.odds ?? o?.moneyLineHome ?? "");
  return { aSpread, hSpread, aML, hML, total };
}

function matchupText(event){
  const comp = event.competitions?.[0]||{};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const a = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
  const h = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
  return `${a} @ ${h}`;
}

function parseFinalScore(s){
  const m = String(s||"").match(/(-?\d+)\s*[-–]\s*(-?\d+)/);
  return m ? { a:Number(m[1]), h:Number(m[2]) } : null;
}

function makeUnderlineRuns(text, favSide){
  const len = text.length, at = text.indexOf("@");
  if (at<0) return [];
  const aStart=0, hStart=at+2;
  const runs=[{ startIndex:0, format:{} }];
  if (favSide==="away" && aStart<len) runs.push({ startIndex:aStart, format:{ underline:true }});
  if (favSide==="home" && hStart<len) runs.push({ startIndex:hStart, format:{ underline:true }});
  return runs;
}

function gradeRow(fromScore, aSpread, hSpread, aML, hML, total){
  const res = { aSpreadBg:null, hSpreadBg:null, aMLBg:null, hMLBg:null, totalBg:null };
  const sc = parseFinalScore(fromScore);
  if (!sc) return res;

  const aSp = parseFloat(aSpread); const hSp = parseFloat(hSpread);
  if (Number.isFinite(aSp)) res.aSpreadBg = ((sc.a - sc.h) + aSp) > 0 ? GREEN : RED;
  if (Number.isFinite(hSp)) res.hSpreadBg = ((sc.h - sc.a) + hSp) > 0 ? GREEN : RED;

  if (String(aML||"").trim()) res.aMLBg = sc.a > sc.h ? GREEN : RED;
  if (String(hML||"").trim()) res.hMLBg = sc.h > sc.a ? GREEN : RED;

  if (String(total||"").trim()){
    const t = parseFloat(total);
    if (Number.isFinite(t)){
      const sum = sc.a + sc.h;
      if (Math.abs(sum - t) < 0.001) res.totalBg = GREEN; // push marker; adjust if you want O/U logic instead
    }
  }
  return res;
}

class BatchValues {
  constructor(tab){ this.tab=tab; this.items=[]; }
  set(row, colIndex, value){
    const A = colLetter(colIndex); const r = `${this.tab}!${A}${row}:${A}${row}`;
    this.items.push({ range:r, values:[[ value ]]});
  }
  async flush(sheets){
    if (!this.items.length) return;
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId:SHEET_ID, requestBody:{ valueInputOption:"RAW", data:this.items }
    });
    this.items=[];
  }
}
async function underlineOrBold(sheets, sheetId, rowIndex0, colIndex, text, { underlineSide=null, boldSide=null }={}){
  if (!text) return;
  const at = text.indexOf("@"); const len=text.length;
  if (at<0) return;

  const aStart=0, hStart=at+2;
  const runs=[{ startIndex:0, format:{} }];
  if (underlineSide==="away" && aStart<len) runs.push({ startIndex:aStart, format:{ underline:true }});
  if (underlineSide==="home" && hStart<len) runs.push({ startIndex:hStart, format:{ underline:true }});
  if (boldSide==="away" && aStart<len) runs.push({ startIndex:aStart, format:{ bold:true }});
  if (boldSide==="home" && hStart<len) runs.push({ startIndex:hStart, format:{ bold:true }});

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{ requests:[{
      updateCells:{
        range:{ sheetId, startRowIndex:rowIndex0, endRowIndex:rowIndex0+1, startColumnIndex:colIndex, endColumnIndex:colIndex+1 },
        rows:[{ values:[{ userEnteredValue:{ stringValue:text }, textFormatRuns:runs }]}],
        fields:"userEnteredValue,textFormatRuns"
      }
    }]}
  });
}

/* =========================
   LIVE scraping helpers
   ========================= */
function parseLiveFromSectionText(txt){
  const clean = txt.replace(/\u00a0/g," ").replace(/[–—]/g,"-").replace(/\s+/g," ").trim();
  const overM  = clean.match(/\bo\s?(\d+(?:\.\d+)?)\b/i);
  const underM = clean.match(/\bu\s?(\d+(?:\.\d+)?)\b/i);
  const liveTotal = overM ? overM[1] : (underM ? underM[1] : "");

  let liveAwaySpread="", liveHomeSpread="";
  const spreadBlock = clean.split(/SPREAD/i)[1] || "";
  const sp = spreadBlock.match(/[+-]\d+(?:\.\d+)?/g) || [];
  if (sp.length>=2){ liveAwaySpread = sp[0]; liveHomeSpread = sp[1]; }

  let liveAwayML="", liveHomeML="";
  const mlBlock = clean.split(/\bML\b/i)[1] || "";
  const ml = (mlBlock.match(/(?:EVEN|[+-]\d{2,4})/g) || []).map(s=>s.toUpperCase()==="EVEN" ? "EVEN" : s);
  if (ml.length>=2){ liveAwayML = ml[0]; liveHomeML = ml[1]; }

  return { liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML };
}

async function scrapeLiveOnce(league, gameId){
  const { default: playwright } = await import("playwright");
  const browser = await playwright.chromium.launch({ headless:true });
  const page = await browser.newPage();
  try{
    await page.goto(gameUrl(league, gameId), { timeout:60000, waitUntil:"domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout:5000 }).catch(()=>{});
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout:8000 });
    const raw = (await section.innerText()).trim();
    if (DEBUG_ODDS){ console.log("SCRAPE DEBUG:", raw.slice(0,400)+"…"); }
    const fields = parseLiveFromSectionText(raw);

    // Try to derive a visible scoreboard number in case we need H Score
    let halfScore="";
    try{
      const body = (await page.locator("body").innerText()).replace(/\s+/g," ");
      const m = body.match(/(\d{1,2})\s*-\s*(\d{1,2})/);
      if (m) halfScore = `${m[1]}-${m[2]}`;
    }catch{}
    return { ...fields, halfScore };
  }catch(err){
    if (DEBUG_ODDS) console.warn("Playwright scrape failed:", err?.message || err);
    return null;
  }finally{
    await page.close(); await browser.close();
  }
}

/* =========================
   MAIN
   ========================= */
(async function main(){
  try{
    if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing secrets.");

    const CREDS = parseServiceAccount(CREDS_RAW);
    const auth  = new google.auth.GoogleAuth({
      credentials:{ client_email:CREDS.client_email, private_key:CREDS.private_key },
      scopes:["https://www.googleapis.com/auth/spreadsheets"]
    });
    const sheets = google.sheets({ version:"v4", auth });

    // ensure tab + header
    const meta = await sheets.spreadsheets.get({ spreadsheetId:SHEET_ID });
    let sheetId = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;
    if (!sheetId){
      const add = await sheets.spreadsheets.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ requests:[{ addSheet:{ properties:{ title:TAB_NAME }} }]}});
      sheetId = add.data.replies?.[0]?.addSheet?.properties?.sheetId;
    }
    const snap = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z` });
    const vals = snap.data.values || [];
    let header = vals[0] || [];
    if (header.length===0){
      await sheets.spreadsheets.values.update({
        spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1`,
        valueInputOption:"RAW", requestBody:{ values:[COLS] }
      });
      header = COLS.slice();
    }
    const h = mapHeadersToIndex(header);

    // index existing rows by ID
    const rows = vals.slice(1);
    const idToRow = new Map();
    rows.forEach((r,i)=>{ const id=String(r[h["game id"]]||"").trim(); if(id) idToRow.set(id, i+2); });

    // Which dates to pull
    const dates = [];
    if (TARGET_GAME_ID){
      const start = new Date(); for (let i=0;i<7;i++) dates.push(yyyymmddInET(new Date(start.getTime()+i*86400000)));
    }else if (RUN_SCOPE==="today"){
      dates.push(yyyymmddInET(new Date()));
    }else{
      const start = new Date(); for (let i=0;i<7;i++) dates.push(yyyymmddInET(new Date(start.getTime()+i*86400000)));
    }

    // Collect events
    let events=[];
    for (const d of dates){ const sb = await fetchJson(sbUrl(LEAGUE,d)); events=events.concat(sb?.events||[]); }
    const seen=new Set(); events=events.filter(e=>!seen.has(e.id)&&seen.add(e.id));
    if (TARGET_GAME_ID){
      const ids = new Set(TARGET_GAME_ID.split(",").map(s=>s.trim()));
      events = events.filter(e=>ids.has(String(e.id)));
    }
    log(`Events found: ${events.length}`);

    /* ─────────────────────
       1) PREFILL (append only; don’t touch “Status” once live)
       ───────────────────── */
    const append = [];
    for (const ev of events){
      const id = String(ev.id);
      if (idToRow.has(id)) continue;
      const comp = ev.competitions?.[0]||{};
      const matchup = matchupText(ev);
      const status  = tidyStatus(ev);
      const lines   = extractPregameLines(ev);
      append.push([
        id,
        fmtETDate(ev.date),
        (normLeague(LEAGUE)==="nfl" ? `Week ${comp.week?.number ?? ""}` : (comp.week?.text || "Week")),
        status,
        matchup,
        "",
        lines.aSpread || "",
        String(lines.aML || ""),
        lines.hSpread || "",
        String(lines.hML || ""),
        String(lines.total || ""),
        "", "", "", "", "", ""
      ]);
    }
    if (append.length){
      await sheets.spreadsheets.values.append({
        spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1`,
        valueInputOption:"RAW", requestBody:{ values:append }
      });
      // re-scan ids
      const snap2 = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z` });
      const vals2 = snap2.data.values || []; const rows2 = vals2.slice(1);
      rows2.forEach((r,i)=>{ const id=String(r[h["game id"]]||"").trim(); if(id && !idToRow.has(id)) idToRow.set(id, i+2); });
    }

    /* ─────────────────────
       2) Pregame underline favorite (safe)
       ───────────────────── */
    // pull a fresh copy for text writes
    const fresh = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z` });
    const allRows = (fresh.data.values||[]).slice(1);
    for (const ev of events){
      const comp = ev.competitions?.[0]||{};
      const statusName = String(comp.status?.type?.name || ev.status?.type?.name || "").toUpperCase();
      if (statusName.includes("FINAL")) continue;        // no underline changes after final

      const id = String(ev.id); const rowNum = idToRow.get(id); if (!rowNum) continue;
      const rowVals = allRows[rowNum-2] || [];
      const odds = (comp.odds || ev.odds || [])[0] || {};
      const favSide = favoriteSideFromOdds(odds, comp.competitors?.find(c=>c.homeAway==="away")?.team?.id, comp.competitors?.find(c=>c.homeAway==="home")?.team?.id);
      const text = String(rowVals[h["matchup"]]||"");
      if (favSide && text){
        await underlineOrBold(sheets, sheetId, rowNum-1, h["matchup"], text, { underlineSide:favSide });
      }
    }

    /* ─────────────────────
       3) LIVE pass (halftime columns; freeze after Q3 starts)
       ───────────────────── */
    for (const ev of events){
      const id = String(ev.id); const rowNum = idToRow.get(id); if (!rowNum) continue;
      const comp = ev.competitions?.[0]||{};
      const stType = comp.status?.type || ev.status?.type || {};
      const statusName = String(stType.name || "").toUpperCase();
      const short = (stType.shortDetail || stType.detail || stType.description || "").replace(/\s+EDT|EST/i,"").trim();

      // write running status during live (this script owns it now)
      const batch = new BatchValues(TAB_NAME);
      if (h["status"]!==undefined) batch.set(rowNum, h["status"], statusName.includes("FINAL") ? "Final" : short);

      // If Final, skip live columns here (finals handled in next step)
      if (statusName.includes("FINAL")){ await batch.flush(sheets); continue; }

      const period = Number(comp.status?.period ?? 0);
      if (period >= 3){
        // freeze: do not change half columns anymore
        await batch.flush(sheets);
        if (DEBUG_ODDS) console.log(`[freeze] Q${period} id=${id}`);
        continue;
      }

      // Pre-3rd: scrape once
      const live = await scrapeLiveOnce(LEAGUE, id);
      if (live){
        const writeIf = (name, v)=>{ const c=h[name]; if (c!==undefined && String(v||"").trim()) batch.set(rowNum,c,v); };
        // also set halftime score via summary if text scrape missed
        if (/HALF/i.test(statusName) && h["h score"]!==undefined){
          // try summary for reliable score
          try{
            const s = await fetchJson(sumUrl(LEAGUE, id));
            const compS = s?.header?.competitions?.[0]||{};
            const a = compS.competitors?.find(c=>c.homeAway==="away")?.score ?? "";
            const hsc = compS.competitors?.find(c=>c.homeAway==="home")?.score ?? "";
            if (a!=="" && hsc!=="") batch.set(rowNum, h["h score"], `${a}-${hsc}`);
            else if (live.halfScore) batch.set(rowNum, h["h score"], live.halfScore);
          }catch{ if (live.halfScore) batch.set(rowNum, h["h score"], live.halfScore); }
        }
        writeIf("half a spread", live.liveAwaySpread);
        writeIf("half h spread", live.liveHomeSpread);
        writeIf("half a ml", live.liveAwayML);
        writeIf("half h ml", live.liveHomeML);
        writeIf("half total", live.liveTotal);
      }
      await batch.flush(sheets);
    }

    /* ─────────────────────
       4) Finals & grading
       ───────────────────── */
    const after = await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z` });
    const curRows = (after.data.values||[]).slice(1);

    for (const ev of events){
      const comp = ev.competitions?.[0]||{};
      const statusName = String((comp.status?.type||{}).name || "").toUpperCase();
      if (!statusName.includes("FINAL")) continue;

      const id = String(ev.id); const rowNum = idToRow.get(id); if (!rowNum) continue;
      const aScore = comp.competitors?.find(c=>c.homeAway==="away")?.score ?? "";
      const hScore = comp.competitors?.find(c=>c.homeAway==="home")?.score ?? "";
      const finalScore = `${aScore}-${hScore}`;

      const batch = new BatchValues(TAB_NAME);
      if (h["final score"]!==undefined) batch.set(rowNum, h["final score"], finalScore);
      if (h["status"]!==undefined)      batch.set(rowNum, h["status"], "Final");
      await batch.flush(sheets);

      // bold winner
      const text = String((await sheets.spreadsheets.values.get({
        spreadsheetId:SHEET_ID, range:`${TAB_NAME}!${colLetter(h["matchup"])}${rowNum}:${colLetter(h["matchup"])}${rowNum}`
      })).data.values?.[0]?.[0] || "");
      const winnerSide = (Number(aScore)||0) > (Number(hScore)||0) ? "away" : "home";
      if (text) await underlineOrBold(sheets, sheetId, rowNum-1, h["matchup"], text, { boldSide:winnerSide });

      // grading paints
      const rowVals = curRows[rowNum-2] || [];
      const grade = gradeRow(
        finalScore,
        rowVals[h["a spread"]], rowVals[h["h spread"]],
        rowVals[h["a ml"]], rowVals[h["h ml"]],
        rowVals[h["total"]]
      );
      const paintReqs = [];
      const addPaint = (colName, color)=>{
        if (!color) return;
        const c = h[colName]; if (c===undefined) return;
        paintReqs.push({
          updateCells:{
            range:{ sheetId, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:c, endColumnIndex:c+1 },
            rows:[{ values:[{ userEnteredFormat:{ backgroundColor:color } }]}],
            fields:"userEnteredFormat.backgroundColor"
          }
        });
      };
      addPaint("a spread", grade.aSpreadBg);
      addPaint("h spread", grade.hSpreadBg);
      addPaint("a ml", grade.aMLBg);
      addPaint("h ml", grade.hMLBg);
      addPaint("total", grade.totalBg);
      if (paintReqs.length){
        await sheets.spreadsheets.batchUpdate({ spreadsheetId:SHEET_ID, requestBody:{ requests: paintReqs }});
      }
    }

    if (GHA_JSON) process.stdout.write(JSON.stringify({ ok:true })+"\n");
  }catch(err){
    if (GHA_JSON) process.stdout.write(JSON.stringify({ ok:false, error:String(err?.message||err) })+"\n");
    else { console.error("❌ Error:", err); process.exit(1); }
  }
})();
