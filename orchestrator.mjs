import { google } from "googleapis";
import * as playwright from "playwright";

/* ==== ENV / CONFIG ==== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim(); // JSON or base64(JSON)
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();       // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || "NFL").trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();  // "today" | "week"
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";

const HALF_EARLY_MIN  = Number(process.env.HALFTIME_EARLY_MIN ?? 60);
const HALF_LATE_MIN   = Number(process.env.HALFTIME_LATE_MIN  ?? 90);
const MIN_RECHECK_MIN = 2;
const MAX_RECHECK_MIN = 20;

const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/* ==== HELPERS ==== */
const log = (...a)=>console.log(...a);
const warn = (...a)=>console.warn(...a);

function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}

const ET_TZ = "America/New_York";

// yyyyMMdd in ET (for ESPN endpoints)
function yyyymmddInET(d=new Date()){
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(d));
  const g = k => p.find(x=>x.type===k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}

// ALWAYS return text "MM/DD/YY" for Sheets (leading apostrophe locks it as text)
function fmtETDate_MDY_2(d) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(d));
  const get = k => parts.find(p => p.type === k)?.value || "";
  const mm = get("month");
  const dd = get("day");
  const yy = (get("year") || "").slice(-2);
  return `'${mm}/${dd}/${yy}`;
}

const fmtETTime = d => new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,hour:"numeric",minute:"2-digit",hour12:true}).format(new Date(d));

async function fetchJson(url) {
  log("GET", url);
  const res = await fetch(url, { headers: { "User-Agent":"halftime-bot", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch ${res.status}: ${url}`);
  return res.json();
}

const normLeague = l => (l==="ncaaf"||l==="college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l,d) => {
  const lg = normLeague(l);
  const extra = lg==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const summaryUrl = (l,id) => `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/summary?event=${id}`;
const gameUrl    = (l,id) => `https://www.espn.com/${normLeague(l)}/game/_/gameId/${id}`;

function pickOdds(arr=[]){
  if (!Array.isArray(arr)||!arr.length) return null;
  const espn = arr.find(o=>/espn\s*bet/i.test(o.provider?.name||o.provider?.displayName||""));
  return espn || arr[0];
}
function mapHeadersToIndex(row){ const m={}; row.forEach((h,i)=>m[(h||"").trim().toLowerCase()]=i); return m; }
const keyOf = (dateStr, matchup) => `${(dateStr||"").trim()}__${(matchup||"").trim()}`;

function tidyStatus(ev){
  const c = ev.competitions?.[0]||{};
  const t = (ev.status?.type?.name || c.status?.type?.name || "").toUpperCase();
  const short = (ev.status?.type?.shortDetail || c.status?.type?.shortDetail || "").trim();
  if (t.includes("FINAL")) return "Final";
  if (t.includes("HALFTIME")) return "Half";
  if (t.includes("IN_PROGRESS")||t.includes("LIVE")) return short || "In Progress";
  return fmtETTime(ev.date);
}

/* Week labels */
function resolveWeekLabelFromCalendar(sb, dISO){
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(dISO).getTime();
  for (const it of cal){
    const ent = Array.isArray(it?.entries) ? it.entries : [it];
    for (const e of ent){
      const label = (e?.label || e?.detail || e?.text || "").trim();
      const s=e?.startDate||e?.start, e2=e?.endDate||e?.end; if(!s||!e2) continue;
      const S=new Date(s).getTime(), E=new Date(e2).getTime();
      if (t>=S && t<=E) return /Week\s*\d+/i.test(label) ? label : (label||"");
    }
  }
  return "";
}
const resolveWeekLabelNFL = (sb,dISO)=> Number.isFinite(sb?.week?.number) ? `Week ${sb.week.number}` : (resolveWeekLabelFromCalendar(sb,dISO)||"Regular Season");
const resolveWeekLabelCFB = (sb,dISO)=> (sb?.week?.text||"").trim() || (resolveWeekLabelFromCalendar(sb,dISO)||"Regular Season");

/* ML extraction */
function numOrBlank(v){
  if (v===0) return "0";
  if (v==null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g,""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}
function extractMoneylines(o, awayId, homeId, competitors=[]){
  let awayML="", homeML="";
  if (o && (o.awayTeamOdds||o.homeTeamOdds)){
    awayML = awayML || numOrBlank(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline);
    homeML = homeML || numOrBlank(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline);
    if (awayML||homeML) return {awayML,homeML};
  }
  if (o?.moneyline){
    const a=numOrBlank(o.moneyline.away?.close?.odds ?? o.moneyline.away?.open?.odds);
    const h=numOrBlank(o.moneyline.home?.close?.odds ?? o.moneyline.home?.open?.odds);
    awayML=awayML||a; homeML=homeML||h; if (awayML||homeML) return {awayML,homeML};
  }
  if (Array.isArray(o?.teamOdds)){
    for (const t of o.teamOdds){
      const tid=String(t?.teamId ?? t?.team?.id ?? "");
      const ml=numOrBlank(t?.moneyLine ?? t?.moneyline);
      if (!ml) continue;
      if (tid===String(awayId)) awayML=awayML||ml;
      if (tid===String(homeId)) homeML=homeML||ml;
    }
    if (awayML||homeML) return {awayML,homeML};
  }
  if (Array.isArray(o?.competitors)){
    const f = c => numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const a=f(o.competitors.find(c=>String(c?.id??c?.teamId)===String(awayId)));
    const h=f(o.competitors.find(c=>String(c?.id??c?.teamId)===String(homeId)));
    awayML=awayML||a; homeML=homeML||h; if (awayML||homeML) return {awayML,homeML};
  }
  awayML = awayML || numOrBlank(o?.awayMoneyLine);
  homeML = homeML || numOrBlank(o?.homeMoneyLine);
  return {awayML,homeML};
}
async function extractMLWithFallback(event, base, away, home){
  const baseML = extractMoneylines(base||{}, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
  if (baseML.awayML && baseML.homeML) return baseML;
  try{
    const s = await fetchJson(summaryUrl(LEAGUE, event.id));
    const arr = [];
    if (Array.isArray(s?.header?.competitions?.[0]?.odds)) arr.push(...s.header.competitions[0].odds);
    if (Array.isArray(s?.odds)) arr.push(...s.odds);
    if (Array.isArray(s?.pickcenter)) arr.push(...s.pickcenter);
    for (const c of arr){
      const out = extractMoneylines(c, away?.team?.id, home?.team?.id, (event.competitions?.[0]?.competitors)||[]);
      if (out.awayML || out.homeML) return out;
    }
  }catch(e){ warn("Summary ML fallback failed:", e?.message||e); }
  return baseML;
}
function favoriteSideFromOdds(o0, away, home){
  if (!o0) return "";
  const favId = String(o0.favorite || o0.favoriteTeamId || "");
  if (favId && String(away?.team?.id||"")===favId) return "away";
  if (favId && String(home?.team?.id||"")===favId) return "home";
  return "";
}

/* Pregame row */
function pregameRowFactory(sbForDay){
  return async function pregameRow(event){
    const comp = event.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const an = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const hn = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${an} @ ${hn}`;

    const isFinal = /final/i.test(event.status?.type?.name || comp.status?.type?.name || "");
    const finalScore = isFinal ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread="", homeSpread="", total="", awayML="", homeML="";
    if (o0){
      total = (o0.overUnder ?? o0.total) ?? "";
      const favId = String(o0.favorite || o0.favoriteTeamId || "");
      const spread = Number.isFinite(o0.spread) ? o0.spread :
                    (typeof o0.spread==="string" ? parseFloat(o0.spread) : NaN);
      if (!Number.isNaN(spread) && favId){
        if (String(away?.team?.id||"")===favId){ awaySpread=`-${Math.abs(spread)}`; homeSpread=`+${Math.abs(spread)}`; }
        else if (String(home?.team?.id||"")===favId){ homeSpread=`-${Math.abs(spread)}`; awaySpread=`+${Math.abs(spread)}`; }
      }
      const ml = await extractMLWithFallback(event, o0, away, home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
    } else {
      const ml = await extractMLWithFallback(event, {}, away, home);
      awayML=ml.awayML||""; homeML=ml.homeML||"";
    }

    const favSide = favoriteSideFromOdds(o0, away, home);
    const weekText = (normLeague(LEAGUE)==="nfl") ? resolveWeekLabelNFL(sbForDay, event.date)
                                                 : resolveWeekLabelCFB(sbForDay, event.date);
    const statusClean = tidyStatus(event);
    const dateET = fmtETDate_MDY_2(event.date);  // << MM/DD/YY (text)

    return {
      values: [dateET, weekText||"", statusClean, matchup, finalScore,
               awaySpread||"", String(awayML||""), homeSpread||"", String(homeML||""), String(total||""),
               "","","","","",""],
      dateET, matchup, awayName: an, homeName: hn, favSide
    };
  };
}

/* Halftime helpers */
const isFinal   = ev => /FINAL/i.test(ev.status?.type?.name || "");
const isHalftimeLike = ev => /HALF/i.test(ev.status?.type?.name || ev.status?.type?.shortDetail || "");
const minutesAfterKickoff = ev => (Date.now() - new Date(ev.date).getTime())/60000;
const clampRecheck = m => Math.max(MIN_RECHECK_MIN, Math.min(MAX_RECHECK_MIN, Math.ceil(m)));
function kickoff65CandidateMinutes(ev){
  const mins = minutesAfterKickoff(ev);
  if (mins < HALF_EARLY_MIN || mins > HALF_LATE_MIN) return null;
  const remaining = 65 - mins;
  return clampRecheck(remaining <= 0 ? MIN_RECHECK_MIN : remaining);
}
function q2AdaptiveCandidateMinutes(ev){
  const s = String(ev.status?.type?.shortDetail||"").toUpperCase();
  const m = s.match(/Q?2\D+(\d{1,2}):(\d{2})/);
  if (!m) return null;
  const left = (+m[1]) + (+m[2])/60;
  if (left >= 10) return null;
  return clampRecheck(2*left);
}

/* Robust LIVE ODDS scrape */
function pickSpread(nums){
  const candidates = nums.map(Number).filter(n=>Number.isFinite(n) && Math.abs(n) <= 60);
  const halves = candidates.filter(n => Math.abs(n*2 - Math.round(n*2)) > 1e-6);
  if (halves.length) return halves[0];
  return candidates[0] ?? "";
}
function pickMoneyline(nums){
  const candidates = nums.map(n=>parseInt(n,10)).filter(n=>Number.isFinite(n) && Math.abs(n) >= 100 && Math.abs(n) <= 10000);
  return candidates[0] ?? "";
}
async function scrapeLiveOddsOnce(league, gameId, awayName, homeName){
  const url = gameUrl(league, gameId);
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  try{
    await page.goto(url, { timeout: 60000, waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(()=>{});
    await page.waitForTimeout(300);

    const data = await page.evaluate(({awayName,homeName})=>{
      const ci = s => (s||"").toLowerCase();
      const an = ci(awayName), hn = ci(homeName);
      const normalize = t => (t||"").replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

      const containers = Array.from(document.querySelectorAll("section,div"))
        .filter(n => /live\s*odds/i.test(n.textContent||""));
      const live = containers[0];
      if (!live) return null;

      const all = Array.from(live.querySelectorAll("*"));
      const rowTxt = needle => {
        const el = all.find(n => ci(n.textContent||"").includes(needle));
        return el ? normalize(el.textContent||"") : "";
      };

      const awayRow = rowTxt(an);
      const homeRow = rowTxt(hn);
      if (!awayRow && !homeRow) return null;

      const signed = /[+\-]\d+(?:\.\d+)?/g;
      const aNums = (awayRow.match(signed)||[]);
      const hNums = (homeRow.match(signed)||[]);

      const awaySpread = pickSpread(aNums);
      const homeSpread = pickSpread(hNums);
      const awayML = pickMoneyline(aNums);
      const homeML = pickMoneyline(hNums);

      const textAll = normalize(live.textContent||"");
      let liveTotal = "";
      const mOver = textAll.match(/Over\s*(\d+(?:\.\d+)?)/i) || textAll.match(/\bO\s*(\d+(?:\.\d+)?)\b/i);
      const mUnder= textAll.match(/Under\s*(\d+(?:\.\d+)?)/i) || textAll.match(/\bU\s*(\d+(?:\.\d+)?)\b/i);
      if (mOver) liveTotal = mOver[1];
      else if (mUnder) liveTotal = mUnder[1];
      else {
        const decLast = textAll.match(/(\d+\.\d+)(?!.*\d)/);
        if (decLast) liveTotal = decLast[1];
      }

      return {
        liveAwaySpread: awaySpread ? String(awaySpread) : "",
        liveHomeSpread: homeSpread ? String(homeSpread) : "",
        liveAwayML:     awayML ? String(awayML) : "",
        liveHomeML:     homeML ? String(homeML) : "",
        liveTotal:      liveTotal || ""
      };
    }, {awayName, homeName});

    return data;
  }catch(e){
    warn("Live scrape failed:", e?.message||e, url);
    return null;
  }finally{
    await browser.close();
  }
}

/* Sheets helpers */
const colLetter = i => String.fromCharCode("A".charCodeAt(0)+i);
class BatchWriter{
  constructor(tab){ this.tab=tab; this.acc=[]; }
  add(row, colIdx, value){
    if (colIdx==null || colIdx<0) return;
    const range = `${this.tab}!${colLetter(colIdx)}${row}:${colLetter(colIdx)}${row}`;
    this.acc.push({ range, values: [[value]] });
  }
  async flush(sheets){
    if (!this.acc.length) return;
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: this.acc }
    });
    log(`🟩 Batched ${this.acc.length} cell update(s).`);
    this.acc=[];
  }
}

/* Formatting */
async function applyCenterFormatting(sheets){
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;
  if (sheetId==null) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [{
      repeatCell: {
        range: { sheetId, startRowIndex:0, startColumnIndex:0, endColumnIndex:16 },
        cell: { userEnteredFormat: { horizontalAlignment:"CENTER" }},
        fields: "userEnteredFormat.horizontalAlignment"
      }
    }/* Optional: lock Date column as TEXT
    ,{
      repeatCell: {
        range: { sheetId, startRowIndex:1, startColumnIndex:0, endColumnIndex:1 },
        cell: { userEnteredFormat: { numberFormat: { type: "TEXT" } } },
        fields: "userEnteredFormat.numberFormat"
      }
    }*/] }
  });
}
async function applyConditionalFormatting(sheets){
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME);
  if (!sheet) return;
  const sheetId = sheet.properties.sheetId;

  const GREEN = { red:0.80, green:1.00, blue:0.80 };
  const RED   = { red:1.00, green:0.80, blue:0.80 };
  const colRange = c => ({ sheetId, startRowIndex:1, startColumnIndex:c, endColumnIndex:c+1 });
  const req = (r,f,color)=>({ addConditionalFormatRule:{ index:0, rule:{ ranges:[r], booleanRule:{ condition:{type:"CUSTOM_FORMULA",values:[{userEnteredValue:f}]}, format:{ backgroundColor: color }}}}});

  const F=5,G=6,H=7,I=8,J=9; // 0-based
  const requests = [
    // Away ML (G)
    req(colRange(G), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) > VALUE(INDEX(SPLIT($E2,"-"),2)))', GREEN),
    req(colRange(G), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) < VALUE(INDEX(SPLIT($E2,"-"),2)))', RED),
    // Home ML (I)
    req(colRange(I), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),2)) > VALUE(INDEX(SPLIT($E2,"-"),1)))', GREEN),
    req(colRange(I), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),2)) < VALUE(INDEX(SPLIT($E2,"-"),1)))', RED),
    // Away Spread (F)
    req(colRange(F), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) - VALUE(INDEX(SPLIT($E2,"-"),2)) + VALUE($F2) > 0)', GREEN),
    req(colRange(F), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) - VALUE(INDEX(SPLIT($E2,"-"),2)) + VALUE($F2) < 0)', RED),
    // Home Spread (H)
    req(colRange(H), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2) > 0)', GREEN),
    req(colRange(H), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),2)) - VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE($H2) < 0)', RED),
    // Total (J)
    req(colRange(J), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE(INDEX(SPLIT($E2,"-"),2)) > VALUE($J2))', GREEN),
    req(colRange(J), '=AND($E2<>"", VALUE(INDEX(SPLIT($E2,"-"),1)) + VALUE(INDEX(SPLIT($E2,"-"),2)) < VALUE($J2))', RED),
  ];

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests }
  });
  log(`🎨 Applied conditional formatting on ${TAB_NAME}.`);
}

/* Rich text for Matchup (bold winner, underline favorite) */
async function applyMatchupRichText(sheets, sheetId, rowNum, awayName, homeName, favSide, winnerSide){
  const spacer=" @ "; const text=`${awayName}${spacer}${homeName}`;
  const awayStart=0; const homeStart=awayStart+awayName.length+spacer.length;
  const fmtAway={}; const fmtHome={};
  if (favSide==="away") fmtAway.underline=true;
  if (favSide==="home") fmtHome.underline=true;
  if (winnerSide==="away") fmtAway.bold=true;
  if (winnerSide==="home") fmtHome.bold=true;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [{
      updateCells: {
        range: { sheetId, startRowIndex:rowNum-1, endRowIndex:rowNum, startColumnIndex:3, endColumnIndex:4 },
        rows: [{ values: [{ userEnteredValue:{stringValue:text}, textFormatRuns:[
          { startIndex: 0, format: fmtAway },
          { startIndex: homeStart, format: fmtHome },
        ] }]}],
        fields: "userEnteredValue,textFormatRuns"
      }
    }]}
  });
}

/* ==== MAIN ==== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){ console.error("Missing secrets."); process.exit(1); }
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version:"v4", auth });

  // ensure tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabObj = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME);
  let sheetId;
  if (!tabObj){
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID, requestBody: { requests:[{ addSheet:{ properties:{ title:TAB_NAME }}}]}
    });
    sheetId = add.data?.replies?.[0]?.addSheet?.properties?.sheetId;
  } else sheetId = tabObj.properties.sheetId;

  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (!header.length){
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`, valueInputOption:"RAW", requestBody:{ values:[COLS] }
    });
    header = COLS.slice();
  }
  const hmap = mapHeadersToIndex(header);
  const rows = values.slice(1);
  const keyToRowNum = new Map();
  rows.forEach((r,i)=> keyToRowNum.set(keyOf(r[hmap["date"]], r[hmap["matchup"]]), i+2));

  await applyCenterFormatting(sheets);
  await applyConditionalFormatting(sheets);

  // dates to pull
  const BACK_DAYS = Number(process.env.WEEK_BACK_DAYS ?? 0);
  const FWD_DAYS  = Number(process.env.WEEK_FWD_DAYS  ?? 0);
  const MS_DAY = 86400000;
  const datesList = RUN_SCOPE==="week"
    ? (()=>{ const start=new Date(Date.now()-BACK_DAYS*MS_DAY);
             return Array.from({length:BACK_DAYS+FWD_DAYS+1},(_,i)=>yyyymmddInET(new Date(start.getTime()+i*MS_DAY))); })()
    : [ yyyymmddInET(new Date()) ];

  let firstDaySB=null; let events=[];
  for (const d of datesList){
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!firstDaySB) firstDaySB=sb;
    events = events.concat(sb?.events||[]);
  }
  const seen=new Set(); events = events.filter(e=>!seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  const buildPregame = pregameRowFactory(firstDaySB);
  let appendBatch=[]; const preMeta=new Map();

  for (const ev of events){
    const it = await buildPregame(ev);
    const { values:rowVals, dateET, matchup, awayName, homeName, favSide } = it;
    const k = keyOf(dateET, matchup);
    preMeta.set(k,{awayName,homeName,favSide});
    if (!keyToRowNum.has(k)) appendBatch.push(rowVals);
  }
  if (appendBatch.length){
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`, valueInputOption:"RAW", requestBody:{ values: appendBatch }
    });
    const re = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
    const v2 = re.data.values || []; const hdr2=v2[0]||header; const h2=mapHeadersToIndex(hdr2);
    v2.slice(1).forEach((r,i)=> keyToRowNum.set(keyOf(r[h2["date"]], r[h2["matchup"]]), i+2));
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // underline favorites on all rows
  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const an = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const hn = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${an} @ ${hn}`;
    const rowNum = keyToRowNum.get(keyOf(fmtETDate_MDY_2(ev.date), matchup)); // << use MM/DD/YY key
    if (!rowNum) continue;
    const o0 = pickOdds((ev.competitions?.[0]?.odds) || ev.odds || []);
    const favSide = favoriteSideFromOdds(o0, away, home) || preMeta.get(keyOf(fmtETDate_MDY_2(ev.date), matchup))?.favSide || "";
    await applyMatchupRichText(sheets, sheetId, rowNum, an, hn, favSide, "");
  }

  const batch = new BatchWriter(TAB_NAME);
  let masterDelayMin = null;

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const an = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const hn = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${an} @ ${hn}`;
    const dateET = fmtETDate_MDY_2(ev.date);
    const rowNum = keyToRowNum.get(keyOf(dateET, matchup));
    if (!rowNum) continue;

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();

    // finals
    if (statusName.includes("FINAL")){
      const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;
      if (hmap["final score"]!==undefined) batch.add(rowNum, hmap["final score"], scorePair);
      if (hmap["status"]!==undefined)      batch.add(rowNum, hmap["status"], "Final");

      const o0 = pickOdds(comp.odds || ev.odds || []);
      const favSide = favoriteSideFromOdds(o0, away, home) || preMeta.get(keyOf(dateET, matchup))?.favSide || "";
      const a = Number(away?.score ?? 0), h = Number(home?.score ?? 0);
      const winnerSide = a>h ? "away" : (h>a ? "home" : "");
      await applyMatchupRichText(sheets, sheetId, rowNum, an, hn, favSide, winnerSide);
      continue;
    }

    // pre-half status updates (don’t overwrite Half/Final)
    const currentRow = values[rowNum-1] || [];
    const curStatus = (currentRow[hmap["status"]] || "").toString().trim();
    if (!/^half$/i.test(curStatus) && !/^final$/i.test(curStatus)) {
      const statusNow = tidyStatus(ev);
      if (statusNow && statusNow !== curStatus) batch.add(rowNum, hmap["status"], statusNow);
    }

    // halftime write (lock)
    if (isHalftimeLike(ev)){
      const halfScore = `${away?.score ?? ""}-${home?.score ?? ""}`;
      const live = await scrapeLiveOddsOnce(LEAGUE, ev.id, an, hn);

      const payload=[];
      const add = (name,val)=>{
        const idx = hmap[name]; if (idx===undefined || !val) return;
        const range = `${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
        payload.push({ range, values: [[val]] });
      };
      add("status","Half");
      add("half score", halfScore);
      if (live){
        add("live away spread", live.liveAwaySpread);
        add("live home spread",  live.liveHomeSpread);
        add("live away ml",      live.liveAwayML);
        add("live home ml",      live.liveHomeML);
        add("live total",        live.liveTotal);
      }
      if (payload.length){
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID, requestBody:{ valueInputOption:"RAW", data: payload }
        });
      }
      log(`🕐 Halftime LIVE written for ${matchup}`);
      continue;
    }

    // adaptive scheduling
    if (ADAPTIVE_HALFTIME){
      const k = kickoff65CandidateMinutes(ev);
      if (k!=null) masterDelayMin = (masterDelayMin==null)? k : Math.min(masterDelayMin, k);
      const q = q2AdaptiveCandidateMinutes(ev);
      if (q!=null) masterDelayMin = (masterDelayMin==null)? q : Math.min(masterDelayMin, q);
    }
  }

  await batch.flush(sheets);

  // optional second pass
  if (ADAPTIVE_HALFTIME && masterDelayMin!=null){
    log(`⏳ Adaptive master wait: ${masterDelayMin} minute(s).`);
    await new Promise(r=>setTimeout(r, Math.ceil(masterDelayMin*60*1000)));

    let events2=[]; const seen2=new Set();
    for (const d of datesList){
      const sb2 = await fetchJson(scoreboardUrl(LEAGUE, d));
      for (const e of (sb2?.events||[])){ if (!seen2.has(e.id)){ seen2.add(e.id); events2.push(e);} }
    }

    for (const ev of events2){
      const comp = ev.competitions?.[0] || {};
      const away = comp.competitors?.find(c=>c.homeAway==="away");
      const home = comp.competitors?.find(c=>c.homeAway==="home");
      const an = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
      const hn = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
      const matchup = `${an} @ ${hn}`;
      const rowNum = keyToRowNum.get(keyOf(fmtETDate_MDY_2(ev.date), matchup));
      if (!rowNum) continue;

      if (isHalftimeLike(ev)){
        const halfScore = `${away?.score ?? ""}-${home?.score ?? ""}`;
        const live = await scrapeLiveOddsOnce(LEAGUE, ev.id, an, hn);

        const payload=[];
        const add=(name,val)=>{ const idx=hmap[name]; if (idx===undefined||!val) return;
          const range=`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`;
          payload.push({range, values:[[val]]}); };
        add("status","Half");
        add("half score", halfScore);
        if (live){
          add("live away spread", live.liveAwaySpread);
          add("live home spread",  live.liveHomeSpread);
          add("live away ml",      live.liveAwayML);
          add("live home ml",      live.liveHomeML);
          add("live total",        live.liveTotal);
        }
        if (payload.length){
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SHEET_ID, requestBody:{ valueInputOption:"RAW", data: payload }
          });
        }
        log(`🕐 (Adaptive) Halftime LIVE written for ${matchup}`);
      }
    }
  }

  log("✅ Run complete.");
})().catch(e=>{ console.error("❌ Error:", e); process.exit(1); });
