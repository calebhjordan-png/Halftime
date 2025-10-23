// Football.mjs
// Prefill (A–K) + Finals & grading (A–K only) + Live odds (L–Q)

import { google } from "googleapis";
import axios from "axios";

/* ============ ENV ============ */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN  = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" | "college-football"
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE_IN==="college-football"?"CFB":"NFL")).trim();

// MODE: "all" (default), "prefill", "finals", "live"
const RUN_MODE   = (process.env.RUN_MODE || "all").toLowerCase();

// Scope for event fetch: "week" (default) or "today"
const RUN_SCOPE  = (process.env.RUN_SCOPE || "week").toLowerCase();

// Optional: comma-separated game ids to force process even if outside scope
const GAME_IDS   = (process.env.GAME_IDS || "").trim();

const ET = "America/New_York";

/* ============ CONSTANTS / HEADERS ============ */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

const HIDX = Object.fromEntries(HEADERS.map((h,i)=>[h.toLowerCase(), i]));
const A1 = (cIdx,row)=>`${String.fromCharCode(65+cIdx)}${row}`;

/* ============ UTILS ============ */
const fmt = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:ET,...opt}).format(new Date(d));
const yyyymmddET = (d=new Date())=>{
  const p = fmt(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return p[2]+p[0]+p[1];
};
const dayList = (n=7)=>Array.from({length:n},(_,i)=>yyyymmddET(new Date(Date.now()+i*86400000)));

const isFinalLike = (evt)=>/(FINAL)/i.test(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "");
const isLiveLike  = (evt)=>{
  const n = (evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  if (/(FINAL)/.test(n)) return false;
  return /(IN|LIVE|HALF)/.test(n);
};

const normLg = (x)=> (x==="college-football"||x==="ncaaf") ? "college-football" : "nfl";
const SB_URL  = (lg,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const SUM_URL = (lg,id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;

async function fetchJSON(url){
  const r = await axios.get(url,{headers:{"User-Agent":"football-bot"}, timeout: 15000});
  return r.data;
}
const cleanNum = (v) => {
  if (v==null || v==="") return "";
  const n = Number(String(v).replace(/[^\d.+-]/g,""));
  return Number.isFinite(n) ? (String(v).trim().startsWith("+")?`+${n}`:`${n}`) : "";
};
const asSigned = (n)=> (n>0?`+${n}`:`${n}`);

/* ============ GOOGLE SHEETS WRAPPER ============ */
class Sheets {
  constructor(auth, spreadsheetId, tab) {
    this.api = google.sheets({version:"v4", auth});
    this.id = spreadsheetId;
    this.tab = tab;
  }
  async ensureHeader() {
    const r = await this.api.spreadsheets.values.get({spreadsheetId:this.id, range:`${this.tab}!A1:Q1`});
    const row = r.data.values?.[0] || [];
    if (row.length < HEADERS.length || HEADERS.some((h,i)=>row[i]!==h)) {
      await this.api.spreadsheets.values.update({
        spreadsheetId:this.id,
        range:`${this.tab}!A1`,
        valueInputOption:"RAW",
        requestBody:{ values:[HEADERS] }
      });
    }
  }
  async readAll() {
    const r = await this.api.spreadsheets.values.get({spreadsheetId:this.id, range:`${this.tab}!A1:Q`});
    return r.data.values || [];
  }
  async batchUpdate(data) {
    if (!data.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId:this.id,
      requestBody:{ valueInputOption:"RAW", data }
    });
  }
  async writeCell(row, colIdx, value) {
    await this.batchUpdate([{ range:`${this.tab}!${A1(colIdx,row)}:${A1(colIdx,row)}`, values:[[value]] }]);
  }
  async formatTextRuns(sheetId, rowsPayload) {
    if (!rowsPayload.length) return;
    await this.api.spreadsheets.batchUpdate({
      spreadsheetId:this.id,
      requestBody:{ requests: rowsPayload.map(p=>({updateCells:p})) }
    });
  }
  async resetAndApplyCF(sheetId, requests) {
    // get current rules and remove those targeting G..K to avoid stackup
    const meta = await this.api.spreadsheets.get({spreadsheetId:this.id, includeGridData:false});
    const sheet = meta.data.sheets?.find(s=>s.properties?.sheetId===sheetId);
    const condRules = (sheet?.conditionalFormats || sheet?.conditionalFormats?.rules) ? [] : []; // API v4 doesn't return rules directly; we just add our rules.
    if (requests.length) {
      await this.api.spreadsheets.batchUpdate({
        spreadsheetId:this.id,
        requestBody:{ requests }
      });
    }
  }
}

/* ============ PREFILL (A–K) ============ */
async function doPrefill({sheets, values, league, events}) {
  const header = values[0] || HEADERS;
  const rows   = values.slice(1);
  const map    = Object.fromEntries(header.map((h,i)=>[h.toLowerCase(), i]));
  const rowById = new Map();
  rows.forEach((r,i)=>{
    const id = (r[map["game id"]]||"").toString().trim();
    if (id) rowById.set(id, i+2);
  });

  const adds = [];
  for (const ev of events) {
    const comp  = ev.competitions?.[0] || {};
    const away  = comp.competitors?.find(c=>c.homeAway==="away");
    const home  = comp.competitors?.find(c=>c.homeAway==="home");
    if (!away || !home) continue;

    const gameId = String(ev.id);
    if (rowById.has(gameId)) continue; // do not overwrite existing prefill

    const dateET = fmt(ev.date,{month:"2-digit",day:"2-digit",year:"numeric"});
    const weekTx = ev.week?.text || (ev.season?.type?.name?.includes("Week")?ev.season?.type?.name: (ev.week?.number ? `Week ${ev.week.number}` : "Week"));
    const status = fmt(ev.date,{month:"2-digit",day:"2-digit"})+" - "+fmt(ev.date,{hour:"numeric",minute:"2-digit",hour12:true});
    const awayName = away.team?.shortDisplayName || away.team?.abbreviation || away.team?.name || "Away";
    const homeName = home.team?.shortDisplayName || home.team?.abbreviation || home.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    // Odds
    let aSpread="", hSpread="", total="", aML="", hML="";
    const odds = (comp.odds || ev.odds || [])[0];
    if (odds) {
      total = cleanNum(odds.overUnder ?? odds.total);
      const favId = String(odds.favoriteTeamId ?? odds.favorite ?? "");
      const spr = Number(odds.spread);
      if (Number.isFinite(spr) && favId) {
        if (String(away.team?.id)===favId) { aSpread = asSigned(-Math.abs(spr)); hSpread = asSigned(+Math.abs(spr)); }
        else { hSpread = asSigned(-Math.abs(spr)); aSpread = asSigned(+Math.abs(spr)); }
      }
      const a = odds.awayTeamOdds || {};
      const h = odds.homeTeamOdds || {};
      aML = cleanNum(a.moneyLine ?? a.moneyline);
      hML = cleanNum(h.moneyLine ?? h.moneyline);
    }

    adds.push([
      gameId, dateET, weekTx, status, matchup, "", // A..F
      aSpread, aML, hSpread, hML, total,           // G..K
      "", "", "", "", "", ""                       // L..Q live-only
    ]);
  }

  if (adds.length) {
    await sheets.batchUpdate([{ range:`${sheets.tab}!A1`, values: adds }]);
  }
}

/* ============ FINALS + GRADING (A–K only) ============ */
function buildRunsForMatchup(text, boldStart, boldEnd, underlineStart, underlineEnd){
  const L = text.length;
  const points = new Set([0, boldStart, boldEnd, underlineStart, underlineEnd].filter(n=>Number.isFinite(n)&&n>=0&&n<L));
  const sorted = Array.from(points).sort((a,b)=>a-b);
  const runs = [];
  for (let i=0;i<sorted.length;i++){
    const idx = sorted[i];
    const prev = runs[runs.length-1];
    const fmt = {
      bold: (idx>=boldStart && idx<boldEnd) || (prev?.format?.bold && !(idx>=boldStart && idx<boldEnd)) ? (idx>=boldStart && idx<boldEnd) : false,
      underline: (idx>=underlineStart && idx<underlineEnd) || (prev?.format?.underline && !(idx>=underlineStart && idx<underlineEnd)) ? (idx>=underlineStart && idx<underlineEnd) : false,
    };
    runs.push({ startIndex: idx, format: fmt });
  }
  if (!runs.length || runs[0].startIndex!==0) runs.unshift({startIndex:0, format:{bold:false, underline:false}});
  return runs;
}

async function doFinals({sheets, sheetId, values, league, events}) {
  const header = values[0] || HEADERS;
  const rows   = values.slice(1);
  const map    = Object.fromEntries(header.map((h,i)=>[h.toLowerCase(), i]));
  const rowById = new Map();
  rows.forEach((r,i)=>{
    const id = (r[map["game id"]]||"").toString().trim();
    if (id) rowById.set(id, i+2);
  });

  const updates = [];
  const textRunPayloads = [];

  for (const ev of events) {
    if (!isFinalLike(ev)) continue;

    const comp  = ev.competitions?.[0] || {};
    const away  = comp.competitors?.find(c=>c.homeAway==="away");
    const home  = comp.competitors?.find(c=>c.homeAway==="home");
    if (!away || !home) continue;

    const gameId = String(ev.id);
    const row = rowById.get(gameId);
    if (!row) continue;

    const finalScore = `${away.score ?? ""}-${home.score ?? ""}`;
    updates.push({ range:`${sheets.tab}!${A1(HIDX["final score"],row)}:${A1(HIDX["final score"],row)}`, values:[[finalScore]] });
    updates.push({ range:`${sheets.tab}!${A1(HIDX["status"],row)}:${A1(HIDX["status"],row)}`, values:[["Final"]] });

    // Bold winner, underline pregame favorite (based on prefill spreads).
    const awayName = (away.team?.shortDisplayName || away.team?.abbreviation || "Away");
    const homeName = (home.team?.shortDisplayName || home.team?.abbreviation || "Home");
    const matchupTxt = `${awayName} @ ${homeName}`;
    updates.push({ range:`${sheets.tab}!${A1(HIDX["matchup"],row)}:${A1(HIDX["matchup"],row)}`, values:[[matchupTxt]] });

    const aScore = Number(away.score||0), hScore = Number(home.score||0);
    const boldStart = aScore>hScore ? 0 : (awayName.length+3);
    const boldEnd   = aScore>hScore ? awayName.length : (awayName.length+3+homeName.length);

    const r = rows[row-2] || [];
    const aSp = Number((r[HIDX["a spread"]]||"").toString());
    const hSp = Number((r[HIDX["h spread"]]||"").toString());
    let underlineStart = 0, underlineEnd = 0;
    if (Number.isFinite(aSp) && aSp<0) { underlineStart=0; underlineEnd=awayName.length; }
    else if (Number.isFinite(hSp) && hSp<0) { underlineStart=awayName.length+3; underlineEnd=awayName.length+3+homeName.length; }
    else { underlineStart = underlineEnd = 0; } // no underline

    const runs = buildRunsForMatchup(matchupTxt, boldStart, boldEnd, underlineStart, underlineEnd);

    textRunPayloads.push({
      range:{ sheetId, startRowIndex:row-1, endRowIndex:row, startColumnIndex:HIDX["matchup"], endColumnIndex:HIDX["matchup"]+1 },
      rows:[{ values:[{ userEnteredValue:{ stringValue: matchupTxt }, textFormatRuns: runs }]}],
      fields:"userEnteredValue,textFormatRuns"
    });
  }

  if (updates.length) await sheets.batchUpdate(updates);
  if (textRunPayloads.length) await sheets.formatTextRuns(sheetId, textRunPayloads);

  // CONDITIONAL FORMATTING — only G..K (A..F untouched)
  // We'll apply green/red for G (A Spread), H (A ML), I (H Spread), J (H ML), K (Total)
  // Use ranges from row 2 to row 2000 for safety.
  const rowStart = 2, rowEnd = 2000;
  const colRange = (idx)=>({sheetId, startRowIndex:rowStart-1, endRowIndex:rowEnd, startColumnIndex:idx, endColumnIndex:idx+1});
  const green = { backgroundColor: { red:0.82, green:0.97, blue:0.85 } };
  const red   = { backgroundColor: { red:0.98, green:0.84, blue:0.84 } };

  const awayScore = (row)=>`VALUE(LEFT($F${row},FIND("-", $F${row})-1))`;
  const homeScore = (row)=>`VALUE(RIGHT($F${row}, LEN($F${row})-FIND("-", $F${row})))`;
  const marginAway = (row)=>`${awayScore(row)} - ${homeScore(row)}`;
  const marginHome = (row)=>`${homeScore(row)} - ${awayScore(row)}`;
  const sumPoints  = (row)=>`${awayScore(row)} + ${homeScore(row)}`;

  // For CF custom formulas: refer to the top-left row (rowStart). Google applies them per-row.
  const R = rowStart;

  const requests = [
    // G (A Spread) green if marginAway + G > 0 ; red if < 0
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["a spread"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$G${R}<>"", (${marginAway(R)} + VALUE($G${R})) > 0)`}]}, format: green }
        }, index:0 }
    },
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["a spread"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$G${R}<>"", (${marginAway(R)} + VALUE($G${R})) < 0)`}]}, format: red }
        }, index:0 }
    },

    // H (A ML) green if awayScore > homeScore ; red if <
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["a ml"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$H${R}<>"", ${awayScore(R)} > ${homeScore(R)})`}]}, format: green }
        }, index:0 }
    },
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["a ml"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$H${R}<>"", ${awayScore(R)} < ${homeScore(R)})`}]}, format: red }
        }, index:0 }
    },

    // I (H Spread) green if marginHome + I > 0 ; red if < 0
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["h spread"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$I${R}<>"", (${marginHome(R)} + VALUE($I${R})) > 0)`}]}, format: green }
        }, index:0 }
    },
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["h spread"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$I${R}<>"", (${marginHome(R)} + VALUE($I${R})) < 0)`}]}, format: red }
        }, index:0 }
    },

    // J (H ML) green if homeScore > awayScore ; red if <
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["h ml"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$J${R}<>"", ${homeScore(R)} > ${awayScore(R)})`}]}, format: green }
        }, index:0 }
    },
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["h ml"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$J${R}<>"", ${homeScore(R)} < ${awayScore(R)})`}]}, format: red }
        }, index:0 }
    },

    // K (Total) green if sumPoints > K ; red if sumPoints < K
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["total"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$K${R}<>"", (${sumPoints(R)}) > VALUE($K${R}))`}]}, format: green }
        }, index:0 }
    },
    {
      addConditionalFormatRule:{
        rule:{ ranges:[colRange(HIDX["total"])],
          booleanRule:{ condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${R}<>"",$K${R}<>"", (${sumPoints(R)}) < VALUE($K${R}))`}]}, format: red }
        }, index:0 }
    },
  ];

  await sheets.resetAndApplyCF(sheetId, requests);
}

/* ============ LIVE (L–Q only) ============ */
async function doLive({sheets, values, league, events}) {
  const header = values[0] || HEADERS;
  const rows   = values.slice(1);
  const map    = Object.fromEntries(header.map((h,i)=>[h.toLowerCase(), i]));
  const rowById = new Map();
  rows.forEach((r,i)=>{
    const id = (r[map["game id"]]||"").toString().trim();
    if (id) rowById.set(id, i+2);
  });

  const liveUpdates = [];

  for (const ev of events) {
    if (!isLiveLike(ev)) continue;
    const row = rowById.get(String(ev.id));
    if (!row) continue;

    const s = await fetchJSON(SUM_URL(league, ev.id)).catch(()=>null);
    if (!s) continue;

    const box = s?.boxscore;
    const away = box?.teams?.find(t=>t.homeAway==="away");
    const home = box?.teams?.find(t=>t.homeAway==="home");
    const aScore = away?.score, hScore = home?.score;
    const hScoreTxt = `${aScore??""}-${hScore??""}`;

    const o = s?.header?.competitions?.[0]?.odds?.[0] || {};
    const aO = o.awayTeamOdds || {};
    const hO = o.homeTeamOdds || {};
    const payload = {
      "h score": hScoreTxt,
      "h a spread": cleanNum(aO.spread),
      "h a ml": cleanNum(aO.moneyLine ?? aO.moneyline),
      "h h spread": cleanNum(hO.spread),
      "h h ml": cleanNum(hO.moneyLine ?? hO.moneyline),
      "h total": cleanNum(o.overUnder ?? o.total),
    };

    for (const [key,val] of Object.entries(payload)) {
      if (val==="" || val==null) continue;
      const c = map[key];
      liveUpdates.push({
        range:`${sheets.tab}!${A1(c,row)}:${A1(c,row)}`,
        values:[[String(val)]]
      });
    }
  }

  if (liveUpdates.length) await sheets.batchUpdate(liveUpdates);
}

/* ============ MAIN ============ */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
    process.exit(1);
  }
  const CREDS = CREDS_RAW.trim().startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials:{client_email:CREDS.client_email, private_key:CREDS.private_key},
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);

  await sheets.ensureHeader();

  const league = normLg(LEAGUE_IN);

  // Pull events
  let events = [];
  if (GAME_IDS) {
    const ids = GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean);
    for (const id of ids) {
      const s = await fetchJSON(SUM_URL(league,id)).catch(()=>null);
      if (s?.header?.competitions?.[0]) {
        const e = s.header.competitions[0];
        e.id = id;
        events.push({ id, competitions:[e], status:e.status, date:e.date, week:s?.week });
      }
    }
  } else {
    const days = RUN_SCOPE==="today" ? [yyyymmddET(new Date())] : dayList(7);
    const seen = new Set();
    for (const d of days) {
      const sb = await fetchJSON(SB_URL(league,d)).catch(()=>null);
      for (const e of (sb?.events||[])) {
        if (!seen.has(e.id)) { seen.add(e.id); events.push(e); }
      }
    }
  }

  const values = await sheets.readAll();
  const meta   = await sheets.api.spreadsheets.get({spreadsheetId:SHEET_ID});
  const sheetId = meta.data.sheets?.find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;

  if (RUN_MODE==="all" || RUN_MODE==="prefill") {
    await doPrefill({sheets, values, league, events});
  }

  if (RUN_MODE==="all" || RUN_MODE==="finals") {
    await doFinals({sheets, sheetId, values: await sheets.readAll(), league, events});
  }

  if (RUN_MODE==="all" || RUN_MODE==="live") {
    await doLive({sheets, values: await sheets.readAll(), league, events});
  }

  console.log("✅ Football.mjs complete.", {mode:RUN_MODE, league, tab:TAB_NAME});
})().catch(err=>{
  console.error("Fatal:", err?.message || err);
  process.exit(1);
});
