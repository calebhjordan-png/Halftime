// Football.mjs
// Prefill (A–K) + Finals & grading (A–K) + Live odds (L–Q)

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

// Adaptive halftime probe (kept but harmless if unused)
const ADAPTIVE_HALFTIME = String(process.env.ADAPTIVE_HALFTIME ?? "1") !== "0";

const ET = "America/New_York";

/* ============ CONSTANTS / HEADERS ============ */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

// column index map helpers
const HIDX = Object.fromEntries(HEADERS.map((h,i)=>[h.toLowerCase(), i]));
const A = (i)=>String.fromCharCode(65+i); // A..Z (Q is 16)

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

// ESPN endpoints
const normLg = (x)=>x==="college-football"||x==="ncaaf"?"college-football":"nfl";
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
  async batchUpdate(cells) {
    if (!cells.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId:this.id,
      requestBody:{ valueInputOption:"RAW", data: cells }
    });
  }
  async formatTextRuns(sheetId, requests) {
    if (!requests.length) return;
    await this.api.spreadsheets.batchUpdate({
      spreadsheetId:this.id,
      requestBody:{ requests: requests.map(r=>({updateCells:r})) }
    });
  }
  async conditionalFormat(sheetId, requests) {
    if (!requests.length) return;
    await this.api.spreadsheets.batchUpdate({
      spreadsheetId:this.id,
      requestBody:{ requests }
    });
  }
}

/* ============ PREFILL PASS (locked) ============ */
/* Writes pregame A–K; does NOT overwrite once the game is live. */
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
    const dateET = fmt(ev.date,{month:"2-digit",day:"2-digit",year:"numeric"});
    const weekTx = ev.week?.text || (ev.season?.type?.name?.includes("Week")?ev.season?.type?.name:"Week " + (ev.week?.number ?? ""));
    const status = fmt(ev.date,{month:"2-digit",day:"2-digit"})+" - "+fmt(ev.date,{hour:"numeric",minute:"2-digit",hour12:true});
    const matchup = `${away.team?.shortDisplayName || away.team?.abbreviation} @ ${home.team?.shortDisplayName || home.team?.abbreviation}`;

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

    // Skip overwrite if row exists and game has started (protect your “locked” behavior)
    const existingRow = rowById.get(gameId);
    if (existingRow) continue;

    adds.push([
      gameId, dateET, weekTx, status, matchup, "", // A..F
      aSpread, aML, hSpread, hML, total,           // G..K
      "", "", "", "", "", ""                       // L..Q reserved for Live
    ]);
  }

  if (adds.length) {
    await sheets.batchUpdate([{ range:`${sheets.tab}!A1`, values: adds }]);
  }
}

/* ============ FINALS PASS (locked) ============ */
/* Writes Final Score into F and applies grading (G–K background only). */
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

    // Write Final score and "Final" status
    updates.push({ range:`${sheets.tab}!${A(map["final score"])}${row}:${A(map["final score"])}${row}`, values:[[finalScore]] });
    updates.push({ range:`${sheets.tab}!${A(map["status"])}${row}:${A(map["status"])}${row}`, values:[["Final"]] });

    // Bold winner in matchup
    const matchupCellCol = map["matchup"];
    const awayName = (away.team?.shortDisplayName || away.team?.abbreviation || "Away");
    const homeName = (home.team?.shortDisplayName || home.team?.abbreviation || "Home");
    const matchupTxt = `${awayName} @ ${homeName}`;
    updates.push({ range:`${sheets.tab}!${A(matchupCellCol)}${row}:${A(matchupCellCol)}${row}`, values:[[matchupTxt]] });

    // apply text runs (bold winner only) + underline favorite (from pregame spread)
    const aScore = Number(away.score||0), hScore = Number(home.score||0);
    const boldStart = aScore>hScore ? 0 : (awayName.length+3);
    const boldEnd   = aScore>hScore ? awayName.length : (awayName.length+3+homeName.length);

    const favIsHome = (()=>{ // favorite determined by spreads in row (pregame)
      const r = rows[row-2] || [];
      const aSp = Number((r[map["a spread"]]||"").toString());
      const hSp = Number((r[map["h spread"]]||"").toString());
      if (Number.isFinite(aSp) && aSp<0) return false;
      if (Number.isFinite(hSp) && hSp<0) return true;
      return null;
    })();

    const underlineStart = favIsHome===false ? 0 : favIsHome===true ? (awayName.length+3) : 0;
    const underlineEnd   = favIsHome===false ? awayName.length : favIsHome===true ? (awayName.length+3+homeName.length) : 0;

    await sheets.formatTextRuns(sheetId, [{
      range:{
        sheetId,
        startRowIndex: row-1, endRowIndex: row,
        startColumnIndex: map["matchup"], endColumnIndex: map["matchup"]+1
      },
      rows:[{
        values:[{
          userEnteredValue:{ stringValue: matchupTxt },
          textFormatRuns:[
            { startIndex: 0, format:{ bold:false, underline:false } },
            ...(underlineEnd>underlineStart ? [{ startIndex: underlineStart, format:{ underline:true }}] : []),
            { startIndex: boldStart, format:{ bold:true } },
            { startIndex: boldEnd,   format:{ bold:false } },
          ]
        }]
      }],
      fields:"userEnteredValue,textFormatRuns"
    }]);
  }

  if (updates.length) await sheets.batchUpdate(updates);

  // CONDITIONAL FORMATTING (grade only G..K, background; do NOT color A..F)
  // Remove previous CF rules in G..K then reapply simple rules.
  const rules = [];

  // Over/Under grading on Total (K) using Final Score (F).
  // Green if (Away+Home) > Total ; Red if < Total ; leave blank if equal / missing.
  const rowStart = 2, rowEnd = rows.length + 2;

  const col = (idx)=>({sheetId, startRowIndex:rowStart-1, endRowIndex:rowEnd, startColumnIndex:idx, endColumnIndex:idx+1});

  // A Spread (G) : green if Away covered; red otherwise.
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["a spread"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($G${rowStart})<>"" , (VALUE($G${rowStart})<0)*(VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) + VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart}))) + VALUE($G${rowStart}))>0)`}]},
          format:{ backgroundColor:{ red:0.82, green:0.97, blue:0.85 } }
        }
      },
      index:0
    }
  });
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["a spread"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($G${rowStart})<>"" , (VALUE($G${rowStart})<0)*(VALUE($G${rowStart}) + VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) + VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart})))<=0)`}]},
          format:{ backgroundColor:{ red:0.98, green:0.84, blue:0.84 } }
        }
      },
      index:0
    }
  });

  // H Spread (I)
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["h spread"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($I${rowStart})<>"" , (VALUE($I${rowStart})<0)*(VALUE($I${rowStart}) + VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) - VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart})))>0)`}]},
          format:{ backgroundColor:{ red:0.82, green:0.97, blue:0.85 } }
        }
      }, index:0
    }
  });
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["h spread"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($I${rowStart})<>"" , (VALUE($I${rowStart})<0)*(VALUE($I${rowStart}) + VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) - VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart})))<=0)`}]},
          format:{ backgroundColor:{ red:0.98, green:0.84, blue:0.84 } }
        }
      }, index:0
    }
  });

  // Total (K) : green if sum > total, red if sum < total
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["total"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($K${rowStart})<>"" , (VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) + VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart})))) > VALUE($K${rowStart})`}]},
          format:{ backgroundColor:{ red:0.82, green:0.97, blue:0.85 } }
        }
      }, index:0
    }
  });
  rules.push({
    addConditionalFormatRule:{
      rule:{
        ranges:[col(HIDX["total"])],
        booleanRule:{
          condition:{ type:"CUSTOM_FORMULA", values:[{userEnteredValue:`=AND($F${rowStart}<>"" , VALUE($K${rowStart})<>"" , (VALUE(LEFT($F${rowStart},FIND("-", $F${rowStart})-1)) + VALUE(RIGHT($F${rowStart}, LEN($F${rowStart})-FIND("-", $F${rowStart})))) < VALUE($K${rowStart})`}]},
          format:{ backgroundColor:{ red:0.98, green:0.84, blue:0.84 } }
        }
      }, index:0
    }
  });

  await sheets.conditionalFormat(sheetId, rules);
}

/* ============ LIVE PASS (new) ============ */
/* Fills L–Q using ESPN summary; never touches A–K (except when Finals runs separately). */
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

    // summary odds
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
    const Lpayload = {
      "h score": hScoreTxt,
      "h a spread": cleanNum(aO.spread),
      "h a ml": cleanNum(aO.moneyLine ?? aO.moneyline),
      "h h spread": cleanNum(hO.spread),
      "h h ml": cleanNum(hO.moneyLine ?? hO.moneyline),
      "h total": cleanNum(o.overUnder ?? o.total),
    };

    for (const [key,val] of Object.entries(Lpayload)) {
      if (val==="" || val==null) continue;
      const c = map[key];
      liveUpdates.push({
        range:`${sheets.tab}!${A(c)}${row}:${A(c)}${row}`,
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

  // Pull events (scope or explicit ids)
  const league = normLg(LEAGUE_IN);
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

  // Prefill (locked)
  if (RUN_MODE==="all" || RUN_MODE==="prefill") {
    await doPrefill({sheets, values, league, events});
  }

  // Finals + grading (locked)
  if (RUN_MODE==="all" || RUN_MODE==="finals") {
    await doFinals({sheets, sheetId, values: await sheets.readAll(), league, events});
  }

  // Live (L–Q)
  if (RUN_MODE==="all" || RUN_MODE==="live") {
    await doLive({sheets, values: await sheets.readAll(), league, events});
  }

  console.log("✅ Football.mjs complete.", {mode:RUN_MODE, league, tab:TAB_NAME});
})().catch(err=>{
  console.error("Fatal:", err?.message || err);
  process.exit(1);
});
