// Football.mjs — Prefill + Finals + Live (safe writes, robust odds, sheet-safe formatting)
import { google } from "googleapis";
import axios from "axios";

/* ---------- ENV ---------- */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase();                // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();           // "today" | "week"
const GAME_IDS  = (process.env.GAME_IDS || "").trim();                        // optional csv to force
const TZ        = "America/New_York";

/* ---------- CONSTANTS ---------- */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];
// Column indices from headers (computed at runtime)
const COLS = {}; // filled after header read

/* ---------- UTIL ---------- */
const fmtET = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:TZ,...opt}).format(new Date(d));
const yyyymmddET = (d=new Date())=>{
  const [mm,dd,yyyy] = fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return `${yyyy}${mm}${dd}`;
};
const sbUrl = (lg,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl=(lg,id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON=async u=>(await axios.get(u,{headers:{"User-Agent":"football-suite"},timeout:15000})).data;
const clamp = (v, lo, hi)=>Math.max(lo, Math.min(hi, v));

function a1FromIndex(idx) {
  // idx is 0-based. We only need up to Q, but this handles > Z too.
  let n = idx + 1, out = "";
  while (n > 0) {
    const r = (n - 1) % 26;
    out = String.fromCharCode(65 + r) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

function fmtListDate(dateStr) {
  // B column (mm/dd/yyyy)
  return fmtET(dateStr, {year:"numeric",month:"2-digit",day:"2-digit"});
}
function fmtStatus(dateStr) {
  // D column: "MM/DD - 7:30 PM"
  const md = fmtET(dateStr, {month:"2-digit",day:"2-digit"});
  const hm = fmtET(dateStr, {hour:"numeric",minute:"2-digit",hour12:true});
  return `${md} - ${hm}`;
}
function weekLabel(ev) {
  const wk = ev?.week?.number;
  return wk ? `Week ${wk}` : "";
}

function normalizeMoneyLine(v) {
  if (v == null) return "";
  const s = String(v).trim().toUpperCase();
  if (s === "EVEN") return "+100";
  if (s === "OFF")  return "";
  return String(v);
}
function normalizeSpread(v) {
  if (v == null) return "";
  const s = String(v).trim().toUpperCase();
  if (s === "OFF") return "";
  return String(v);
}
function normalizeTotal(v) {
  if (v == null) return "";
  const s = String(v).trim().toUpperCase();
  if (s === "OFF") return "";
  return String(v);
}

function pickOddsEntry(oddsArr = []) {
  if (!Array.isArray(oddsArr) || !oddsArr.length) return null;
  const preferred = oddsArr.find(o => /espn bet/i.test(o?.provider?.name || ""));
  if (preferred && (preferred.homeTeamOdds || preferred.awayTeamOdds)) return preferred;
  return oddsArr.find(o => {
    const a = o?.awayTeamOdds || {};
    const h = o?.homeTeamOdds || {};
    return (a?.spread != null || h?.spread != null || a?.moneyLine != null || h?.moneyLine != null || o?.overUnder != null || o?.total != null);
  }) || oddsArr[0];
}

/* ---------- GOOGLE SHEETS ---------- */
class Sheets {
  constructor(auth,id,tab) { this.api=google.sheets({version:"v4",auth}); this.id=id; this.tab=tab; }
  async readAll() { const r = await this.api.spreadsheets.values.get({spreadsheetId:this.id,range:`${this.tab}!A1:Q`}); return r.data.values||[]; }
  async batchValues(values) {
    if (!values.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId: this.id,
      requestBody: { valueInputOption: "RAW", data: values }
    });
  }
  async batchRequests(requests) {
    if (!requests.length) return;
    await this.api.spreadsheets.batchUpdate({ spreadsheetId:this.id, requestBody:{ requests } });
  }
  async metadata()  { return (await this.api.spreadsheets.get({spreadsheetId:this.id})).data; }
}

function pushNonEmpty(updates, tab, row, colIdx, value) {
  if (value == null || value === "") return;
  const col = a1FromIndex(colIdx);
  updates.push({ range: `${tab}!${col}${row}:${col}${row}`, values: [[ String(value) ]] });
}

/* ---------- ODDS + LIVE ---------- */
async function getOddsFromSummary(league, gameId) {
  try {
    const s = await fetchJSON(sumUrl(league, gameId));
    const comp = s?.header?.competitions?.[0] || {};
    const odds = pickOddsEntry(comp.odds || []);
    if (!odds) return null;

    const a = odds.awayTeamOdds || {};
    const h = odds.homeTeamOdds || {};
    const total = odds.overUnder ?? odds.total ?? "";

    return {
      aSpread: normalizeSpread(a.spread),
      aML:     normalizeMoneyLine(a.moneyLine),
      hSpread: normalizeSpread(h.spread),
      hML:     normalizeMoneyLine(h.moneyLine),
      total:   normalizeTotal(total)
    };
  } catch {
    return null;
  }
}

async function getLiveBoard(league, gameId) {
  try {
    const s = await fetchJSON(sumUrl(league, gameId));
    const box   = s?.boxscore;
    const away  = box?.teams?.find(t => t.homeAway === "away");
    const home  = box?.teams?.find(t => t.homeAway === "home");
    const aScr  = away?.score;
    const hScr  = home?.score;

    const comp  = s?.header?.competitions?.[0] || {};
    const odds  = pickOddsEntry(comp.odds || []);
    const a = odds?.awayTeamOdds || {};
    const h = odds?.homeTeamOdds || {};
    const total = odds?.overUnder ?? odds?.total ?? "";

    return {
      hScore:  (aScr!=null && hScr!=null) ? `${aScr}-${hScr}` : "",
      haSpread: normalizeSpread(a.spread),
      haML:     normalizeMoneyLine(a.moneyLine),
      hhSpread: normalizeSpread(h.spread),
      hhML:     normalizeMoneyLine(h.moneyLine),
      hTotal:   normalizeTotal(total),
      status:   comp?.status?.type?.shortDetail || comp?.status?.type?.name || ""
    };
  } catch {
    return null;
  }
}

/* ---------- SHEET FORMATTING (G–K only) ---------- */
async function fixFormatting(sh, sheetId) {
  const api = sh.api;

  // Delete up to 25 existing CF rules (safe no-op if none)
  for (let i=0;i<25;i++) {
    try {
      await api.spreadsheets.batchUpdate({ spreadsheetId: sh.id, requestBody: { requests: [{ deleteConditionalFormatRule:{ sheetId, index:0 } }] } });
    } catch { break; }
  }

  const requests = [];
  // Ensure no global bold/underline below header
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 1 },
      cell:  { userEnteredFormat: { textFormat: { bold:false, underline:false } } },
      fields: "userEnteredFormat.textFormat"
    }
  });

  const col = (c) => ({ sheetId, startRowIndex: 1, startColumnIndex: c, endColumnIndex: c+1 });
  const green = { red: 0.85, green: 0.94, blue: 0.88 };
  const red   = { red: 0.98, green: 0.85, blue: 0.86 };

  const rules = [
    // G (A Spread) win / lose
    ['=AND($F2<>"",$G2<>"", INDEX(SPLIT($F2,"-"),1)+VALUE($G2) >  INDEX(SPLIT($F2,"-"),2))', green, 6],
    ['=AND($F2<>"",$G2<>"", INDEX(SPLIT($F2,"-"),1)+VALUE($G2) <= INDEX(SPLIT($F2,"-"),2))', red,   6],
    // I (H Spread) win / lose
    ['=AND($F2<>"",$I2<>"", INDEX(SPLIT($F2,"-"),2)+VALUE($I2) >  INDEX(SPLIT($F2,"-"),1))', green, 8],
    ['=AND($F2<>"",$I2<>"", INDEX(SPLIT($F2,"-"),2)+VALUE($I2) <= INDEX(SPLIT($F2,"-"),1))', red,   8],
    // H (A ML) winner green / red
    ['=AND($F2<>"",$H2<>"", VALUE($H2)<>0, INDEX(SPLIT($F2,"-"),1) >  INDEX(SPLIT($F2,"-"),2))', green, 7],
    ['=AND($F2<>"",$H2<>"", VALUE($H2)<>0, INDEX(SPLIT($F2,"-"),1) <= INDEX(SPLIT($F2,"-"),2))', red,   7],
    // J (H ML) winner green / red
    ['=AND($F2<>"",$J2<>"", VALUE($J2)<>0, INDEX(SPLIT($F2,"-"),2) >  INDEX(SPLIT($F2,"-"),1))', green, 9],
    ['=AND($F2<>"",$J2<>"", VALUE($J2)<>0, INDEX(SPLIT($F2,"-"),2) <= INDEX(SPLIT($F2,"-"),1))', red,   9],
    // K (Total) — Over green / Under red  (ties treated as red; change if you want PUSH gray)
    ['=AND($F2<>"",$K2<>"", (INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2)) >  VALUE($K2))', green, 10],
    ['=AND($F2<>"",$K2<>"", (INDEX(SPLIT($F2,"-"),1)+INDEX(SPLIT($F2,"-"),2)) <= VALUE($K2))', red,   10],
  ];

  for (const [formula, color, colIdx] of rules) {
    requests.push({
      addConditionalFormatRule: {
        rule: {
          ranges: [ col(colIdx) ],
          booleanRule: { condition:{ type:"CUSTOM_FORMULA", values:[{ userEnteredValue: formula }] }, format:{ backgroundColor: color } }
        },
        index: 0
      }
    });
  }

  await sh.batchRequests(requests);
}

/* ---------- PREFILL / FINALS / LIVE ---------- */
function collectDates() {
  const out = [];
  if (GAME_IDS) return out;
  const span = RUN_SCOPE === "today" ? 1 : 7;
  for (let i=0;i<span;i++) {
    out.push(yyyymmddET(new Date(Date.now() + i*86400000)));
  }
  return out;
}

function mapHeaderRow(hdr) {
  COLS.map = {};
  hdr.forEach((h,i)=> COLS[h.trim().toLowerCase()] = i);
}

function rowForGameId(map, gameId) { return map.get(String(gameId)) || null; }

function buildRowValuesFromEvent(ev) {
  const comp = ev.competitions?.[0] || {};
  const a = comp.competitors?.find(t=>t.homeAway==="away");
  const h = comp.competitors?.find(t=>t.homeAway==="home");
  const date = comp.date || ev.date || new Date().toISOString();
  const status = comp.status?.type || {};
  const aTeam = a?.team?.abbreviation || a?.team?.shortDisplayName || a?.team?.name || "";
  const hTeam = h?.team?.abbreviation || h?.team?.shortDisplayName || h?.team?.name || "";
  const matchup = `${aTeam} @ ${hTeam}`;

  return {
    id: String(ev.id),
    date: fmtListDate(date),
    wk: weekLabel(ev),
    status: fmtStatus(date),
    matchup
  };
}

async function writePrefillOdds(sh, rowIndex1based, league, gameId) {
  const odds = await getOddsFromSummary(league, gameId);
  if (!odds) return 0;

  const updates = [];
  pushNonEmpty(updates, TAB_NAME, rowIndex1based, COLS["a spread"], odds.aSpread);
  pushNonEmpty(updates, TAB_NAME, rowIndex1based, COLS["a ml"],     odds.aML);
  pushNonEmpty(updates, TAB_NAME, rowIndex1based, COLS["h spread"], odds.hSpread);
  pushNonEmpty(updates, TAB_NAME, rowIndex1based, COLS["h ml"],     odds.hML);
  pushNonEmpty(updates, TAB_NAME, rowIndex1based, COLS["total"],    odds.total);

  if (updates.length) await sh.batchValues(updates);
  return updates.length;
}

async function writeFinals(sh, rowIndex1based, finalScore, statusText) {
  const ups = [];
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["final score"], finalScore);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["status"],      statusText || "Final");
  if (ups.length) await sh.batchValues(ups);
}

async function writeLive(sh, rowIndex1based, live) {
  const ups = [];
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h score"],   live.hScore);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h a spread"], live.haSpread);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h a ml"],     live.haML);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h h spread"], live.hhSpread);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h h ml"],     live.hhML);
  pushNonEmpty(ups, TAB_NAME, rowIndex1based, COLS["h total"],    live.hTotal);
  if (ups.length) await sh.batchValues(ups);
}

/* ---------- MAIN ---------- */
(async ()=>{
  if (!SHEET_ID || !CREDS_RAW) throw new Error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");

  // Auth
  const creds = CREDS_RAW.trim().startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: creds.client_email, private_key: creds.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const client = await auth.getClient();
  const sh = new Sheets(client, SHEET_ID, TAB_NAME);

  // Sheet + header + index
  const meta  = await sh.metadata();
  const sheet = meta.sheets.find(s=>s.properties.title===TAB_NAME);
  if (!sheet) throw new Error(`Tab ${TAB_NAME} not found`);
  const sheetId = sheet.properties.sheetId;

  const values = await sh.readAll();
  const header = values[0] || HEADERS;
  mapHeaderRow(header);

  // Build row map by Game ID
  const rows = values.slice(1);
  const rowById = new Map();
  rows.forEach((r,i)=>{
    const id = r[COLS["game id"]] || "";
    if (id) rowById.set(String(id), i+2); // 1-based row index (header is row 1)
  });

  // Collect events
  const dates = collectDates();
  let events = [];

  if (GAME_IDS) {
    GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean).forEach(id=>{
      events.push({ id, force:true });
    });
  } else {
    for (const d of dates) {
      const sb = await fetchJSON(sbUrl(LEAGUE_IN, d));
      events.push(...(sb?.events || []));
    }
  }

  // Process
  let prefilled=0, finals=0, lives=0, createdRows=0;

  // Append helper if row missing
  async function ensureRowFor(ev) {
    const id = String(ev.id);
    let row = rowById.get(id);
    if (row) return row;

    // Build row data
    const base = ev.force
      ? { id, date:"", wk:"", status:"", matchup: "" }
      : buildRowValuesFromEvent(ev);

    // Append at end (not A2)
    const nextRow = rows.length + 2; // header + existing rows
    const vals = []; for (let i=0;i<HEADERS.length;i++) vals.push("");

    vals[COLS["game id"]]   = base.id;
    vals[COLS["date"]]      = base.date;
    vals[COLS["week"]]      = base.wk;
    vals[COLS["status"]]    = base.status;
    vals[COLS["matchup"]]   = base.matchup;

    await sh.batchValues([{ range: `${TAB_NAME}!A${nextRow}:Q${nextRow}`, values: [vals] }]);
    rowById.set(id, nextRow);
    rows.push(vals);
    createdRows++;
    return nextRow;
  }

  for (const ev of events) {
    const id = String(ev.id);
    const comp = ev.competitions?.[0] || {};
    const state = (comp.status?.type?.state || "").toLowerCase();

    const row = await ensureRowFor(ev);

    // PREFILL (upcoming) — when odds columns are empty
    if (!state || state === "pre") {
      const existing = values[row-1] || []; // 0-based
      const gFilled = (existing[COLS["a spread"]] || existing[COLS["a ml"]] || existing[COLS["h spread"]] || existing[COLS["h ml"]] || existing[COLS["total"]]);
      if (!gFilled || ev.force) {
        prefilled += await writePrefillOdds(sh, row, LEAGUE_IN, id);
      }
      // also write status/date/week/matchup if we built them
      if (!ev.force && comp.date) {
        const b = buildRowValuesFromEvent(ev);
        const ups = [];
        pushNonEmpty(ups, TAB_NAME, row, COLS["date"],    b.date);
        pushNonEmpty(ups, TAB_NAME, row, COLS["week"],    b.wk);
        pushNonEmpty(ups, TAB_NAME, row, COLS["status"],  b.status);
        pushNonEmpty(ups, TAB_NAME, row, COLS["matchup"], b.matchup);
        if (ups.length) await sh.batchValues(ups);
      }
      continue;
    }

    // LIVE — update L–Q
    if (state === "in") {
      const live = await getLiveBoard(LEAGUE_IN, id);
      if (live) {
        await writeLive(sh, row, live);
        // keep D status in sync for live
        pushNonEmpty([], "", 0, 0, ""); // no-op; keeps structure
        const ups = [];
        pushNonEmpty(ups, TAB_NAME, row, COLS["status"], live.status || "Live");
        if (ups.length) await sh.batchValues(ups);
        lives++;
      }
      continue;
    }

    // FINALS — write final score + status "Final"
    if (state === "post") {
      const aTeam = comp.competitors?.find(t=>t.homeAway==="away");
      const hTeam = comp.competitors?.find(t=>t.homeAway==="home");
      const aScore = aTeam?.score;
      const hScore = hTeam?.score;
      const final = (aScore!=null && hScore!=null) ? `${aScore}-${hScore}` : "Final";
      await writeFinals(sh, row, final, "Final");
      finals++;
      continue;
    }
  }

  // Re-apply G–K conditional formatting once
  await fixFormatting(sh, sheetId);

  console.log(JSON.stringify({ ok:true, tab:TAB_NAME, league:LEAGUE_IN, events: events.length, prefilled, finals, lives, createdRows }));
})().catch(e=>{
  console.error("Fatal:", e?.message || e);
  process.exit(1);
});
