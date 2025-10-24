// Football.mjs — Prefill + Finals + Live with throttled/batched writes

import { google } from "googleapis";
import axios from "axios";

/* ---------- ENV ---------- */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB       = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const GAME_IDS  = (process.env.GAME_IDS || "").trim();

// throttle knobs
const MAX_WRITES_PER_MIN = Number(process.env.MAX_WRITES_PER_MIN || 45); // safe vs 60 limit
const BATCH_SIZE         = Number(process.env.BATCH_SIZE || 250);        // cells per values batch

const ET = "America/New_York";

/* ---------- HEADERS (A:Q) ---------- */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ---------- UTIL ---------- */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const fmtET = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:ET,...opt}).format(new Date(d));
const yyyymmddET = (d=new Date())=>{
  const s = fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return s[2]+s[0]+s[1];
};
const sbUrl = (lg,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl=(lg,id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON=async u=>(await axios.get(u,{timeout:12000,headers:{'User-Agent':'sports-football'}})).data;
const toNum = (x)=> (x===null||x===undefined||x==="") ? null : (Number.isFinite(+x) ? +x : null);

/* ---------- GOOGLE ---------- */
class Sheets {
  constructor(auth,id,tab){
    this.api = google.sheets({version:"v4",auth});
    this.id  = id;
    this.tab = tab;
  }
  async read(range=`${this.tab}!A1:Q`){
    const r = await this.api.spreadsheets.values.get({spreadsheetId:this.id, range});
    return r.data.values || [];
  }
  async batchUpdate(data){
    if(!data.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId: this.id,
      requestBody: { valueInputOption: "RAW", data }
    });
  }
}

/* ---------- WRITE FUNNEL (throttled & idempotent) ---------- */
class ThrottledWriter {
  constructor(sheets, currentValues){
    this.sheets = sheets;
    this.current = currentValues; // 2D array
    this.queue = [];              // [{range, values}]
    this.lastMinute = Date.now();
    this.writesThisMinute = 0;
  }
  // record an intended write if the value actually changes
  setCell(r, cIndex, value){
    const colA = String.fromCharCode(65 + cIndex);
    const range = `${TAB}!${colA}${r}:${colA}${r}`;
    // current is 0-based rows; header is row 1 => current row index = r-1
    const rowIdx = r-1;
    const curRow = this.current[rowIdx] || [];
    const curVal = (curRow[cIndex] ?? "");
    const newVal = value == null ? "" : String(value);
    if(curVal === newVal) return; // no-op
    this.queue.push({range, values:[[newVal]]});
    // also mirror into local cache to keep idempotence across subsequent sets
    if(!this.current[rowIdx]) this.current[rowIdx] = [];
    this.current[rowIdx][cIndex] = newVal;
  }
  // flush in batches, throttling by MAX_WRITES_PER_MIN
  async flush(){
    if(!this.queue.length) return;

    // chunk queue into batches that together count as 1 write call each
    const chunks = [];
    for(let i=0;i<this.queue.length;i+=BATCH_SIZE){
      chunks.push(this.queue.slice(i, i+BATCH_SIZE));
    }

    for(const chunk of chunks){
      // throttle loop
      const now = Date.now();
      if(now - this.lastMinute >= 60000){
        this.lastMinute = now;
        this.writesThisMinute = 0;
      }
      if(this.writesThisMinute >= MAX_WRITES_PER_MIN){
        const wait = 60000 - (now - this.lastMinute) + 50;
        await sleep(wait);
        this.lastMinute = Date.now();
        this.writesThisMinute = 0;
      }

      await this.sheets.batchUpdate(chunk);
      this.writesThisMinute += 1;
      // light spacing between calls to smooth bursts
      await sleep(200);
    }
    this.queue.length = 0;
  }
}

/* ---------- ESPN PARSERS (prefill/finals/live) ---------- */
function parseTeams(evt){
  const comp = evt.competitions?.[0];
  const h = comp?.competitors?.find(t=>t.homeAway==="home");
  const a = comp?.competitors?.find(t=>t.homeAway==="away");
  return { comp, a, h };
}
function matchupString(aName,hName){ return `${aName} @ ${hName}`; }

function statusString(evt){
  const comp = evt.competitions?.[0];
  const st = comp?.status?.type?.state || "";
  if (st.toLowerCase().includes("post")) return "Final";
  if (st.toLowerCase().includes("in")) {
    // "Q4 - 2:32" / "Half" etc.
    const detail = comp?.status?.type?.shortDetail || comp?.status?.type?.detail || "Live";
    return detail.replace("ET","").trim();
  }
  const dateISO = comp?.date;
  if(!dateISO) return "";
  const md = fmtET(dateISO,{month:"2-digit",day:"2-digit"});
  const hm = fmtET(dateISO,{hour:"numeric",minute:"2-digit",hour12:true});
  return `${md} - ${hm}`;
}

function weekString(evt){
  const w = evt?.week?.number ?? evt?.week ?? "";
  return w ? `Week ${w}` : "";
}

function underlineFavorite(aName,hName,aFav){
  // returns textFormatRuns indices for Sheets API
  const s = `${aName} @ ${hName}`;
  const aStart = 0;
  const aEnd   = aName.length;
  const hStart = aName.length + 3;
  const hEnd   = hStart + hName.length;
  // favorite underline range
  const favStart = aFav ? aStart : hStart;
  const favEnd   = aFav ? aEnd   : hEnd;
  return { text: s, runs: [
    {startIndex: favStart, format:{underline:true}},
    {startIndex: favEnd,   format:{underline:false}}
  ]};
}

/* live odds from summary */
function readLiveOdds(summary){
  const comp = summary?.header?.competitions?.[0];
  const tAway = comp?.competitors?.find(t=>t.homeAway==="away");
  const tHome = comp?.competitors?.find(t=>t.homeAway==="home");
  const aScore = tAway?.score, hScore = tHome?.score;

  const odds = comp?.odds?.[0] || summary?.odds?.[0];
  const overUnder = odds?.overUnder ?? odds?.total ?? "";
  const aOdds = odds?.awayTeamOdds ?? {};
  const hOdds = odds?.homeTeamOdds ?? {};
  const aSpread = aOdds?.spread ?? "";
  const hSpread = hOdds?.spread ?? "";
  let aML = aOdds?.moneyLine ?? "";
  let hML = hOdds?.moneyLine ?? "";

  // normalize EVEN → +100; OFF → blank
  const normML = (x)=>{
    if(x==null || x==="") return "";
    const s = String(x).toUpperCase();
    if(s === "OFF") return "";
    if(s === "EVEN") return "+100";
    return String(x);
  };
  aML = normML(aML);
  hML = normML(hML);

  return {
    scoreText: (aScore!=null && hScore!=null) ? `${aScore}-${hScore}` : "",
    aSpread, aML, hSpread, hML, total: overUnder ?? ""
  };
}

/* ---------- MAIN ---------- */
(async ()=>{
  const creds = CREDS_RAW.trim().startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: creds.client_email, private_key: creds.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB);

  // read entire tab once; we will only write changed cells
  const grid = await sheets.read();            // A1:Q*
  const header = grid[0] || [];
  // build column map (by header name lowercase)
  const colMap = new Map();
  header.forEach((h,i)=> colMap.set(String(h).trim().toLowerCase(), i));

  // ensure headers exist (write once if missing)
  const writer = new ThrottledWriter(sheets, grid);
  if(header.join("|") !== HEADERS.join("|")){
    // write HEADERS into row 1, but only if different
    HEADERS.forEach((h, idx)=> writer.setCell(1, idx, h));
  }

  // create row lookup by Game ID
  const idxGameId = colMap.get("game id") ?? 0;
  const rowById = new Map();
  for(let r=1;r<grid.length;r++){
    const id = (grid[r][idxGameId] || "").toString();
    if(id) rowById.set(id, r+1); // store as 1-based row
  }

  // which days to scan (scope=week uses 7 days forward)
  const dates = [];
  if (GAME_IDS) {
    // if caller passed explicit IDs we still need dates for finals/live cross-check
    const today = new Date();
    for(let i=0;i<7;i++) dates.push(yyyymmddET(new Date(+today + i*86400000)));
  } else if (RUN_SCOPE === "today") {
    dates.push(yyyymmddET(new Date()));
  } else {
    const today = new Date();
    for(let i=0;i<7;i++) dates.push(yyyymmddET(new Date(+today + i*86400000)));
  }

  // pull events
  const events = [];
  for(const d of dates){
    const sb = await fetchJSON(sbUrl(LEAGUE, d));
    if (Array.isArray(sb?.events)) events.push(...sb.events);
    await sleep(100); // be polite to ESPN
  }

  // optionally filter by GAME_IDS
  const forceSet = GAME_IDS
    ? new Set(GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean))
    : null;

  // process: prefill + finals + live
  const writeCell = (row, headerName, val) => {
    const idx = colMap.get(headerName.toLowerCase());
    if (idx == null) return;
    writer.setCell(row, idx, val);
  };

  for (const evt of events) {
    const id = String(evt?.id || "");
    if (!id) continue;
    if (forceSet && !forceSet.has(id)) continue;

    const { comp, a, h } = parseTeams(evt);
    if (!comp || !a || !h) continue;

    // locate or append row
    let row = rowById.get(id);
    if (!row) {
      // append at bottom (after existing rows)
      row = (grid.length + 1);
      grid.length = Math.max(grid.length, row); // extend mirror
      rowById.set(id, row);
      // write key fields once
      writeCell(row, "Game ID", id);
      writeCell(row, "Date", fmtET(comp.date,{month:"2-digit",day:"2-digit",year:"numeric"}));
      writeCell(row, "Week", weekString(evt));
      writeCell(row, "Status", statusString(evt));
      writeCell(row, "Matchup", matchupString(a.team?.shortDisplayName ?? a.team?.name ?? a.team?.displayName ?? "Away",
                                              h.team?.shortDisplayName ?? h.team?.name ?? h.team?.displayName ?? "Home"));
    } else {
      // update dynamic fields
      writeCell(row, "Week", weekString(evt));
      writeCell(row, "Status", statusString(evt));
    }

    // pregame odds (A/H spreads, ML, total) from competition.odds[0]
    const preg = comp?.odds?.[0];
    if (preg) {
      const aOdds = preg.awayTeamOdds ?? {};
      const hOdds = preg.homeTeamOdds ?? {};
      const ou = preg.overUnder ?? preg.total ?? "";

      const fixML = (x)=>{
        if(x == null || x === "") return "";
        const s = String(x).toUpperCase();
        if (s === "OFF") return "";
        if (s === "EVEN") return "+100";
        return String(x);
      };

      writeCell(row, "A Spread", aOdds.spread ?? "");
      writeCell(row, "A ML",     fixML(aOdds.moneyLine ?? ""));
      writeCell(row, "H Spread", hOdds.spread ?? "");
      writeCell(row, "H ML",     fixML(hOdds.moneyLine ?? ""));
      writeCell(row, "Total",    ou ?? "");
    }

    // finals score (if post)
    const isPost = (comp?.status?.type?.state || "").toLowerCase().includes("post");
    if (isPost) {
      const aScore = a?.score != null ? String(a.score) : "";
      const hScore = h?.score != null ? String(h.score) : "";
      if (aScore && hScore) writeCell(row, "Final Score", `${aScore}-${hScore}`);
    }

    // live odds (and live score) if in-game
    const isLive = (comp?.status?.type?.state || "").toLowerCase().includes("in");
    if (isLive || isPost) {
      const s = await fetchJSON(sumUrl(LEAGUE, id));
      const live = readLiveOdds(s);
      writeCell(row, "H Score", live.scoreText);
      writeCell(row, "H A Spread", live.aSpread ?? "");
      writeCell(row, "H A ML",     live.aML ?? "");
      writeCell(row, "H H Spread", live.hSpread ?? "");
      writeCell(row, "H H ML",     live.hML ?? "");
      writeCell(row, "H Total",    live.total ?? "");
    }
  }

  await writer.flush();
  console.log(`***"ok":true,"tab":"${TAB}","events":${events.length}***`);
})().catch(e=>{
  console.error("Fatal:", e?.message || e);
  process.exit(1);
});
