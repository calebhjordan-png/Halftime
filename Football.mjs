// Football.mjs — Prefill + Finals + Live (throttled, idempotent, full team names)

import { google } from "googleapis";
import axios from "axios";

/* ---------- ENV ---------- */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB       = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const GAME_IDS  = (process.env.GAME_IDS || "").trim();

// throttle knobs (very conservative)
const MAX_WRITES_PER_MIN = Number(process.env.MAX_WRITES_PER_MIN || 28);
const BATCH_SIZE         = Number(process.env.BATCH_SIZE || 1200);

const ET = "America/New_York";

/* ---------- HEADERS A:Q ---------- */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ---------- helpers ---------- */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const fmtET = (d,opt)=> new Intl.DateTimeFormat("en-US",{timeZone:ET,...opt}).format(new Date(d));
const yyyymmddET = (d=new Date())=>{
  const s = fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/");
  return s[2]+s[0]+s[1];
};
const sbUrl = (lg,d)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg==="college-football"?"&groups=80&limit=300":""}`;
const sumUrl=(lg,id)=>`https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
const fetchJSON=async u=>(await axios.get(u,{timeout:12000,headers:{'User-Agent':'sports-football'}})).data;

/* ---------- google sheets ---------- */
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

/* ---------- throttled writer ---------- */
class ThrottledWriter {
  constructor(sheets, mirror){
    this.sheets = sheets;
    this.m = mirror; // 2D mirror of current sheet values
    this.q = [];     // queued {range, values}
    this.bucketStart = Date.now();
    this.callsThisMinute = 0;
  }
  set(r, c, value){
    const colA = String.fromCharCode(65 + c);
    const range = `${TAB}!${colA}${r}:${colA}${r}`;
    const rowIdx = r-1;
    const curRow = this.m[rowIdx] || [];
    const newVal = value == null ? "" : String(value);
    const curVal = (curRow[c] ?? "");
    if(curVal === newVal) return;          // idempotent
    if(!this.m[rowIdx]) this.m[rowIdx]=[];
    this.m[rowIdx][c] = newVal;
    this.q.push({range, values:[[newVal]]});
  }
  async flush(){
    if(!this.q.length) return;
    // big chunks
    for(let i=0;i<this.q.length;i+=BATCH_SIZE){
      const chunk = this.q.slice(i, i+BATCH_SIZE);
      // per-minute throttle
      const now = Date.now();
      if (now - this.bucketStart >= 60000){
        this.bucketStart = now;
        this.callsThisMinute = 0;
      }
      if (this.callsThisMinute >= MAX_WRITES_PER_MIN){
        const wait = 60000 - (now - this.bucketStart) + 100;
        await sleep(wait);
        this.bucketStart = Date.now();
        this.callsThisMinute = 0;
      }
      await this.sheets.batchUpdate(chunk);
      this.callsThisMinute += 1;
      await sleep(250);
    }
    this.q.length = 0;
  }
}

/* ---------- espn parsers ---------- */
const teamName = (t)=> t?.displayName || t?.shortDisplayName || t?.name || t?.abbreviation || "Team";
const matchupText = (aName,hName)=> `${aName} @ ${hName}`;

function statusText(evt){
  const comp = evt?.competitions?.[0];
  const st = comp?.status?.type?.state || "";
  const sLower = st.toLowerCase();
  if (sLower.includes("post")) return "Final";
  if (sLower.includes("in")) {
    const detail = comp?.status?.type?.shortDetail || comp?.status?.type?.detail || "Live";
    return detail.replace("ET","").trim();
  }
  const dateISO = comp?.date;
  if(!dateISO) return "";
  const md = fmtET(dateISO,{month:"2-digit",day:"2-digit"});
  const hm = fmtET(dateISO,{hour:"numeric",minute:"2-digit",hour12:true});
  return `${md} - ${hm} EDT`;
}

function weekText(evt){
  const w = evt?.week?.number ?? evt?.week ?? "";
  return w ? `Week ${w}` : "";
}

const normML = (x)=>{
  if(x==null || x==="") return "";
  const s = String(x).toUpperCase();
  if (s === "OFF")  return "";
  if (s === "EVEN") return "+100";
  return String(x);
};

function parseLive(summary){
  const comp = summary?.header?.competitions?.[0];
  const a = comp?.competitors?.find(t=>t.homeAway==="away");
  const h = comp?.competitors?.find(t=>t.homeAway==="home");
  const aScore = a?.score, hScore = h?.score;
  const odds = comp?.odds?.[0] || summary?.odds?.[0];

  return {
    scoreText: (aScore!=null && hScore!=null) ? `${aScore}-${hScore}` : "",
    aSpread : odds?.awayTeamOdds?.spread ?? "",
    aML     : normML(odds?.awayTeamOdds?.moneyLine ?? ""),
    hSpread : odds?.homeTeamOdds?.spread ?? "",
    hML     : normML(odds?.homeTeamOdds?.moneyLine ?? ""),
    total   : (odds?.overUnder ?? odds?.total ?? "") || ""
  };
}

/* ---------- main ---------- */
(async ()=>{
  const creds = CREDS_RAW.trim().startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: creds.client_email, private_key: creds.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB);
  const grid   = await sheets.read();           // A1:Q
  const header = grid[0] || [];
  const colMap = new Map();
  header.forEach((h,i)=> colMap.set(String(h).trim().toLowerCase(), i));

  // writer
  const writer = new ThrottledWriter(sheets, grid);

  // Ensure headers are present (written in ONE batch)
  if (header.join("|") !== HEADERS.join("|")){
    // set all headers in memory then one flush
    HEADERS.forEach((h,idx)=> writer.set(1, idx, h));
  }

  const idxGameId = colMap.get("game id") ?? 0;
  const rowById = new Map();
  for(let r=1;r<grid.length;r++){
    const id = (grid[r][idxGameId] || "").toString();
    if(id) rowById.set(id, r+1);
  }

  // dates to scan
  const dates = [];
  if (RUN_SCOPE === "today") {
    dates.push(yyyymmddET(new Date()));
  } else {
    const base = new Date();
    for(let i=0;i<7;i++) dates.push(yyyymmddET(new Date(+base + i*86400000)));
  }

  // fetch events
  const events = [];
  for(const d of dates){
    const sb = await fetchJSON(sbUrl(LEAGUE, d));
    if(Array.isArray(sb?.events)) events.push(...sb.events);
    await sleep(100);
  }

  const forceSet = GAME_IDS ? new Set(GAME_IDS.split(",").map(s=>s.trim()).filter(Boolean)) : null;

  const setCell = (row, name, val)=>{
    const idx = colMap.get(name.toLowerCase());
    if(idx==null) return;
    writer.set(row, idx, val);
  };

  for(const evt of events){
    const id = String(evt?.id || "");
    if(!id) continue;
    if(forceSet && !forceSet.has(id)) continue;

    const comp = evt?.competitions?.[0];
    const a    = comp?.competitors?.find(t=>t.homeAway==="away");
    const h    = comp?.competitors?.find(t=>t.homeAway==="home");
    if(!a || !h) continue;

    // row
    let row = rowById.get(id);
    if(!row){
      row = grid.length + 1;
      grid.length = row;  // extend mirror
      rowById.set(id, row);

      setCell(row, "Game ID", id);
      setCell(row, "Date", fmtET(comp.date,{month:"2-digit",day:"2-digit",year:"numeric"}));
      setCell(row, "Week", weekText(evt));
      setCell(row, "Status", statusText(evt));
      setCell(row, "Matchup", matchupText(teamName(a.team), teamName(h.team)));
    } else {
      setCell(row, "Week", weekText(evt));
      setCell(row, "Status", statusText(evt));
      setCell(row, "Matchup", matchupText(teamName(a.team), teamName(h.team))); // keep names current
    }

    // pregame odds
    const preg = comp?.odds?.[0];
    if (preg){
      const aOdds = preg.awayTeamOdds ?? {};
      const hOdds = preg.homeTeamOdds ?? {};
      const ou    = preg.overUnder ?? preg.total ?? "";

      setCell(row, "A Spread", aOdds.spread ?? "");
      setCell(row, "A ML",     normML(aOdds.moneyLine ?? ""));
      setCell(row, "H Spread", hOdds.spread ?? "");
      setCell(row, "H ML",     normML(hOdds.moneyLine ?? ""));
      setCell(row, "Total",    ou ?? "");
    }

    // finals
    const isPost = (comp?.status?.type?.state || "").toLowerCase().includes("post");
    if (isPost){
      const aScore = a?.score!=null ? String(a.score) : "";
      const hScore = h?.score!=null ? String(h.score) : "";
      if(aScore && hScore) setCell(row, "Final Score", `${aScore}-${hScore}`);
    }

    // live (or final) odds from summary
    const isLive = (comp?.status?.type?.state || "").toLowerCase().includes("in") || isPost;
    if (isLive){
      const s = await fetchJSON(sumUrl(LEAGUE, id));
      const live = parseLive(s);
      setCell(row, "H Score",    live.scoreText);
      setCell(row, "H A Spread", live.aSpread);
      setCell(row, "H A ML",     live.aML);
      setCell(row, "H H Spread", live.hSpread);
      setCell(row, "H H ML",     live.hML);
      setCell(row, "H Total",    live.total);
    }
  }

  await writer.flush();
  console.log(`***"ok":true,"tab":"${TAB}","events":${events.length}***`);
})().catch(e=>{
  console.error("Fatal:", e?.message || e);
  process.exit(1);
});
