// orchestrator.mjs
import { google } from "googleapis";
import axios from "axios";

/* ======================= ENV / FLAGS ======================= */
const SHEET_ID   = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW  = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE     = (process.env.LEAGUE || "nfl").toLowerCase();            // nfl | college-football | both (workflow can call twice)
const TAB_NAME   = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE  = (process.env.RUN_SCOPE || "week").toLowerCase();        // week | today
const TARGET_IDS_RAW = (process.env.TARGET_GAME_ID || "").trim();          // "4017...,4017..."
const GHA_JSON   = (process.env.GHA_JSON || "") === "1";

/* orchestrator owns pregame only; live-game owns live status */
const DONT_TOUCH_STATUS_IF_LIVE = true;

/* ======================= CONSTANTS ========================= */
const ET_TZ = "America/New_York";

/* Short column set (A..Q) */
const HEADER = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

/* ======================= HELPERS =========================== */
const out = (s,...a)=>s.write(a.map(x=>typeof x==='string'?x:String(x)).join(" ")+"\n");
const log = (...a)=> GHA_JSON ? out(process.stderr,...a) : console.log(...a);

function parseServiceAccount(raw){
  if (raw.startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}

function leagueKey(x){
  const s = (x||"").toLowerCase();
  if (s === "ncaaf" || s.includes("college")) return "college-football";
  return "nfl";
}
function sbUrl(lg,dates){
  const l = leagueKey(lg);
  const extra = l === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${l}/scoreboard?dates=${dates}${extra}`;
}
function summaryUrl(lg,id){
  const l = leagueKey(lg);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${l}/summary?event=${id}`;
}
function gameUrl(lg,id){
  const l = leagueKey(lg);
  return `https://www.espn.com/${l}/game/_/gameId/${id}`;
}

async function getJSON(url){
  log("GET", url);
  const { data } = await axios.get(url, { timeout: 15000, headers: { "User-Agent": "halftime-orchestrator" } });
  return data;
}

function yyyymmddET(d=new Date()){
  const parts = new Intl.DateTimeFormat("en-US",{ timeZone:ET_TZ, year:"numeric", month:"2-digit", day:"2-digit" }).formatToParts(d);
  const g = t => parts.find(p=>p.type===t)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
function fmtDateET(d){
  return new Intl.DateTimeFormat("en-US", { timeZone:ET_TZ, year:"numeric", month:"2-digit", day:"2-digit" }).format(new Date(d));
}
function fmtStatusPregameNoYear(d){
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, month:"2-digit", day:"2-digit", hour:"numeric", minute:"2-digit", hour12:true }).formatToParts(new Date(d));
  const g = k => parts.find(p=>p.type===k)?.value || "";
  // e.g. 10/26 - 1:00 PM
  return `${g("month")}/${g("day")} - ${g("hour")}:${g("minute")} ${g("dayPeriod")}`.replace(/\s+/g," ").trim();
}

function mapHeaderIdx(h){ const m={}; (h||[]).forEach((x,i)=>m[String(x||"").trim().toLowerCase()]=i); return m; }
function A1col(i){ let n=i+1,s=""; while(n>0){ n--; s=String.fromCharCode(65+(n%26))+s; n=Math.floor(n/26);} return s; }
function cellA1(row1, colIdx, tab){ const col=A1col(colIdx); return `${tab}!${col}${row1}:${col}${row1}`; }

/* odds helpers */
function firstEspnBet(oddsArr=[]) {
  if (!Array.isArray(oddsArr)) return null;
  return oddsArr.find(o=>/espn\s*bet/i.test(o?.provider?.name||o?.provider?.displayName||"")) || oddsArr[0] || null;
}
function numStr(x){
  if (x==null) return "";
  const s = String(x).trim();
  if (!s) return "";
  return s.startsWith("+") || s.startsWith("-") ? s : (Number(s)>=0 ? `+${s}` : s);
}

/* live/started detection */
function isFinal(evt){
  const t=(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return t.includes("FINAL");
}
function isLive(evt){
  const t=(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  const short=(evt?.status?.type?.shortDetail||"").toUpperCase();
  return t.includes("IN_PROGRESS") || t.includes("LIVE") || short.includes("HALF") || /Q[1-4]/.test(short);
}

/* ================= SHEETS ================== */
async function sheetsClient(){
  const svc = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials:{ client_email:svc.client_email, private_key:svc.private_key },
    scopes:["https://www.googleapis.com/auth/spreadsheets"]
  });
  return google.sheets({ version:"v4", auth });
}

class Batch {
  constructor(tab){ this.tab=tab; this.acc=[]; }
  set(row1, cidx, val){
    if (cidx==null || cidx<0) return;
    this.acc.push({ range: cellA1(row1, cidx, this.tab), values: [[ val ]] });
  }
  async flush(s){
    if (!this.acc.length) return;
    await s.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data: this.acc }
    });
    log(`Batched ${this.acc.length} cell update(s).`);
    this.acc.length=0;
  }
}

/* -------- text formatting (underline favorite / bold winner) -------- */
function buildTextRuns(matchup, underlineSpan, boldSpan){
  // Clear first, then apply a few well-formed runs.
  // Each entry: { startIndex, format:{ underline?, bold? } }
  // We’ll write value + runs together via updateCells.
  const runs = [];
  // clear run (no format) for entire cell
  runs.push({ startIndex: 0, format: { underline:false, bold:false } });
  const L = matchup.length;

  const pushSpan = (span, key) => {
    if (!span) return;
    const { start, end } = span;
    if (start==null || end==null) return;
    const s = Math.max(0, Math.min(start, L));
    const e = Math.max(0, Math.min(end, L));
    if (e <= s) return;
    // Mark start of style
    runs.push({ startIndex: s, format: { [key]: true } });
    // Mark end of style (turn it off)
    if (e < L) runs.push({ startIndex: e, format: { [key]: false } });
  };

  pushSpan(underlineSpan, "underline");
  pushSpan(boldSpan, "bold");

  // Sort by startIndex ascending, and coalesce identical starts (Sheet API tolerant)
  runs.sort((a,b)=>a.startIndex-b.startIndex);
  return runs;
}

function spanOfTeam(matchup, teamName){
  const start = matchup.indexOf(teamName);
  if (start < 0) return null;
  return { start, end: start + teamName.length };
}

async function writeMatchupWithRuns(sheets, sheetId, row0, matchup, runs){
  // row0 = zero-based row of the data row; column E = idx=4
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        updateCells: {
          range: { sheetId, startRowIndex: row0, endRowIndex: row0+1, startColumnIndex: 4, endColumnIndex: 5 },
          rows: [{
            values: [{
              userEnteredValue: { stringValue: matchup },
              textFormatRuns: runs
            }]
          }],
          fields: "userEnteredValue,textFormatRuns"
        }
      }]
    }
  });
}

/* ================= MAIN ================== */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    const msg = "Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT.";
    if (GHA_JSON) return process.stdout.write(JSON.stringify({ ok:false, error: msg })+"\n");
    console.error(msg); process.exit(1);
  }

  const sheets = await sheetsClient();

  // Ensure tab + header
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const target = (meta.data.sheets||[]).find(s=>s.properties?.title===TAB_NAME);
  let sheetId;
  if (!target){
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
    sheetId = add.data?.replies?.[0]?.addSheet?.properties?.sheetId;
  } else {
    sheetId = target.properties.sheetId;
  }

  const r0 = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Q` });
  let values = r0.data.values || [];
  let header = values[0] || [];
  if (header.join("|") !== HEADER.join("|")){
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [ HEADER ] }
    });
    header = HEADER.slice();
    const r1 = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Q` });
    values = r1.data.values || [];
  }
  const H = mapHeaderIdx(header);
  const rows = values.slice(1);

  // Build row index by Game ID (A)
  const rowById = new Map();
  rows.forEach((r,i)=>{ const id = String(r[0]||"").trim(); if (id) rowById.set(id, i+2); });

  // Which ids to process?
  const targetIds = TARGET_IDS_RAW
    ? TARGET_IDS_RAW.split(",").map(s=>s.trim()).filter(Boolean)
    : null;

  // Dates list
  const dates = (RUN_SCOPE === "today")
    ? [ yyyymmddET(new Date()) ]
    : Array.from({length:7},(_,i)=> yyyymmddET(new Date(Date.now()+i*86400000)));

  // Pull events
  let events = [];
  for (const d of dates){
    const sb = await getJSON(sbUrl(LEAGUE, d));
    events.push(...(sb?.events||[]));
  }
  // De-dupe
  const seen = new Set();
  events = events.filter(e => !seen.has(e?.id) && seen.add(e?.id));

  // Filter to targets if provided
  if (targetIds && targetIds.length){
    events = events.filter(e => targetIds.includes(String(e?.id)));
  }

  log(`Events found: ${events.length}`);

  // Append block for brand-new rows
  const append = [];

  const batch = new Batch(TAB_NAME);

  for (const ev of events){
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    if (!away || !home) continue;

    const awayName = away.team?.shortDisplayName || away.team?.abbreviation || away.team?.name || "Away";
    const homeName = home.team?.shortDisplayName || home.team?.abbreviation || home.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;

    const gid = String(ev.id);
    let row1 = rowById.get(gid);

    // Determine pregame odds (and fix ML map)
    const o0 = firstEspnBet(comp.odds || ev.odds || []);
    let aSpread = "", hSpread = "", total = "", aML = "", hML = "";

    if (o0){
      // Spread
      const favId = String(o0.favorite ?? o0.favoriteTeamId ?? "");
      const spread = Number.isFinite(o0.spread) ? Number(o0.spread) :
                     (typeof o0.spread === "string" ? parseFloat(o0.spread) : NaN);
      if (!Number.isNaN(spread) && favId){
        const line = Math.abs(spread).toString();
        if (String(away.team?.id) === favId){ aSpread = `-${line}`; hSpread = `+${line}`; }
        else if (String(home.team?.id) === favId){ hSpread = `-${line}`; aSpread = `+${line}`; }
      } else if (o0.details){
        const m = o0.details.match(/([+-]?\d+(\.\d+)?)/);
        if (m){
          const v = parseFloat(m[1]);
          if (v>0){ aSpread = `+${v}`; hSpread = `-${v}`; }
          else { aSpread = `${v}`; hSpread = `+${Math.abs(v)}`; }
        }
      }
      // Total
      total = (o0.overUnder ?? o0.total) ?? "";

      // ML strict by teamId
      const moneylineStrict = () => {
        // many shapes, but teamOdds[] is solid when present
        if (Array.isArray(o0.teamOdds)){
          for (const t of o0.teamOdds){
            const tid = String(t?.teamId ?? t?.team?.id ?? "");
            const ml = t?.moneyLine ?? t?.moneyline ?? t?.money_line;
            if (tid === String(away.team?.id)) aML = numStr(ml);
            if (tid === String(home.team?.id)) hML = numStr(ml);
          }
        }
        if (!aML || !hML){
          // try competitors mapping
          if (Array.isArray(comp.competitors)){
            for (const c of comp.competitors){
              const ml = c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line;
              if (c.homeAway==="away") aML = aML || numStr(ml);
              if (c.homeAway==="home") hML = hML || numStr(ml);
            }
          }
        }
        // fallbacks
        aML = aML || numStr(o0.moneyLineAway || o0.awayTeamMoneyLine || o0.awayMl);
        hML = hML || numStr(o0.moneyLineHome || o0.homeTeamMoneyLine || o0.homeMl);
      };
      moneylineStrict();
    }

    const finalScore = isFinal(ev) ? `${away.score||""}-${home.score||""}` : "";
    const preStatus = fmtStatusPregameNoYear(ev.date);
    const weekLabel = (() => {
      const w = ev?.week?.number;
      if (Number.isFinite(w)) return `Week ${w}`;
      const tx = ev?.week?.text;
      return tx || "";
    })();

    if (!row1){
      // append at next available row (no gaps)
      append.push([
        gid,                               // A Game ID
        fmtDateET(ev.date),                // B Date (with year)
        weekLabel,                         // C Week
        preStatus,                         // D Status (pregame only)
        matchup,                           // E Matchup
        finalScore,                        // F Final Score
        aSpread, aML, hSpread, hML,        // G..J lines
        total,                             // K Total
        "", "", "", "", "", ""             // L..Q half/live (left blank)
      ]);
      continue;
    }

    // Update in-place for existing rows
    const batchRow = new Batch(TAB_NAME);
    // Refresh lines pre-kickoff
    if (!isLive(ev) && !isFinal(ev)){
      if (H["a spread"]!=null) batchRow.set(row1, H["a spread"], aSpread || "");
      if (H["a ml"]!=null)     batchRow.set(row1, H["a ml"], aML || "");
      if (H["h spread"]!=null) batchRow.set(row1, H["h spread"], hSpread || "");
      if (H["h ml"]!=null)     batchRow.set(row1, H["h ml"], hML || "");
      if (H["total"]!=null)    batchRow.set(row1, H["total"], total || "");
      // set pregame status only if we aren't live and not final
      if (H["status"]!=null){
        const cur = (values[row1-1]?.[H["status"]]||"").toString();
        if (!DONT_TOUCH_STATUS_IF_LIVE || !/Q\d|HALF|IN PROGRESS/i.test(cur)){
          batchRow.set(row1, H["status"], preStatus);
        }
      }
    }

    if (isFinal(ev)){
      if (H["final score"]!=null) batchRow.set(row1, H["final score"], finalScore);
      if (H["status"]!=null)      batchRow.set(row1, H["status"], "Final");
    }

    await batchRow.flush(sheets);

    // Favorite underline (pregame only)
    if (!isLive(ev) && !isFinal(ev)){
      let favTeamName = "";
      if (o0?.favorite ?? o0?.favoriteTeamId){
        const fid = String(o0.favorite ?? o0.favoriteTeamId);
        favTeamName = String(away.team?.id) === fid ? awayName
                      : (String(home.team?.id) === fid ? homeName : "");
      } else if (aSpread && /^[+-]/.test(aSpread)){
        // Infer from spread signs if needed
        const as = parseFloat(aSpread);
        const hs = parseFloat(hSpread);
        favTeamName = (as<0) ? awayName : (hs<0 ? homeName : "");
      }

      const underlineSpan = favTeamName ? spanOfTeam(matchup, favTeamName) : null;
      const runs = buildTextRuns(matchup, underlineSpan, null);
      await writeMatchupWithRuns(sheets, sheetId, row1-1, matchup, runs);
    }

    // Winner bold (final only)
    if (isFinal(ev)){
      const a = Number(away.score||0), h = Number(home.score||0);
      const winner = a>h ? awayName : (h>a ? homeName : "");
      const boldSpan = winner ? spanOfTeam(matchup, winner) : null;
      const runs = buildTextRuns(matchup, null, boldSpan);
      await writeMatchupWithRuns(sheets, sheetId, row1-1, matchup, runs);
    }
  }

  if (append.length){
    // append right after the last non-empty row
    const startRow = (values.length ? values.length+1 : 2);
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A${startRow}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: append }
    });

    // After append, underline favorites for those brand-new rows
    const after = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Q` });
    const vals = after.data.values || [];
    const hdr = vals[0] || HEADER;
    const map = mapHeaderIdx(hdr);
    const nowRows = vals.slice(1);

    // Make a quick map gid->row1
    const idToRow = new Map();
    nowRows.forEach((r,i)=>{ const id=String(r[0]||"").trim(); if (id) idToRow.set(id, i+2); });

    for (const ev of events){
      const gid = String(ev.id);
      if (!idToRow.has(gid)) continue;               // only the appended ones
      const row1 = idToRow.get(gid);
      const comp = ev.competitions?.[0] || {};
      const away = comp.competitors?.find(c=>c.homeAway==="away");
      const home = comp.competitors?.find(c=>c.homeAway==="home");
      if (!away||!home) continue;

      const awayName = away.team?.shortDisplayName || away.team?.abbreviation || away.team?.name || "Away";
      const homeName = home.team?.shortDisplayName || home.team?.abbreviation || home.team?.name || "Home";
      const matchup  = `${awayName} @ ${homeName}`;

      const o0 = firstEspnBet(comp.odds || ev.odds || []);
      let favTeamName = "";
      if (o0?.favorite ?? o0?.favoriteTeamId){
        const fid = String(o0.favorite ?? o0.favoriteTeamId);
        favTeamName = String(away.team?.id) === fid ? awayName
                      : (String(home.team?.id) === fid ? homeName : "");
      }

      const underlineSpan = favTeamName ? spanOfTeam(matchup, favTeamName) : null;
      const runs = buildTextRuns(matchup, underlineSpan, null);
      await writeMatchupWithRuns(sheets, sheetId, row1-1, matchup, runs);
    }
  }

  if (GHA_JSON){
    process.stdout.write(JSON.stringify({ ok:true, tab:TAB_NAME, events: events.length })+"\n");
  } else {
    log("Done.");
  }
})().catch(err=>{
  if (GHA_JSON) process.stdout.write(JSON.stringify({ ok:false, error: String(err?.message||err) })+"\n");
  else { console.error(err); process.exit(1); }
});
