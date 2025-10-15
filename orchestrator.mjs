import { google } from "googleapis";

/** CONFIG **/
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // nfl | college-football
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
/**
 * RUN_SCOPE:
 *   - "today"  : only ET-today
 *   - "week"   : ET today + next 6 days
 *   - "window" : rolling ET window of DAYS_BACK..DAYS_FWD around now
 */
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();
const DAYS_BACK = Number(process.env.DAYS_BACK || 1);
const DAYS_FWD  = Number(process.env.DAYS_FWD  || 0);

if (!SHEET_ID || !CREDS_RAW) { console.error("Missing secrets."); process.exit(1); }

/** HELPERS **/
function parseSA(raw) { return raw.trim().startsWith("{") ? JSON.parse(raw) : JSON.parse(Buffer.from(raw, "base64").toString("utf8")); }
const ET = "America/New_York";
function fmtETDate(d) { return new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"numeric",day:"numeric"}).format(new Date(d)); }
function yyyymmddET(dateLike) {
  const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(dateLike));
  const g = k => parts.find(p=>p.type===k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
function addDaysET(base, delta) {
  // anchor: ET midnight for base
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(base));
  const y=+p.find(x=>x.type==="year").value, m=+p.find(x=>x.type==="month").value, d=+p.find(x=>x.type==="day").value;
  const etMid = new Date(Date.UTC(y, m-1, d, 5)); // ~ET=UTC-5 anchor (DST-safe enough for dates list)
  return new Date(etMid.getTime() + delta*86400000);
}
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent":"orchestrator/2.5", "Referer":"https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const pickOdds = arr => Array.isArray(arr) && arr.length ? (arr.find(o=>/espn\s*bet/i.test(o.provider?.name||"")||/espn\s*bet/i.test(o.provider?.displayName||"")) || arr[0]) : null;
const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g,""));
  return Number.isFinite(n) ? (s.startsWith("+")?`+${n}`:`${n}`) : "";
};
const colMap = (hdr=[]) => Object.fromEntries(hdr.map((h,i)=>[(h||"").trim().toLowerCase(), i]));
const keyOf = (d, m) => `${(d||"").trim()}__${(m||"").trim()}`;

/** COLUMNS **/
const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

function resolveWeekLabel(sb, iso, lg) {
  if (normLeague(lg) === "nfl" && sb?.week?.number) return `Week ${sb.week.number}`;
  const t = (sb?.week?.text || "").trim();
  return t || "Regular Season";
}

/** Spread assignment per team (away/home true values) **/
function assignSpreads(odds, away, home) {
  if (!odds) return { awaySpread:"", homeSpread:"" };
  const raw = parseFloat(odds.spread ?? odds.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1] ?? NaN);
  if (Number.isNaN(raw)) return { awaySpread:"", homeSpread:"" };
  const mag = Math.abs(raw);
  const fav = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || "");
  const homeId = String(home?.team?.id || "");
  if (fav === awayId) return { awaySpread:`-${mag}`, homeSpread:`+${mag}` };
  if (fav === homeId) return { awaySpread:`+${mag}`, homeSpread:`-${mag}` };
  // fallback parse by details text
  const det = (odds.details || "").toLowerCase();
  const aName = (away?.team?.shortDisplayName || away?.team?.abbreviation || "").toLowerCase();
  const hName = (home?.team?.shortDisplayName || home?.team?.abbreviation || "").toLowerCase();
  if (aName && det.includes(aName)) return { awaySpread:`-${mag}`, homeSpread:`+${mag}` };
  if (hName && det.includes(hName)) return { awaySpread:`+${mag}`, homeSpread:`-${mag}` };
  return { awaySpread:"", homeSpread:"" };
}

/** MAIN **/
(async ()=>{
  const CREDS = parseSA(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version:"v4", auth });

  // Ensure tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets||[]).map(s=>s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range:`${TAB_NAME}!A1:Z` });
  const vals = read.data.values || [];
  let header = vals[0] || [];
  if (!header.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW", requestBody:{ values:[COLS] }
    });
    header = COLS;
  } else {
    const have = header.map(h=>(h||"").toLowerCase());
    for (const want of COLS) if (!have.includes(want.toLowerCase())) header.push(want);
    if (header.length !== vals[0].length) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range:`${TAB_NAME}!A1`,
        valueInputOption:"RAW", requestBody:{ values:[header] }
      });
    }
  }
  const h = colMap(header);

  // Build date list
  let dates = [];
  if (RUN_SCOPE === "week") {
    for (let i=0;i<7;i++) dates.push(yyyymmddET(addDaysET(Date.now(), i)));
  } else if (RUN_SCOPE === "window") {
    for (let d=-DAYS_BACK; d<=DAYS_FWD; d++) dates.push(yyyymmddET(addDaysET(Date.now(), d)));
  } else {
    dates = [ yyyymmddET(new Date()) ];
  }

  // Fetch events
  let firstSB = null;
  let events = [];
  for (const d of dates) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!firstSB) firstSB = sb;
    events.push(...(Array.isArray(sb?.events) ? sb.events : []));
  }
  const seen = new Set();
  events = events.filter(e => e && !seen.has(e.id) && seen.add(e.id));
  console.log(`Events in scope: ${events.length} across ${dates.join(",")}`);

  // Index existing rows
  const rows = vals.slice(1);
  const idToRow = new Map();
  const keyToRow = new Map();
  rows.forEach((r,i)=>{
    const row = i+2;
    const gid = (r[h["game id"]]||"").toString().trim();
    if (gid) idToRow.set(gid, row);
    keyToRow.set(keyOf(r[h["date"]], r[h["matchup"]]), row);
  });

  // Append missing rows (pregame)
  const append = [];
  for (const e of events) {
    const comp = e.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
    const dateET = fmtETDate(e.date);
    const gid = String(e.id);
    if (idToRow.has(gid)) continue;

    const odds = pickOdds(comp.odds || e.odds || []);
    const { awaySpread, homeSpread } = assignSpreads(odds, away, home);
    const total  = odds?.overUnder ?? odds?.total ?? "";
    const awayML = numOrBlank(odds?.awayTeamOdds?.moneyLine ?? odds?.awayTeamOdds?.moneyline);
    const homeML = numOrBlank(odds?.homeTeamOdds?.moneyLine ?? odds?.homeTeamOdds?.moneyline);
    const week   = resolveWeekLabel(firstSB, e.date, LEAGUE);

    append.push([gid, dateET, week, fmtETDate(e.date), matchup, "",
      awaySpread, awayML, homeSpread, homeML, String(total),
      "", "", "", "", "", ""]);
  }
  if (append.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range:`${TAB_NAME}!A1`,
      valueInputOption:"RAW", requestBody:{ values: append }
    });
    console.log(`Appended ${append.length} new rows.`);
  }

  // Finals sweep (Game ID first, robust final detection)
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range:`${TAB_NAME}!A1:Z` });
  const hdr2 = snap.data.values?.[0] || header;
  const h2   = colMap(hdr2);
  const rowsNow = (snap.data.values || []).slice(1);

  const idMap  = new Map();
  const keyMap = new Map();
  rowsNow.forEach((r,i)=>{
    const row = i+2;
    const gid = (r[h2["game id"]]||"").toString().trim();
    if (gid) idMap.set(gid, row);
    keyMap.set(keyOf(r[h2["date"]], r[h2["matchup"]]), row);
  });

  let finalsWritten = 0;
  for (const e of events) {
    const comp = e.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");
    const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
    const dateET  = fmtETDate(e.date);

    const statusName = (e.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const state      = (e.status?.type?.state || comp.status?.type?.state || "").toLowerCase();
    const isFinal    = /FINAL/.test(statusName) || state === "post";
    if (!isFinal) continue;

    const row = idMap.get(String(e.id)) || keyMap.get(keyOf(dateET, matchup));
    if (!row) continue;

    const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
    const updates = [];
    const add = (name,val)=>{
      const idx = h2[name.toLowerCase()];
      if (idx == null) return;
      const col = String.fromCharCode("A".charCodeAt(0)+idx);
      updates.push({ range:`${TAB_NAME}!${col}${row}`, values:[[val]] });
    };
    add("final score", score);
    add("status", "Final");

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption:"RAW", data: updates }
      });
      finalsWritten++;
    }
  }

  console.log(`✅ Finals written: ${finalsWritten}`);
})().catch(e => { console.error("❌ Orchestrator fatal:", e); process.exit(1); });
