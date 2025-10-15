import { google } from "googleapis";

/** ========== CONFIG ========== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
/**
 * PREFILL_MODE: "off" | "today" | "week"
 *   - "today": prefill ET-today only
 *   - "week" : prefill ET next 7 days
 *   - "off"  : skip prefill (finals-only sweep)
 */
const PREFILL_MODE = (process.env.PREFILL_MODE || "off").toLowerCase();

/** ========== GUARDS ========== */
if (!SHEET_ID || !CREDS_RAW) {
  console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT.");
  process.exit(1);
}

/** ========== HELPERS ========== */
function parseSA(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET_TZ = "America/New_York";

function fmtETDate(dLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dLike));
}
function fmtETKick(dLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true
  }).format(new Date(dLike)).replace(" ", "").toUpperCase() + " ET";
}
function yyyymmddET(dateLike) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(dateLike));
  const g = k => parts.find(p => p.type === k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
function addDaysET(base, days) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit" })
    .formatToParts(new Date(base));
  const y=+parts.find(p=>p.type==="year").value, m=+parts.find(p=>p.type==="month").value, d=+parts.find(p=>p.type==="day").value;
  const etMid = new Date(Date.UTC(y, m-1, d, 5)); // ~ET midnight
  return new Date(etMid.getTime() + days*86400000);
}
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/3.3", "Referer": "https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const summaryUrl = (l, id) => `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/summary?event=${id}`;

const pickOdds = arr => Array.isArray(arr) && arr.length
  ? (arr.find(o => /espn\s*bet/i.test(o.provider?.name || "") || /espn\s*bet/i.test(o.provider?.displayName || "")) || arr[0])
  : null;

const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g,""));
  return Number.isFinite(n) ? (s.startsWith("+")?`+${n}`:`${n}`) : "";
};

const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

const toLowerMap = (a=[]) => { const m={}; a.forEach((h,i)=>m[(h||"").toLowerCase()]=i); return m; };
const findIdx = (m, names) => { for (const n of names) { const i = m[n.toLowerCase()]; if (i!=null) return i; } return null; };

/** Team-true spread assignment */
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
  const det=(odds.details||"").toLowerCase();
  const aName=(away?.team?.shortDisplayName||away?.team?.abbreviation||"").toLowerCase();
  const hName=(home?.team?.shortDisplayName||home?.team?.abbreviation||"").toLowerCase();
  if (aName && det.includes(aName)) return { awaySpread:`-${mag}`, homeSpread:`+${mag}` };
  if (hName && det.includes(hName)) return { awaySpread:`+${mag}`, homeSpread:`-${mag}` };
  return { awaySpread:"", homeSpread:"" };
}

/** Week label */
function resolveWeekLabel(sb, lg) {
  const lgN = normLeague(lg);
  if (lgN === "nfl") {
    const n = sb?.week?.number;
    return Number.isFinite(n) ? `Week ${n}` : "Week ?";
  } else {
    const n = sb?.week?.number;
    return Number.isFinite(n) ? `Week ${n}` : (sb?.week?.text || "Regular Season");
  }
}

/** ========== MAIN ========== */
(async () => {
  const CREDS = parseSA(CREDS_RAW);
  const auth  = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = google.sheets({ version:"v4", auth });

  // Ensure sheet + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets||[]).map(s=>s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const vals = read.data.values || [];
  let header = vals[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption:"RAW", requestBody: { values: [COLS] }
    });
    header = COLS.slice();
  } else {
    const lower = header.map(h=>(h||"").toLowerCase());
    for (const want of COLS) if (!lower.includes(want.toLowerCase())) header.push(want);
    if (header.length !== vals[0].length) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption:"RAW", requestBody: { values: [header] }
      });
    }
  }
  const H = toLowerMap(header);

  const idx = {
    gameId:     findIdx(H, ["game id","id"]),
    date:       findIdx(H, ["date"]),
    week:       findIdx(H, ["week"]),
    status:     findIdx(H, ["status"]),
    matchup:    findIdx(H, ["matchup"]),
    final:      findIdx(H, ["final score"]),
    awaySpread: findIdx(H, ["away spread"]),
    awayML:     findIdx(H, ["away ml"]),
    homeSpread: findIdx(H, ["home spread"]),
    homeML:     findIdx(H, ["home ml"]),
    total:      findIdx(H, ["total"]),
  };

  /* ---------------- Prefill: full week or today ---------------- */
  if (PREFILL_MODE === "today" || PREFILL_MODE === "week") {
    const dates = (PREFILL_MODE === "week")
      ? Array.from({length:7}, (_,i)=>yyyymmddET(addDaysET(Date.now(), i)))
      : [yyyymmddET(new Date())];

    // fetch all scoreboards in parallel
    const fetches = await Promise.allSettled(dates.map(d => fetchJson(scoreboardUrl(LEAGUE, d)).then(j => ({d, j}))));
    const boards  = fetches.filter(f=>f.status==="fulfilled").map(f=>f.value);
    if (boards.length === 0) console.log("No scoreboard data returned.");

    // de-dup events across days
    const seenIds = new Set();
    const events = [];
    for (const { j } of boards) {
      for (const e of (j?.events || [])) {
        if (!e || seenIds.has(e.id)) continue;
        seenIds.add(e.id);
        events.push(e);
      }
    }

    // current sheet rows indexed by Game ID
    const rows = vals.slice(1);
    const idToRow = new Map();
    rows.forEach((r,i)=>{
      const gid = (r[idx.gameId] || "").toString().trim();
      if (gid) idToRow.set(gid, i+2);
    });

    // prefill append or repair odds for existing rows
    const append = [];
    const updates = [];
    for (const e of events) {
      const comp = e.competitions?.[0] || {};
      const away = comp.competitors?.find(c=>c.homeAway==="away");
      const home = comp.competitors?.find(c=>c.homeAway==="home");
      const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
      const dateET  = fmtETDate(e.date);
      const gid     = String(e.id);

      // pick odds quickly from scoreboard
      const odds = pickOdds(comp.odds || e.odds || null);
      const { awaySpread, homeSpread } = assignSpreads(odds, away, home);
      const total  = odds?.overUnder ?? odds?.total ?? "";
      const awayML = numOrBlank(odds?.awayTeamOdds?.moneyLine ?? odds?.awayTeamOdds?.moneyline);
      const homeML = numOrBlank(odds?.homeTeamOdds?.moneyLine ?? odds?.homeTeamOdds?.moneyline);

      const weekLbl = resolveWeekLabel(boards[0]?.j || {}, LEAGUE); // same week across fetched days
      const schedStatus = fmtETKick(e.date); // kickoff time in ET

      const existingRow = idToRow.get(gid);

      if (!existingRow) {
        append.push([
          gid, dateET, weekLbl, schedStatus, matchup, "",
          awaySpread, awayML, homeSpread, homeML, String(total || ""),
          "", "", "", "", "", ""
        ]);
      } else {
        // repair blanks on existing row (odds/total were empty before)
        const rowIdx = existingRow;
        const pushCell = (cIndex, val) => {
          if (cIndex == null || val === "" || val == null) return;
          const col = String.fromCharCode("A".charCodeAt(0) + cIndex);
          updates.push({ range:`${TAB_NAME}!${col}${rowIdx}`, values:[[val]] });
        };
        // only fill cells that were blank
        const snapRow = rows[rowIdx-2] || [];
        if (!snapRow[idx.awaySpread]) pushCell(idx.awaySpread, awaySpread);
        if (!snapRow[idx.homeSpread]) pushCell(idx.homeSpread, homeSpread);
        if (!snapRow[idx.awayML])     pushCell(idx.awayML,     awayML);
        if (!snapRow[idx.homeML])     pushCell(idx.homeML,     homeML);
        if (!snapRow[idx.total])      pushCell(idx.total,      String(total || ""));
        if (!snapRow[idx.week])       pushCell(idx.week,       weekLbl);
        if (!snapRow[idx.status])     pushCell(idx.status,     schedStatus);
      }
    }

    if (append.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption:"RAW", requestBody: { values: append }
      });
      console.log(`Prefill: appended ${append.length} new game(s).`);
    }
    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption:"RAW", data: updates }
      });
      console.log(`Prefill: repaired ${updates.length} cell(s).`);
    }
  }

  /* ---------------- Finals sweep (sheet-driven) ---------------- */
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const hdr2 = snap.data.values?.[0] || header;
  const H2   = toLowerMap(hdr2);
  const rows2 = (snap.data.values || []).slice(1);

  const gi = findIdx(H2, ["game id","id"]);
  const fi = findIdx(H2, ["final score"]);
  const si = findIdx(H2, ["status"]);
  const candidates = [];
  rows2.forEach((r,i)=>{
    const gid=(r[gi]||"").toString().trim();
    const fin=(r[fi]||"").toString().trim();
    if (gid && !fin) candidates.push({ row:i+2, gid });
  });
  console.log(`Finals sweep: ${candidates.length} candidate row(s) with blank Final Score.`);

  // resolve finals in modest parallel (limit 5 at a time)
  let writes = 0;
  const chunks = (arr, n)=>arr.reduce((a,_,i)=>i%n?a:(a.push(arr.slice(i,i+n)),a),[]);
  for (const group of chunks(candidates, 5)) {
    const settled = await Promise.allSettled(group.map(async c => {
      const sum = await fetchJson(summaryUrl(LEAGUE, c.gid));
      const comp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
      const away = comp.competitors?.find(x=>x.homeAway==="away");
      const home = comp.competitors?.find(x=>x.homeAway==="home");
      const st   = comp?.status?.type || {};
      const statusName = (st.name || "").toUpperCase();
      const state      = (st.state || "").toLowerCase();
      const isFinal    = /FINAL/.test(statusName) || state === "post";
      if (!isFinal) return null;

      const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
      const data = [];
      const put = (colIdx,val)=>{
        const col = String.fromCharCode("A".charCodeAt(0) + colIdx);
        data.push({ range:`${TAB_NAME}!${col}${c.row}`, values:[[val]] });
      };
      put(fi, score);
      if (si != null) put(si, "Final");
      return data;
    }));

    const batch = [];
    for (const s of settled) if (s.status==="fulfilled" && s.value) batch.push(...s.value);
    if (batch.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption:"RAW", data: batch }
      });
      writes += group.length;
    }
  }

  console.log(`✅ Finals written: ${writes} | Spreads updated: 0`);
})().catch(e => { console.error("❌ Orchestrator fatal:", e); process.exit(1); });
