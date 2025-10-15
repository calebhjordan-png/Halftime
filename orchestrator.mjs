import { google } from "googleapis";

/** ===================== CONFIG ===================== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // nfl | college-football
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();

/**
 * PREFILL_MODE:
 *   - "off"     : skip appending new pregame rows (default for finals sweeper)
 *   - "today"   : append ET-today’s games (for prefill jobs)
 *   - "week"    : ET today + next 6 days
 */
const PREFILL_MODE = (process.env.PREFILL_MODE || "off").toLowerCase();

/**
 * UPDATE_SPREADS:
 *   - "0" / unset : do not touch existing spreads
 *   - "1"         : update/repair Away/Home Spread, MLs, Total for existing rows in a date ET window
 *                   window controlled by SPREAD_DAYS_BACK / SPREAD_DAYS_FWD (defaults 7 / 0)
 */
const UPDATE_SPREADS    = process.env.UPDATE_SPREADS === "1";
const SPREAD_DAYS_BACK  = Number(process.env.SPREAD_DAYS_BACK || 7);
const SPREAD_DAYS_FWD   = Number(process.env.SPREAD_DAYS_FWD  || 0);

if (!SHEET_ID || !CREDS_RAW) {
  console.error("Missing secrets.");
  process.exit(1);
}

/** ===================== HELPERS ===================== */
function parseSA(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET = "America/New_York";

function fmtETDate(d) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(d));
}
function yyyymmddET(dateLike) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(dateLike));
  const g = k => parts.find(p => p.type === k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
function addDaysET(base, delta) {
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(base));
  const y=+p.find(x=>x.type==="year").value, m=+p.find(x=>x.type==="month").value, d=+p.find(x=>x.type==="day").value;
  const etMid = new Date(Date.UTC(y, m-1, d, 5)); // ~UTC-5 anchor; good enough for day lists
  return new Date(etMid.getTime() + delta*86400000);
}
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/3.1", "Referer": "https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const summaryUrl   = (l, id) => `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/summary?event=${id}`;

const pickOdds = arr => Array.isArray(arr) && arr.length
  ? (arr.find(o => /espn\s*bet/i.test(o.provider?.name || "") || /espn\s*bet/i.test(o.provider?.displayName || "")) || arr[0])
  : null;

const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  return Number.isFinite(n) ? (s.startsWith("+") ? `+${n}` : `${n}`) : "";
};

const colMap = (hdr = []) => Object.fromEntries(hdr.map((h, i) => [(h || "").trim().toLowerCase(), i]));
const keyOf = (d, m) => `${(d || "").trim()}__${(m || "").trim()}`;

/** ===================== SHEET COLUMNS ===================== */
const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** Accept header aliases (e.g. "ID") */
function findIndexAlias(map, names) {
  for (const n of names) {
    const i = map[n.toLowerCase()];
    if (i != null) return i;
  }
  return null;
}

/** Week label (simple) */
function resolveWeekLabel(sb, iso, lg) {
  if (normLeague(lg) === "nfl" && sb?.week?.number) return `Week ${sb.week.number}`;
  const t = (sb?.week?.text || "").trim();
  return t || "Regular Season";
}

/** Assign each team’s own spread */
function assignSpreads(odds, away, home) {
  if (!odds) return { awaySpread: "", homeSpread: "" };
  const raw = parseFloat(odds.spread ?? odds.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1] ?? NaN);
  if (Number.isNaN(raw)) return { awaySpread: "", homeSpread: "" };
  const mag = Math.abs(raw);
  const fav = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || "");
  const homeId = String(home?.team?.id || "");
  if (fav === awayId) return { awaySpread: `-${mag}`, homeSpread: `+${mag}` };
  if (fav === homeId) return { awaySpread: `+${mag}`, homeSpread: `-${mag}` };
  const det = (odds.details || "").toLowerCase();
  const aName = (away?.team?.shortDisplayName || away?.team?.abbreviation || "").toLowerCase();
  const hName = (home?.team?.shortDisplayName || home?.team?.abbreviation || "").toLowerCase();
  if (aName && det.includes(aName)) return { awaySpread: `-${mag}`, homeSpread: `+${mag}` };
  if (hName && det.includes(hName)) return { awaySpread: `+${mag}`, homeSpread: `-${mag}` };
  return { awaySpread: "", homeSpread: "" };
}

/** ===================== MAIN ===================== */
(async () => {
  const CREDS = parseSA(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Ensure tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets || []).map(s => s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const vals = read.data.values || [];
  let header = vals[0] || [];
  if (!header.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [COLS] }
    });
    header = COLS;
  } else {
    const have = header.map(h => (h || "").toLowerCase());
    for (const want of COLS) if (!have.includes(want.toLowerCase())) header.push(want);
    if (header.length !== vals[0].length) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW", requestBody: { values: [header] }
      });
    }
  }
  const h = colMap(header);

  // --- resolve critical columns with aliases ---
  const idxGameId   = findIndexAlias(h, ["game id","id"]);
  const idxDate     = findIndexAlias(h, ["date"]);
  const idxMatchup  = findIndexAlias(h, ["matchup"]);
  const idxFinal    = findIndexAlias(h, ["final score"]);
  const idxStatus   = findIndexAlias(h, ["status"]);
  if (idxGameId == null || idxDate == null || idxMatchup == null || idxFinal == null || idxStatus == null) {
    console.error("Missing required headers. Need at least: Game ID/ID, Date, Matchup, Status, Final Score.");
    process.exit(1);
  }

  /** ---------- OPTIONAL: PREFILL APPEND ---------- */
  if (PREFILL_MODE !== "off") {
    let dates = [];
    if (PREFILL_MODE === "week") {
      for (let i = 0; i < 7; i++) dates.push(yyyymmddET(addDaysET(Date.now(), i)));
    } else {
      dates = [yyyymmddET(new Date())];
    }

    let firstSB = null, events = [];
    for (const d of dates) {
      const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
      if (!firstSB) firstSB = sb;
      events.push(...(Array.isArray(sb?.events) ? sb.events : []));
    }
    const seen = new Set();
    events = events.filter(e => e && !seen.has(e.id) && seen.add(e.id));

    const existing = vals.slice(1);
    const idToRow = new Map();
    existing.forEach((r,i) => {
      const row = i+2;
      const gid = (r[idxGameId] || "").toString().trim();
      if (gid) idToRow.set(gid, row);
    });

    const append = [];
    for (const e of events) {
      const comp = e.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");
      const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
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
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW", requestBody: { values: append }
      });
      console.log(`Prefill: appended ${append.length} row(s).`);
    }
  }

  /** ---------- FINALS SWEEP: SHEET-DRIVEN ---------- */
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const hdr2 = snap.data.values?.[0] || header;
  const rowsNow = (snap.data.values || []).slice(1);
  const h2 = colMap(hdr2);

  const idx2 = {
    gameId:  findIndexAlias(h2, ["game id","id"]),
    date:    findIndexAlias(h2, ["date"]),
    matchup: findIndexAlias(h2, ["matchup"]),
    final:   findIndexAlias(h2, ["final score"]),
    status:  findIndexAlias(h2, ["status"]),
    awaySpread: findIndexAlias(h2, ["away spread"]),
    homeSpread: findIndexAlias(h2, ["home spread"]),
    awayML:     findIndexAlias(h2, ["away ml"]),
    homeML:     findIndexAlias(h2, ["home ml"]),
    total:      findIndexAlias(h2, ["total"]),
  };

  const candidates = [];
  rowsNow.forEach((r, i) => {
    const gid = (r[idx2.gameId] || "").toString().trim();
    const finalScore = (r[idx2.final] || "").toString().trim();
    if (gid && !finalScore) candidates.push({ row: i + 2, gid });
  });

  console.log(`Finals sweep: ${candidates.length} candidate row(s) with blank Final Score.`);

  const updates = [];
  let finalsWrites = 0;

  for (const c of candidates) {
    try {
      const sum = await fetchJson(summaryUrl(LEAGUE, c.gid));
      const comp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
      const away = comp.competitors?.find(x => x.homeAway === "away");
      const home = comp.competitors?.find(x => x.homeAway === "home");
      const st   = comp?.status?.type || sum?.header?.competitions?.[0]?.status?.type || {};
      const statusName = (st.name || "").toUpperCase();
      const state      = (st.state || "").toLowerCase();
      const isFinal    = /FINAL/.test(statusName) || state === "post";
      if (!isFinal) continue;

      const score = `${away?.score ?? ""}-${home?.score ?? ""}`;

      const add = (name, val) => {
        const idx = idx2[name];
        if (idx == null) return;
        const col = String.fromCharCode("A".charCodeAt(0) + idx);
        updates.push({ range: `${TAB_NAME}!${col}${c.row}`, values: [[val]] });
      };
      add("final", score);
      add("status", "Final");
      finalsWrites++;
      await new Promise(r => setTimeout(r, 100));
    } catch (e) {
      console.error(`Finals fetch failed for ${c.gid}:`, e.message || e);
    }
  }

  /** ---------- OPTIONAL: REPAIR/UPDATE SPREADS FOR EXISTING ROWS ---------- */
  let spreadsWrites = 0;
  if (UPDATE_SPREADS) {
    // Build ET date set we need to fetch (for performance)
    const datesNeeded = new Set();
    rowsNow.forEach(r => {
      const dStr = (r[idx2.date] || "").toString().trim();
      if (!dStr) return;
      // simple filter: only include rows within the ET window
      const [m,d,y] = dStr.split("/").map(s => parseInt(s,10));
      const target = new Date(Date.UTC(y, m-1, d, 5)); // ~ET midnight
      const nowET0 = addDaysET(Date.now(), 0);
      const minET  = addDaysET(nowET0, -SPREAD_DAYS_BACK);
      const maxET  = addDaysET(nowET0,  SPREAD_DAYS_FWD);
      if (target >= minET && target <= maxET) datesNeeded.add(yyyymmddET(target));
    });

    const scoreboards = new Map();
    for (const d of datesNeeded) {
      scoreboards.set(d, await fetchJson(scoreboardUrl(LEAGUE, d)));
      await new Promise(r => setTimeout(r, 120));
    }

    // Map eventId -> odds
    const oddsById = new Map();
    for (const d of datesNeeded) {
      const sb = scoreboards.get(d);
      for (const ev of (sb?.events || [])) {
        const comp = ev.competitions?.[0] || {};
        oddsById.set(String(ev.id), pickOdds(comp.odds || ev.odds || []));
      }
    }

    for (let i=0;i<rowsNow.length;i++) {
      const rowNum = i + 2;
      const r = rowsNow[i];
      const gid = (r[idx2.gameId] || "").toString().trim();
      if (!gid) continue;

      const odds = oddsById.get(gid);
      if (!odds) continue;

      // We need team identities to assign signs — grab from summary
      try {
        const sum = await fetchJson(summaryUrl(LEAGUE, gid));
        const comp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
        const away = comp.competitors?.find(x => x.homeAway === "away");
        const home = comp.competitors?.find(x => x.homeAway === "home");
        const { awaySpread, homeSpread } = assignSpreads(odds, away, home);
        const awayML = numOrBlank(odds?.awayTeamOdds?.moneyLine ?? odds?.awayTeamOdds?.moneyline);
        const homeML = numOrBlank(odds?.homeTeamOdds?.moneyLine ?? odds?.homeTeamOdds?.moneyline);
        const total  = odds?.overUnder ?? odds?.total ?? "";

        const addCell = (idx, val) => {
          if (idx == null) return;
          const col = String.fromCharCode("A".charCodeAt(0) + idx);
          updates.push({ range: `${TAB_NAME}!${col}${rowNum}`, values: [[val]] });
        };
        addCell(idx2.awaySpread, awaySpread);
        addCell(idx2.homeSpread, homeSpread);
        addCell(idx2.awayML,     awayML);
        addCell(idx2.homeML,     homeML);
        addCell(idx2.total,      String(total || ""));
        spreadsWrites++;
        await new Promise(r => setTimeout(r, 80));
      } catch (e) {
        console.error(`Spread repair failed for ${gid}:`, e.message || e);
      }
    }
  }

  if (updates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: updates }
    });
  }

  console.log(`✅ Finals written: ${finalsWrites} | Spreads updated: ${spreadsWrites}`);
})().catch(e => { console.error("❌ Orchestrator fatal:", e); process.exit(1); });
