import { google } from "googleapis";

/** ===================== CONFIG ===================== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // nfl | college-football
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();

/**
 * PREFILL_MODE:
 *   - "off"     : skip appending new pregame rows, do only finals sweep
 *   - "today"   : append just ET-today’s games (for prefill jobs)
 *   - "week"    : ET today + next 6 days
 */
const PREFILL_MODE = (process.env.PREFILL_MODE || "off").toLowerCase();

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
  // anchor roughly to ET midnight for base
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(new Date(base));
  const y=+p.find(x=>x.type==="year").value, m=+p.find(x=>x.type==="month").value, d=+p.find(x=>x.type==="day").value;
  const etMid = new Date(Date.UTC(y, m-1, d, 5)); // ~UTC-5 anchor (ok for date lists)
  return new Date(etMid.getTime() + delta*86400000);
}

async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/3.0", "Referer": "https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const summaryUrl = (league, id) => `https://site.api.espn.com/apis/site/v2/sports/football/${league}/summary?event=${id}`;

const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};

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

/** Week label (simple) */
function resolveWeekLabel(sb, iso, lg) {
  if (normLeague(lg) === "nfl" && sb?.week?.number) return `Week ${sb.week.number}`;
  const t = (sb?.week?.text || "").trim();
  return t || "Regular Season";
}

/** Assign each team’s own spread (no guessing) */
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
  // fallback: try details text
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

  /** ---------- OPTIONAL: PREFILL APPEND ---------- */
  if (PREFILL_MODE !== "off") {
    let dates = [];
    if (PREFILL_MODE === "week") {
      for (let i = 0; i < 7; i++) dates.push(yyyymmddET(addDaysET(Date.now(), i)));
    } else { // today
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
    const keyToRow = new Map();
    existing.forEach((r,i) => {
      const row = i+2;
      const gid = (r[h["game id"]] || "").toString().trim();
      if (gid) idToRow.set(gid, row);
      keyToRow.set(keyOf(r[h["date"]], r[h["matchup"]]), row);
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
  // Re-read after potential append
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const hdr2 = snap.data.values?.[0] || header;
  const h2 = colMap(hdr2);
  const rowsNow = (snap.data.values || []).slice(1);

  // Any row with Game ID and empty Final Score is a candidate
  const candidates = [];
  rowsNow.forEach((r, i) => {
    const gid = (r[h2["game id"]] || "").toString().trim();
    const finalScore = (r[h2["final score"]] || "").toString().trim();
    if (gid && !finalScore) candidates.push({ row: i + 2, gid });
  });

  console.log(`Finals sweep: ${candidates.length} candidate row(s) with blank Final Score.`);

  let writes = 0;
  const lg = normLeague(LEAGUE);
  const updates = [];

  for (const c of candidates) {
    try {
      const sum = await fetchJson(summaryUrl(lg, c.gid));
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
        const idx = h2[name.toLowerCase()];
        if (idx == null) return;
        const col = String.fromCharCode("A".charCodeAt(0) + idx);
        updates.push({ range: `${TAB_NAME}!${col}${c.row}`, values: [[val]] });
      };
      add("final score", score);
      add("status", "Final");
      writes++;
      await new Promise(r => setTimeout(r, 120)); // mild pacing
    } catch (e) {
      console.error(`Finals sweep fetch failed for ${c.gid}:`, e.message || e);
    }
  }

  if (updates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: updates }
    });
  }

  console.log(`✅ Finals sweep complete. Rows updated: ${writes}`);
})().catch(e => { console.error("❌ Orchestrator fatal:", e); process.exit(1); });
