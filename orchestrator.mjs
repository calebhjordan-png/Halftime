import { google } from "googleapis";

/** CONFIG **/
const SHEET_ID  = process.env.GOOGLE_SHEET_ID?.trim();
const CREDS_RAW = process.env.GOOGLE_SERVICE_ACCOUNT?.trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();

if (!SHEET_ID || !CREDS_RAW) {
  console.error("Missing secrets.");
  process.exit(1);
}

/** HELPERS **/
function parseServiceAccount(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET = "America/New_York";
const fmtETDate = d => new Intl.DateTimeFormat("en-US", { timeZone: ET, year: "numeric", month: "numeric", day: "numeric" }).format(new Date(d));
const yyyymmddInET = (d = new Date()) => {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: ET, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(d);
  const g = k => parts.find(p => p.type === k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
};
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/2.4" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const pickOdds = arr => Array.isArray(arr) && arr.length ? (arr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) || arr[0]) : null;
const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  return Number.isFinite(n) ? (s.startsWith("+") ? `+${n}` : `${n}`) : "";
};
const colMap = (hdr = []) => Object.fromEntries(hdr.map((h, i) => [(h || "").trim().toLowerCase(), i]));
const keyOf = (d, m) => `${(d || "").trim()}__${(m || "").trim()}`;

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

/** Spread assignment per team **/
function assignSpreads(odds, away, home) {
  if (!odds) return { awaySpread: "", homeSpread: "" };
  const raw = parseFloat(odds.spread ?? odds.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1] ?? NaN);
  if (Number.isNaN(raw)) return { awaySpread: "", homeSpread: "" };
  const fav = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || "");
  const homeId = String(home?.team?.id || "");
  if (fav === awayId) return { awaySpread: `-${Math.abs(raw)}`, homeSpread: `+${Math.abs(raw)}` };
  if (fav === homeId) return { awaySpread: `+${Math.abs(raw)}`, homeSpread: `-${Math.abs(raw)}` };
  // fallback: use details text
  const d = (odds.details || "").toLowerCase();
  const a = (away?.team?.abbreviation || away?.team?.shortDisplayName || "").toLowerCase();
  const h = (home?.team?.abbreviation || home?.team?.shortDisplayName || "").toLowerCase();
  if (d.includes(a)) return { awaySpread: `-${Math.abs(raw)}`, homeSpread: `+${Math.abs(raw)}` };
  if (d.includes(h)) return { awaySpread: `+${Math.abs(raw)}`, homeSpread: `-${Math.abs(raw)}` };
  return { awaySpread: "", homeSpread: "" };
}

/** MAIN **/
(async () => {
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // header check
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
  }
  const hmap = colMap(header);

  const dates = RUN_SCOPE === "week"
    ? Array.from({ length: 7 }, (_, i) => yyyymmddInET(new Date(Date.now() + i * 86400000)))
    : [yyyymmddInET(new Date())];

  let firstSB = null, events = [];
  for (const d of dates) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!firstSB) firstSB = sb;
    events.push(...(Array.isArray(sb?.events) ? sb.events : []));
  }
  const seen = new Set();
  events = events.filter(e => e && !seen.has(e.id) && seen.add(e.id));
  console.log(`Events: ${events.length}`);

  // Index existing rows
  const rows = vals.slice(1);
  const idToRow = new Map();
  const keyToRow = new Map();
  rows.forEach((r, i) => {
    const row = i + 2;
    const id = (r[hmap["game id"]] || "").toString().trim();
    if (id) idToRow.set(id, row);
    keyToRow.set(keyOf(r[hmap["date"]], r[hmap["matchup"]]), row);
  });

  // Append missing rows
  const append = [];
  for (const e of events) {
    const comp = e.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
    const dateET = fmtETDate(e.date);
    const gid = String(e.id);
    if (idToRow.has(gid)) continue;
    const o = pickOdds(comp.odds || e.odds || []);
    const { awaySpread, homeSpread } = assignSpreads(o, away, home);
    const total = o?.overUnder ?? o?.total ?? "";
    const awayML = numOrBlank(o?.awayTeamOdds?.moneyLine ?? o?.awayTeamOdds?.moneyline);
    const homeML = numOrBlank(o?.homeTeamOdds?.moneyLine ?? o?.homeTeamOdds?.moneyline);
    const week = resolveWeekLabel(firstSB, e.date, LEAGUE);
    append.push([gid, dateET, week, fmtETDate(e.date), matchup, "", awaySpread, awayML, homeSpread, homeML, String(total), "", "", "", "", "", ""]);
  }
  if (append.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: append }
    });
    console.log(`Added ${append.length} new games.`);
  }

  // Finals sweep
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const hdr2 = snap.data.values?.[0] || header;
  const h2 = colMap(hdr2);
  const rowsNow = (snap.data.values || []).slice(1);
  const idMap = new Map();
  const keyMap = new Map();
  rowsNow.forEach((r, i) => {
    const row = i + 2;
    const gid = (r[h2["game id"]] || "").toString().trim();
    if (gid) idMap.set(gid, row);
    keyMap.set(keyOf(r[h2["date"]], r[h2["matchup"]]), row);
  });

  let finals = 0;
  for (const e of events) {
    const comp = e.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
    const dateET = fmtETDate(e.date);
    const row = idMap.get(String(e.id)) || keyMap.get(keyOf(dateET, matchup));
    if (!row) continue;

    const status = (e.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    const state = (e.status?.type?.state || comp.status?.type?.state || "").toLowerCase();
    const isFinal = /FINAL/.test(status) || state === "post";
    if (!isFinal) continue;

    const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
    const data = [];
    const add = (name, val) => {
      const idx = h2[name.toLowerCase()];
      if (idx == null) return;
      const col = String.fromCharCode("A".charCodeAt(0) + idx);
      data.push({ range: `${TAB_NAME}!${col}${row}`, values: [[val]] });
    };
    add("final score", score);
    add("status", "Final");

    if (data.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data }
      });
      finals++;
    }
  }
  console.log(`✅ Finals written: ${finals}`);
})().catch(e => {
  console.error("❌ Orchestrator fatal:", e);
  process.exit(1);
});
