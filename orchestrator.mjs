import { google } from "googleapis";

/** ===== Config ===== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();           // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football" ? "CFB" : "NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase();      // "today" | "week"

if (!SHEET_ID || !CREDS_RAW) {
  console.error("Missing secrets.");
  process.exit(1);
}

/** ===== Helpers ===== */
function parseServiceAccount(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET_TZ = "America/New_York";

function fmtETDate(dLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dLike));
}
function yyyymmddInET(d = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(d);
  const g = k => parts.find(p => p.type === k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
async function fetchJson(url) {
  try {
    const r = await fetch(url, { headers: { "User-Agent": "orchestrator/2.1", "Referer": "https://www.espn.com/" } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    throw new Error(`Fetch failed for ${url}: ${e.message}`);
  }
}
function normLeague(x) {
  return (x === "ncaaf" || x === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, dates) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function mapHeadersToIndex(headerRow = []) {
  const m = {};
  headerRow.forEach((h, i) => m[(h || "").trim().toLowerCase()] = i);
  return m;
}
function keyOf(dateStr, matchup) {
  return `${(dateStr || "").trim()}__${(matchup || "").trim()}`;
}
function pickOdds(oddsArr = []) {
  if (!Array.isArray(oddsArr) || !oddsArr.length) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}
function numOrBlank(v) {
  if (v === 0) return "0";
  if (v == null) return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+") ? `+${n}` : `${n}`;
}

/** ===== Columns (Game ID first; no 'Last Pre-Half Status') ===== */
const COLS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
];

/** ===== Week label helpers ===== */
function resolveWeekLabel(sb, eventDateISO, league) {
  if (normLeague(league) === "nfl") {
    const w = sb?.week?.number;
    if (Number.isFinite(w)) return `Week ${w}`;
  } else {
    const t = (sb?.week?.text || "").trim();
    if (t) return t;
  }
  const cal = sb?.leagues?.[0]?.calendar || sb?.calendar || [];
  const t = new Date(eventDateISO).getTime();
  for (const item of cal) {
    const entries = Array.isArray(item?.entries) ? item.entries : [item];
    for (const e of entries) {
      const label = (e?.label || e?.detail || e?.text || "").trim();
      const s = new Date(e?.startDate || e?.start || 0).getTime();
      const ed = new Date(e?.endDate || e?.end || 0).getTime();
      if (t >= s && t <= ed) return label || "Regular Season";
    }
  }
  return "Regular Season";
}

/** ===== Main ===== */
(async function main() {
  const CREDS = parseServiceAccount(CREDS_RAW);
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

  const read = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z`
  });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [COLS] }
    });
    header = COLS.slice();
  } else {
    // upgrade headers to include new columns if missing
    const lower = header.map(h => (h || "").toLowerCase());
    for (const want of COLS) {
      if (!lower.includes(want.toLowerCase())) header.push(want);
    }
    if (header.length !== values[0].length) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW", requestBody: { values: [header] }
      });
    }
  }
  const hmap = mapHeadersToIndex(header);

  // Pull scoreboard for today or week
  const datesList = RUN_SCOPE === "week"
    ? Array.from({ length: 7 }, (_, i) => yyyymmddInET(new Date(Date.now() + i * 86400000)))
    : [yyyymmddInET(new Date())];

  let firstDaySB = null;
  let events = [];
  for (const d of datesList) {
    const url = scoreboardUrl(LEAGUE, d);
    const sb = await fetchJson(url);
    if (!firstDaySB) firstDaySB = sb;
    events = events.concat(Array.isArray(sb?.events) ? sb.events : []);
  }
  const seen = new Set();
  events = events.filter(e => e && !seen.has(e.id) && seen.add(e.id));
  console.log(`Events found: ${events.length}`);

  // Build index by (Date, Matchup)
  const existingRows = values.slice(1);
  const keyToRow = new Map();
  existingRows.forEach((r, i) => {
    const k = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
    keyToRow.set(k, i + 2);
  });

  // Append pregame rows if missing
  const appendBatch = [];
  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = fmtETDate(ev.date);
    const key = keyOf(dateET, matchup);
    if (keyToRow.has(key)) continue;

    const o0 = pickOdds(comp.odds || ev.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";
    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";
      const favId = String(o0.favorite || o0.favoriteTeamId || "");
      const sp = Number.isFinite(Number(o0.spread)) ? Number(o0.spread)
        : (typeof o0.spread === "string" ? parseFloat(o0.spread) : NaN);
      if (!Number.isNaN(sp) && favId) {
        if (String(away?.team?.id || "") === favId) {
          awaySpread = `-${Math.abs(sp)}`; homeSpread = `+${Math.abs(sp)}`;
        } else if (String(home?.team?.id || "") === favId) {
          homeSpread = `-${Math.abs(sp)}`; awaySpread = `+${Math.abs(sp)}`;
        }
      }
      // moneylines (best-effort)
      const aML = numOrBlank(o0.awayTeamOdds?.moneyLine ?? o0.awayTeamOdds?.moneyline);
      const hML = numOrBlank(o0.homeTeamOdds?.moneyLine ?? o0.homeTeamOdds?.moneyline);
      awayML = aML || ""; homeML = hML || "";
    }
    const weekText = resolveWeekLabel(firstDaySB, ev.date, LEAGUE);
    const scheduledTime = fmtETDate(ev.date); // you may prefer time, but Date column already holds date

    appendBatch.push([
      String(ev.id),           // Game ID
      dateET,                  // Date
      weekText || "",          // Week
      scheduledTime,           // Status (pregame display)
      matchup,                 // Matchup
      "",                      // Final Score
      awaySpread, awayML,      // Away Spread/ML
      homeSpread, homeML,      // Home Spread/ML
      String(total || ""),     // Total
      "", "", "", "", "", ""   // Half/Live columns (owned by live engine)
    ]);
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: appendBatch },
    });
    console.log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // Finals sweep
  const snap = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z`
  });
  const hdr2 = snap.data.values?.[0] || header;
  const h2 = mapHeadersToIndex(hdr2);
  const rowsNow = (snap.data.values || []).slice(1);
  const rowByKey = new Map();
  rowsNow.forEach((r, i) => {
    const k = keyOf(r[h2["date"]], r[h2["matchup"]]);
    rowByKey.set(k, i + 2);
  });

  let finalsWritten = 0;
  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || "Away";
    const homeName = home?.team?.shortDisplayName || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET = fmtETDate(ev.date);
    const rowNum = rowByKey.get(keyOf(dateET, matchup));
    if (!rowNum) continue;

    const statusName = (ev.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
    if (!statusName.includes("FINAL")) continue;

    const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;
    const data = [];
    const add = (name, val) => {
      const idx = h2[name.toLowerCase()]; if (idx == null) return;
      const col = String.fromCharCode("A".charCodeAt(0) + idx);
      data.push({ range: `${TAB_NAME}!${col}${rowNum}`, values: [[val]] });
    };
    add("final score", scorePair);
    add("status", "Final");

    if (data.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data }
      });
      finalsWritten++;
    }
  }

  console.log(`✅ Orchestrator complete. Finals written: ${finalsWritten}.`);
})().catch(err => {
  console.error("❌ Orchestrator fatal:", err.stack || err.message || String(err));
  process.exit(1);
});
