import { google } from "googleapis";

/** ====== CONFIG ====== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE==="college-football"?"CFB":"NFL")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "today").toLowerCase(); // "today" | "week"

/** ====== Helpers ====== */
function parseServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT is empty");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET_TZ = "America/New_York";
const log = (...a)=>console.log(...a);

function fmtETTime(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true
  }).format(new Date(dateLike));
}
function fmtETDate(dateLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dateLike));
}
function yyyymmddInET(d=new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(d));
  const g = t => parts.find(p=>p.type===t)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
async function fetchJson(url) {
  const res = await fetch(url, { headers: { "User-Agent": "orchestrator/trimmed", "Referer":"https://www.espn.com/" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}
function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function scoreboardUrl(league, dates) {
  const lg = normLeague(league);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function mapHeadersToIndex(headerRow) {
  const map = {}; headerRow.forEach((h,i)=> map[(h||"").trim().toLowerCase()] = i);
  return map;
}
function keyOf(dateStr, matchup) { return `${(dateStr||"").trim()}__${(matchup||"").trim()}`; }
function pickOdds(oddsArr=[]) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn bet/i.test(o.provider?.displayName || ""));
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

/** ===== Headers we own ===== */
const COLS = [
  "Date","Week","Status","Matchup","Final Score",
  "Away Spread","Away ML","Home Spread","Home ML","Total",
  "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total",
  "Game ID","Last Pre-Half Status"
];

/** ===== Status formatting (ET) ===== */
function tidyStatus(evt) {
  const comp = evt.competitions?.[0] || {};
  const tName = (evt.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
  const short = (evt.status?.type?.shortDetail || comp.status?.type?.shortDetail || "").trim();
  if (tName.includes("FINAL")) return "Final";
  if (tName.includes("HALFTIME")) return "Half";
  if (tName.includes("IN_PROGRESS") || tName.includes("LIVE")) return short || "In Progress";
  return fmtETTime(evt.date); // Scheduled time in ET
}

/** ===== Week label ===== */
function weekLabel(sb, eventDateISO) {
  const lg = normLeague(LEAGUE);
  if (lg === "nfl") {
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
      const ed= new Date(e?.endDate   || e?.end   || 0).getTime();
      if (t >= s && t <= ed) return label || "Regular Season";
    }
  }
  return "Regular Season";
}

/** ===== Moneylines extractor (PickCenter shapes, variants) ===== */
function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";
  if (o && (o.awayTeamOdds || o.homeTeamOdds)) {
    awayML = numOrBlank(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline);
    homeML = numOrBlank(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline);
    if (awayML || homeML) return { awayML, homeML };
  }
  if (o && o.moneyline && (o.moneyline.away || o.moneyline.home)) {
    const a = numOrBlank(o.moneyline.away?.close?.odds ?? o.moneyline.away?.open?.odds);
    const h = numOrBlank(o.moneyline.home?.close?.odds ?? o.moneyline.home?.open?.odds);
    if (a || h) return { awayML:a, homeML:h };
  }
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline);
      if (!ml) continue;
      if (tid === String(awayId)) awayML = awayML || ml;
      if (tid === String(homeId)) homeML = homeML || ml;
    }
    if (awayML || homeML) return { awayML, homeML };
  }
  if (Array.isArray(o?.competitors)) {
    const findML = c => numOrBlank(c?.moneyLine ?? c?.moneyline ?? c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const aML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(awayId)));
    const hML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(homeId)));
    if (aML || hML) return { awayML:aML||"", homeML:hML||"" };
  }
  return { awayML, homeML };
}

/** ===== Pregame row builder (no halftime) ===== */
function pregameRowFactory(sbForDay) {
  return function pregameRow(event) {
    const comp = event.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup  = `${awayName} @ ${homeName}`;
    const dateET   = fmtETDate(event.date);
    const weekText = weekLabel(sbForDay, event.date);
    const status   = tidyStatus(event);

    // Odds (pregame)
    const o0 = pickOdds(comp.odds || event.odds || []);
    let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";
    if (o0) {
      total = (o0.overUnder ?? o0.total) ?? "";
      const favId = String(o0.favorite || o0.favoriteTeamId || "");
      // spread sign by favorite
      const sp = Number.isFinite(Number(o0.spread)) ? Number(o0.spread) :
                 (typeof o0.spread === "string" ? parseFloat(o0.spread) : NaN);
      if (!Number.isNaN(sp) && favId) {
        if (String(away?.team?.id||"") === favId) {
          awaySpread = `-${Math.abs(sp)}`; homeSpread = `+${Math.abs(sp)}`;
        } else if (String(home?.team?.id||"") === favId) {
          homeSpread = `-${Math.abs(sp)}`; awaySpread = `+${Math.abs(sp)}`;
        }
      }
      const ml = extractMoneylines(o0, away?.team?.id, home?.team?.id, comp.competitors||[]);
      awayML = ml.awayML || ""; homeML = ml.homeML || "";
    }

    const finalScore = /FINAL/i.test(event.status?.type?.name || comp.status?.type?.name || "")
      ? `${away?.score ?? ""}-${home?.score ?? ""}` : "";

    return {
      values: [
        dateET, weekText, status, matchup, finalScore,
        awaySpread, String(awayML||""), homeSpread, String(homeML||""), String(total||""),
        "", "", "", "", "", "",  // live columns (not our responsibility)
        String(event.id),        // Game ID
        ""                       // Last Pre-Half Status (written by live engine)
      ],
      dateET, matchup
    };
  };
}

/** ====== MAIN (no halftime logic) ====== */
(async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing secrets.");
    process.exit(1);
  }
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
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const values = read.data.values || [];
  let header = values[0] || [];
  if (header.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: [COLS] }
    });
    header = COLS.slice();
  } else {
    // upgrade header if missing new columns
    const lower = header.map(h=>(h||"").toLowerCase());
    const wantLower = COLS.map(c=>c.toLowerCase());
    if (wantLower.some(c=>!lower.includes(c))) {
      const merged = Array.from(new Set([...header, ...COLS])).slice(0, COLS.length);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW", requestBody: { values: [merged] }
      });
      header = merged;
    }
  }
  const hmap = mapHeadersToIndex(header);

  const datesList = RUN_SCOPE === "week"
    ? (()=>{ const start = new Date(); return Array.from({length:7}, (_,i)=> yyyymmddInET(new Date(start.getTime()+i*86400000))); })()
    : [ yyyymmddInET(new Date()) ];

  // Pull events
  let firstDaySB = null;
  let events = [];
  for (const d of datesList) {
    const sb = await fetchJson(scoreboardUrl(LEAGUE, d));
    if (!firstDaySB) firstDaySB = sb;
    events = events.concat(sb?.events || []);
  }
  const seen = new Set(); events = events.filter(e => !seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  // Build pregame rows & index existing rows
  const rows = values.slice(1);
  const keyToRowNum = new Map();
  rows.forEach((r, i) => {
    const key = keyOf(r[hmap["date"]], r[hmap["matchup"]]);
    keyToRowNum.set(key, i + 2);
  });

  const buildPregame = pregameRowFactory(firstDaySB);
  const appendBatch = [];
  for (const ev of events) {
    const { values: rowVals, dateET, matchup } = buildPregame(ev);
    const k = keyOf(dateET, matchup);
    if (!keyToRowNum.has(k)) appendBatch.push(rowVals);
  }
  if (appendBatch.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW", requestBody: { values: appendBatch },
    });
    log(`✅ Appended ${appendBatch.length} pregame row(s).`);
  }

  // Finals pass (no halftime writes here)
  const fresh = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const hdr2 = fresh.data.values?.[0] || header;
  const h2 = mapHeadersToIndex(hdr2);
  const current = (fresh.data.values || []).slice(1);
  const rowByKey = new Map();
  current.forEach((r,i)=> rowByKey.set(keyOf(r[h2["date"]], r[h2["matchup"]]), i+2));

  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
    const dateET = fmtETDate(ev.date);
    const rowNum = rowByKey.get(keyOf(dateET, matchup));
    if (!rowNum) continue;

    const isFinal = /FINAL/i.test(ev.status?.type?.name || comp.status?.type?.name || "");
    if (isFinal) {
      const scorePair = `${away?.score ?? ""}-${home?.score ?? ""}`;
      const data = [];
      const add = (name,val) => {
        const idx = h2[name.toLowerCase()]; if (idx==null) return;
        const col = String.fromCharCode("A".charCodeAt(0)+idx);
        data.push({ range:`${TAB_NAME}!${col}${rowNum}`, values:[[val]] });
      };
      add("final score", scorePair);
      add("status", "Final");
      if (data.length) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID, requestBody: { valueInputOption:"RAW", data }
        });
      }
    }
  }

  log("✅ Orchestrator complete (pregame + finals only).");
})().catch(err => { console.error("❌ Error:", err); process.exit(1); });
