import { google } from "googleapis";

/** ========== CONFIG ========== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const PREFILL_MODE = (process.env.PREFILL_MODE || "week").toLowerCase();  // default to week

if (!SHEET_ID || !CREDS_RAW) {
  console.error("❌ Missing required environment variables: GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
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
  const y = +parts.find(p => p.type === "year").value,
        m = +parts.find(p => p.type === "month").value,
        d = +parts.find(p => p.type === "day").value;
  const etMid = new Date(Date.UTC(y, m - 1, d, 5));
  return new Date(etMid.getTime() + days * 86400000);
}
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/3.4", "Referer": "https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return r.json();
}

const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const summaryUrl = (l, id) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/summary?event=${id}`;

const pickOdds = arr =>
  Array.isArray(arr) && arr.length
    ? (arr.find(o => /espn\s*bet/i.test(o.provider?.name || "") ||
      /espn\s*bet/i.test(o.provider?.displayName || "")) || arr[0])
    : null;

const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
  return Number.isFinite(n) ? (s.startsWith("+") ? `+${n}` : `${n}`) : "";
};

const assignSpreads = (odds, away, home) => {
  if (!odds) return { awaySpread: "", homeSpread: "" };
  const raw = parseFloat(odds.spread ?? odds.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1] ?? NaN);
  if (Number.isNaN(raw)) return { awaySpread: "", homeSpread: "" };
  const mag = Math.abs(raw);
  const fav = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || ""), homeId = String(home?.team?.id || "");
  if (fav === awayId) return { awaySpread: `-${mag}`, homeSpread: `+${mag}` };
  if (fav === homeId) return { awaySpread: `+${mag}`, homeSpread: `-${mag}` };
  const det = (odds.details || "").toLowerCase();
  const aName = (away?.team?.abbreviation || "").toLowerCase();
  const hName = (home?.team?.abbreviation || "").toLowerCase();
  if (aName && det.includes(aName)) return { awaySpread: `-${mag}`, homeSpread: `+${mag}` };
  if (hName && det.includes(hName)) return { awaySpread: `+${mag}`, homeSpread: `-${mag}` };
  return { awaySpread: "", homeSpread: "" };
};

function resolveWeekLabel(sb, lg) {
  const n = sb?.week?.number;
  return Number.isFinite(n) ? `Week ${n}` : (sb?.week?.text || "Regular Season");
}

const colMap = (hdr = []) =>
  Object.fromEntries(hdr.map((h, i) => [(h || "").trim().toLowerCase(), i]));

/** ========== MAIN ========== */
(async () => {
  const CREDS = parseSA(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = google.sheets({ version: "v4", auth });

  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const vals = read.data.values || [];
  let header = vals[0] || [];
  if (!header.length) {
    header = [
      "Game ID","Date","Week","Status","Matchup","Final Score",
      "Away Spread","Away ML","Home Spread","Home ML","Total",
      "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [header] }
    });
  }
  const H = colMap(header);
  const idx = {
    gid: H["game id"] ?? H["id"],
    date: H["date"],
    week: H["week"],
    status: H["status"],
    matchup: H["matchup"],
    final: H["final score"],
    awaySpread: H["away spread"],
    awayML: H["away ml"],
    homeSpread: H["home spread"],
    homeML: H["home ml"],
    total: H["total"]
  };

  // ----- PREFILL MODE -----
  if (PREFILL_MODE === "week" || PREFILL_MODE === "today") {
    const dates =
      PREFILL_MODE === "week"
        ? Array.from({ length: 7 }, (_, i) => yyyymmddET(addDaysET(Date.now(), i)))
        : [yyyymmddET(new Date())];

    const scoreboards = (
      await Promise.allSettled(dates.map(d => fetchJson(scoreboardUrl(LEAGUE, d)).then(j => ({ d, j })))))
    ).filter(r => r.status === "fulfilled").map(r => r.value);

    const seen = new Set();
    const events = [];
    for (const { j } of scoreboards)
      for (const e of j?.events || [])
        if (!seen.has(e.id)) {
          seen.add(e.id);
          events.push(e);
        }

    const rows = vals.slice(1);
    const idToRow = new Map();
    rows.forEach((r, i) => {
      const gid = (r[idx.gid] || "").trim();
      if (gid) idToRow.set(gid, i + 2);
    });

    const append = [];
    const updates = [];
    const now = Date.now();

    for (const e of events) {
      const comp = e.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");
      const matchup = `${away?.team?.shortDisplayName || "Away"} @ ${home?.team?.shortDisplayName || "Home"}`;
      const kickoff = new Date(e.date).getTime();
      const diffHr = Math.abs((kickoff - now) / 3600000);

      const odds = pickOdds(comp.odds || e.odds || null);
      const { awaySpread, homeSpread } = assignSpreads(odds, away, home);
      const total = odds?.overUnder ?? odds?.total ?? "";
      const awayML = numOrBlank(odds?.awayTeamOdds?.moneyLine ?? odds?.awayTeamOdds?.moneyline);
      const homeML = numOrBlank(odds?.homeTeamOdds?.moneyLine ?? odds?.homeTeamOdds?.moneyline);
      const weekLbl = resolveWeekLabel(scoreboards[0]?.j || {}, LEAGUE);
      const status = fmtETKick(e.date);

      const row = idToRow.get(e.id);
      if (!row) {
        append.push([
          e.id, fmtETDate(e.date), weekLbl, status, matchup, "",
          awaySpread, awayML, homeSpread, homeML, String(total),
          "", "", "", "", "", ""
        ]);
      } else if (diffHr <= 2) {
        // Auto-refresh odds within 2h window
        const col = c => String.fromCharCode("A".charCodeAt(0) + c);
        const set = (i, v) => {
          if (i == null || v == "") return;
          updates.push({ range: `${TAB_NAME}!${col(i)}${row}`, values: [[v]] });
        };
        set(idx.awaySpread, awaySpread);
        set(idx.homeSpread, homeSpread);
        set(idx.awayML, awayML);
        set(idx.homeML, homeML);
        set(idx.total, String(total));
        set(idx.status, status);
        set(idx.week, weekLbl);
      }
    }

    if (append.length)
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: append }
      });
    if (updates.length)
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates }
      });

    console.log(`✅ Prefill complete. Appended ${append.length}, refreshed ${updates.length}.`);
  }

  // ----- FINALS SWEEP -----
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const rows2 = (snap.data.values || []).slice(1);
  const cands = [];
  rows2.forEach((r, i) => {
    const gid = (r[idx.gid] || "").trim();
    const fin = (r[idx.final] || "").trim();
    if (gid && !fin) cands.push({ row: i + 2, gid });
  });
  console.log(`Finals sweep: ${cands.length} candidate row(s) with blank Final Score.`);

  for (const c of cands) {
    try {
      const sum = await fetchJson(summaryUrl(LEAGUE, c.gid));
      const comp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
      const away = comp.competitors?.find(x => x.homeAway === "away");
      const home = comp.competitors?.find(x => x.homeAway === "home");
      const st = comp?.status?.type || {};
      const isFinal = /FINAL/i.test(st.name || "") || st.state === "post";
      if (!isFinal) continue;

      const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
      const data = [];
      const col = cIdx => String.fromCharCode("A".charCodeAt(0) + cIdx);
      data.push({ range: `${TAB_NAME}!${col(idx.final)}${c.row}`, values: [[score]] });
      if (idx.status != null)
        data.push({ range: `${TAB_NAME}!${col(idx.status)}${c.row}`, values: [["Final"]] });

      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data }
      });
    } catch (e) {
      console.error(`Error updating final for ${c.gid}:`, e.message);
    }
  }

  console.log("✅ Finals sweep complete.");
})();
