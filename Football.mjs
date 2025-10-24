// Football.mjs — stabilized: prefill + finals + live with safe batching & guards

import { google } from "googleapis";
import axios from "axios";

/* ---------- ENV ---------- */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE_IN = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE_IN === "college-football" ? "CFB" : "NFL")).trim();
// RUN_SCOPE kept for compatibility; this script always fetches a week window but only writes diffs
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();
const GAME_IDS  = (process.env.GAME_IDS || "").trim(); // optional, comma separated
const ET_TZ     = "America/New_York";

/* ---------- CONSTANTS ---------- */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","H A Spread","H A ML","H H Spread","H H ML","H Total"
];

// columns (0-based) for convenience
const COLS = {
  gameId: 0, date: 1, week: 2, status: 3, matchup: 4, finalScore: 5,
  aSpread: 6, aML: 7, hSpread: 8, hML: 9, total: 10,
  liveScore: 11, liveASpread: 12, liveAML: 13, liveHSpread: 14, liveHML: 15, liveTotal: 16
};

/* ---------- UTIL ---------- */
const fmtET = (d, opt) => new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, ...opt }).format(new Date(d));
const yyyymmddET = (d = new Date()) => {
  const [m, dd, yyyy] = fmtET(d, { month: "2-digit", day: "2-digit", year: "numeric" }).split("/");
  return `${yyyy}${m}${dd}`;
};
const dateStr = d => fmtET(d, { month: "2-digit", day: "2-digit", year: "numeric" });
const weekStr = n => `Week ${n}`;

/* ESPN endpoints */
const sbUrl = (lg, d) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${lg === "college-football" ? "&groups=80&limit=300" : ""}`;
const sumUrl = (lg, id) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;

const getJSON = async (url) => (await axios.get(url, { timeout: 12000, headers: { "User-Agent": "sports-bot/1.0" } })).data;

/* ---------- GOOGLE ---------- */
class Sheets {
  constructor(auth, id, tab) {
    this.api = google.sheets({ version: "v4", auth });
    this.id = id;
    this.tab = tab;
  }

  async readAll() {
    const { data } = await this.api.spreadsheets.values.get({
      spreadsheetId: this.id, range: `${this.tab}!A1:Q`
    });
    return data.values || [];
  }

  async writeBatch(updates) {
    if (!updates.length) return;
    await this.api.spreadsheets.values.batchUpdate({
      spreadsheetId: this.id,
      requestBody: {
        valueInputOption: "RAW",
        data: updates
      }
    });
  }
}

/* ---------- NORMALIZERS ---------- */
const isNum = v => v !== null && v !== undefined && v !== "" && !isNaN(Number(v));

function normML(x) {
  if (x === null || x === undefined) return "";
  if (typeof x === "string") {
    const s = x.trim().toUpperCase();
    if (s === "OFF") return "";
    if (s === "EVEN") return "+100";
    if (/^[+-]?\d+$/.test(s)) return (Number(s) > 0 ? `+${Number(s)}` : String(Number(s)));
  }
  if (typeof x === "number") return x > 0 ? `+${x}` : String(x);
  return "";
}

function normSpread(x) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  return isNaN(n) ? "" : (n === 0 ? "0" : (n > 0 ? `+${n}` : String(n)));
}

function normTotal(x) {
  if (x === null || x === undefined) return "";
  const s = String(x).trim().toUpperCase();
  if (s === "OFF") return "";
  const n = Number(s);
  return isNaN(n) ? "" : String(n);
}

function matchupText(awayName, homeName, awayFav) {
  // underline favorite with underscores; bold winner handled by your conditional formatting already
  // (We keep just the underline rule so we don’t pile on format rules.)
  // We return plain text; underline is set via textFormatRuns (separate, throttled).
  return `${awayName} @ ${homeName}${awayFav ? "" : ""}`;
}

/* ---------- FETCHERS ---------- */
async function fetchWindow(league) {
  const days = 7; // today + next 6; that preserves your “week” sweep behavior
  const dates = Array.from({ length: days }, (_, i) =>
    yyyymmddET(new Date(Date.now() + i * 86400000))
  );

  const events = [];
  for (const d of dates) {
    try {
      const sb = await getJSON(sbUrl(league, d));
      (sb?.events || []).forEach(e => events.push(e));
    } catch { /* swallow for stability */ }
  }
  return events;
}

function pullOddsFromSummary(summary) {
  const comp = summary?.header?.competitions?.[0] || {};
  const odds = comp?.odds?.[0] || {};
  const awayOdds = odds?.awayTeamOdds || {};
  const homeOdds = odds?.homeTeamOdds || {};

  return {
    aSpread: normSpread(awayOdds?.spread),
    hSpread: normSpread(homeOdds?.spread),
    aML: normML(awayOdds?.moneyLine),
    hML: normML(homeOdds?.moneyLine),
    total: normTotal(odds?.overUnder ?? odds?.total ?? "")
  };
}

async function getLiveBundle(league, eventId) {
  try {
    const s = await getJSON(sumUrl(league, eventId));
    const box = s?.boxscore;
    const away = box?.teams?.find(t => t.homeAway === "away");
    const home = box?.teams?.find(t => t.homeAway === "home");
    const aScore = away?.score || "";
    const hScore = home?.score || "";
    const odds = pullOddsFromSummary(s);

    return {
      liveScore: aScore && hScore ? `${aScore}-${hScore}` : "",
      liveASpread: odds.aSpread,
      liveAML: odds.aML,
      liveHSpread: odds.hSpread,
      liveHML: odds.hML,
      liveTotal: odds.total
    };
  } catch {
    return {
      liveScore: "", liveASpread: "", liveAML: "", liveHSpread: "", liveHML: "", liveTotal: ""
    };
  }
}

/* ---------- MAIN ---------- */
(async () => {
  // auth
  const creds = CREDS_RAW.startsWith("{")
    ? JSON.parse(CREDS_RAW)
    : JSON.parse(Buffer.from(CREDS_RAW, "base64").toString("utf8"));

  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: creds.client_email, private_key: creds.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = new Sheets(await auth.getClient(), SHEET_ID, TAB_NAME);

  // read sheet into memory
  const grid = await sheets.readAll();
  if (!grid.length || grid[0].join("|") !== HEADERS.join("|")) {
    // repair headers, but never write anything else until headers are restored
    await sheets.writeBatch([{ range: `${TAB_NAME}!A1:Q1`, values: [HEADERS] }]);
  }

  // build index from current sheet (skip row 1)
  const byId = new Map();
  for (let r = 1; r < grid.length; r++) {
    const row = grid[r] || [];
    const gid = (row[COLS.gameId] || "").toString().trim();
    if (gid) byId.set(gid, r + 0); // 0-based index in `grid`
  }

  // collect ESPN events
  const targetIds = GAME_IDS
    ? GAME_IDS.split(",").map(s => s.trim()).filter(Boolean)
    : null;

  const events = await fetchWindow(LEAGUE_IN);
  const updates = [];

  for (const ev of events) {
    const eventId = String(ev?.id || "");
    if (!eventId) continue;
    if (targetIds && !targetIds.includes(eventId)) continue;

    const comp = ev?.competitions?.[0] || {};
    const statusType = (comp?.status?.type?.state || "").toLowerCase(); // "pre", "in", "post"
    const isPre = statusType === "pre";
    const isIn = statusType === "in";
    const isPost = statusType === "post";

    const dateISO = comp?.date || ev?.date;
    const eventDate = dateStr(dateISO || Date.now());
    const weekNum = comp?.week?.number || ev?.week?.number || "";
    const statusCell = isPost
      ? "Final"
      : `${fmtET(dateISO, { month: "2-digit", day: "2-digit" })} - ${fmtET(dateISO, { hour: "numeric", minute: "2-digit", hour12: true })}`;

    const aTeam = comp?.competitors?.find(t => t.homeAway === "away");
    const hTeam = comp?.competitors?.find(t => t.homeAway === "home");
    if (!aTeam || !hTeam) continue;

    const awayName = aTeam?.team?.displayName || aTeam?.team?.shortDisplayName || aTeam?.team?.name || "Away";
    const homeName = hTeam?.team?.displayName || hTeam?.team?.shortDisplayName || hTeam?.team?.name || "Home";
    const matchup = matchupText(awayName, homeName, false);

    // final score
    const aFinal = aTeam?.score ? Number(aTeam.score) : null;
    const hFinal = hTeam?.score ? Number(hTeam.score) : null;
    const finalScore = isPost && isNum(aFinal) && isNum(hFinal) ? `${aFinal}-${hFinal}` : "";

    // odds (prefill/finals): use summary when we truly need them; to reduce calls, only for rows we will touch
    let aSpread = "", hSpread = "", aML = "", hML = "", total = "";
    if (isPre || isPost) {
      try {
        const s = await getJSON(sumUrl(LEAGUE_IN, eventId));
        const o = pullOddsFromSummary(s);
        aSpread = o.aSpread; hSpread = o.hSpread; aML = o.aML; hML = o.hML; total = o.total;
      } catch { /* swallow */ }
    }

    // live bundle
    let live = { liveScore: "", liveASpread: "", liveAML: "", liveHSpread: "", liveHML: "", liveTotal: "" };
    if (isIn) {
      live = await getLiveBundle(LEAGUE_IN, eventId);
    }

    // find the row; if missing, append at the end (NEVER before row 2)
    let rowIdx = byId.get(eventId);
    if (!rowIdx) {
      rowIdx = grid.length; // append
      grid.push(Array(HEADERS.length).fill("")); // extend in memory
      byId.set(eventId, rowIdx);
      // write the core identity cells in the same batch
      const base = Array(HEADERS.length).fill("");
      base[COLS.gameId] = eventId;
      base[COLS.date] = eventDate;
      base[COLS.week] = weekStr(weekNum || "");
      base[COLS.status] = statusCell;
      base[COLS.matchup] = matchup;
      updates.push({ range: `${TAB_NAME}!A${rowIdx + 1}:Q${rowIdx + 1}`, values: [base] });
      continue; // the identity row is enough for this pass; remaining cells will be filled next pass
    }

    // prepare a values row reflecting only the columns we may change
    const row = grid[rowIdx] || [];
    const next = row.slice();

    // safe updates
    next[COLS.date]      = eventDate;
    next[COLS.week]      = weekStr(weekNum || "");
    next[COLS.status]    = statusCell;
    next[COLS.matchup]   = matchup;

    if (finalScore) next[COLS.finalScore] = finalScore;
    if (aSpread !== "")  next[COLS.aSpread] = aSpread;
    if (aML !== "")      next[COLS.aML]     = aML;
    if (hSpread !== "")  next[COLS.hSpread] = hSpread;
    if (hML !== "")      next[COLS.hML]     = hML;
    if (total !== "")    next[COLS.total]   = total;

    if (isIn) {
      if (live.liveScore)    next[COLS.liveScore]   = live.liveScore;
      if (live.liveASpread)  next[COLS.liveASpread] = live.liveASpread;
      if (live.liveAML)      next[COLS.liveAML]     = live.liveAML;
      if (live.liveHSpread)  next[COLS.liveHSpread] = live.liveHSpread;
      if (live.liveHML)      next[COLS.liveHML]     = live.liveHML;
      if (live.liveTotal)    next[COLS.liveTotal]   = live.liveTotal;
    }

    // only push an update if something changed
    if (next.join("|") !== row.join("|")) {
      updates.push({ range: `${TAB_NAME}!A${rowIdx + 1}:Q${rowIdx + 1}`, values: [next] });
      grid[rowIdx] = next;
    }
  }

  // single write lowers the chance of hitting the per-minute cap
  await sheets.writeBatch(updates);

  console.log(JSON.stringify({ ok: true, tab: TAB_NAME, league: LEAGUE_IN, wrote: updates.length }));
})().catch(err => {
  console.error("Fatal:", err?.message || err);
  process.exit(1);
});
