// live-game.mjs
// Updates Status, Half Score, and Live Odds (M..Q) for live games.
// Debug mode and GAME_ID filtering supported.

import axios from "axios";
import { google } from "googleapis";

/* ───────────── ENV ───────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",
  TAB_NAME = LEAGUE === "nfl" ? "NFL" : "CFB",
  GAME_ID = "",
  MARKET_PREFERENCE = "live,in-play,inplay,2h,second half,halftime",
  DEBUG_MODE = "",
} = process.env;

const DEBUG = String(DEBUG_MODE).trim() === "1" || !!GAME_ID;

for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

/* ───────── Google Sheets ───────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────── Helpers ───────── */
const norm = (s) => (s || "").toLowerCase();

function idxToA1(n0) {
  let n = n0 + 1,
    s = "";
  while (n > 0) {
    n--;
    s = String.fromCharCode(65 + (n % 26)) + s;
    n = Math.floor(n / 26);
  }
  return s;
}

const todayKey = (() => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "2-digit",
    day: "2-digit",
    year: "2-digit",
  }).formatToParts(new Date());
  const mm = parts.find((p) => p.type === "month")?.value ?? "00";
  const dd = parts.find((p) => p.type === "day")?.value ?? "00";
  const yy = parts.find((p) => p.type === "year")?.value ?? "00";
  return `${mm}/${dd}/${yy}`;
})();

const looksLiveStatus = (s) =>
  /\bhalf\b|\bin\s*progress\b|\bq[1-4]\b|\bot\b|\blive\b/i.test(s || "");
const isFinalCell = (s) => /^final$/i.test(s || "");

/* ───────── ESPN FETCHERS ───────── */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  if (DEBUG) console.log(`🔎 Fetching summary: ${url}`);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function espnMarkets(gameId) {
  const url = `https://sports.core.api.espn.com/v2/markets?sport=football&league=${LEAGUE}&event=${gameId}`;
  if (DEBUG) console.log(`🔎 Fetching markets: ${url}`);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}
async function fetchRefMaybe(ref) {
  if (!ref || typeof ref !== "string") return {};
  try {
    const { data } = await axios.get(ref, { timeout: 10000 });
    return data || {};
  } catch {
    return {};
  }
}

/* ───────── STATUS / HALF SCORE ───────── */
const periodName = (n) =>
  ({ 1: "1st", 2: "2nd", 3: "3rd", 4: "4th" }[n] || (n ? `Q${n}` : ""));

function formatStatus(st) {
  if (!st) return "";
  const state = norm(st?.type?.state || st?.state || "");
  const clock = st.displayClock || st.clock || "";
  const period = st.period;
  const detail =
    st.type?.shortDetail || st.type?.detail || st.detail || st.description || "";

  if (state === "inprogress" || state === "live" || /in\s*progress/i.test(detail))
    return `${clock || ""} - ${periodName(period)}`;
  if (state === "halftime") return "Halftime";
  if (state === "endperiod" || /end of/i.test(detail))
    return `End of ${periodName((period || 0) - 1)}`;
  if (state === "final") return "Final";
  if (state === "pre" || state === "scheduled")
    return "Scheduled";
  return detail || "In Progress";
}

function sumFirstTwoPeriods(linescores) {
  if (!Array.isArray(linescores)) return null;
  return linescores.slice(0, 2).reduce((a, p) => a + (Number(p?.value) || 0), 0);
}
function parseHalfScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find((c) => c.homeAway === "home");
    const away = comp?.competitors?.find((c) => c.homeAway === "away");
    const hHome = sumFirstTwoPeriods(home?.linescores);
    const hAway = sumFirstTwoPeriods(away?.linescores);
    if (Number.isFinite(hHome) && Number.isFinite(hAway))
      return `${hAway}-${hHome}`;
  } catch {}
  return "";
}

/* ───────── MARKETS ───────── */
const prefTokens = (list = MARKET_PREFERENCE) =>
  list.split(",").map((x) => x.trim().toLowerCase()).filter(Boolean);

function labelMatchesPreferred(mk, tokens) {
  const l = norm(
    `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.state || ""} ${
      mk?.period?.displayName || ""
    }`
  );
  return (
    /live|inplay|in-play/.test(l) ||
    mk?.state === "LIVE" ||
    mk?.inPlay === true ||
    tokens.some((tok) => l.includes(tok))
  );
}

async function extractFromMarket(market) {
  const book0 = (market?.books && market.books[0]) || null;
  const book = book0?.$ref ? await fetchRefMaybe(book0.$ref) : book0 || {};
  const n = (v) => (v == null || v === "" ? "" : Number(v));
  const aw = book?.awayTeamOdds || {};
  const hm = book?.homeTeamOdds || {};
  return {
    spreadAway: n(aw?.current?.spread ?? aw?.spread),
    spreadHome: n(hm?.current?.spread ?? hm?.spread),
    mlAway: n(aw?.current?.moneyLine ?? aw?.moneyLine),
    mlHome: n(hm?.current?.moneyLine ?? hm?.moneyLine),
    total: n(book?.current?.total ?? book?.total),
  };
}

async function pickLiveFromMarkets(allMarkets, tokens, gameId) {
  if (!Array.isArray(allMarkets) || !allMarkets.length) return undefined;
  const liveMkts = allMarkets.filter((mk) => labelMatchesPreferred(mk, tokens));

  if (DEBUG)
    console.log(
      `🎯 ${gameId}: found ${liveMkts.length}/${allMarkets.length} markets labeled live.`
    );

  const looks = (mk, words) =>
    words.some((w) =>
      norm(`${mk?.name || ""} ${mk?.displayName || ""}`).includes(w)
    );

  const mkSpread =
    liveMkts.find((m) => looks(m, ["spread", "line"])) || liveMkts[0];
  const mkTotal =
    liveMkts.find((m) => looks(m, ["total", "over", "under"])) || liveMkts[0];

  let final = {};
  if (mkSpread) Object.assign(final, await extractFromMarket(mkSpread));
  if (mkTotal) final.total = (await extractFromMarket(mkTotal))?.total || "";

  if (
    DEBUG &&
    Object.values(final).some((v) => v !== "")
  )
    console.log("📊 Live market chosen:", JSON.stringify(final, null, 2));

  return Object.values(final).some((v) => v !== "") ? final : undefined;
}

/* ───────── SHEETS ───────── */
const makeValue = (range, val) => ({ range, values: [[val]] });
const a1 = (r, c) => `${TAB_NAME}!${idxToA1(c)}${r + 1}:${idxToA1(c)}${r + 1}`;

async function getValues() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!A1:Q2000`,
  });
  return res.data.values || [];
}
function mapCols(header) {
  const lower = (s) => (s || "").trim().toLowerCase();
  const find = (name, fb) => {
    const i = header.findIndex((h) => lower(h) === lower(name));
    return i >= 0 ? i : fb;
  };
  return {
    GAME_ID: find("Game ID", 0),
    DATE: find("Date", 1),
    STATUS: find("Status", 3),
    HALF: find("Half Score", 11),
    LA_S: find("Live Away Spread", 12),
    LA_ML: find("Live Away ML", 13),
    LH_S: find("Live Home Spread", 14),
    LH_ML: find("Live Home ML", 15),
    L_TOT: find("Live Total", 16),
  };
}

/* ───────── MAIN ───────── */
async function main() {
  const ts = new Date().toISOString();
  const values = await getValues();
  if (values.length === 0) return console.log(`[${ts}] Sheet empty.`);
  const col = mapCols(values[0]);
  const rows = values.slice(1);

  const targets = rows
    .map((r, i) => {
      const id = (r[col.GAME_ID] || "").trim();
      const date = (r[col.DATE] || "").trim();
      const status = (r[col.STATUS] || "").trim();
      if (!id) return null;
      if (GAME_ID && id !== GAME_ID) return null;
      if (isFinalCell(status)) return null;
      if (looksLiveStatus(status) || date === todayKey)
        return { r: i + 1, id };
      return null;
    })
    .filter(Boolean);

  if (!targets.length)
    return console.log(`[${ts}] No matching targets (GAME_ID=${GAME_ID || "ALL"})`);

  console.log(
    `[${ts}] Found ${targets.length} game(s) to update: ${targets
      .map((t) => t.id)
      .join(", ")}`
  );

  const tokens = prefTokens(MARKET_PREFERENCE);
  const data = [];

  for (const t of targets) {
    console.log(`\n=== 🏈 GAME ${t.id} ===`);
    let summary;
    try {
      summary = await espnSummary(t.id);
      const stObj =
        summary?.header?.competitions?.[0]?.status || summary?.header?.status;
      const newStatus = formatStatus(stObj);
      const half = parseHalfScore(summary);

      if (DEBUG) {
        console.log("Status object:", stObj);
        console.log(`→ Parsed status="${newStatus}", half="${half}"`);
      }

      if (newStatus) data.push(makeValue(a1(t.r, col.STATUS), newStatus));
      if (half) data.push(makeValue(a1(t.r, col.HALF), half));
    } catch (e) {
      console.log(`Summary fetch failed ${t.id}: ${e.message}`);
    }

    try {
      const marketsRoot = await espnMarkets(t.id);
      const items = Array.isArray(marketsRoot?.items) ? marketsRoot.items : [];
      const markets = [];
      for (const itm of items) {
        if (itm?.$ref) {
          const m = await fetchRefMaybe(itm.$ref);
          if (m && Object.keys(m).length) markets.push(m);
        } else markets.push(itm);
      }
      const live = await pickLiveFromMarkets(markets, tokens, t.id);
      if (live) {
        const write = (c, v) =>
          v !== "" && Number.isFinite(Number(v))
            ? data.push(makeValue(a1(t.r, c), Number(v)))
            : null;
        write(col.LA_S, live.spreadAway);
        write(col.LA_ML, live.mlAway);
        write(col.LH_S, live.spreadHome);
        write(col.LH_ML, live.mlHome);
        write(col.L_TOT, live.total);
      } else {
        console.log(`⚠️ No live market found for ${t.id}`);
      }
    } catch (e) {
      console.log(`Markets fetch failed ${t.id}: ${e.message}`);
    }
  }

  if (!data.length) return console.log(`No updates applied.`);

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: GOOGLE_SHEET_ID,
    requestBody: { valueInputOption: "USER_ENTERED", data },
  });
  console.log(`✅ Updated ${data.length} cell(s).`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
