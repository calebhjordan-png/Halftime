// live-game.mjs
// Updates: Status (D), Half Score (L), Live odds (M..Q).
// ESPN Summary for clock/status, ESPN Markets for live lines.

import axios from "axios";
import { google } from "googleapis";

/* ───────────── ENV ───────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl", // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",
  MARKET_PREFERENCE = "live,in-play,inplay,2h,second half,halftime",
} = process.env;

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

// Today key in US/Eastern (MM/DD/YY)
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

function looksLiveStatus(s) {
  const x = norm(s);
  return (
    /\bhalf\b/.test(x) ||
    /\bin\s*progress\b/.test(x) ||
    /\bq[1-4]\b/.test(x) ||
    /\bot\b/.test(x) ||
    /\blive\b/.test(x)
  );
}
const isFinalCell = (s) => /^final$/i.test(String(s || ""));

/* ===== ESPN fetchers ===== */
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

async function espnMarkets(gameId) {
  const url = `https://sports.core.api.espn.com/v2/markets?sport=football&league=${LEAGUE}&event=${gameId}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

async function fetchRefMaybe(ref) {
  if (!ref || typeof ref !== "string") return {};
  try {
    const { data } = await axios.get(ref, { timeout: 12000 });
    return data || {};
  } catch {
    return {};
  }
}

/* ===== Status + Half ===== */
function formatStatusFromObj(st) {
  if (!st) return "";

  // ESPN can put these fields on either header.status or competitions[0].status
  const type = st.type || {};
  const stateRaw =
    type.state || type.name || st.state || st.name || type.id || "";
  const state = norm(stateRaw);
  const detail =
    type.shortDetail || type.detail || type.description || st.detail || "";

  const clock = st.displayClock || st.clock || "";
  const periodNum = st.period ?? st.periodNumber;

  const periodName = (n) =>
    ({ 1: "1st", 2: "2nd", 3: "3rd", 4: "4th" }[n] || (n ? `Q${n}` : ""));

  // Scheduled / pre
  if (state === "pre" || state === "scheduled") {
    try {
      const iso =
        st.startDate || st.date || st.gameTime || st.gameDate || undefined;
      if (iso) {
        const dt = new Date(iso);
        const et = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/New_York",
          hour: "numeric",
          minute: "2-digit",
          hour12: true,
        }).format(dt);
        return et;
      }
    } catch {}
    return detail || "Scheduled";
  }

  // In progress (ESPN uses: "in", "inprogress", "live")
  if (
    state === "in" ||
    state === "inprogress" ||
    state === "live" ||
    /in[-\s]?progress/i.test(detail)
  ) {
    const p = periodName(periodNum);
    if (clock && p) return `${clock} - ${p}`;
    if (detail) return detail;
    return "In Progress";
  }

  // Halftime
  if (state === "halftime" || /halftime/i.test(detail)) return "Halftime";

  // End of period (ESPN sometimes uses endperiod / endofperiod)
  if (state === "endperiod" || state === "endofperiod" || /end of/i.test(detail)) {
    const p = periodName((periodNum || 0) - 1) || periodName(periodNum);
    return p ? `End of ${p}` : detail || "End of period";
  }

  // Final
  if (state === "final" || /final/i.test(detail)) return "Final";

  // Fallback to detail
  return detail || "";
}

function sumFirstTwoPeriods(linescores) {
  if (!Array.isArray(linescores) || linescores.length === 0) return null;
  const take = linescores.slice(0, 2);
  let tot = 0;
  for (const p of take) {
    const v = Number(p?.value ?? p?.score ?? 0);
    if (!Number.isFinite(v)) return null;
    tot += v;
  }
  return tot;
}
function parseHalfScore(summary) {
  try {
    const comp = summary?.header?.competitions?.[0];
    const home = comp?.competitors?.find((c) => c.homeAway === "home");
    const away = comp?.competitors?.find((c) => c.homeAway === "away");
    const hHome = sumFirstTwoPeriods(home?.linescores);
    const hAway = sumFirstTwoPeriods(away?.linescores);
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`;
  } catch {}
  return "";
}

/* ===== Markets ===== */
const normTokens = (list = MARKET_PREFERENCE) =>
  list.split(",").map((x) => x.trim().toLowerCase()).filter(Boolean);

function labelMatchesPreferred(mk, tokens) {
  const l = norm(
    `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.state || ""} ${
      mk?.period?.displayName || ""
    }`
  );
  if (/live/.test(l) || mk?.state === "LIVE" || mk?.inPlay === true) return true;
  return tokens.some((tok) => l.includes(tok));
}

async function extractFromMarket(market) {
  const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));
  try {
    const book0 = (market?.books && market.books[0]) || null;
    const book = book0?.$ref ? await fetchRefMaybe(book0.$ref) : book0 || {};
    const aw = book?.awayTeamOdds || {};
    const hm = book?.homeTeamOdds || {};

    const spreadAway = n(aw?.current?.spread ?? aw?.spread ?? book?.current?.spread);
    const spreadHome = n(
      hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : "")
    );
    const mlAway = n(aw?.current?.moneyLine ?? aw?.moneyLine);
    const mlHome = n(hm?.current?.moneyLine ?? hm?.moneyLine);
    const total = n(book?.current?.total ?? book?.total);

    const any = [spreadAway, spreadHome, mlAway, mlHome, total].some((v) => v !== "");
    return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
  } catch {
    return undefined;
  }
}

async function pickLiveFromMarkets(allMarkets, tokens) {
  if (!Array.isArray(allMarkets) || !allMarkets.length) return undefined;
  const liveMkts = allMarkets.filter((mk) => labelMatchesPreferred(mk, tokens));
  if (!liveMkts.length) return undefined;

  const looks = (mk, words) => {
    const s = norm(`${mk?.name || ""} ${mk?.displayName || ""}`);
    return words.some((w) => s.includes(w));
  };
  const choose = (words) => liveMkts.find((mk) => looks(mk, words)) || liveMkts[0];
  const mkSpread = choose(["spread", "line"]);
  const mkTotal = choose(["total", "over", "under"]);

  let spreadAway = "",
    spreadHome = "",
    mlAway = "",
    mlHome = "",
    total = "";
  if (mkSpread) {
    const part = await extractFromMarket(mkSpread);
    if (part) ({ spreadAway, spreadHome, mlAway, mlHome } = part);
  }
  if (mkTotal) {
    const part = await extractFromMarket(mkTotal);
    if (part && part.total !== "") total = part.total;
  }

  return [spreadAway, spreadHome, mlAway, mlHome, total].some((v) => v !== "")
    ? { spreadAway, spreadHome, mlAway, mlHome, total }
    : undefined;
}

/* ===== Sheets ===== */
function makeValue(range, val) {
  return { range, values: [[val]] };
}
function a1For(row0, col0, tab = TAB_NAME) {
  const row1 = row0 + 1;
  const colA = idxToA1(col0);
  return `${tab}!${colA}${row1}:${colA}${row1}`;
}
async function getValues() {
  const range = `${TAB_NAME}!A1:Q2000`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
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

function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;
    const dateCell = (row[col.DATE] || "").trim();
    const status = (row[col.STATUS] || "").trim();

    if (isFinalCell(status)) continue;
    if (GAME_ID && id === GAME_ID) {
      targets.push({ r, id, reason: "GAME_ID" });
      continue;
    }
    if (looksLiveStatus(status)) {
      targets.push({ r, id, reason: "live-like status" });
      continue;
    }
    if (dateCell === todayKey) {
      targets.push({ r, id, reason: "today" });
    }
  }
  return targets;
}

/* ===== MAIN ===== */
async function main() {
  try {
    const ts = new Date().toISOString();
    const values = await getValues();
    if (values.length === 0) return console.log(`[${ts}] Sheet empty—nothing to do.`);
    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) return console.log(`[${ts}] Nothing to update.`);

    const tokens = normTokens(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      const curStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(curStatus)) continue;

      // 1) STATUS + HALF (robust: try both header.status and competitions[0].status)
      let summary;
      try {
        summary = await espnSummary(t.id);
        const comp = summary?.header?.competitions?.[0] || {};
        const stObj =
          comp.status ||
          summary?.header?.status ||
          comp?.statusType || // rare alt
          {};
        const statusText =
          formatStatusFromObj(stObj) ||
          formatStatusFromObj(summary?.header?.status) ||
          comp?.status?.type?.shortDetail ||
          comp?.status?.type?.detail ||
          "";

        const half = parseHalfScore(summary);

        if (statusText && statusText !== curStatus)
          data.push(makeValue(a1For(t.r, col.STATUS), statusText));
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));
      } catch (e) {
        console.log(`Summary warn ${t.id}:`, e?.message || e);
      }

      // 2) LIVE ODDS (unchanged)
      try {
        const marketsRoot = await espnMarkets(t.id);
        const items = Array.isArray(marketsRoot?.items) ? marketsRoot.items : [];
        const markets = [];
        for (const itm of items) {
          if (itm?.$ref) {
            const m = await fetchRefMaybe(itm.$ref);
            if (m && Object.keys(m).length) markets.push(m);
          } else if (itm) {
            markets.push(itm);
          }
        }
        const live = await pickLiveFromMarkets(markets, tokens);
        if (live) {
          const w = (c, v) => {
            if (v !== "" && Number.isFinite(Number(v)))
              data.push(makeValue(a1For(t.r, c), Number(v)));
          };
          w(col.LA_S, live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S, live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        }
      } catch (e) {
        console.log(`Markets warn ${t.id}:`, e?.message || e);
      }
    }

    if (!data.length)
      return console.log(`[${new Date().toISOString()}] Built 0 updates across ${targets.length} games.`);

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });
    console.log(`[${ts}] Updated ${targets.length} row(s). ${data.length} cells written.`);
  } catch (err) {
    console.error("Live updater fatal:", err);
    process.exit(1);
  }
}

main();
