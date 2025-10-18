// live-game.mjs
// Updates only: Status (D), Half Score (L), Live odds (M..Q).
// Leaves pregame columns untouched. Supports optional GAME_ID focus.
// Live odds pulled from ESPN BET (Core Markets API), with fallbacks.

import axios from "axios";
import { google } from "googleapis";

/* ─────────────────────────────── ENV ─────────────────────────────── */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "nfl",                                 // "nfl" | "college-football"
  TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
  GAME_ID = "",                                   // optional: only update this game
  MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
} = process.env;

for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
  if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
}

/* ───────────────────── Google Sheets bootstrap ───────────────────── */
const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
const jwt = new google.auth.JWT(
  svc.client_email,
  undefined,
  svc.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

/* ───────────────────────────── Helpers ───────────────────────────── */
const norm = (s) => (s || "").toLowerCase();
const tokensFromPref = (list = MARKET_PREFERENCE) =>
  (list || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

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

// Today key in **US/Eastern** to match the sheet’s date in column B
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

// status helpers (from /summary)
function shortStatusFromEspn(statusObj) {
  const t = statusObj?.type || {};
  return t.shortDetail || t.detail || t.description || "In Progress";
}
function isFinalFromEspn(statusObj) {
  return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
}

// Half score from first two period linescores
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
    if (Number.isFinite(hHome) && Number.isFinite(hAway)) return `${hAway}-${hHome}`; // away-first
  } catch {}
  return "";
}

/* ───────────────────────── ESPN fetchers ─────────────────────────── */
const SPORT = "football";
const LCODE = LEAGUE; // "nfl" | "college-football"

// Basic safe fetch with timeout & cache-buster support
async function getJson(url, params = {}, timeout = 15000) {
  const { data } = await axios.get(url, {
    timeout,
    params,
    headers: { "User-Agent": "halftime-updater/1.0" },
  });
  return data;
}

// /summary (cache-busted)
async function espnSummary(gameId) {
  const url = `https://site.api.espn.com/apis/site/v2/sports/${SPORT}/${LCODE}/summary`;
  return getJson(url, { event: gameId, _cb: Date.now() });
}

// ESPN Core Markets (ESPN BET source)
// 1) list markets for event
// 2) dereference items -> market objects
// 3) dereference books for provider "ESPN BET"
async function coreMarketsForEvent(gameId) {
  const base = `https://sports.core.api.espn.com/v2/markets`;
  const list = await getJson(base, { sport: SPORT, league: LCODE, event: gameId, region: "us", lang: "en" });

  const items = Array.isArray(list?.items) ? list.items : [];
  const deref = async (ref) => {
    if (!ref) return null;
    if (typeof ref === "string") return getJson(ref);
    if (ref.$ref) return getJson(ref.$ref);
    return ref;
  };

  // deref every market
  const markets = [];
  for (const it of items) {
    const mk = await deref(it);
    if (mk) markets.push(mk);
  }
  return markets;
}

async function fetchEspnBetBooks(market) {
  // books can be array of refs or a single ref
  const booksField = market?.books;
  if (!booksField) return [];

  const refs = [];
  if (Array.isArray(booksField)) refs.push(...booksField);
  else refs.push(booksField);

  const books = [];
  for (const r of refs) {
    const b = await (typeof r === "string" ? getJson(r) : r.$ref ? getJson(r.$ref) : r);
    if (b) books.push(b);
  }
  // Keep only ESPN BET
  return books.filter((b) => norm(b?.provider?.name) === "espn bet" || norm(b?.provider?.id) === "espnbet");
}

// Robust number getter (first numeric)
function num(...candidates) {
  for (const v of candidates) {
    if (v === "" || v === null || v === undefined) continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return "";
}

// Try to extract spread/ML/total from a book object that might have different shapes
function extractFromBook(book) {
  // Common (odds tree) shape
  const aw = book?.awayTeamOdds || {};
  const hm = book?.homeTeamOdds || {};
  const spreadAway =
    num(aw?.current?.spread, aw?.spread, book?.current?.spread);
  const spreadHome =
    num(hm?.current?.spread, hm?.spread, spreadAway !== "" ? -spreadAway : "");
  const mlAway = num(aw?.current?.moneyLine, aw?.moneyLine);
  const mlHome = num(hm?.current?.moneyLine, hm?.moneyLine);
  const total = num(book?.current?.total, book?.total, book?.overUnder);

  // If this yielded anything, use it
  if ([spreadAway, spreadHome, mlAway, mlHome, total].some((v) => v !== "")) {
    return { spreadAway, spreadHome, mlAway, mlHome, total };
  }

  // Alternative: outcome-based payloads
  // book.outcomes = [{type:'spread'|'total'|'moneyline', team:{...}, price:{american}, point}]
  const outcomes = Array.isArray(book?.outcomes) ? book.outcomes : [];
  if (outcomes.length) {
    let sA = "", sH = "", mA = "", mH = "", tT = "";

    for (const o of outcomes) {
      const t = norm(o?.type || o?.betType || "");
      const side = norm(o?.side || o?.team?.homeAway || "");
      if (t.includes("spread")) {
        if (side === "away") sA = num(sA, o?.point);
        if (side === "home") sH = num(sH, o?.point);
      } else if (t.includes("money")) {
        const am = num(o?.price?.american, o?.price);
        if (side === "away") mA = num(mA, am);
        if (side === "home") mH = num(mH, am);
      } else if (t.includes("total") || t.includes("over/under")) {
        tT = num(tT, o?.point);
      }
    }
    if ([sA, sH, mA, mH, tT].some((v) => v !== "")) {
      // if only one spread is present, mirror sign
      if (sA !== "" && sH === "") sH = -sA;
      if (sH !== "" && sA === "") sA = -sH;
      return { spreadAway: sA, spreadHome: sH, mlAway: mA, mlHome: mH, total: tT };
    }
  }

  return { spreadAway: "", spreadHome: "", mlAway: "", mlHome: "", total: "" };
}

// Choose the best ESPN BET market/book for LIVE numbers
function chooseEspnBetLiveBook(markets, prefTokensArr) {
  // Filter to markets whose label hints at live/2H/etc
  const looksLiveMarket = (mk) => {
    const label = `${mk?.name || ""} ${mk?.displayName || ""} ${mk?.period?.displayName || ""} ${mk?.period?.abbreviation || ""}`;
    return prefTokensArr.some((tok) => norm(label).includes(tok)) || norm(mk?.state) === "live";
  };

  const preferred = markets.filter(looksLiveMarket);
  const pool = preferred.length ? preferred : markets;

  // Within pool, require ESPN BET books and pick first with extractable numbers
  for (const mk of pool) {
    // eslint-disable-next-line no-await-in-loop
    const books = await fetchEspnBetBooks(mk);
    for (const b of books) {
      const parsed = extractFromBook(b);
      if ([parsed.spreadAway, parsed.spreadHome, parsed.mlAway, parsed.mlHome, parsed.total].some((v) => v !== "")) {
        return parsed;
      }
    }
  }
  return undefined;
}

/* ─────────────────────── values/A1 helpers ───────────────────────── */
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
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range,
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

// Only row selection we want: GAME_ID, already-live rows, or rows with today’s date (ET) and not Final
function chooseTargets(rows, col) {
  const targets = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const id = (row[col.GAME_ID] || "").trim();
    if (!id) continue;

    const dateCell = (row[col.DATE] || "").trim(); // MM/DD/YY
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

/* ─────────────────────────────── MAIN ────────────────────────────── */
async function main() {
  try {
    const ts = new Date().toISOString();
    const values = await getValues();
    if (values.length === 0) {
      console.log(`[${ts}] Sheet empty—nothing to do.`);
      return;
    }

    const col = mapCols(values[0]);
    const targets = chooseTargets(values, col);
    if (targets.length === 0) {
      console.log(`[${ts}] Nothing to update.`);
      return;
    }

    const pref = tokensFromPref(MARKET_PREFERENCE);
    const data = [];

    for (const t of targets) {
      const currentStatus = values[t.r]?.[col.STATUS] || "";
      if (isFinalCell(currentStatus)) continue;

      // 1) STATUS + HALF
      let summary;
      let liveish = looksLiveStatus(currentStatus);
      try {
        summary = await espnSummary(t.id);
        const compStatus = summary?.header?.competitions?.[0]?.status;
        const newStatus = shortStatusFromEspn(compStatus);
        const nowFinal = isFinalFromEspn(compStatus);

        if (newStatus && newStatus !== currentStatus) {
          data.push(makeValue(a1For(t.r, col.STATUS), newStatus));
        }
        const half = parseHalfScore(summary);
        if (half) data.push(makeValue(a1For(t.r, col.HALF), half));

        // set liveish if period indicates game has started
        const period = Number(summary?.header?.period || summary?.header?.competitions?.[0]?.status?.period || 0);
        if (period > 0) liveish = true;

        if (nowFinal) {
          // no live odds if Final
          continue;
        }
      } catch (e) {
        if (e?.response?.status !== 404) {
          console.log(`Summary warn ${t.id}:`, e?.message || e);
        }
      }

      // Only write live odds when we believe the game is live (or GAME_ID explicitly provided)
      if (!liveish && !GAME_ID) continue;

      // 2) LIVE ODDS from ESPN BET (Core Markets)
      try {
        const markets = await coreMarketsForEvent(t.id);
        let live = await chooseEspnBetLiveBook(markets, pref);

        // If nothing matched preferred tokens, try again with a very permissive pass
        if (!live) {
          live = await chooseEspnBetLiveBook(markets, ["live", "line", "spread", "total"]);
        }

        if (live) {
          const w = (c, v) => {
            if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v)));
          };
          w(col.LA_S, live.spreadAway);
          w(col.LA_ML, live.mlAway);
          w(col.LH_S, live.spreadHome);
          w(col.LH_ML, live.mlHome);
          w(col.L_TOT, live.total);
        } else {
          console.log(`No ESPN BET live market found for ${t.id} — left M..Q as-is.`);
        }
      } catch (e) {
        console.log(`Markets warn ${t.id}:`, e?.message || e);
      }
    }

    if (!data.length) {
      console.log(`[${new Date().toISOString()}] Built 0 cell updates across ${targets.length} target(s).`);
      return;
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data },
    });

    console.log(
      `[${ts}] Updated ${targets.length} row(s). Wrote ${data.length} cell update(s). ` +
        `Targets: ${targets.map((t) => `${t.id}(${t.reason})`).join(", ")}`
    );
  } catch (err) {
    const code = err?.response?.status || err?.code || err?.message || err;
    console.error("Live updater fatal:", "*** code:", code, "***");
    process.exit(1);
  }
}

main();
