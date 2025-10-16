import { google } from "googleapis";
import axios from "axios";

/* ==================== ENV ==================== */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football", // "nfl" | "college-football"
  TAB_NAME = "CFB",            // target sheet tab name
  PREFILL_MODE = "week",       // "today" | "week" | "live_daily" | "finals"
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("❌ Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/* ==================== GOOGLE AUTH ==================== */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/* ==================== ESPN HELPERS ==================== */
const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football";
const scoreboardUrl = (league, yyyymmdd) =>
  `${ESPN_BASE}/${league}/scoreboard?dates=${yyyymmdd}`;
const summaryUrl = (league, gameId) =>
  `${ESPN_BASE}/${league}/summary?event=${gameId}`;

const fetchJson = async (url) => (await axios.get(url)).data;
const asNum = (v) =>
  (v === 0 || (typeof v === "number" && !Number.isNaN(v)))
    ? v
    : (v != null && v !== "" ? Number(v) : null);

/* ========== date / time formatting ========== */
function fmtESPNDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}
function fmtDisplayDateMDY(d) {
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}
function fmtKickET(isoStr) {
  const dt = new Date(isoStr);
  return dt.toLocaleTimeString("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
  });
}

/* ====== underline the pregame favorite inside the matchup cell ====== */
function richTextUnderlineForMatchup(matchup, favSide, awayName, homeName) {
  const text = matchup || "";
  const len = text.length;
  if (!text || !favSide) return { text, runs: [] };

  const favName = favSide === "away" ? awayName : homeName;
  if (!favName) return { text, runs: [] };

  const start = text.indexOf(favName);
  if (start < 0 || start >= len) return { text, runs: [] };

  const end = start + favName.length;
  const runs = [];
  if (start > 0) runs.push({ startIndex: 0 });
  runs.push({ startIndex: start, format: { underline: true } });
  if (end < len) runs.push({ startIndex: end });
  return { text, runs };
}

/* ========== parse spreads & odds (robust across ESPN shapes) ========== */
function parseSpreadsFromOdds(odds, awayName, homeName, awayAbbr, homeAbbr) {
  let spreadAway = null;
  let spreadHome = null;

  const aSpread = asNum(odds?.awayTeamOdds?.spread);
  const hSpread = asNum(odds?.homeTeamOdds?.spread);
  if (aSpread != null && hSpread != null) {
    return { spreadAway: aSpread, spreadHome: hSpread };
  }

  const sVal = asNum(odds?.spread);
  if (sVal != null) {
    const favText = (odds?.favoriteDetails || "").toLowerCase();
    const awayKey = [awayName, awayAbbr].filter(Boolean).map(x => x.toLowerCase());
    const homeKey = [homeName, homeAbbr].filter(Boolean).map(x => x.toLowerCase());
    const awayFav = awayKey.some(k => favText.includes(k));
    const homeFav = homeKey.some(k => favText.includes(k));

    if (awayFav) return { spreadAway: -Math.abs(sVal), spreadHome: Math.abs(sVal) };
    if (homeFav) return { spreadAway: Math.abs(sVal), spreadHome: -Math.abs(sVal) };
  }

  return { spreadAway, spreadHome };
}

/* ==================== PREFILL (today / week / live_daily) ==================== */
async function runPrefill() {
  console.log(`🏈 Running orchestrator for ${LEAGUE}, mode=${PREFILL_MODE}`);

  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (PREFILL_MODE === "today") {
    // just today
  } else {
    // default to whole week-ish
    start.setDate(start.getDate() - 1);
    end.setDate(end.getDate() + 7);
  }

  const days = [];
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) days.push(new Date(d));

  const boards = (
    await Promise.allSettled(days.map((d) =>
      fetchJson(scoreboardUrl(LEAGUE, fmtESPNDate(d))).then((j) => ({ d, j }))
    ))
  ).filter(r => r.status === "fulfilled").map(r => r.value);

  const events = boards.flatMap(b => b.j?.events ?? []);
  console.log(`Fetched ${events.length} games for ${LEAGUE}`);

  const outRows = [];

  for (const ev of events) {
    const { id, date, competitions, week } = ev || {};
    const comp = competitions?.[0];
    if (!comp) continue;

    const away = comp.competitors?.find(c => c.homeAway === "away");
    const home = comp.competitors?.find(c => c.homeAway === "home");
    if (!away || !home) continue;

    const awayName = away.team?.shortDisplayName || away.team?.displayName || "";
    const homeName = home.team?.shortDisplayName || home.team?.displayName || "";
    const awayAbbr = away.team?.abbreviation || "";
    const homeAbbr = home.team?.abbreviation || "";

    const displayDate = fmtDisplayDateMDY(new Date(date));
    const kickoff = fmtKickET(date);
    const weekLabel = week?.number ? `Week ${week.number}` : "Week ?";

    const odds = comp?.odds?.[0] || {};
    let total = asNum(odds?.overUnder);
    let mlAway = asNum(odds?.awayTeamOdds?.moneyLine);
    let mlHome = asNum(odds?.homeTeamOdds?.moneyLine);

    let { spreadAway, spreadHome } = parseSpreadsFromOdds(
      odds, awayName, homeName, awayAbbr, homeAbbr
    );

    // very light fallback if only MLs exist
    if (spreadAway == null && spreadHome == null && mlAway != null && mlHome != null) {
      if (mlAway < 0 && mlHome > 0) { spreadAway = -2.5; spreadHome = 2.5; }
      else if (mlHome < 0 && mlAway > 0) { spreadHome = -2.5; spreadAway = 2.5; }
    }

    const matchup = `${awayName} @ ${homeName}`;
    let favSide = null;
    if (spreadAway != null && spreadHome != null) {
      favSide = spreadAway < 0 ? "away" : (spreadHome < 0 ? "home" : null);
    } else if (mlAway != null && mlHome != null) {
      favSide = mlAway < mlHome ? "away" : (mlHome < mlAway ? "home" : null);
    }
    const { text, runs } = richTextUnderlineForMatchup(matchup, favSide, awayName, homeName);

    // Build new row (A..F): Game ID, Date, Week, Status, Matchup, Final Score
    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },       // A Game ID
        { userEnteredValue: { stringValue: displayDate } },      // B Date (MM/DD/YY)
        { userEnteredValue: { stringValue: weekLabel } },        // C Week
        { userEnteredValue: { stringValue: kickoff } },          // D Status (kick time)
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E Matchup (fav underlined)
        { userEnteredValue: { stringValue: "" } },               // F Final Score
        // NOTE: Odds columns (G..K) are left for other jobs if you populate them elsewhere.
      ],
    });
  }

  if (outRows.length === 0) {
    console.log("No new rows to write.");
    return;
  }

  // Find target sheet
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    includeGridData: false,
  });
  const targetSheet = meta.data.sheets?.find((s) => s.properties?.title === TAB_NAME);
  if (!targetSheet) {
    console.error(`Tab "${TAB_NAME}" not found`);
    process.exit(1);
  }
  const sheetId = targetSheet.properties.sheetId;

  // Dedup by Game ID (col A)
  const existing = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: `${TAB_NAME}!A2:A`,
  });
  const existingIds = new Set((existing.data.values || []).map((r) => r[0]));
  const newRows = outRows.filter(
    (r) => !existingIds.has(r.values[0].userEnteredValue.stringValue)
  );

  if (newRows.length === 0) {
    console.log("No new rows to write.");
    return;
  }

  const startRowIndex = (existing.data.values?.length || 0) + 1; // 1-based after header
  const endRowIndex = startRowIndex + newRows.length;

  // Write rows (A..F) with textFormatRuns intact
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: GOOGLE_SHEET_ID,
    requestBody: {
      requests: [
        {
          updateCells: {
            rows: newRows,
            fields: "userEnteredValue,textFormatRuns",
            range: {
              sheetId,
              startRowIndex,
              endRowIndex,
              startColumnIndex: 0,
              endColumnIndex: 6, // A..F
            },
          },
        },
      ],
    },
  });

  console.log(`✅ Prefill completed: ${newRows.length} new rows`);
}

/* ==================== FINALS SWEEP ==================== */
async function runFinalsSweep() {
  console.log(`🏁 Finals sweep started for ${LEAGUE}/${TAB_NAME}`);

  const readRange = `${TAB_NAME}!A2:F`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: readRange,
  });
  const rows = res.data.values || [];
  if (rows.length === 0) {
    console.log("No rows found.");
    return;
  }

  // Candidates: rows that either have blank final score OR status is not "Final"
  const candidates = [];
  rows.forEach((r, idx) => {
    const gameId = (r[0] || "").trim();
    const status = (r[3] || "").trim();
    const finalScore = (r[5] || "").trim();
    if (!gameId) return;
    if (!finalScore || status.toLowerCase() !== "final") {
      candidates.push({ rowIndex: idx, gameId });
    }
  });

  console.log(`Found ${candidates.length} candidate(s) with no final score.`);

  if (candidates.length === 0) return;

  let finalsWritten = 0;
  const updates = [];

  for (const c of candidates) {
    try {
      const sum = await fetchJson(summaryUrl(LEAGUE, c.gameId));
      const comp = sum?.header?.competitions?.[0];
      const st = comp?.status?.type?.name || comp?.status?.type?.state;
      const isFinal = typeof st === "string" && st.toLowerCase().includes("final");
      if (!isFinal) continue;

      const away = comp?.competitors?.find(x => x.homeAway === "away");
      const home = comp?.competitors?.find(x => x.homeAway === "home");
      if (!away || !home) continue;

      const finalScoreStr = `${away.score}-${home.score}`;
      const targetRow = c.rowIndex + 2; // account for header

      // ✅ Only update Status (D) and Final Score (F) — DO NOT touch Matchup (E)
      updates.push({
        range: `${TAB_NAME}!D${targetRow}:D${targetRow}`,
        values: [["Final"]],
      });
      updates.push({
        range: `${TAB_NAME}!F${targetRow}:F${targetRow}`,
        values: [[finalScoreStr]],
      });

      finalsWritten++;
    } catch (e) {
      console.warn(`Skipping ${c.gameId} (${e.message})`);
    }
  }

  if (updates.length === 0) {
    console.log("No finals ready.");
    return;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: GOOGLE_SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: updates,
    },
  });

  console.log(`✅ Finals written: ${finalsWritten}`);
}

/* ==================== ROUTER ==================== */
(async () => {
  try {
    if (PREFILL_MODE === "finals") await runFinalsSweep();
    else await runPrefill();
  } catch (e) {
    console.error("❌ Orchestrator fatal:", e?.response?.data ?? e);
    process.exit(1);
  }
})();
