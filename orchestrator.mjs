// orchestrator.mjs
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

/* ====== underline favorite in matchup cell (prefill), later we overwrite runs with bold winner too ====== */
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

    // Build row (A..F) only — Odds columns can be filled by your other task
    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },       // A Game ID
        { userEnteredValue: { stringValue: displayDate } },      // B Date (MM/DD/YY)
        { userEnteredValue: { stringValue: weekLabel } },        // C Week
        { userEnteredValue: { stringValue: kickoff } },          // D Status (kick time)
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E Matchup (fav underlined)
        { userEnteredValue: { stringValue: "" } },               // F Final Score
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

/* ==================== FINALS SWEEP + FORMATTING ==================== */
const COLORS = {
  green: { red: 0.85, green: 0.95, blue: 0.85 },
  red:   { red: 0.98, green: 0.85, blue: 0.85 },
  gray:  { red: 0.93, green: 0.93, blue: 0.93 },
};

async function runFinalsSweep() {
  console.log(`🏁 Finals sweep started for ${LEAGUE}/${TAB_NAME}`);

  // We’ll read A..K so we have odds columns
  const readRange = `${TAB_NAME}!A2:K`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: readRange,
  });
  const rows = res.data.values || [];
  if (rows.length === 0) {
    console.log("No rows found.");
    return;
  }

  // Sheet meta for formatting
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

  // col indices (0-based) for updateCells convenience
  const COL = {
    A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10,
  };

  const valueUpdates = [];   // values API (status & final score)
  const formatRequests = []; // batchUpdate (formatting + rich text on E)

  let finalsWritten = 0;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i] || [];
    const rowNum = i + 2; // 1-based + header

    const gameId = (r[0] || "").trim();
    if (!gameId) continue;

    const status = (r[3] || "").trim();
    const finalScoreExisting = (r[5] || "").trim();

    // If already final with a score, we can still do formatting; otherwise we also fetch summary
    let awayScore = null, homeScore = null, isFinal = false;
    let matchupText = r[4] || ""; // E
    try {
      const sum = await fetchJson(summaryUrl(LEAGUE, gameId));
      const comp = sum?.header?.competitions?.[0];
      const st = comp?.status?.type?.name || comp?.status?.type?.state;
      isFinal = typeof st === "string" && st.toLowerCase().includes("final");

      const away = comp?.competitors?.find(x => x.homeAway === "away");
      const home = comp?.competitors?.find(x => x.homeAway === "home");
      if (!away || !home) continue;

      awayScore = asNum(away?.score);
      homeScore = asNum(home?.score);

      // Write final score & status only if needed
      if (isFinal && (!finalScoreExisting || status.toLowerCase() !== "final")) {
        const finalScoreStr = `${awayScore}-${homeScore}`;
        valueUpdates.push({
          range: `${TAB_NAME}!D${rowNum}:D${rowNum}`,
          values: [["Final"]],
        });
        valueUpdates.push({
          range: `${TAB_NAME}!F${rowNum}:F${rowNum}`,
          values: [[finalScoreStr]],
        });
        finalsWritten++;
      }

      // ====== Build rich text for Matchup: Bold the winner; keep favorite underline if we can detect ======
      const winnerSide = awayScore > homeScore ? "away" : (homeScore > awayScore ? "home" : null);

      // Guess the favorite from spreads or ML present on the row
      const awaySpread = asNum(r[6]); // G
      const homeSpread = asNum(r[8]); // I
      const awayML     = asNum(r[7]); // H
      const homeML     = asNum(r[9]); // J
      const total      = asNum(r[10]); // K

      let favSide = null;
      if (awaySpread != null && homeSpread != null) {
        favSide = awaySpread < 0 ? "away" : (homeSpread < 0 ? "home" : null);
      } else if (awayML != null && homeML != null) {
        favSide = awayML < homeML ? "away" : (homeML < awayML ? "home" : null);
      }

      // Extract away/home names from the matchup ("Away @ Home")
      const [awayName, homeName] = (matchupText || "").split(" @ ").map(s => (s || "").trim());

      // Build runs: bold winner, underline favorite if we can
      const text = matchupText || "";
      const runs = [];
      const pushRun = (start, fmt) => {
        if (start > 0) runs.push({ startIndex: 0 });
        runs.push({ startIndex: start, format: fmt });
      };
      const applyStyleTo = (team, style) => {
        if (!team) return;
        const start = text.indexOf(team);
        if (start >= 0) runs.push({ startIndex: start, format: style });
      };

      // winner bold
      if (winnerSide === "away") applyStyleTo(awayName, { bold: true });
      if (winnerSide === "home") applyStyleTo(homeName, { bold: true });

      // favorite underline
      if (favSide === "away") applyStyleTo(awayName, { underline: true });
      if (favSide === "home") applyStyleTo(homeName, { underline: true });

      // Normalize runs order & dedupe by startIndex, merging styles
      const byIndex = new Map();
      for (const run of runs) {
        const idx = run.startIndex ?? 0;
        const prev = byIndex.get(idx) || { startIndex: idx, format: {} };
        byIndex.set(idx, { startIndex: idx, format: { ...prev.format, ...run.format } });
      }
      const finalRuns = Array.from(byIndex.values()).sort((a, b) => a.startIndex - b.startIndex);

      // Apply textFormatRuns to the single cell E(row)
      formatRequests.push({
        updateCells: {
          rows: [{
            values: [{
              userEnteredValue: { stringValue: text },
              textFormatRuns: finalRuns,
            }],
          }],
          fields: "userEnteredValue,textFormatRuns",
          range: {
            sheetId,
            startRowIndex: rowNum - 1,
            endRowIndex: rowNum,
            startColumnIndex: COL.E,
            endColumnIndex: COL.E + 1,
          },
        },
      });

      // ====== Color ML/Spread/Total cells based on result ======
      // Helpers
      const bg = (color) => ({ userEnteredFormat: { backgroundColor: color } });
      const colorCell = (col, color) => {
        formatRequests.push({
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: rowNum - 1,
              endRowIndex: rowNum,
              startColumnIndex: col,
              endColumnIndex: col + 1,
            },
            cell: { userEnteredFormat: { backgroundColor: color } },
            fields: "userEnteredFormat.backgroundColor",
          },
        });
      };
      const PUSH = COLORS.gray;

      // Moneyline: green for the winner side, red for loser (if values exist)
      if (awayML != null || homeML != null) {
        if (awayScore > homeScore) {
          colorCell(COL.H, COLORS.green); // away ML
          colorCell(COL.J, COLORS.red);   // home ML
        } else if (homeScore > awayScore) {
          colorCell(COL.H, COLORS.red);
          colorCell(COL.J, COLORS.green);
        }
      }

      // Spread cover
      if (awaySpread != null) {
        const awayCovers = awayScore + awaySpread > homeScore ? true
                         : awayScore + awaySpread < homeScore ? false
                         : null; // push
        colorCell(COL.G, awayCovers === null ? PUSH : (awayCovers ? COLORS.green : COLORS.red));
      }
      if (homeSpread != null) {
        const homeCovers = homeScore + homeSpread > awayScore ? true
                         : homeScore + homeSpread < awayScore ? false
                         : null;
        colorCell(COL.I, homeCovers === null ? PUSH : (homeCovers ? COLORS.green : COLORS.red));
      }

      // Total Over/Under (no pick column; just color by outcome)
      if (total != null && awayScore != null && homeScore != null) {
        const sum = awayScore + homeScore;
        const color = sum > total ? COLORS.green
                    : sum < total ? COLORS.red
                    : PUSH;
        colorCell(COL.K, color);
      }
    } catch (e) {
      console.warn(`Skipping row ${rowNum} / game ${gameId}: ${e.message}`);
    }
  }

  // write values (status + finals) first
  if (valueUpdates.length > 0) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: {
        valueInputOption: "USER_ENTERED",
        data: valueUpdates,
      },
    });
  }

  // then formatting updates
  if (formatRequests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { requests: formatRequests },
    });
  }

  console.log(`✅ Finals written: ${finalsWritten} | formatted: ${rows.length}`);
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
