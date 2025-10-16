import { google } from "googleapis";
import axios from "axios";

/* ========== ENV ========== */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",   // "nfl" | "college-football"
  TAB_NAME = "CFB",
  PREFILL_MODE = "week",         // "today" | "week" | "live_daily" | "finals"
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("❌ Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/* ========== Google Sheets ========== */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/* ========== ESPN ========== */
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

/* ========== Date helpers ========== */
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

/* ========== Rich text (underline favorite) ========== */
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

/* ========== Parse spreads accurately ========== */
/**
 * Strategy:
 * 1) Prefer team-specific odds: awayTeamOdds.spread / homeTeamOdds.spread
 * 2) Else parse odds.details/summary like "CLE -3.5"
 * 3) Else (no trustworthy source) return nulls (do NOT invent numbers)
 */
function parseSpreadsFromOdds(odds, awayName, homeName, awayAbbr, homeAbbr) {
  // Step 1: team-specific spreads (most reliable when present)
  const aTeam = asNum(odds?.awayTeamOdds?.spread);
  const hTeam = asNum(odds?.homeTeamOdds?.spread);
  if (aTeam != null && hTeam != null) {
    return { spreadAway: aTeam, spreadHome: hTeam };
  }

  // Step 2: parse “TEAM -X.X” from details/summary
  const text = String(odds?.details ?? odds?.summary ?? "").trim();
  if (text) {
    // Match like "CLE -3.5", "JAX -2", "NE +1.5", etc.
    const m = text.match(/\b([A-Z]{2,4})\s*([+-]?\d+(?:\.\d+)?)/);
    if (m) {
      const favAbbr = m[1];
      const val = Number(m[2]);
      if (!Number.isNaN(val)) {
        // If value is positive there, ESPN’s string might not encode sign as “favorite is -x”.
        // We’ll interpret the captured number as the absolute line and apply sign via favorite.
        const line = Math.abs(val);

        const favIsAway = [awayAbbr].filter(Boolean)
          .some(a => a.toUpperCase() === favAbbr.toUpperCase());
        const favIsHome = [homeAbbr].filter(Boolean)
          .some(a => a.toUpperCase() === favAbbr.toUpperCase());

        if (favIsAway) return { spreadAway: -line, spreadHome: line };
        if (favIsHome) return { spreadAway: line, spreadHome: -line };
      }
    }
  }

  // Step 3: give up (no fallback to ±2.5!)
  return { spreadAway: null, spreadHome: null };
}

/* ========== PREFILL ========== */
async function runPrefill() {
  console.log(`🏈 Prefill for ${LEAGUE}, mode=${PREFILL_MODE}`);

  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (PREFILL_MODE === "today") {
    // only today
  } else {
    start.setDate(start.getDate() - 1);
    end.setDate(end.getDate() + 7);
  }

  const days = [];
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    days.push(new Date(d));
  }

  const boards = (
    await Promise.allSettled(
      days.map((d) =>
        fetchJson(scoreboardUrl(LEAGUE, fmtESPNDate(d))).then((j) => ({ d, j }))
      )
    )
  ).filter(r => r.status === "fulfilled").map(r => r.value);

  const events = boards.flatMap(b => b.j?.events ?? []);
  console.log(`Fetched ${events.length} games`);

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

    // Odds
    const odds = comp?.odds?.[0] || {};
    const total = asNum(odds?.overUnder);

    let mlAway = asNum(odds?.awayTeamOdds?.moneyLine);
    let mlHome = asNum(odds?.homeTeamOdds?.moneyLine);

    const { spreadAway, spreadHome } = parseSpreadsFromOdds(
      odds, awayName, homeName, awayAbbr, homeAbbr
    );

    // Favorite detection just for underlining (don’t invent spreads)
    let favSide = null;
    if (spreadAway != null && spreadHome != null) {
      favSide = spreadAway < 0 ? "away" : (spreadHome < 0 ? "home" : null);
    } else if (mlAway != null && mlHome != null) {
      favSide = mlAway < mlHome ? "away" : (mlHome < mlAway ? "home" : null);
    }

    const matchup = `${awayName} @ ${homeName}`;
    const { text, runs } = richTextUnderlineForMatchup(matchup, favSide, awayName, homeName);

    // Compose A..K row
    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },           // A Game ID
        { userEnteredValue: { stringValue: displayDate } },          // B Date
        { userEnteredValue: { stringValue: weekLabel } },            // C Week
        { userEnteredValue: { stringValue: kickoff } },              // D Status
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E Matchup
        { userEnteredValue: { stringValue: "" } },                   // F Final Score
        { userEnteredValue: spreadAway != null ? { numberValue: spreadAway } : {} }, // G Away Spread
        { userEnteredValue: mlAway     != null ? { numberValue: mlAway }     : {} }, // H Away ML
        { userEnteredValue: spreadHome != null ? { numberValue: spreadHome } : {} }, // I Home Spread
        { userEnteredValue: mlHome     != null ? { numberValue: mlHome }     : {} }, // J Home ML
        { userEnteredValue: total      != null ? { numberValue: total }      : {} }, // K Total
      ],
    });
  }

  if (outRows.length === 0) {
    console.log("No new rows to write.");
    return;
  }

  // Find sheet id
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

  // Filter out already-existing Game IDs
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

  const startRowIndex = (existing.data.values?.length || 0) + 1;
  const endRowIndex = startRowIndex + newRows.length;

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
              endColumnIndex: 11, // A..K
            },
          },
        },
      ],
    },
  });

  console.log(`✅ Prefill completed: ${newRows.length} new rows`);
}

/* ========== Finals sweep (no underline changes here) ========== */
const COLORS = {
  green: { red: 0.85, green: 0.95, blue: 0.85 },
  red:   { red: 0.98, green: 0.85, blue: 0.85 },
  gray:  { red: 0.93, green: 0.93, blue: 0.93 },
};

async function runFinalsSweep() {
  console.log(`🏁 Finals sweep for ${LEAGUE}/${TAB_NAME}`);

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

  const COL = { A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10 };

  const valueUpdates = [];
  const formatRequests = [];
  let finalsWritten = 0;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i] || [];
    const rowNum = i + 2;

    const gameId = (r[0] || "").trim();
    if (!gameId) continue;

    const statusCell = (r[3] || "").trim();
    const finalScoreCell = (r[5] || "").trim();

    let isFinal = false;
    let awayScore = null, homeScore = null;

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

      if (isFinal && (!finalScoreCell || statusCell.toLowerCase() !== "final")) {
        const finalScoreStr = `${awayScore}-${homeScore}`;
        valueUpdates.push({ range: `${TAB_NAME}!D${rowNum}:D${rowNum}`, values: [["Final"]] });
        valueUpdates.push({ range: `${TAB_NAME}!F${rowNum}:F${rowNum}`, values: [[finalScoreStr]] });
        finalsWritten++;
      }

      if (!isFinal) continue; // ✅ Only grade when Final

      // Winner bold only (do NOT add underline here — preserves prefill underline)
      const matchupText = r[4] || "";
      const [awayName, homeName] = (matchupText || "").split(" @ ").map(s => (s || "").trim());
      const winnerSide = awayScore > homeScore ? "away" : (homeScore > awayScore ? "home" : null);

      if (winnerSide) {
        const text = matchupText;
        const idx = text.indexOf(winnerSide === "away" ? awayName : homeName);
        if (idx >= 0) {
          const end = idx + (winnerSide === "away" ? awayName.length : homeName.length);
          const runs = [];
          if (idx > 0) runs.push({ startIndex: 0 });
          runs.push({ startIndex: idx, format: { bold: true } });
          if (end < text.length) runs.push({ startIndex: end });

          formatRequests.push({
            updateCells: {
              rows: [{ values: [{ userEnteredValue: { stringValue: text }, textFormatRuns: runs }] }],
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
        }
      }

      // Grade markets
      const awaySpread = asNum(r[6]);
      const awayML     = asNum(r[7]);
      const homeSpread = asNum(r[8]);
      const homeML     = asNum(r[9]);
      const total      = asNum(r[10]);

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

      if (awayML != null || homeML != null) {
        if (awayScore > homeScore) {
          colorCell(COL.H, COLORS.green);
          colorCell(COL.J, COLORS.red);
        } else if (homeScore > awayScore) {
          colorCell(COL.H, COLORS.red);
          colorCell(COL.J, COLORS.green);
        }
      }

      if (awaySpread != null) {
        const awayCovers = awayScore + awaySpread > homeScore ? true
                         : awayScore + awaySpread < homeScore ? false
                         : null;
        colorCell(COL.G, awayCovers === null ? COLORS.gray : (awayCovers ? COLORS.green : COLORS.red));
      }
      if (homeSpread != null) {
        const homeCovers = homeScore + homeSpread > awayScore ? true
                         : homeScore + homeSpread < awayScore ? false
                         : null;
        colorCell(COL.I, homeCovers === null ? COLORS.gray : (homeCovers ? COLORS.green : COLORS.red));
      }

      if (total != null && awayScore != null && homeScore != null) {
        const sum = awayScore + homeScore;
        const color = sum > total ? COLORS.green
                    : sum < total ? COLORS.red
                    : COLORS.gray;
        colorCell(COL.K, color);
      }
    } catch (e) {
      console.warn(`Skipping row ${rowNum} / game ${gameId}: ${e.message}`);
    }
  }

  if (valueUpdates.length > 0) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { valueInputOption: "USER_ENTERED", data: valueUpdates },
    });
  }
  if (formatRequests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { requests: formatRequests },
    });
  }

  console.log(`✅ Finals written: ${finalsWritten} | formatted finals`);
}

/* ========== Router ========== */
(async () => {
  try {
    if (PREFILL_MODE === "finals") await runFinalsSweep();
    else await runPrefill();
  } catch (e) {
    console.error("❌ Orchestrator fatal:", e?.response?.data ?? e);
    process.exit(1);
  }
})();
