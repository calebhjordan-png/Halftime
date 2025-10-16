// orchestrator.mjs
import { google } from "googleapis";
import axios from "axios";

/* ==================== ENV ==================== */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football", // "nfl" | "college-football"
  TAB_NAME = "CFB",
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

/* ====== underline favorite in matchup cell (prefill) ====== */
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

    // Fallback: infer small default if MLs show clear favorite/underdog (rare case)
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

    // ⬇️ WRITE A..K (note the numeric cells use numberValue)
    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },           // A Game ID
        { userEnteredValue: { stringValue: displayDate } },          // B Date
        { userEnteredValue: { stringValue: weekLabel } },            // C Week
        { userEnteredValue: { stringValue: kickoff } },              // D Status
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E Matchup
        { userEnteredValue: { stringValue: "" } },                   // F Final Score (blank)
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
              endColumnIndex: 11, // ✅ A..K (end is exclusive)
            },
          },
        },
      ],
    },
  });

  console.log(`✅ Prefill completed: ${newRows.length} new rows`);
}

/* ==================== FINALS SWEEP + FORMATTING (unchanged logic) ==================== */
const COLORS = {
  green: { red: 0.85, green: 0.95, blue: 0.85 },
  red:   { red: 0.98, green: 0.85, blue: 0.85 },
  gray:  { red: 0.93, green: 0.93, blue: 0.93 },
};

async function runFinalsSweep() {
  console.log(`🏁 Finals sweep started for ${LEAGUE}/${TAB_NAME}`);

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

      // Only grade if Final
      if (!isFinal) continue;

      // Winner bold + keep favorite underline
      const matchupText = r[4] || "";
      const [awayName, homeName] = (matchupText || "").split(" @ ").map(s => (s || "").trim());
      const winnerSide = awayScore > homeScore ? "away" : (homeScore > awayScore ? "home" : null);

      const awaySpread = asNum(r[6]);
      const awayML     = asNum(r[7]);
      const homeSpread = asNum(r[8]);
      const homeML     = asNum(r[9]);
      const total      = asNum(r[10]);

      let favSide = null;
      if (awaySpread != null && homeSpread != null) {
        favSide = awaySpread < 0 ? "away" : (homeSpread < 0 ? "home" : null);
      } else if (awayML != null && homeML != null) {
        favSide = awayML < homeML ? "away" : (homeML < awayML ? "home" : null);
      }

      const text = matchupText || "";
      const byIndex = new Map();
      const apply = (team, style) => {
        if (!team) return;
        const idx = text.indexOf(team);
        if (idx >= 0) {
          const prev = byIndex.get(idx) || { startIndex: idx, format: {} };
          byIndex.set(idx, { startIndex: idx, format: { ...prev.format, ...style } });
        }
      };
      if (winnerSide === "away") apply(awayName, { bold: true });
      if (winnerSide === "home") apply(homeName, { bold: true });
      if (favSide === "away") apply(awayName, { underline: true });
      if (favSide === "home") apply(homeName, { underline: true });

      const finalRuns = Array.from(byIndex.values()).sort((a,b)=>a.startIndex-b.startIndex);
      formatRequests.push({
        updateCells: {
          rows: [{ values: [{ userEnteredValue: { stringValue: text }, textFormatRuns: finalRuns }] }],
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
      const COLORS = {
        green: { red: 0.85, green: 0.95, blue: 0.85 },
        red:   { red: 0.98, green: 0.85, blue: 0.85 },
        gray:  { red: 0.93, green: 0.93, blue: 0.93 },
      };

      // ML grading
      if (awayML != null || homeML != null) {
        if (awayScore > homeScore) {
          colorCell(COL.H, COLORS.green);
          colorCell(COL.J, COLORS.red);
        } else if (homeScore > awayScore) {
          colorCell(COL.H, COLORS.red);
          colorCell(COL.J, COLORS.green);
        }
      }

      // Spread grading
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

      // Total grading
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

  console.log(`✅ Finals written: ${finalsWritten} | formatted finals: done`);
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
