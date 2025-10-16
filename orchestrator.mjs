import { google } from "googleapis";
import axios from "axios";

/* ===== ENV ===== */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",     // "nfl" | "college-football"
  TAB_NAME = "CFB",
  PREFILL_MODE = "week",           // "today" | "week" | "live_daily" | "finals"
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("❌ Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/* ===== Google Sheets ===== */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/* ===== ESPN URLs ===== */
const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football";
const CORE_BASE = "https://sports.core.api.espn.com/v2/sports/football";
const scoreboardUrl = (league, yyyymmdd) =>
  `${ESPN_BASE}/${league}/scoreboard?dates=${yyyymmdd}`;
const summaryUrl = (league, gameId) =>
  `${ESPN_BASE}/${league}/summary?event=${gameId}`;
const coreOddsUrl = (league, eventId, compId) =>
  `${CORE_BASE}/${league}/events/${eventId}/competitions/${compId}/odds?region=us&lang=en`;

const fetchJson = async (url) => (await axios.get(url)).data;
const asNum = (v) =>
  (v === 0 || (typeof v === "number" && !Number.isNaN(v)))
    ? v
    : (v != null && v !== "" ? Number(v) : null);

/* ===== Dates/Times ===== */
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

/* ===== Rich text helper (underline favorite team only) ===== */
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

/* ===== Spreads (robust parse) ===== */
function parseSpreadsFromOdds(odds, awayName, homeName, awayAbbr, homeAbbr) {
  const aTeam = asNum(odds?.awayTeamOdds?.spread);
  const hTeam = asNum(odds?.homeTeamOdds?.spread);
  if (aTeam != null && hTeam != null) {
    return { spreadAway: aTeam, spreadHome: hTeam };
  }

  const text = String(odds?.details ?? odds?.summary ?? "").trim();
  if (text) {
    const m = text.match(/\b([A-Z]{2,4})\s*([+-]?\d+(?:\.\d+)?)/);
    if (m) {
      const favAbbr = m[1];
      const val = Number(m[2]);
      if (!Number.isNaN(val)) {
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
  return { spreadAway: null, spreadHome: null };
}

/* ===== Deep odds digger for ML (handles ESPN’s many shapes) ===== */
function digMoneylines(obj) {
  if (!obj || typeof obj !== "object") return { a: null, h: null };

  // direct known shapes
  const candidates = [
    obj?.awayTeamOdds?.moneyLine,
    obj?.homeTeamOdds?.moneyLine,
    obj?.moneyLineAway,
    obj?.moneyLineHome,
    obj?.away?.moneyLine,
    obj?.home?.moneyLine,
    obj?.awayMoneyLine,
    obj?.homeMoneyLine,
  ];
  const aIdx = [0, 2, 4, 6].find(i => asNum(candidates[i]) != null);
  const hIdx = [1, 3, 5, 7].find(i => asNum(candidates[i]) != null);
  if (aIdx != null || hIdx != null) {
    return { a: asNum(candidates[aIdx]), h: asNum(candidates[hIdx]) };
  }

  // arrays
  if (Array.isArray(obj)) {
    for (const it of obj) {
      const { a, h } = digMoneylines(it);
      if (a != null || h != null) return { a, h };
    }
  }

  // objects (breadth-first)
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    if (v && typeof v === "object") {
      const { a, h } = digMoneylines(v);
      if (a != null || h != null) return { a, h };
    }
  }
  return { a: null, h: null };
}

/* ===== Moneyline resolver (beefed up for CFB) ===== */
async function extractMoneylines(comp, league, eventId) {
  const compId = comp?.id || comp?.uid?.split(":").pop();

  // 1) inline odds on competition
  const inline = comp?.odds?.[0];
  if (inline) {
    const { a, h } = digMoneylines(inline);
    if (a != null || h != null) return { mlAway: a, mlHome: h };
  }

  // 2) odds link or $ref found on inline odds or competition odds array
  const oddsHref =
    inline?.links?.find?.(l => (l.rel || []).includes("odds"))?.href ||
    inline?.$ref ||
    comp?.odds?.find?.(o => o?.$ref)?.$ref ||
    comp?.odds?.find?.(o => o?.links?.some?.(l => (l.rel || []).includes("odds")))?.links?.find?.(l => (l.rel || []).includes("odds"))?.href;

  if (oddsHref) {
    try {
      const data = await fetchJson(oddsHref);
      // The response can be a page with items[], or the object itself
      const srcs = [];
      if (data?.items) srcs.push(...data.items);
      srcs.push(data);

      for (const s of srcs) {
        const { a, h } = digMoneylines(s);
        if (a != null || h != null) return { mlAway: a, mlHome: h };
      }
    } catch (_) { /* ignore */ }
  }

  // 3) summary header competitions odds
  try {
    const sum = await fetchJson(summaryUrl(league, eventId));
    const hdrComp = sum?.header?.competitions?.[0];
    const sOdds = hdrComp?.odds?.[0];
    if (sOdds) {
      const { a, h } = digMoneylines(sOdds);
      if (a != null || h != null) return { mlAway: a, mlHome: h };
    }

    // ESPN sometimes nests a richer odds object behind a $ref here as well
    const ref =
      sOdds?.$ref ||
      sOdds?.links?.find?.(l => (l.rel || []).includes("odds"))?.href;
    if (ref) {
      try {
        const refData = await fetchJson(ref);
        const { a, h } = digMoneylines(refData?.items || refData);
        if (a != null || h != null) return { mlAway: a, mlHome: h };
      } catch (_) {}
    }
  } catch (_) { /* ignore */ }

  // 4) core odds endpoint (usually present for CFB when public ML exists)
  if (compId) {
    try {
      const core = await fetchJson(coreOddsUrl(league, eventId, compId));
      const pool = core?.items || core;
      const { a, h } = digMoneylines(pool);
      if (a != null || h != null) return { mlAway: a, mlHome: h };
    } catch (_) { /* ignore */ }
  }

  // no ML available
  return { mlAway: null, mlHome: null };
}

/* ===== PREFILL ===== */
async function runPrefill() {
  console.log(`🏈 Prefill for ${LEAGUE}, mode=${PREFILL_MODE}`);

  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);
  if (PREFILL_MODE !== "today") {
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
    const { id: eventId, date, competitions, week } = ev || {};
    const comp = competitions?.[0];
    if (!comp) continue;

    const away = comp?.competitors?.find(c => c.homeAway === "away");
    const home = comp?.competitors?.find(c => c.homeAway === "home");
    if (!away || !home) continue;

    const awayName = away.team?.shortDisplayName || away.team?.displayName || "";
    const homeName = home.team?.shortDisplayName || home.team?.displayName || "";
    const awayAbbr = away.team?.abbreviation || "";
    const homeAbbr = home.team?.abbreviation || "";

    const displayDate = fmtDisplayDateMDY(new Date(date));
    const kickoff = fmtKickET(date);
    const weekLabel = week?.number ? `Week ${week.number}` : "Week ?";

    // Base odds object & total
    const odds = comp?.odds?.[0] || {};
    const total = asNum(odds?.overUnder);

    // Moneylines (new robust resolver)
    const { mlAway, mlHome } = await extractMoneylines(comp, LEAGUE, eventId);

    // Spreads
    const { spreadAway, spreadHome } = parseSpreadsFromOdds(
      odds, awayName, homeName, awayAbbr, homeAbbr
    );

    // Favorite to underline
    let favSide = null;
    if (spreadAway != null && spreadHome != null) {
      favSide = spreadAway < 0 ? "away" : (spreadHome < 0 ? "home" : null);
    } else if (mlAway != null && mlHome != null) {
      favSide = mlAway < mlHome ? "away" : (mlHome < mlAway ? "home" : null);
    }

    const matchup = `${awayName} @ ${homeName}`;
    const { text, runs } = richTextUnderlineForMatchup(matchup, favSide, awayName, homeName);

    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(eventId) } },       // A
        { userEnteredValue: { stringValue: displayDate } },           // B
        { userEnteredValue: { stringValue: weekLabel } },             // C
        { userEnteredValue: { stringValue: kickoff } },               // D
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs,
          userEnteredFormat: { textFormat: { underline: false, bold: false } } }, // E
        { userEnteredValue: { stringValue: "" } },                    // F
        { userEnteredValue: spreadAway != null ? { numberValue: spreadAway } : {} }, // G
        { userEnteredValue: mlAway     != null ? { numberValue: mlAway }     : {} }, // H
        { userEnteredValue: spreadHome != null ? { numberValue: spreadHome } : {} }, // I
        { userEnteredValue: mlHome     != null ? { numberValue: mlHome }     : {} }, // J
        { userEnteredValue: total      != null ? { numberValue: total }      : {} }, // K
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

  // Filter out already-present Game IDs
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
            fields:
              "userEnteredValue,textFormatRuns," +
              "userEnteredFormat.textFormat.underline," +
              "userEnteredFormat.textFormat.bold",
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

/* ===== Finals sweep (unchanged from previous) ===== */
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

      if (!isFinal) continue; // only grade once Final

      // Winner bold (preserving underline)
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
          colorCell(COL.H, COLORS.green); // away ML wins
          colorCell(COL.J, COLORS.red);
        } else if (homeScore > awayScore) {
          colorCell(COL.H, COLORS.red);
          colorCell(COL.J, COLORS.green); // home ML wins
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

/* ===== Entry ===== */
(async () => {
  try {
    if (PREFILL_MODE === "finals") await runFinalsSweep();
    else await runPrefill();
  } catch (e) {
    console.error("❌ Orchestrator fatal:", e?.response?.data ?? e);
    process.exit(1);
  }
})();
