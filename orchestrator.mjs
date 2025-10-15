import { google } from "googleapis";
import axios from "axios";

// ---- ENV ----
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",   // "nfl" or "college-football"
  TAB_NAME = "CFB",              // "NFL" or "CFB" (sheet tab title)
  PREFILL_MODE = "week",         // "today" | "week"
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

// ---- Google Auth ----
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// ---- Helpers ----
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ESPN needs YYYYMMDD (no dashes)
function fmtESPNDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

// For sheet display
function fmtISODate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function fmtKickET(isoStr) {
  const dt = new Date(isoStr);
  return dt.toLocaleTimeString("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
  }).replace(" ET", ""); // avoid “ET” literal in cell
}

async function fetchJson(url) {
  const res = await axios.get(url);
  return res.data;
}

const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football";
const scoreboardUrl = (league, yyyymmdd) =>
  `${ESPN_BASE}/${league}/scoreboard?dates=${yyyymmdd}`;
const summaryUrl = (league, gameId) =>
  `${ESPN_BASE}/${league}/summary?event=${gameId}`;

// Underline favorite safely (Google Sheets textFormatRuns rules)
function richTextUnderlineForMatchup(matchup, favSide, awayName, homeName) {
  const text = matchup || "";
  const len = text.length;
  if (!text || !favSide) return { text, runs: [] };

  const favName = favSide === "away" ? (awayName || "") : (homeName || "");
  if (!favName) return { text, runs: [] };

  const start = text.indexOf(favName);
  if (start < 0 || start >= len) return { text, runs: [] };

  const end = start + favName.length;
  const runs = [];
  if (start > 0) runs.push({ startIndex: 0 });                 // default styling
  runs.push({ startIndex: start, format: { underline: true } }); // favorite
  if (end < len) runs.push({ startIndex: end });                 // trailing default
  return { text, runs };
}

// ---- Main ----
async function main() {
  console.log(`Running orchestrator for ${LEAGUE}, mode=${PREFILL_MODE}`);

  // Build the date window
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);

  if (PREFILL_MODE === "today") {
    // just today
  } else {
    // whole week window
    start.setDate(start.getDate() - 1);
    end.setDate(end.getDate() + 7);
  }

  const days = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    days.push(new Date(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }

  // Fetch all scoreboards in parallel
  const scoreboardPairs = (
    await Promise.allSettled(
      days.map((d) =>
        fetchJson(scoreboardUrl(LEAGUE, fmtESPNDate(d))).then((j) => ({
          d,
          j,
        }))
      )
    )
  )
    .filter((r) => r.status === "fulfilled")
    .map((r) => r.value);

  const events = scoreboardPairs.flatMap((p) => p.j?.events ?? []);
  console.log(`Fetched ${events.length} games for ${LEAGUE}`);

  // Build rows
  const outRows = [];
  for (const ev of events) {
    const { id, date, competitions, week } = ev || {};
    const comp = competitions?.[0];

    const away = comp?.competitors?.find((c) => c.homeAway === "away");
    const home = comp?.competitors?.find((c) => c.homeAway === "home");
    const awayName = away?.team?.shortDisplayName || away?.team?.displayName || "";
    const homeName = home?.team?.shortDisplayName || home?.team?.displayName || "";
    const matchup = `${awayName} @ ${homeName}`;

    const kickoff = fmtKickET(date);
    const dateStr = fmtISODate(new Date(date));
    const weekLabel = week?.number ? `Week ${week.number}` : "Week ?";

    // Pre-game odds
    const odds = comp?.odds?.[0];
    let overUnder = odds?.overUnder ?? "";
    let spreadAway = "";
    let spreadHome = "";
    let mlAway = odds?.awayTeamOdds?.moneyLine ?? "";
    let mlHome = odds?.homeTeamOdds?.moneyLine ?? "";

    // Determine spreads (favoriteDetails & spread)
    if (odds?.spread && typeof odds.spread === "number") {
      const favText = odds.favoriteDetails || "";
      const spr = Math.abs(odds.spread);
      if (favText.includes(awayName)) {
        spreadAway = -spr;
        spreadHome = spr;
      } else if (favText.includes(homeName)) {
        spreadHome = -spr;
        spreadAway = spr;
      }
    }

    // If MLs are missing, pull from summary
    if (mlAway === "" || mlHome === "") {
      try {
        const sum = await fetchJson(summaryUrl(LEAGUE, id));
        const pc = sum?.pickcenter?.[0];
        if (pc) {
          if (mlAway === "" && pc.awayTeamOdds?.moneyLine != null) {
            mlAway = pc.awayTeamOdds.moneyLine;
          }
          if (mlHome === "" && pc.homeTeamOdds?.moneyLine != null) {
            mlHome = pc.homeTeamOdds.moneyLine;
          }
          if (!overUnder && pc.overUnder != null) overUnder = pc.overUnder;
        }
      } catch {
        // ignore summary fetch failure
      }
    }

    // Favorite underline
    let favSide = null;
    if (spreadAway !== "" && spreadHome !== "") {
      if (Number(spreadAway) < 0) favSide = "away";
      else if (Number(spreadHome) < 0) favSide = "home";
    }
    const { text, runs } = richTextUnderlineForMatchup(
      matchup,
      favSide,
      awayName,
      homeName
    );

    // Compose row for batchUpdate -> updateCells
    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },             // A Game ID
        { userEnteredValue: { stringValue: dateStr } },                // B Date
        { userEnteredValue: { stringValue: weekLabel } },              // C Week
        { userEnteredValue: { stringValue: kickoff } },                // D Status (kick)
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E Matchup (underline fav)
        { userEnteredValue: { stringValue: "" } },                     // F Final Score
        { userEnteredValue: spreadAway !== "" ? { numberValue: Number(spreadAway) } : {} }, // G Away Spread
        { userEnteredValue: mlAway !== "" ? { numberValue: Number(mlAway) } : {} },         // H Away ML
        { userEnteredValue: spreadHome !== "" ? { numberValue: Number(spreadHome) } : {} }, // I Home Spread
        { userEnteredValue: mlHome !== "" ? { numberValue: Number(mlHome) } : {} },         // J Home ML
        { userEnteredValue: overUnder !== "" ? { numberValue: Number(overUnder) } : {} },   // K Total
      ],
    });
  }

  // Nothing to do
  if (outRows.length === 0) {
    console.log("No new rows to build.");
    return;
  }

  // ---- Read existing IDs to avoid duplicates ----
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    includeGridData: false,
  });

  const sheet = meta.data.sheets?.find(
    (s) => s.properties?.title === TAB_NAME
  );
  if (!sheet) {
    console.error(`Tab "${TAB_NAME}" not found in the sheet`);
    process.exit(1);
  }
  const sheetId = sheet.properties.sheetId;

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

  // Compute start row (A2 is row index 1 zero-based; add current length)
  const startRowIndex = (existing.data.values?.length || 0) + 1; // zero-based
  const endRowIndex = startRowIndex + newRows.length;

  console.log(`Writing ${newRows.length} new rows to ${TAB_NAME}...`);

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

  console.log("✅ Prefill completed successfully.");
}

main().catch((e) => {
  console.error("❌ Orchestrator fatal:", e);
  process.exit(1);
});
