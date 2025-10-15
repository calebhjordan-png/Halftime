import { google } from "googleapis";
import axios from "axios";

const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football",
  TAB_NAME = "CFB",
  PREFILL_MODE = "week",
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("Missing required env vars");
  process.exit(1);
}

// -----------------------
// AUTH
// -----------------------
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// -----------------------
// UTILITIES
// -----------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fmtDate = (d) => d.toISOString().split("T")[0];
const fmtTime = (d) =>
  d.toLocaleTimeString("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
  }).replace(" ET", "");

function richTextUnderlineForMatchup(matchup, favSide, awayName, homeName) {
  const text = matchup || "";
  const len = text.length;

  if (!favSide || !text) return { text, runs: [] };

  const favName = favSide === "away" ? awayName || "" : homeName || "";
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

async function fetchJson(url) {
  const res = await axios.get(url);
  return res.data;
}

// -----------------------
// ESPN URL BUILDERS
// -----------------------
const base = "https://site.api.espn.com/apis/site/v2/sports/football";
const scoreboardUrl = (league, date) =>
  `${base}/${league}/scoreboard?dates=${date}`;
const summaryUrl = (league, gameId) =>
  `${base}/${league}/summary?event=${gameId}`;

// -----------------------
// CORE LOGIC
// -----------------------
async function main() {
  console.log(`Running orchestrator for ${LEAGUE}, mode=${PREFILL_MODE}`);

  const today = new Date();
  const start = new Date(today);
  const end = new Date(today);
  start.setDate(start.getDate() - 1);
  end.setDate(end.getDate() + 7);

  const dates = [];
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    dates.push(fmtDate(d));
  }

  // Batch fetch all scoreboards
  const scoreboards = (
    await Promise.allSettled(
      dates.map((d) =>
        fetchJson(scoreboardUrl(LEAGUE, d)).then((j) => ({ d, j }))
      )
    )
  )
    .filter((r) => r.status === "fulfilled")
    .map((r) => r.value);

  const games = scoreboards.flatMap((s) => s.j.events || []);
  console.log(`Fetched ${games.length} games for ${LEAGUE}`);

  const rows = [];

  for (const g of games) {
    const { id, date, shortName, competitions, week } = g;
    const comp = competitions?.[0];
    const odds = comp?.odds?.[0];
    const details = odds?.details || "";
    const overUnder = odds?.overUnder || "";

    const away = comp?.competitors?.find((c) => c.homeAway === "away");
    const home = comp?.competitors?.find((c) => c.homeAway === "home");

    const awayName = away?.team?.shortDisplayName || "";
    const homeName = home?.team?.shortDisplayName || "";

    const status = new Date(date);
    const kickoff = fmtTime(status);
    const weekLabel = week?.number ? `Week ${week.number}` : "Week ?";

    let spreadAway = "",
      spreadHome = "",
      mlAway = "",
      mlHome = "";

    // Parse odds
    if (odds?.spread) {
      const fav = odds?.favoriteDetails || "";
      const spread = parseFloat(odds.spread);
      if (fav.includes(awayName)) {
        spreadAway = -Math.abs(spread);
        spreadHome = Math.abs(spread);
      } else if (fav.includes(homeName)) {
        spreadHome = -Math.abs(spread);
        spreadAway = Math.abs(spread);
      }
    }

    mlAway = odds?.awayTeamOdds?.moneyLine || "";
    mlHome = odds?.homeTeamOdds?.moneyLine || "";

    // If missing MLs, fetch from summary (force fill)
    if (!mlAway || !mlHome) {
      try {
        const s = await fetchJson(summaryUrl(LEAGUE, id));
        const sOdds = s?.pickcenter?.[0];
        if (sOdds) {
          mlAway = sOdds.awayTeamOdds?.moneyLine ?? mlAway;
          mlHome = sOdds.homeTeamOdds?.moneyLine ?? mlHome;
        }
      } catch {
        /* ignore */
      }
    }

    const matchup = `${awayName} @ ${homeName}`;

    // Determine favorite for underline
    let favSide = null;
    if (spreadAway && spreadHome) {
      if (spreadAway < 0) favSide = "away";
      else if (spreadHome < 0) favSide = "home";
    }

    const { text, runs } = richTextUnderlineForMatchup(
      matchup,
      favSide,
      awayName,
      homeName
    );

    rows.push({
      values: [
        { userEnteredValue: { stringValue: id } }, // A: Game ID
        { userEnteredValue: { stringValue: fmtDate(new Date(date)) } }, // B: Date
        { userEnteredValue: { stringValue: weekLabel } }, // C: Week
        { userEnteredValue: { stringValue: kickoff } }, // D: Status (kickoff time)
        {
          userEnteredValue: { stringValue: text },
          textFormatRuns: runs,
        }, // E: Matchup
        { userEnteredValue: { stringValue: "" } }, // F: Final Score
        { userEnteredValue: { numberValue: spreadAway || null } }, // G: Away Spread
        { userEnteredValue: { numberValue: mlAway || null } }, // H: Away ML
        { userEnteredValue: { numberValue: spreadHome || null } }, // I: Home Spread
        { userEnteredValue: { numberValue: mlHome || null } }, // J: Home ML
        { userEnteredValue: { numberValue: overUnder || null } }, // K: Total
      ],
    });
  }

  // Write to sheet
  try {
    const sheetRes = await sheets.spreadsheets.values.get({
      spreadsheetId: GOOGLE_SHEET_ID,
      range: `${TAB_NAME}!A2:A`,
    });

    const existingIds = new Set(sheetRes.data.values?.flat() || []);
    const newRows = rows.filter((r) => !existingIds.has(r.values[0].userEnteredValue.stringValue));

    if (newRows.length === 0) {
      console.log("No new rows to write.");
      return;
    }

    console.log(`Writing ${newRows.length} new rows to ${TAB_NAME}...`);

    const requests = [
      {
        updateCells: {
          rows: newRows,
          fields: "userEnteredValue,textFormatRuns",
          range: {
            sheetId: null, // automatic sheet
            startRowIndex: sheetRes.data.values?.length + 1 || 1,
            startColumnIndex: 0,
          },
        },
      },
    ];

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: GOOGLE_SHEET_ID,
      requestBody: { requests },
    });

    console.log("✅ Prefill completed successfully.");
  } catch (err) {
    console.error("❌ Orchestrator fatal:", err);
    process.exit(1);
  }
}

main();
