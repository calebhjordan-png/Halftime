import { google } from "googleapis";
import axios from "axios";

/** ========= ENV ========= */
const {
  GOOGLE_SHEET_ID,
  GOOGLE_SERVICE_ACCOUNT,
  LEAGUE = "college-football", // "nfl" or "college-football"
  TAB_NAME = "CFB",            // "NFL" or "CFB"
  PREFILL_MODE = "week",       // "today" | "week"
} = process.env;

if (!GOOGLE_SHEET_ID || !GOOGLE_SERVICE_ACCOUNT) {
  console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/** ========= GOOGLE AUTH ========= */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(GOOGLE_SERVICE_ACCOUNT),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/** ========= HELPERS ========= */
const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football";
const scoreboardUrl = (league, yyyymmdd) =>
  `${ESPN_BASE}/${league}/scoreboard?dates=${yyyymmdd}`;
const summaryUrl = (league, gameId) =>
  `${ESPN_BASE}/${league}/summary?event=${gameId}`;

const fetchJson = async (url) => (await axios.get(url)).data;

const asNum = (v) =>
  (v === 0 || (typeof v === "number" && !Number.isNaN(v))) ? v : (v != null && v !== "" ? Number(v) : null);

// ESPN needs YYYYMMDD
function fmtESPNDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

// Display date: DD/MM/YY  ← requested format
function fmtDisplayDateDMY(d) {
   const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}

// Status: kickoff time (no ET suffix)
function fmtKickET(isoStr) {
  const dt = new Date(isoStr);
  return dt.toLocaleTimeString("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "2-digit",
  }).replace(" ET", "");
}

/** Safely build underline runs for favorite (Google Sheets rule: startIndex < text.length) */
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
  if (start > 0) runs.push({ startIndex: 0 }); // default styling block
  runs.push({ startIndex: start, format: { underline: true } }); // underline favorite
  if (end < len) runs.push({ startIndex: end }); // trailing default
  return { text, runs };
}

/** Robust odds extraction:
 *  1) Prefer team spreads from awayTeamOdds/homeTeamOdds
 *  2) Else parse odds.spread + favoriteDetails
 *  3) Else parse odds.details text
 *  4) Else infer from moneylines
 */
function parseSpreadsFromOdds(odds, awayName, homeName, awayAbbr, homeAbbr) {
  let spreadAway = null;
  let spreadHome = null;

  // 1) Direct team odds spreads
  const aSpread = asNum(odds?.awayTeamOdds?.spread);
  const hSpread = asNum(odds?.homeTeamOdds?.spread);
  if (aSpread != null && hSpread != null) {
    spreadAway = aSpread;
    spreadHome = hSpread;
    return { spreadAway, spreadHome };
  }

  // 2) Use odds.spread + favoriteDetails
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

  // 3) Parse odds.details
  const details = (odds?.details || "").toLowerCase();
  if (details) {
    const rxNum = /([+-]?\d+(?:\.\d+)?)/; // first signed number after team token

    const posOf = (nm, abbr) => {
      for (const k of [nm, abbr].filter(Boolean)) {
        const i = details.indexOf(k.toLowerCase());
        if (i >= 0) return i;
      }
      return -1;
    };
    const aIdx = posOf(awayName, awayAbbr);
    const hIdx = posOf(homeName, homeAbbr);

    const numAfter = (idx) => {
      if (idx < 0) return null;
      const m = details.slice(idx).match(rxNum);
      return m ? asNum(m[1]) : null;
    };

    const aNum = numAfter(aIdx);
    const hNum = numAfter(hIdx);
    if (aNum != null && hNum != null) return { spreadAway: aNum, spreadHome: hNum };
    if (aNum != null) return { spreadAway: aNum, spreadHome: -aNum };
    if (hNum != null) return { spreadAway: -hNum, spreadHome: hNum };
  }

  return { spreadAway, spreadHome };
}

/** ========= MAIN ========= */
async function main() {
  console.log(`Running orchestrator for ${LEAGUE}, mode=${PREFILL_MODE}`);

  // Date window
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

  // Pull scoreboards
  const ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football";
  const boards = (
    await Promise.allSettled(
      days.map((d) =>
        fetchJson(`${ESPN_SCOREBOARD}/${LEAGUE}/scoreboard?dates=${
          d.getFullYear()}${String(d.getMonth()+1).padStart(2,"0")}${String(d.getDate()).padStart(2,"0")}`)
          .then((j) => ({ d, j }))
      )
    )
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

    const displayDate = fmtDisplayDateDMY(new Date(date)); // DD/MM/YY
    const kickoff = fmtKickET(date);
    const weekLabel = week?.number ? `Week ${week.number}` : "Week ?";

    const odds = comp?.odds?.[0] || {};
    let total = asNum(odds?.overUnder);
    let mlAway = asNum(odds?.awayTeamOdds?.moneyLine);
    let mlHome = asNum(odds?.homeTeamOdds?.moneyLine);

    let { spreadAway, spreadHome } = parseSpreadsFromOdds(odds, awayName, homeName, awayAbbr, homeAbbr);

    // Fill from summary if needed
    if (mlAway == null || mlHome == null || total == null || spreadAway == null || spreadHome == null) {
      try {
        const sum = await fetchJson(summaryUrl(LEAGUE, id));
        const pc = sum?.pickcenter?.[0];
        if (pc) {
          if (mlAway == null && pc.awayTeamOdds?.moneyLine != null) mlAway = asNum(pc.awayTeamOdds.moneyLine);
          if (mlHome == null && pc.homeTeamOdds?.moneyLine != null) mlHome = asNum(pc.homeTeamOdds.moneyLine);
          if (total == null && pc.overUnder != null) total = asNum(pc.overUnder);
          if (spreadAway == null && pc.awayTeamOdds?.spread != null) spreadAway = asNum(pc.awayTeamOdds.spread);
          if (spreadHome == null && pc.homeTeamOdds?.spread != null) spreadHome = asNum(pc.homeTeamOdds.spread);
        }
      } catch {}
    }

    // Last resort—infer from MLs
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

    outRows.push({
      values: [
        { userEnteredValue: { stringValue: String(id) } },         // A
        { userEnteredValue: { stringValue: displayDate } },        // B  (DD/MM/YY)
        { userEnteredValue: { stringValue: weekLabel } },          // C
        { userEnteredValue: { stringValue: kickoff } },            // D
        { userEnteredValue: { stringValue: text }, textFormatRuns: runs }, // E
        { userEnteredValue: { stringValue: "" } },                 // F
        { userEnteredValue: spreadAway != null ? { numberValue: spreadAway } : {} }, // G
        { userEnteredValue: mlAway != null ? { numberValue: mlAway } : {} },         // H
        { userEnteredValue: spreadHome != null ? { numberValue: spreadHome } : {} }, // I
        { userEnteredValue: mlHome != null ? { numberValue: mlHome } : {} },         // J
        { userEnteredValue: total != null ? { numberValue: total } : {} },           // K
      ],
    });
  }

  if (outRows.length === 0) {
    console.log("No new rows to build.");
    return;
  }

  // Avoid duplicates by Game ID
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
              endColumnIndex: 11,
            },
          },
        },
      ],
    },
  });

  console.log("✅ Prefill completed successfully.");
}

main().catch((e) => {
  console.error("❌ Orchestrator fatal:", e?.response?.data ?? e);
  process.exit(1);
});


