// cfb-halftime-check.mjs
// Single-game halftime watcher for CFB
// - Detects halftime reliably (handles ESPN's two summary shapes)
// - Writes Half Score + Live lines to Google Sheet
// - Adaptive polling; DEBUG_MODE=true -> very short sleeps for testing

import { readFile } from "node:fs/promises";
import { google } from "googleapis";

// ---------- ENV ----------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT; // full JSON string
const TAB_NAME = process.env.TAB_NAME || "CFB";
const EVENT_ID = process.env.TARGET_GAME_ID;
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

// ---------- HELPERS ----------
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "cfb-halftime-check/1.0" } });
  if (!r.ok) {
    throw new Error(`HTTP ${r.status} ${url}`);
  }
  return r.json();
}

function pick(obj, path) {
  try {
    return path
      .replace(/\[(\d+)\]/g, ".$1")
      .split(".")
      .reduce((acc, k) => (acc == null ? undefined : acc[k]), obj);
  } catch {
    return undefined;
  }
}

function parseStatus(summary) {
  // ESPN summary has two commonly-seen shapes:
  //  A) header.competitions[0].status.*
  //  B) competitions[0].status.*
  // Try A then B
  const a = pick(summary, "header.competitions.0.status") || {};
  const b = pick(summary, "competitions.0.status") || {};

  const src = a.type?.shortDetail ? a : b;
  const type = src.type || {};
  const shortDetail = type.shortDetail || "";
  const state = type.state || ""; // "pre","in","post"
  const period = (src.period ?? 0);
  const displayClock = src.displayClock ?? "0:00";

  return { shortDetail, state, period, displayClock };
}

function getTeams(summary) {
  // Prefer header path; fallback to competitions
  const compA = pick(summary, "header.competitions.0") || {};
  const compB = pick(summary, "competitions.0") || {};
  const comp = compA.competitors ? compA : compB;

  const competitors = comp.competitors || [];
  const awayObj = competitors.find(c => c.homeAway === "away") || {};
  const homeObj = competitors.find(c => c.homeAway === "home") || {};

  // Use shortDisplayName if available; fallback to displayName
  const awayName = awayObj.team?.shortDisplayName || awayObj.team?.displayName || "Away";
  const homeName = homeObj.team?.shortDisplayName || homeObj.team?.displayName || "Home";

  const awayScore = Number(awayObj.score ?? 0);
  const homeScore = Number(homeObj.score ?? 0);

  return { awayName, homeName, awayScore, homeScore };
}

function getKickoffISO(summary) {
  return pick(summary, "header.competitions.0.date") ||
         pick(summary, "competitions.0.date") || null;
}

// Normalize to match your sheet’s abbreviations (e.g., “Boise State” -> “Boise St”)
function normalizeTeamName(name) {
  let n = String(name).trim();
  n = n.replace(/\./g, "");
  // Common sheet-style abbreviations
  n = n.replace(/\bState\b/gi, "St");
  n = n.replace(/\bSaint\b/gi, "St");
  n = n.replace(/\bHawai'i\b/gi, "Hawai'i"); // keep as-is
  n = n.replace(/\s+/g, " ");
  return n;
}

function buildMatchup(away, home) {
  return `${normalizeTeamName(away)} @ ${normalizeTeamName(home)}`;
}

function etDateStr(iso) {
  if (!iso) return "";
  const dt = new Date(iso);
  // Convert to ET by formatting with that tz (Node 20+)
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(dt);

  const fx = (t) => parts.find(x => x.type === t)?.value ?? "";
  return `${fx("month")}/${fx("day")}/${fx("year")}`;
}

function ms(mins) {
  return Math.max(60_000, Math.round(mins * 60_000)); // clamp >= 60s
}

function decideSleepMins(period, displayClock) {
  // default cadence: 20m
  let sleepMins = 20;

  // When Q2 and <= 10:00, sleep = 2 × remaining (clamped)
  if (Number(period) === 2) {
    const m = /(\d{1,2}):(\d{2})/.exec(displayClock || "");
    if (m) {
      const left = Number(m[1]) + Number(m[2]) / 60;
      if (left <= 10) {
        sleepMins = Math.max(1, Math.ceil(left * 2));
      }
    }
  }

  // DEBUG mode: shorten to ~12 seconds
  if (DEBUG_MODE) sleepMins = Math.min(sleepMins, 0.2);

  return sleepMins;
}

// ---------- Google Sheets ----------
async function getSheetsClient() {
  const creds = JSON.parse(SA_JSON);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  const jwt = new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}

async function findRowIndex(sheets, dateET, matchup) {
  // read header + up to 2000 rows (adjust if your sheet is bigger)
  const range = `${TAB_NAME}!A1:P2000`;
  const resp = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
  const rows = resp.data.values || [];
  if (rows.length === 0) return -1;

  // find columns
  const header = rows[0].map(h => h.toLowerCase());
  const colDate = header.findIndex(h => h.startsWith("date"));
  const colMatch = header.findIndex(h => h === "matchup");

  if (colDate < 0 || colMatch < 0) return -1;

  for (let i = 1; i < rows.length; i++) {
    const r = rows[i] || [];
    const d = (r[colDate] || "").trim();
    const m = (r[colMatch] || "").trim();
    if (d === dateET && m === matchup) return i; // zero-based index
  }
  return -1;
}

async function writeHalftime(sheets, rowIndex, values) {
  // Map header → column letter quickly by reading header row again
  const headerResp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z1`
  });
  const header = (headerResp.data.values?.[0] || []).map(s => s.trim());

  const writeMap = {
    "Status": values.status,
    "Half Score": values.halfScore,

    // Try "Live ..." columns first; if your sheet uses base names, it will still match those
    "Live Away Spread": values.liveAwaySpread,
    "Live Away ML": values.liveAwayMl,
    "Live Home Spread": values.liveHomeSpread,
    "Live Home ML": values.liveHomeMl,
    "Live Total": values.liveTotal,

    // Fallbacks in case your sheet doesn't have the "Live ..." variants
    "Away Spread": values.liveAwaySpread,
    "Away ML": values.liveAwayMl,
    "Home Spread": values.liveHomeSpread,
    "Home ML": values.liveHomeMl,
    "Total": values.liveTotal,
  };

  // Build updates only for columns that exist AND we have non-empty values
  const updates = [];
  Object.entries(writeMap).forEach(([name, val]) => {
    if (val === undefined || val === null || val === "") return;
    const idx = header.findIndex(h => h.toLowerCase() === name.toLowerCase());
    if (idx >= 0) {
      const col = String.fromCharCode("A".charCodeAt(0) + idx);
      const rowNum = rowIndex + 1; // 1-based
      updates.push({ range: `${TAB_NAME}!${col}${rowNum + 1}`, values: [[val]] });
    }
  });

  if (updates.length === 0) {
    console.log("No writable columns found or nothing to write.");
    return 0;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: updates
    }
  });
  return updates.length;
}

// ---------- MAIN ----------
async function main() {
  const sheets = await getSheetsClient();

  let totalMin = 0;
  for (;;) {
    // 1) Fetch summary
    const sum = await fetchJson(`https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=${EVENT_ID}`);

    const { shortDetail, state, period, displayClock } = parseStatus(sum);
    const { awayName, homeName, awayScore, homeScore } = getTeams(sum);
    const kickoffISO = getKickoffISO(sum);
    const dateET = etDateStr(kickoffISO);
    const matchup = buildMatchup(awayName, homeName);

    const tag = shortDetail || state || "unknown";
    const qStr = `Q${period || 0}`;
    console.log(`[${tag}] ${matchup}  ${qStr}  (clock ${displayClock})`);

    // Halftime?
    const isHalftime =
      /halftime/i.test(shortDetail) ||
      (state === "in" && Number(period) === 2 && /^0?:?0{1,2}\b/.test(displayClock || "")); // safety

    if (isHalftime) {
      // 2) Prepare values
      const halfScoreText = `${awayScore}-${homeScore}`;

      // TODO: Live odds block – if you already had Playwright-based scraping, plug it in here.
      // For stability, we write what we have; undefined values are ignored by writeHalftime().
      const values = {
        status: "Halftime",
        halfScore: halfScoreText,
        // liveAwaySpread: ..., liveAwayMl: ..., liveHomeSpread: ..., liveHomeMl: ..., liveTotal: ...
      };

      // 3) Find row & write
      const rowIdx = await findRowIndex(sheets, dateET, matchup);
      if (rowIdx < 0) {
        console.log(`No matching row found for Date=${dateET} & Matchup="${matchup}".`);
        return;
      }
      const wrote = await writeHalftime(sheets, rowIdx, values);
      console.log(`✅ wrote ${wrote} cell(s).`);
      return; // done
    }

    // Not halftime yet — sleep with adaptive cadence
    const sleepMins = decideSleepMins(period, displayClock);
    console.log(`Sleeping ${sleepMins}m (${sleepMins * 60}s)…`);

    await new Promise(r => setTimeout(r, ms(sleepMins)));
    totalMin += sleepMins;
    if (totalMin >= MAX_TOTAL_MIN) {
      console.log("⏹ Reached MAX_TOTAL_MIN, exiting.");
      return;
    }
  }
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
