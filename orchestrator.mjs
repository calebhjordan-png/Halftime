// orchestrator.mjs — stateless, ESPN/ESPN BET only
// Writes to two tabs: NFL, CFB
// Columns: Date | Week | Status | Matchup | Live Score | Live Spread | Live Total | Opening Line | Opening Total | Live Fav ML | Live Dog ML | 2H Spread | 2H Total | Final Score

import { execSync } from "child_process";
import fetch from "node-fetch";
import { google } from "googleapis";

// ---------- CONFIG ----------
const PREGAME_OFFSET_MIN = 2;               // capture pregame at kickoff + 2 minutes
const DEFAULT_PROVIDER = "ESPN BET";        // prefer ESPN BET, fall back to ESPN if blank
const TABS = { nfl: "NFL", ncaaf: "CFB" };  // sheet tab names
// --------------------------------

const SHEET_ID = process.env.SHEET_ID;
const CREDS = JSON.parse(process.env.GOOGLE_CREDENTIALS || "{}");
if (!SHEET_ID || !CREDS.client_email) {
  console.error("Missing SHEET_ID or GOOGLE_CREDENTIALS in GitHub secrets.");
  process.exit(1);
}

// Google Sheets client
const jwt = new google.auth.JWT(
  CREDS.client_email, null, CREDS.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth: jwt });

// Helpers ---------------------------------------------------------
function mmdd(d = new Date()) {
  return `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")}`;
}
function isoPlusMinutes(iso, mins) {
  return new Date(new Date(iso).getTime() + mins * 60000);
}
async function fetchJSON(url) {
  const r = await fetch(url, { headers: { "User-Agent": "halftime-orchestrator" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}
function scoreString(comp) {
  const home = comp.competitors.find(c => c.homeAway === "home");
  const away = comp.competitors.find(c => c.homeAway === "away");
  return `${away.score}-${home.score}`;
}
function matchupString(away, home) {
  return `${away} @ ${home}`;
}
async function readRange(range) {
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
  return res.data.values || [];
}
async function appendRow(tab, row) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${tab}!A2`,
    valueInputOption: "RAW",
    requestBody: { values: [row] }
  });
}
// dedupe by (Date, Matchup, Status)
async function alreadyLogged(tab, date, matchup, status) {
  const rows = await readRange(`${tab}!A2:N`); // A..N (through Final Score column)
  const key = `${date}|${matchup}|${status}`.toUpperCase();
  for (const r of rows) {
    if (!r?.length) continue;
    const rowKey = `${(r[0]||"")}|${(r[3]||"")}|${(r[2]||"")}`.toUpperCase(); // Date (A), Matchup (D), Status (C)
    if (rowKey === key) return true;
  }
  return false;
}

// call your espn.mjs; prefer ESPN BET, fall back to ESPN if needed
function runEspn({ league, away, home, provider = DEFAULT_PROVIDER }) {
  const tryOnce = (prov) => {
    const cmd = `node espn.mjs --league=${league} --away="${away}" --home="${home}" --provider="${prov}" --dom`;
    try {
      const out = execSync(cmd, { encoding: "utf8" });
      const json = (out.match(/JSON:\s*\n([\s\S]+)$/) || [])[1];
      return json ? JSON.parse(json) : null;
    } catch (e) {
      return null;
    }
  };
  // try ESPN BET, then ESPN (generic)
  return tryOnce(provider) || tryOnce("ESPN");
}

// ESPN scoreboards
const LEAGUES = [
  { key: "nfl",   tab: TABS.nfl,   url: "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard" },
  { key: "ncaaf", tab: TABS.ncaaf, url: "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard" }
];

function getWeekLabel(sb) {
  try {
    const lg = sb?.leagues?.[0];
    const wk = lg?.calendar?.find?.(c => c?.label && c?.entries)?.entries?.find?.(e => e?.startDate && e?.endDate);
    // fallback to simple "Week" if provided
    return lg?.season?.type?.name === "Regular Season" ? (lg?.week?.number ? `Week ${lg.week.number}` : "") : (lg?.season?.type?.name || "");
  } catch { return ""; }
}

// Main per-league handler -----------------------------------------
async function handleLeague(lg) {
  const sb = await fetchJSON(lg.url);
  const weekLabel = getWeekLabel(sb);
  const events = sb.events || [];
  const today = mmdd();

  for (const ev of events) {
    const comp = ev.competitions?.[0]; if (!comp) continue;
    const home = comp.competitors.find(c => c.homeAway === "home")?.team?.shortDisplayName;
    const away = comp.competitors.find(c => c.homeAway === "away")?.team?.shortDisplayName;
    if (!home || !away) continue;

    const matchup = matchupString(away, home);
    const kickoffIso = comp.date || ev.date;
    const statusName = comp?.status?.type?.name || "";

    // ---------- PREGAME at Kickoff + 2 min ----------
    if (new Date() >= isoPlusMinutes(kickoffIso, PREGAME_OFFSET_MIN)) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "PREGAME");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          // row: Date | Week | Status | Matchup | Live Score | Live Spread | Live Total | Opening Line | Opening Total | Live Fav ML | Live Dog ML | 2H Spread | 2H Total | Final Score
          await appendRow(lg.tab, [
            today, weekLabel, "PREGAME", matchup, "", "", "",          // A..G
            r.spread ?? "", r.total ?? "",                               // Opening Line, Opening Total (H..I)
            "", "",                                                      // Live Fav ML, Live Dog ML (J..K) blank at pregame
            "", "",                                                      // 2H Spread, 2H Total (L..M) blank
            ""                                                           // Final Score (N)
          ]);
          console.log(`[${lg.tab}] PREGAME wrote: ${matchup}`);
        }
      }
    }

    // ---------- HALFTIME (live) ----------
    if (statusName === "STATUS_HALFTIME") {
      const logged = await alreadyLogged(lg.tab, today, matchup, "HALFTIME");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          await appendRow(lg.tab, [
            today, weekLabel, "HALFTIME", matchup,                      // A..D
            scoreString(comp),                                          // Live Score (E)
            r.spread ?? "", r.total ?? "",                              // Live Spread, Live Total (F..G)
            "", "",                                                     // Opening Line/Total (H..I) blank here; already captured at PREGAME
            r.favML ?? "", r.dogML ?? "",                               // Live Fav ML, Live Dog ML (J..K)
            "", "",                                                     // 2H Spread, 2H Total (L..M) — TODO if you want later
            ""                                                          // Final Score (N)
          ]);
          console.log(`[${lg.tab}] HALFTIME wrote: ${matchup}`);
        }
      }
    }
  }
}

// Entry ------------------------------------------------------------
async function main() {
  for (const lg of LEAGUES) {
    if (!lg.tab) continue;
    try { await handleLeague(lg); }
    catch (e) { console.error(`[${lg.key}] error:`, e.message); }
  }
  console.log("✓ run complete.");
}
await main();
