// orchestrator.mjs — stateless scheduler for espn.mjs
// Tabs: NFL, CFB
// Columns (A→P): 
// Date | Week | Status | Matchup | Final Score | Away Spread | Away ML | Home Spread | Home ML | Total | Live Score | Live Away Spread | Live Away ML | Live Home Spread | Live Home ML | Live Total

import { execSync } from "child_process";
import fetch from "node-fetch";
import { google } from "googleapis";

// ---------- CONFIG ----------
const OPENING_OFFSET_MIN_BEFORE_KICK = 15;   // capture opening 15 min BEFORE kickoff
const DEFAULT_PROVIDER = "ESPN BET";         // prefer ESPN BET, fallback to ESPN
const TABS = { nfl: "NFL", ncaaf: "CFB" };   // sheet tab names
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

// ---------- Helpers ----------
function mmdd(d = new Date()) {
  return `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")}`;
}
function isoMinusMinutes(iso, mins) {
  return new Date(new Date(iso).getTime() - mins * 60000);
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
// de-dupe by (Date, Matchup, Status)
async function alreadyLogged(tab, date, matchup, status) {
  const rows = await readRange(`${tab}!A2:P`); // through Live Total
  const key = `${date}|${matchup}|${status}`.toUpperCase();
  for (const r of rows) {
    if (!r?.length) continue;
    // A=Date, C=Status, D=Matchup
    const rowKey = `${(r[0]||"")}|${(r[3]||"")}|${(r[2]||"")}`.toUpperCase();
    if (rowKey === key) return true;
  }
  return false;
}

// run your espn.mjs; try ESPN BET, then ESPN
function runEspn({ league, away, home, provider = DEFAULT_PROVIDER }) {
  const tryOnce = (prov) => {
    const cmd = `node espn.mjs --league=${league} --away="${away}" --home="${home}" --provider="${prov}" --dom`;
    try {
      const out = execSync(cmd, { encoding: "utf8" });
      const json = (out.match(/JSON:\s*\n([\s\S]+)$/) || [])[1];
      return json ? JSON.parse(json) : null;
    } catch {
      return null;
    }
  };
  return tryOnce(provider) || tryOnce("ESPN");
}

// ESPN scoreboards
const LEAGUES = [
  { key: "nfl",   tab: TABS.nfl,   url: "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard" },
  { key: "ncaaf", tab: TABS.ncaaf, url: "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard" }
];

// pull a readable "Week" label if ESPN provides it
function getWeekLabel(sb) {
  try {
    const lg = sb?.leagues?.[0];
    if (lg?.week?.number) return `Week ${lg.week.number}`;
    // Fallback to season type (e.g., "Bowl Season" / "Regular Season")
    return lg?.season?.type?.name || "";
  } catch { return ""; }
}

// ---------- Per-league work ----------
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
    const isFinal = (comp?.status?.type?.completed === true) || statusName === "STATUS_FINAL";
    const now = new Date();

    // 1) OPENING — 15 minutes BEFORE kickoff
    if (now >= isoMinusMinutes(kickoffIso, OPENING_OFFSET_MIN_BEFORE_KICK)) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "OPENING");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          // A..P: Date | Week | Status | Matchup | Final Score | AwaySpr | AwayML | HomeSpr | HomeML | Total | LiveScore | LiveAwaySpr | LiveAwayML | LiveHomeSpr | LiveHomeML | LiveTotal
          await appendRow(lg.tab, [
            today, weekLabel, "OPENING", matchup,
            "",                           // Final Score (E) — not known yet
            r.spread ?? "", r.favML ?? "",  // F,G   (Away side)
            "", "",                          // H,I   (Home side) — opening lines usually quoted from favorite side
            r.total ?? "",                   // J     (Total)
            "", "", "", "", "",              // K..O  Live fields blank for opening
            ""                                // P     Live Total blank for opening
          ]);
          console.log(`[${lg.tab}] OPENING wrote: ${matchup}`);
        }
      }
    }

    // 2) HALFTIME — one capture
    if (statusName === "STATUS_HALFTIME") {
      const logged = await alreadyLogged(lg.tab, today, matchup, "HALFTIME");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          // Map favorite/dog to Away/Home buckets if you want; here we just write the single live line on both "away/home" slots as neutral live data
          await appendRow(lg.tab, [
            today, weekLabel, "HALFTIME", matchup,
            "",                               // Final Score unknown at halftime
            "", "", "", "", "",               // F..J pregame columns not re-written here
            scoreString(comp),                // K  Live Score
            r.spread ?? "", r.favML ?? "",    // L..M Live Away Spread / Live Away ML (generic live line)
            "", "",                           // N..O Live Home Spread / Live Home ML (leave blank unless you split sides)
            r.total ?? ""                     // P  Live Total
          ]);
          console.log(`[${lg.tab}] HALFTIME wrote: ${matchup}`);
        }
      }
    }

    // 3) FINAL — always record final score once
    if (isFinal) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "FINAL");
      if (!logged) {
        await appendRow(lg.tab, [
          today, weekLabel, "FINAL", matchup,
          scoreString(comp),                 // Final Score (E)
          "", "", "", "", "",                // F..J unchanged
          "", "", "", "", "",                // K..O live blanks
          ""                                 // P
        ]);
        console.log(`[${lg.tab}] FINAL wrote: ${matchup}`);
      }
    }
  }
}

// ---------- Entry ----------
async function main() {
  for (const lg of LEAGUES) {
    if (!lg.tab) continue;
    try { await handleLeague(lg); }
    catch (e) { console.error(`[${lg.key}] error:`, e.message); }
  }
  console.log("✓ orchestrator run complete.");
}

await main();
