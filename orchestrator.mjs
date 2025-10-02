// orchestrator.mjs — stateless writer for your NFL/CFB sheet
// Sheet columns (A..P):
// Date | Week | Status | Matchup | Final Score | Away Spread | Away ML | Home Spread | Home ML | Total | Half Score | Live Away Spread | Live Away ML | Live Home Spread | Live Home ML | Live Total

import { execSync } from "child_process";
import fetch from "node-fetch";
import { google } from "googleapis";

// ---------- CONFIG ----------
const OPENING_MIN_BEFORE_KICK = 15;           // write OPENING 15m before kickoff
const DEFAULT_PROVIDER = "ESPN BET";          // prefer ESPN BET, then fall back to ESPN
const TABS = { nfl: "NFL", ncaaf: "CFB" };    // tab names in your sheet
// --------------------------------

// ---- Secrets (GitHub Actions -> Settings -> Secrets and variables -> Actions) ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const CREDS = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT || "{}");

if (!SHEET_ID || !CREDS.client_email) {
  console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT secrets.");
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
  const rows = await readRange(`${tab}!A2:P`);
  const key = `${date}|${matchup}|${status}`.toUpperCase();
  for (const r of rows) {
    if (!r?.length) continue;
    // A=Date, C=Status, D=Matchup
    const rowKey = `${(r[0]||"")}|${(r[3]||"")}|${(r[2]||"")}`.toUpperCase();
    if (rowKey === key) return true;
  }
  return false;
}

// call your espn.mjs; try ESPN BET then ESPN
function runEspn({ league, away, home, provider = DEFAULT_PROVIDER }) {
  const tryOnce = (prov) => {
    const cmd = `node espn.mjs --league=${league} --away="${away}" --home="${home}" --provider="${prov}" --dom`;
    try {
      const out = execSync(cmd, { encoding: "utf8" });
      const json = (out.match(/JSON:\\s*\\n([\\s\\S]+)$/) || [])[1];
      return json ? JSON.parse(json) : null;
    } catch {
      return null;
    }
  };
  return tryOnce(provider) || tryOnce("ESPN");
}

// map a single spread/ML pair to Away/Home columns (best-effort)
function mapToAwayHome(spread, favML, dogML) {
  if (spread == null && favML == null && dogML == null) {
    return { aSpr: "", aML: "", hSpr: "", hML: "" };
  }
  const s = Number(spread);
  if (!Number.isFinite(s)) {
    return { aSpr: "", aML: "", hSpr: "", hML: "" };
  }
  // Heuristic:
  // if spread < 0 => treat Away as favorite; if spread > 0 => Home as favorite
  if (s < 0) {
    return { aSpr: s, aML: favML ?? "", hSpr: Math.abs(s), hML: dogML ?? "" };
  } else if (s > 0) {
    return { aSpr: -Math.abs(s), aML: dogML ?? "", hSpr: s, hML: favML ?? "" };
  } else {
    // pick ML sign to infer favorite if available
    if (typeof favML === "number" && favML < 0) {
      // assume away favorite
      return { aSpr: -0, aML: favML, hSpr: 0, hML: dogML ?? "" };
    }
    return { aSpr: 0, aML: "", hSpr: 0, hML: "" };
  }
}

// ESPN scoreboards (NFL + CFB)
const LEAGUES = [
  { key: "nfl",   tab: TABS.nfl,   url: "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard" },
  { key: "ncaaf", tab: TABS.ncaaf, url: "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard" }
];

// try to pull week label
function getWeekLabel(sb) {
  try {
    const lg = sb?.leagues?.[0];
    if (lg?.week?.number) return `Week ${lg.week.number}`;
    return lg?.season?.type?.name || "";
  } catch { return ""; }
}

// ---------- Per-league handler ----------
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
    const status = comp?.status?.type || {};
    const statusName = status.name || "";
    const isFinal = status.completed === true || statusName === "STATUS_FINAL";
    const now = new Date();

    // 1) OPENING — 15 minutes BEFORE kickoff
    if (now >= isoMinusMinutes(kickoffIso, OPENING_MIN_BEFORE_KICK)) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "OPENING");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          const { aSpr, aML, hSpr, hML } = mapToAwayHome(r.spread, r.favML, r.dogML);
          const row = [
            today,                       // A Date
            weekLabel,                   // B Week
            "OPENING",                   // C Status
            matchup,                     // D Matchup
            "",                          // E Final Score
            aSpr ?? "",                  // F Away Spread
            aML ?? "",                   // G Away ML
            hSpr ?? "",                  // H Home Spread
            hML ?? "",                   // I Home ML
            r.total ?? "",               // J Total
            "",                          // K Half Score
            "", "",                      // L,M Live Away Spread/ML
            "", "",                      // N,O Live Home Spread/ML
            ""                           // P Live Total
          ];
          await appendRow(lg.tab, row);
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
          const { aSpr, aML, hSpr, hML } = mapToAwayHome(r.spread, r.favML, r.dogML);
          const row = [
            today,                       // A Date
            weekLabel,                   // B Week
            "HALFTIME",                  // C Status
            matchup,                     // D Matchup
            "",                          // E Final Score (not yet)
            "", "", "", "", "",          // F..J opening columns untouched here
            scoreString(comp),           // K Half Score
            aSpr ?? "", aML ?? "",       // L,M Live Away Spread / ML
            hSpr ?? "", hML ?? "",       // N,O Live Home Spread / ML
            r.total ?? ""                // P Live Total
          ];
          await appendRow(lg.tab, row);
          console.log(`[${lg.tab}] HALFTIME wrote: ${matchup}`);
        }
      }
    }

    // 3) FINAL — always capture once
    if (isFinal) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "FINAL");
      if (!logged) {
        const row = [
          today,            // A
          weekLabel,        // B
          "FINAL",          // C
          matchup,          // D
          scoreString(comp),// E Final Score
          "", "", "", "", "", // F..J (leave as-is)
          "", "", "", "", "", // K..O (live fields blank in FINAL row)
          ""                 // P
        ];
        await appendRow(lg.tab, row);
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
