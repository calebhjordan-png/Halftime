// orchestrator.mjs — writes OPENING / HALFTIME / FINAL rows to your NFL/CFB tabs
// Expected columns (A..P):
// Date | Week | Status | Matchup | Final Score | Away Spread | Away ML | Home Spread | Home ML | Total | Half Score | Live Away Spread | Live Away ML | Live Home Spread | Live Home ML | Live Total

import { execSync } from "child_process";
import { google } from "googleapis";

// ---------- CONFIG ----------
const OPENING_MIN_BEFORE_KICK = 15;             // write OPENING 15 minutes before kickoff
const DEFAULT_PROVIDER = "ESPN BET";            // prefer ESPNBET then ESPN
const TABS = { nfl: "NFL", ncaaf: "CFB" };
// --------------------------------

// ---- Secrets ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const RAW_CREDS = process.env.GOOGLE_SERVICE_ACCOUNT || "";
console.log("[preflight] SHEET_ID len:", SHEET_ID ? String(SHEET_ID).length : 0);
console.log("[preflight] SERVICE_ACCOUNT present:", !!RAW_CREDS);

if (!SHEET_ID || !RAW_CREDS) {
  console.error("[fatal] Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(2);
}

let CREDS;
try { CREDS = JSON.parse(RAW_CREDS); }
catch (e) { console.error("[fatal] Bad GOOGLE_SERVICE_ACCOUNT JSON:", e.message); process.exit(3); }

const normalizedKey = (CREDS.private_key || "").replace(/\\n/g, "\n");

let sheets;
try {
  const jwt = new google.auth.JWT(
    CREDS.client_email,
    null,
    normalizedKey,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );
  sheets = google.sheets({ version: "v4", auth: jwt });
  console.log("[auth] Google Sheets auth: OK");
} catch (err) {
  console.error("[fatal] Sheets auth error:", err.message);
  process.exit(4);
}

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
  return `${away?.score ?? 0}-${home?.score ?? 0}`;
}
function matchupString(away, home) { return `${away} @ ${home}`; }

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
  console.log(`[sheets] append -> ${tab}`, row.slice(0, 5).join(" | "), "...");
}

// de-dupe by (Date, Matchup, Status)
async function alreadyLogged(tab, date, matchup, status) {
  const rows = await readRange(`${tab}!A2:P`);
  const key = `${date}|${matchup}|${status}`.toUpperCase();
  for (const r of rows) {
    if (!r?.length) continue;
    const rowKey = `${(r[0]||"")}|${(r[3]||"")}|${(r[2]||"")}`.toUpperCase();
    if (rowKey === key) return true;
  }
  return false;
}

// run espn.mjs, try preferred provider then fallback
function runEspn({ league, away, home, provider = DEFAULT_PROVIDER }) {
  const tryOnce = (prov) => {
    const cmd = `node espn.mjs --league=${league} --away="${away}" --home="${home}" --provider="${prov}" --dom`;
    try {
      console.log("[espn] run:", cmd);
      const out = execSync(cmd, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
      const m = out.match(/JSON:\s*\n([\s\S]+)$/);
      if (!m) { console.warn("[espn] JSON output not found"); return null; }
      return JSON.parse(m[1]);
    } catch (e) {
      console.warn("[espn] failed with provider:", prov, "|", e.message);
      return null;
    }
  };
  return tryOnce(provider) || tryOnce("ESPN");
}

// simple mapping of spread/ML to away/home columns
function mapToAwayHome(spread, favML, dogML) {
  if (spread == null && favML == null && dogML == null) return { aSpr:"",aML:"",hSpr:"",hML:"" };
  const s = Number(spread);
  if (!Number.isFinite(s)) return { aSpr:"",aML:"",hSpr:"",hML:"" };
  if (s < 0) return { aSpr: s, aML: favML ?? "", hSpr: Math.abs(s), hML: dogML ?? "" };
  if (s > 0) return { aSpr: -Math.abs(s), aML: dogML ?? "", hSpr: s, hML: favML ?? "" };
  if (typeof favML === "number" && favML < 0) return { aSpr: -0, aML: favML, hSpr: 0, hML: dogML ?? "" };
  return { aSpr: 0, aML: "", hSpr: 0, hML: "" };
}

// scoreboards
const LEAGUES = [
  { key: "nfl",   tab: TABS.nfl,   url: "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard" },
  { key: "ncaaf", tab: TABS.ncaaf, url: "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard" }
];

function getWeekLabel(sb) {
  try {
    const lg = sb?.leagues?.[0];
    if (lg?.week?.number) return `Week ${lg.week.number}`;
    return lg?.season?.type?.name || "";
  } catch { return ""; }
}

// per-league
async function handleLeague(lg) {
  console.log(`[league] ${lg.key} (${lg.tab})`);
  const sb = await fetchJSON(lg.url);
  const weekLabel = getWeekLabel(sb);
  const events = sb.events || [];
  const today = mmdd();
  console.log(`[league] events=${events.length}`);

  for (const ev of events) {
    const comp = ev.competitions?.[0]; if (!comp) continue;
    const home = comp.competitors.find(c => c.homeAway === "home")?.team?.shortDisplayName;
    const away = comp.competitors.find(c => c.homeAway === "away")?.team?.shortDisplayName;
    if (!home || !away) { console.log("[skip] missing names"); continue; }

    const matchup = matchupString(away, home);
    const kickoffIso = comp.date || ev.date;
    const st = comp?.status?.type || {};
    const statusName = st.name || "";
    const isFinal = st.completed === true || statusName === "STATUS_FINAL";
    const now = new Date();

    console.log(`[game] ${matchup} | status=${statusName} | kickoff=${kickoffIso}`);

    // OPENING
    if (now >= isoMinusMinutes(kickoffIso, OPENING_MIN_BEFORE_KICK)) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "OPENING");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          const { aSpr, aML, hSpr, hML } = mapToAwayHome(r.spread, r.favML, r.dogML);
          const row = [ today, weekLabel, "OPENING", matchup, "", aSpr??"", aML??"", hSpr??"", hML??"", r.total??"", "", "", "", "", "", "" ];
          await appendRow(lg.tab, row);
          console.log(`[write] OPENING -> ${matchup}`);
        } else {
          console.log("[skip] OPENING: espn.mjs returned null");
        }
      } else { console.log("[skip] OPENING already logged"); }
    } else { console.log("[info] not yet OPENING window"); }

    // HALFTIME
    if (statusName === "STATUS_HALFTIME") {
      const logged = await alreadyLogged(lg.tab, today, matchup, "HALFTIME");
      if (!logged) {
        const r = runEspn({ league: lg.key, away, home });
        if (r) {
          const { aSpr, aML, hSpr, hML } = mapToAwayHome(r.spread, r.favML, r.dogML);
          const row = [ today, weekLabel, "HALFTIME", matchup, "", "", "", "", "", "", scoreString(comp), aSpr??"", aML??"", hSpr??"", hML??"", r.total??"" ];
          await appendRow(lg.tab, row);
          console.log(`[write] HALFTIME -> ${matchup}`);
        } else {
          console.log("[skip] HALFTIME: espn.mjs returned null");
        }
      } else { console.log("[skip] HALFTIME already logged"); }
    }

    // FINAL
    if (isFinal) {
      const logged = await alreadyLogged(lg.tab, today, matchup, "FINAL");
      if (!logged) {
        const row = [ today, weekLabel, "FINAL", matchup, scoreString(comp), "", "", "", "", "", "", "", "", "", "", "" ];
        await appendRow(lg.tab, row);
        console.log(`[write] FINAL -> ${matchup}`);
      } else { console.log("[skip] FINAL already logged"); }
    }
  }
}

// entry
async function main() {
  try {
    for (const lg of LEAGUES) await handleLeague(lg);
    console.log("✓ orchestrator run complete.");
  } catch (e) {
    console.error("[fatal] orchestrator:", e.message);
    process.exit(1);
  }
}
await main();
