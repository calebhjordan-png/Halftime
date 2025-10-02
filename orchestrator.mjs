// orchestrator.mjs — stateless writer for your NFL/CFB Google Sheet
// Columns (A..P) expected in both tabs (NFL, CFB):
// Date | Week | Status | Matchup | Final Score | Away Spread | Away ML | Home Spread | Home ML | Total | Half Score | Live Away Spread | Live Away ML | Live Home Spread | Live Home ML | Live Total

import { execSync } from "child_process";
import fetch from "node-fetch";
import { google } from "googleapis";

// ---------- CONFIG ----------
const OPENING_MIN_BEFORE_KICK = 15;             // write OPENING 15 minutes before kickoff
const DEFAULT_PROVIDER = "ESPN BET";            // prefer ESPN BET, then fallback to ESPN
const TABS = { nfl: "NFL", ncaaf: "CFB" };      // tab names in your sheet
// --------------------------------

// ---- Secrets (GitHub Actions -> Settings -> Secrets and variables -> Actions) ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const RAW_CREDS = process.env.GOOGLE_SERVICE_ACCOUNT || "";

// Preflight (safe) — do NOT print secrets, only presence/length
console.log("[preflight] SHEET_ID len:", SHEET_ID ? String(SHEET_ID).length : 0);
console.log("[preflight] SERVICE_ACCOUNT present:", RAW_CREDS ? "yes" : "no");

if (!SHEET_ID || !RAW_CREDS) {
  console.error("[fatal] Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT. Check workflow env: block & repo secrets.");
  process.exit(2);
}

let CREDS;
try {
  CREDS = JSON.parse(RAW_CREDS);
} catch (e) {
  console.error("[fatal] GOOGLE_SERVICE_ACCOUNT is not valid JSON:", e.message);
  process.exit(3);
}

// Normalize private key newlines (handles \\n and real newlines)
const normalizedKey = (CREDS.private_key || "").replace(/\\n/g, "\n");

// Google Sheets client
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
  console.error("[fatal] Google Sheets auth error:", err.message);
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
  try {
    const r = await fetch(url, { headers: { "User-Agent": "halftime-orchestrator" } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    console.error("[error] fetchJSON failed:", url, "|", e.message);
    throw e;
  }
}
function scoreString(comp) {
  const home = comp.competitors.find(c => c.homeAway === "home");
  const away = comp.competitors.find(c => c.homeAway === "away");
  return `${away?.score ?? 0}-${home?.score ?? 0}`;
}
function matchupString(away, home) {
  return `${away} @ ${home}`;
}
async function readRange(range) {
  try {
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
    return res.data.values || [];
  } catch (e) {
    console.error("[error] readRange failed:", range, "|", e.message);
    throw e;
  }
}
async function appendRow(tab, row) {
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${tab}!A2`,
      valueInputOption: "RAW",
      requestBody: { values: [row] }
    });
    console.log(`[sheets] appended -> ${tab}:`, row.slice(0, 6).join(" | "), "...");
  } catch (e) {
    console.error("[error] appendRow failed:", tab, "|", e.message);
    throw e;
  }
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
      console.log("[espn] run:", cmd);
      const out = execSync(cmd, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
      const m = out.match(/JSON:\s*\n([\s\S]+)$/);
      if (!m) {
        console.warn("[espn] JSON block not found in output; returning null");
        return null;
      }
      const parsed = JSON.parse(m[1]);
      return parsed;
    } catch (e) {
      console.warn("[espn] attempt failed (provider:", prov, "):", e.message);
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
    // if spread is 0, try ML sign to infer
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
  console.log(`[league] start: ${lg.key} (${lg.tab})`);
  let sb;
  try {
    sb = await fetchJSON(lg.url);
  } catch (e) {
    console.error(`[league] ${lg.key} scoreboard fetch failed:`, e.message);
    return;
  }

  const weekLabel = getWeekLabel(sb);
  const events = sb.events || [];
  const today = mmdd();

  console.log(`[league] ${lg.key}: events today =`, events.length);

  for (const ev of events) {
    const comp = ev.competitions?.[0]; if (!comp) continue;

    const home = comp.competitors.find(c => c.homeAway === "home")?.team?.shortDisplayName;
    const away = comp.competitors.find(c => c.homeAway === "away")?.team?.shortDisplayName;
    if (!home || !away) {
      console.log("[skip] missing team names");
      continue;
    }

    const matchup = matchupString(away, home);
    const kickoffIso = comp.date || ev.date;
    const statusObj = comp?.status?.type || {};
    const statusName = statusObj.name || "";
    const isFinal = statusObj.completed === true || statusName === "STATUS_FINAL";
    const now = new Date();

    console.log(`[game] ${matchup} | status=${statusName} | kickoff=${kickoffIso}`);

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
          console.log(`[write] OPENING -> ${matchup}`);
        } else {
          console.log("[skip] OPENING: espn.mjs returned null");
        }
      } else {
        console.log("[skip] OPENING already logged:", matchup);
      }
    } else {
      console.log("[info] not yet OPENING window:", matchup);
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
            "", "", "", "", "",          // F..J opening columns not changed here
            scoreString(comp),           // K Half Score
            aSpr ?? "", aML ?? "",       // L,M Live Away Spread / ML
            hSpr ?? "", hML ?? "",       // N,O Live Home Spread / ML
            r.total ?? ""                // P Live Total
          ];
          await appendRow(lg.tab, row);
          console.log(`[write] HALFTIME -> ${matchup}`);
        } else {
          console.log("[skip] HALFTIME: espn.mjs returned null");
        }
      } else {
        console.log("[skip] HALFTIME already logged:", matchup);
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
          "", "", "", "", "", // F..J
          "", "", "", "", "", // K..O
          ""                 // P
        ];
        await appendRow(lg.tab, row);
        console.log(`[write] FINAL -> ${matchup}`);
      } else {
        console.log("[skip] FINAL already logged:", matchup);
      }
    }
  }
}

// ---------- Entry ----------
async function main() {
  let hadError = false;
  for (const lg of LEAGUES) {
    if (!lg.tab) continue;
    try { await handleLeague(lg); }
    catch (e) { hadError = true; console.error(`[fatal] ${lg.key} error:`, e.message); }
  }
  console.log("✓ orchestrator run complete.");
  process.exit(hadError ? 1 : 0);
}

await main();
