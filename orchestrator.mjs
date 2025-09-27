// orchestrator.mjs
// Finds halftime games (NFL + NCAAF) and calls your existing espn.mjs with proper args.
// Node 18+

import { spawn } from "node:child_process";
import fs from "node:fs/promises";

const LEAGUES = [
  { key: "nfl",   sportKey: "football", leagueKey: "nfl" },
  { key: "ncaaf", sportKey: "football", leagueKey: "college-football" },
];

function yyyymmdd(d = new Date()) {
  const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,"0"), dd = String(d.getDate()).padStart(2,"0");
  return `${y}${m}${dd}`;
}

function scoreboardUrl({sportKey, leagueKey}, date=yyyymmdd()){
  return `https://site.api.espn.com/apis/site/v2/sports/${sportKey}/${leagueKey}/scoreboard?dates=${date}`;
}

async function fetchJSON(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator.mjs" }});
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}

function normalizeName(s){ return (s||"").toUpperCase().replace(/[^A-Z0-9]/g,""); }
function isHalftime(comp){
  const t = comp?.status?.type;
  if ((t?.name||"").toUpperCase() === "STATUS_HALFTIME") return true;
  const period = comp?.status?.period ?? 0;
  const clock = (comp?.status?.displayClock || "").toUpperCase();
  return (period === 2 && (clock === "0:00" || clock.includes("HALFTIME")));
}

function getTeamsAbbr(comp){
  const comps = comp?.competitors || [];
  const home = comps.find(x => x.homeAway === "home") || comps[1];
  const away = comps.find(x => x.homeAway === "away") || comps[0];
  const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name;
  const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name;
  return { homeName, awayName };
}

// simple local dedupe store
const STATE_FILE = "./orchestrator_state.json";
async function loadState(){
  try { return JSON.parse(await fs.readFile(STATE_FILE, "utf8")); }
  catch { return { logged: {} }; }
}
async function saveState(state){ await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2)); }

async function callEspnMjs({ leagueKey, away, home }) {
  return new Promise((resolve) => {
    const args = ["espn.mjs", `--league=${leagueKey}`, `--away=${away}`, `--home=${home}`, "--dom", "--debug"];
    const child = spawn("node", args, { stdio: ["ignore", "pipe", "pipe"] });

    let out = "", err = "";
    child.stdout.on("data", d => out += d.toString());
    child.stderr.on("data", d => err += d.toString());
    child.on("close", code => resolve({ code, out, err }));
  });
}

async function processLeague(leagueCfg, state){
  const url = scoreboardUrl(leagueCfg);
  const sb = await fetchJSON(url);
  const events = sb?.events || [];
  const results = [];

  for (const e of events) {
    const comp = e?.competitions?.[0];
    if (!comp) continue;
    const eventId = comp.id || e.id;
    const already = state.logged[eventId];
    if (!isHalftime(comp) || already) continue;

    const { homeName, awayName } = getTeamsAbbr(comp);
    if (!homeName || !awayName) continue;

    // Mark first to avoid duplicate triggers if espn.mjs is slow; if it fails, we can unmark or let the next run try again based on output.
    state.logged[eventId] = { at: new Date().toISOString(), league: leagueCfg.key, home: homeName, away: awayName };

    // Invoke your existing script
    const { code, out, err } = await callEspnMjs({ leagueKey: leagueCfg.key, away: awayName, home: homeName });

    // Optional: if call failed, roll back dedupe so we try again next tick
    if (code !== 0) {
      delete state.logged[eventId];
      await saveState(state);
      console.error(`[orchestrator] espn.mjs failed for ${leagueCfg.key} ${awayName} @ ${homeName}:`, err || out);
      continue;
    }

    // Extract JSON block your espn.mjs prints (after "JSON:")
    const jsonMatch = out.match(/JSON:\s*\n([\s\S]+)$/);
    let parsed = null;
    if (jsonMatch) {
      try { parsed = JSON.parse(jsonMatch[1]); }
      catch { /* ignore parse error, keep raw out */ }
    }

    results.push({ league: leagueCfg.key, eventId, homeName, awayName, output: parsed || null, raw: out });
  }

  return results;
}

async function main(){
  const state = await loadState();
  const all = [];
  for (const lg of LEAGUES) {
    try {
      const res = await processLeague(lg, state);
      all.push(...res);
    } catch (e) {
      console.error(`[orchestrator] league ${lg.key} error:`, e.message);
    }
  }
  await saveState(state);

  if (all.length) {
    console.log(`\n[orchestrator] captured ${all.length} halftime game(s):`);
    for (const r of all) {
      console.log(`- ${r.league.toUpperCase()}: ${r.awayName} @ ${r.homeName}`);
      if (r.output) console.log(`  spread=${r.output.spread} total=${r.output.total} favML=${r.output.favML} dogML=${r.output.dogML}`);
    }
  } else {
    console.log("[orchestrator] no new halftimes this run.");
  }
}

main().catch(e => { console.error("[orchestrator fatal]", e); process.exit(1); });
