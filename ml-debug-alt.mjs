// ml-debug-alt.mjs
// Deep-inspects ESPN APIs for a single event and prints every odds block
// Run locally:  node ml-debug-alt.mjs
// Or via Actions with env GAME_ID and LEAGUE

const GAME_ID = process.env.GAME_ID || "401772940"; // Eagles @ Giants (2025-10-09)
const LEAGUE  = (process.env.LEAGUE || "nfl").toLowerCase();

const HDRS = {
  "User-Agent": "halftime-ml-inspector",
  "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
  "Referer": "https://www.espn.com/"
};

function normLeague(lg) {
  return (lg === "ncaaf" || lg === "college-football") ? "college-football" : "nfl";
}
function summaryUrl(lg, id) {
  const s = normLeague(lg);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${s}/summary?event=${id}`;
}
function scoreboardUrlByDate(lg, yyyymmdd) {
  const s = normLeague(lg);
  const extra = s === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${s}/scoreboard?dates=${yyyymmdd}${extra}`;
}
const toISODateET = (d) => {
  const et = new Date(new Date(d).toLocaleString("en-US", { timeZone: "America/New_York" }));
  return `${et.getFullYear()}${String(et.getMonth()+1).padStart(2,"0")}${String(et.getDate()).padStart(2,"0")}`;
};

async function fetchJson(url) {
  const r = await fetch(url, { headers: HDRS });
  if (!r.ok) throw new Error(`${r.status} ${url}`);
  return r.json();
}
const nb = (v) => v===0 ? "0" : (v==null ? "" : String(v));
const safe = (o,k)=> (k in (o||{})) ? o[k] : undefined;

function printMoneyline(label, o) {
  const fields = [
    "moneyLine","moneyline","money_line","favoriteMoneyLine","underdogMoneyLine",
    "moneyLineAway","moneyLineHome","awayMoneyLine","homeMoneyLine","awayMl","homeMl"
  ];
  const picked = {};
  for (const f of fields) if (o && o[f] != null) picked[f] = o[f];
  const teamOdds = Array.isArray(o?.teamOdds) ? o.teamOdds.map(t => ({
    teamId: t?.teamId ?? t?.team?.id, moneyLine: t?.moneyLine ?? t?.moneyline ?? t?.money_line
  })) : undefined;
  const awayTeamOdds = o?.awayTeamOdds ? {
    ...Object.fromEntries(Object.entries(o.awayTeamOdds).filter(([k])=>/money/i.test(k)))
  } : undefined;
  const homeTeamOdds = o?.homeTeamOdds ? {
    ...Object.fromEntries(Object.entries(o.homeTeamOdds).filter(([k])=>/money/i.test(k)))
  } : undefined;

  console.log(`    ${label} ML candidates:`, JSON.stringify({ ...picked, teamOdds, awayTeamOdds, homeTeamOdds }, null, 2));
}

(async () => {
  console.log("=== ML DEEP INSPECTOR START ===");
  console.log("GAME_ID:", GAME_ID, " LEAGUE:", LEAGUE);

  // 1) SUMMARY endpoint (usually richest)
  const summary = await fetchJson(summaryUrl(LEAGUE, GAME_ID));

  const comp = summary?.header?.competitions?.[0];
  if (!comp) throw new Error("No competition found in summary.");
  const start = comp?.date;
  const compId = comp?.id || GAME_ID;

  const away = comp?.competitors?.find(c=>c.homeAway==="away");
  const home = comp?.competitors?.find(c=>c.homeAway==="home");
  console.log(`Matchup: ${away?.team?.displayName} @ ${home?.team?.displayName}`);
  console.log("");

  const buckets = [];
  if (Array.isArray(comp?.odds)) buckets.push(["header.competitions[0].odds", comp.odds]);
  if (Array.isArray(summary?.odds)) buckets.push(["summary.odds", summary.odds]);
  if (Array.isArray(summary?.pickcenter)) buckets.push(["summary.pickcenter", summary.pickcenter]);

  if (!buckets.length) {
    console.log("No odds arrays found in SUMMARY.");
  } else {
    console.log("SUMMARY odds blocks:");
    for (const [label, arr] of buckets) {
      for (let i=0;i<arr.length;i++){
        const o = arr[i];
        const provider = o?.provider?.displayName || o?.provider?.name || "(unknown)";
        const keys = Object.keys(o || {});
        console.log(`  - ${label}[${i}] provider=${provider}; keys=${keys.join(", ")}`);
        // print most likely ML spots
        printMoneyline(" root", o);
        if (o?.awayTeamOdds || o?.homeTeamOdds) {
          printMoneyline(" awayTeamOdds", o?.awayTeamOdds||{});
          printMoneyline(" homeTeamOdds", o?.homeTeamOdds||{});
        }
        if (Array.isArray(o?.teamOdds)) {
          for (const t of o.teamOdds) printMoneyline(` teamOdds(teamId=${t?.teamId ?? t?.team?.id})`, t);
        }
      }
    }
  }

  // 2) SCOREBOARD for the same date (sometimes has a different provider)
  const ymd = toISODateET(start || Date.now());
  const sb = await fetchJson(scoreboardUrlByDate(LEAGUE, ymd));
  const ev = (sb?.events || []).find(e => String(e?.id) === String(compId));
  console.log("\nSCOREBOARD odds block(s) for this event:");
  if (!ev) {
    console.log("  - Event not found on scoreboard for", ymd);
  } else {
    const co = ev?.competitions?.[0] || {};
    const arr = (co.odds || ev.odds || []);
    if (!arr.length) console.log("  - None");
    arr.forEach((o, i) => {
      const provider = o?.provider?.displayName || o?.provider?.name || "(unknown)";
      const keys = Object.keys(o || {});
      console.log(`  - odds[${i}] provider=${provider}; keys=${keys.join(", ")}`);
      printMoneyline(" root", o);
      if (o?.awayTeamOdds || o?.homeTeamOdds) {
        printMoneyline(" awayTeamOdds", o?.awayTeamOdds||{});
        printMoneyline(" homeTeamOdds", o?.homeTeamOdds||{});
      }
      if (Array.isArray(o?.teamOdds)) {
        for (const t of o.teamOdds) printMoneyline(` teamOdds(teamId=${t?.teamId ?? t?.team?.id})`, t);
      }
    });
  }

  console.log("\n=== ML DEEP INSPECTOR END ===");
})().catch(err => {
  console.error("ML inspector failed:", err);
  process.exit(1);
});
