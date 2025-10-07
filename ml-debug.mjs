// ml-debug.mjs
// Usage locally: `node ml-debug.mjs`
// Or set GAME_ID / LEAGUE via env (defaults below)

const GAME_ID = process.env.GAME_ID || "401772940"; // Eagles @ Giants (Oct 9, 2025)
const LEAGUE  = (process.env.LEAGUE || "nfl").toLowerCase();

const HEADERS = {
  "User-Agent": "halftime-ml-debug",
  "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
  "Referer": "https://www.espn.com/"
};

function normLeague(league) {
  return (league === "ncaaf" || league === "college-football") ? "college-football" : "nfl";
}
function summaryUrl(league, id) {
  const lg = normLeague(league);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`;
}
function contentUrl(league, id) {
  const lg = normLeague(league);
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/playbyplay?event=${id}`; // extra context; sometimes carries odds refs
}
function numOrBlank(v){
  if (v===0) return "0";
  if (v==null) return "";
  const s=String(v).trim();
  const n=parseFloat(s.replace(/[^\d.+-]/g,""));
  if (!Number.isFinite(n)) return "";
  return s.startsWith("+")?`+${n}`:`${n}`;
}

function extractMoneylines(oddsObj, awayId, homeId, competitors=[]) {
  let awayML="", homeML="";

  // teamOdds[]
  if (Array.isArray(oddsObj?.teamOdds)) {
    for (const t of oddsObj.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid === String(awayId)) awayML = awayML || ml;
      if (tid === String(homeId)) homeML = homeML || ml;
    }
  }

  // direct away/home
  awayML = awayML || numOrBlank(oddsObj?.moneyLineAway ?? oddsObj?.awayMoneyLine ?? oddsObj?.awayMl);
  homeML = homeML || numOrBlank(oddsObj?.moneyLineHome ?? oddsObj?.homeMoneyLine ?? oddsObj?.homeMl);

  // favorite/underdog map
  const favId = String(oddsObj?.favorite ?? oddsObj?.favoriteId ?? "");
  const favML = numOrBlank(oddsObj?.favoriteMoneyLine);
  const dogML = numOrBlank(oddsObj?.underdogMoneyLine);
  if ((!awayML || !homeML) && favId && (favML || dogML)) {
    if (String(awayId)===favId) { awayML = awayML||favML; homeML = homeML||dogML; }
    else if (String(homeId)===favId) { homeML=homeML||favML; awayML=awayML||dogML; }
  }

  // competitors odds
  if ((!awayML || !homeML) && Array.isArray(competitors)) {
    for (const c of competitors) {
      const ml = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!ml) continue;
      if (c.homeAway==="away") awayML = awayML || ml;
      if (c.homeAway==="home") homeML = homeML || ml;
    }
  }

  return { awayML, homeML };
}

async function fetchJson(url){
  const res = await fetch(url, { headers: HEADERS });
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.json();
}

(async () => {
  const sum = await fetchJson(summaryUrl(LEAGUE, GAME_ID));

  const comp = sum?.header?.competitions?.[0];
  if (!comp) throw new Error("No competition found in summary");

  const competitors = comp?.competitors || [];
  const away = competitors.find(c=>c.homeAway==="away");
  const home = competitors.find(c=>c.homeAway==="home");

  console.log("Matchup:", away?.team?.displayName, "at", home?.team?.displayName);
  console.log("Provider candidates & raw odds shapes found:");

  const buckets = [];
  if (Array.isArray(comp?.odds)) buckets.push(["header.competitions[0].odds", comp.odds]);
  if (Array.isArray(sum?.odds)) buckets.push(["summary.odds", sum.odds]);
  if (Array.isArray(sum?.pickcenter)) buckets.push(["summary.pickcenter", sum.pickcenter]);

  let resolved = { awayML:"", homeML:"" };
  for (const [label, arr] of buckets) {
    for (const o of arr) {
      const provider = o?.provider?.displayName || o?.provider?.name || "(unknown)";
      const ml = extractMoneylines(o, away?.team?.id, home?.team?.id, competitors);
      console.log(`  - ${label} | provider=${provider} | keys=${Object.keys(o||{}).slice(0,12).join(", ")}`);
      if (ml.awayML || ml.homeML) resolved = ml; // prefer first filled
    }
  }

  console.log("\nResolved ML:");
  console.log(`  Away (${away?.team?.abbreviation}):`, resolved.awayML || "(none)");
  console.log(`  Home (${home?.team?.abbreviation}):`, resolved.homeML || "(none)");
})().catch(e => {
  console.error("ML debug failed:", e);
  process.exit(1);
});
