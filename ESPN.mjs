// ============================================================================
// PROMPT CHEAT-SHEET (copy/paste)
// ----------------------------------------------------------------------------
// NFL (default league)
//   node espn.mjs --away="Seahawks" --home="Cardinals" --dom --debug
//   node espn.mjs --away="Cowboys"  --home="Eagles"    --dom --watch=5
//
// College Football (NCAAF)
//   node espn.mjs --league=ncaaf --away="Army" --home="East Carolina" --dom --debug
//   node espn.mjs --league=ncaaf --away="Florida State" --home="Virginia" --dom --watch=5
//
// Options you can mix in:
//   --date=YYYYMMDD        # query a past/future scoreboard date (default: today)
//   --provider="ESPN BET"  # prefer a specific book if API returns multiple
//   --watch=5              # poll every N seconds and print only when values change
//   --dom                  # enable network-sniff + DOM adapters (recommended)
//   --debug                # verbose logs: which adapter won, raw parses, etc.
//
// First time using DOM/network on a machine:
//   npm i playwright
//   npx playwright install chromium
//
// Add more leagues later by extending LEAGUE_CFG (e.g., nba, mlb, etc.).
// ============================================================================

// espn.mjs — Live line with multi-adapter failover (NFL + NCAAF)
// Node >= 18

import fs from "fs";

// ---------------------- CLI ----------------------
const args = Object.fromEntries(
  process.argv.slice(2).map(a => {
    const [k, ...rest] = a.replace(/^--/, '').split('=');
    return [k, rest.join('=') || true];
  })
);

const AWAY = (args.away || "").trim();
const HOME = (args.home || "").trim();
if (!AWAY || !HOME) {
  console.error('[usage] node espn.mjs --away="Team A" --home="Team B" [--league=nfl|ncaaf] [--date=YYYYMMDD] [--provider="ESPN BET"] [--dom] [--watch=5] [--debug]');
  process.exit(1);
}

const LEAGUE = String(args.league || "nfl").toLowerCase(); // nfl | ncaaf

// ESPN path pieces per league
const LEAGUE_CFG = {
  nfl: {
    leagueKey: "nfl",
    sportKey: "football",
    siteLeaguePath: "nfl",
    coreLeaguePath: "nfl",
  },
  ncaaf: {
    leagueKey: "college-football",
    sportKey: "football",
    siteLeaguePath: "college-football",
    coreLeaguePath: "college-football",
  }
};
if (!LEAGUE_CFG[LEAGUE]) {
  console.error(`[error] Unsupported --league=${LEAGUE}. Use nfl or ncaaf.`);
  process.exit(1);
}

const DEBUG = !!args.debug;
const USE_DOM = !!args.dom; // enables DOM + network sniff adapters
const WATCH_SEC = args.watch ? Number(args.watch) : 0;
const PREFERRED_PROVIDER = (args.provider || "").trim();
const PROVIDER_ORDER = ["FanDuel","ESPN BET","DraftKings","Caesars","BetMGM","WynnBET","PointsBet","FOX Bet","Barstool","Circa Sports","Pinnacle"];

// ---------------------- Utils ----------------------
function yyyymmdd(d = new Date()) {
  const y = d.getFullYear(), m = String(d.getMonth() + 1).padStart(2, '0'), dd = String(d.getDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}
const DATE = (args.date && /^\d{8}$/.test(args.date)) ? args.date : yyyymmdd();

const norm = s => (s||"").toUpperCase().replace(/[^A-Z0-9]/g, "");
const sameTeam = (a,b) => { if(!a||!b) return false; const A=norm(a),B=norm(b); return A===B || A.includes(B) || B.includes(A); };
const isNum = v => typeof v === "number" && !Number.isNaN(v);
const LIVE_RX = /live|in-?game/i;

function favoritePerspectiveSpread(hS, aS, hML, aML) {
  let fav = null;
  if (isNum(hML) && isNum(aML)) fav = hML < aML ? "home" : "away";
  else if (isNum(hS) && isNum(aS)) fav = hS < aS ? "home" : "away";
  if (fav === "home" && isNum(hS)) return hS;
  if (fav === "away" && isNum(aS)) return aS;
  const spreads = [hS, aS].filter(isNum);
  return spreads.length ? spreads.sort((x,y)=>x-y)[0] : null;
}
function toCsvRow(arr){return arr.map(x=>{const s=(x==null)?"":String(x);return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s;}).join(",");}
function mmdd(){const d=new Date();return `${String(d.getMonth()+1).padStart(2,"0")}/${String(d.getDate()).padStart(2,"0")}`;}
function nowIso(){return new Date().toISOString();}

function withLiveParams(url){
  const u = new URL(url);
  u.searchParams.set("oddsType","live");
  u.searchParams.set("oddsFormat","american");
  u.searchParams.set("region","us");
  u.searchParams.set("lang","en");
  u.searchParams.set("contentorigin","espn");
  return u.toString();
}

function saneOdds({spread,total,homeML,awayML}) {
  if (isNum(spread) && Math.abs(spread) > 60) return false;
  if (isNum(total) && (total < 10 || total > 150)) return false; // college totals can be higher
  if (isNum(homeML) && isNum(awayML) && (Math.sign(homeML) === Math.sign(awayML))) return false;
  return true;
}

// If books turn ML off on big spreads, blank ML instead of reporting one side
function maybeBlankML(spread, mlA, mlB) {
  const bigSpread = isNum(spread) && Math.abs(spread) >= 20;
  const oneSideOnly = (isNum(mlA) ^ isNum(mlB)) === 1; // xor: exactly one present
  if (!isNum(mlA) && !isNum(mlB)) return { mlA: null, mlB: null };
  if (oneSideOnly || (bigSpread && (!isNum(mlA) || !isNum(mlB)))) {
    return { mlA: null, mlB: null };
  }
  return { mlA, mlB };
}

// ---------------------- HTTP helpers ----------------------
async function fetchJSON(url) {
  const r = await fetch(url, { headers: { "User-Agent": "espn.mjs" }});
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return r.json();
}
async function deref(obj, forceLive=true){
  const ref = obj?.$ref || obj?.href || obj?.ref;
  if (!ref) return null;
  const url = forceLive ? withLiveParams(ref) : ref;
  try { return await fetchJSON(url); } catch { return null; }
}

// ---------------------- ESPN endpoints (league-aware) ----------------------
function leagueScoreboardUrl(date){
  const { sportKey, leagueKey } = LEAGUE_CFG[LEAGUE];
  return `https://site.api.espn.com/apis/site/v2/sports/${sportKey}/${leagueKey}/scoreboard?dates=${date}`;
}
function gamePageUrl(eventId){
  const { siteLeaguePath } = LEAGUE_CFG[LEAGUE];
  return `https://www.espn.com/${siteLeaguePath}/game/_/gameId/${eventId}`;
}
function eventOddsUrl(eventId){
  const { sportKey, coreLeaguePath } = LEAGUE_CFG[LEAGUE];
  return withLiveParams(`https://sports.core.api.espn.com/v2/sports/${sportKey}/leagues/${coreLeaguePath}/events/${eventId}/odds`);
}
function competitionOddsUrl(eventId){
  const { sportKey, coreLeaguePath } = LEAGUE_CFG[LEAGUE];
  return withLiveParams(`https://sports.core.api.espn.com/v2/sports/${sportKey}/leagues/${coreLeaguePath}/events/${eventId}/competitions/${eventId}/odds`);
}

async function getScoreboard(date){ return fetchJSON(leagueScoreboardUrl(date)); }
async function getEventOdds(eventId){ return fetchJSON(eventOddsUrl(eventId)); }
async function getCompetitionOdds(eventId){ return fetchJSON(competitionOddsUrl(eventId)); }

async function collectSnapshots(coll){
  const items = coll?.items || [];
  const snaps = [];
  for (const it of items) {
    const s = await deref(it,true) || it;
    if (s) snaps.push(s);
  }
  return snaps;
}
function ts(o){return Date.parse(o?.lastUpdated || o?.updateTime || 0) || 0;}
function isLiveSnapshot(o){ const t=`${o?.displayName||""} ${o?.type||""}`; return o?.isLive===true || LIVE_RX.test(t); }

function summarizeSnapshot(snapshot){
  const provider = snapshot?.provider?.name || "Unknown";
  const isLive = isLiveSnapshot(snapshot);
  const lastUpdated = snapshot?.lastUpdated || snapshot?.updateTime || "";
  const homeML = isNum(Number(snapshot?.homeTeamOdds?.moneyLine)) ? Number(snapshot.homeTeamOdds.moneyLine) : null;
  const awayML = isNum(Number(snapshot?.awayTeamOdds?.moneyLine)) ? Number(snapshot.awayTeamOdds.moneyLine) : null;
  const hS = (snapshot?.homeTeamOdds?.spread != null) ? Number(snapshot.homeTeamOdds.spread) : (snapshot?.spread != null ? Number(snapshot.spread) : null);
  const aS = (snapshot?.awayTeamOdds?.spread != null) ? Number(snapshot.awayTeamOdds.spread) : (snapshot?.spread != null ? -Number(snapshot.spread) : null);
  const total = (snapshot?.overUnder != null) ? Number(snapshot.overUnder) : null;
  return { provider, isLive, lastUpdated, homeSpread:hS, awaySpread:aS, total, homeML, awayML };
}

const PROVIDER_PRIORITY = ["ESPN BET", ...PROVIDER_ORDER.filter(p=>p!=="ESPN BET")];
function pickProviderSnapshot(allSnaps, preferred){
  if (!allSnaps?.length) return null;
  const byProv = new Map();
  for (const s of allSnaps) {
    const name = s?.provider?.name || "Unknown";
    if (!byProv.has(name)) byProv.set(name, []);
    byProv.get(name).push(s);
  }
  const order = [];
  if (preferred) order.push(preferred);
  for (const n of PROVIDER_PRIORITY) if (!order.includes(n)) order.push(n);
  for (const n of byProv.keys()) if (!order.includes(n)) order.push(n);

  for (const prov of order) {
    const arr = byProv.get(prov); if (!arr) continue;
    const live = arr.filter(isLiveSnapshot).sort((a,b)=>ts(b)-ts(a));
    const fresh = arr.slice().sort((a,b)=>ts(b)-ts(a));
    const choice = live[0] || fresh[0];
    if (choice) return { provider: prov, snapshot: choice };
  }
  return null;
}

// ---------------------- Adapter: ESPN API ----------------------
async function adapterEspnApi(eventId) {
  try {
    const [collComp, collEvent] = await Promise.all([
      getCompetitionOdds(eventId).catch(()=>null),
      getEventOdds(eventId).catch(()=>null)
    ]);
    const snapsComp = collComp ? await collectSnapshots(collComp) : [];
    const snapsEvent = collEvent ? await collectSnapshots(collEvent) : [];
    const allSnaps = [...snapsComp, ...snapsEvent];

    if (DEBUG) {
      const dbg = allSnaps.map(s => ({
        prov: s?.provider?.name, isLive: isLiveSnapshot(s),
        lastUpdated: s?.lastUpdated || s?.updateTime || "n/a",
        hasMarkets: !!(s?.markets || s?.lines),
        marketCount: (s?.markets?.items?.length||0) + (s?.lines?.items?.length||0)
      }));
      console.log("[api] snapshots:", dbg);
    }

    const picked = pickProviderSnapshot(allSnaps, PREFERRED_PROVIDER);
    if (!picked) return null;
    const s = summarizeSnapshot(picked.snapshot);
    const spread = favoritePerspectiveSpread(s.homeSpread, s.awaySpread, s.homeML, s.awayML);
    const favSide = spread!=null ? (s.homeSpread===spread ? "HOME" : "AWAY") : null;

    let favML = favSide==="HOME" ? s.homeML : s.awayML;
    let dogML = favSide==="HOME" ? s.awayML : s.homeML;
    ({ mlA: favML, mlB: dogML } = maybeBlankML(spread, favML, dogML));

    const result = {
      source: "espnApi",
      provider: picked.provider,
      isLive: s.isLive,
      lastUpdated: s.lastUpdated || "",
      spread,
      total: s.total ?? null,
      favML, dogML,
      homeSpread: s.homeSpread ?? null,
      awaySpread: s.awaySpread ?? null
    };
    if (!saneOdds(result)) return null;
    return result;
  } catch (e) {
    if (DEBUG) console.log("[api] error:", e.message);
    return null;
  }
}

// ---------------------- Adapter: ESPN Network Sniff (Playwright) ----------------------
async function adapterEspnNetwork(eventId) {
  let pw;
  try { pw = await import("playwright"); } catch { return null; }

  const url = gamePageUrl(eventId);
  const browser = await pw.chromium.launch({ headless: true });
  const page = await browser.newPage({ userAgent: "Mozilla/5.0" });

  const packets = [];
  page.on("response", async (resp) => {
    try {
      const ct = resp.headers()["content-type"] || "";
      const u = resp.url();
      const isJson = /json/i.test(ct);
      const looksOdds =
        /odds|sportsbook|wager|market|bet|line|espnbet/i.test(u) &&
        !/image|css|font|png|jpg|jpeg/i.test(u);
      if (isJson && looksOdds) {
        const json = await resp.json().catch(()=>null);
        if (json) packets.push({ url: u, json, time: Date.now() });
      }
    } catch {}
  });

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    for (let y=0; y<7; y++) { await page.mouse.wheel(0, 1000); await page.waitForTimeout(500); }
    await page.waitForTimeout(2000);
  } catch (e) {
    if (DEBUG) console.log("[net] nav error:", e.message);
  }

  await browser.close();

  if (DEBUG) console.log("[net] captured", packets.length, "responses");

  let best = null;
  for (const p of packets) {
    const stack = [p.json];
    while (stack.length) {
      const node = stack.pop();
      if (!node || typeof node !== "object") continue;

      const flatSpread = Number(node.spread ?? node.line ?? node.handicap);
      const overUnder = Number(node.overUnder ?? node.total ?? node.pointsTotal);
      const homeML0 = Number(node.homeMoneyLine ?? node.homeML ?? node.homePrice);
      const awayML0 = Number(node.awayMoneyLine ?? node.awayML ?? node.awayPrice);

      const homeTeamOdds = node.homeTeamOdds || node.home || null;
      const awayTeamOdds = node.awayTeamOdds || node.away || node.awayTeam || null;

      let rec = null;
      if (!([flatSpread, overUnder, homeML0, awayML0].every(Number.isNaN))) {
        let homeML = Number.isNaN(homeML0) ? null : homeML0;
        let awayML = Number.isNaN(awayML0) ? null : awayML0;
        const spreadForBlank = Number.isNaN(flatSpread) ? null : flatSpread;
        ({ mlA: homeML, mlB: awayML } = maybeBlankML(spreadForBlank, homeML, awayML));

        rec = {
          provider: (node.provider?.name || node.provider || "ESPN BET (NET)"),
          isLive: !!(node.isLive || LIVE_RX.test(`${node.displayName||""} ${node.type||""}`)),
          lastUpdated: node.lastUpdated || node.updateTime || nowIso(),
          homeSpread: Number.isNaN(flatSpread) ? null : flatSpread,
          awaySpread: Number.isNaN(flatSpread) ? null : -flatSpread,
          total: Number.isNaN(overUnder) ? null : overUnder,
          homeML, awayML
        };
      } else if (homeTeamOdds || awayTeamOdds) {
        const hS = Number(homeTeamOdds?.spread ?? homeTeamOdds?.line ?? homeTeamOdds?.handicap);
        const aS = Number(awayTeamOdds?.spread ?? awayTeamOdds?.line ?? awayTeamOdds?.handicap);
        const ou = Number(node.overUnder ?? node.total);
        let hML2 = Number(homeTeamOdds?.moneyLine ?? homeTeamOdds?.price);
        let aML2 = Number(awayTeamOdds?.moneyLine ?? awayTeamOdds?.price);
        hML2 = Number.isNaN(hML2) ? null : hML2;
        aML2 = Number.isNaN(aML2) ? null : aML2;
        const spreadForBlank = Number.isNaN(hS) ? (Number.isNaN(aS) ? null : aS) : hS;
        ({ mlA: hML2, mlB: aML2 } = maybeBlankML(spreadForBlank, hML2, aML2));
        if (!([hS,aS,ou].every(Number.isNaN))) {
          rec = {
            provider: (node.provider?.name || node.provider || "ESPN BET (NET)"),
            isLive: !!(node.isLive || LIVE_RX.test(`${node.displayName||""} ${node.type||""}`)),
            lastUpdated: node.lastUpdated || node.updateTime || nowIso(),
            homeSpread: Number.isNaN(hS) ? null : hS,
            awaySpread: Number.isNaN(aS) ? null : aS,
            total: Number.isNaN(ou) ? null : ou,
            homeML: hML2,
            awayML: aML2
          };
        }
      }

      if (rec && saneOdds({spread: favoritePerspectiveSpread(rec.homeSpread, rec.awaySpread, rec.homeML, rec.awayML), total: rec.total, homeML: rec.homeML, awayML: rec.awayML})) {
        if (!best || (rec.isLive && !best.isLive) || (rec.lastUpdated > best.lastUpdated)) {
          best = rec;
        }
      }

      for (const k in node) {
        const v = node[k];
        if (v && typeof v === "object") stack.push(v);
      }
    }
  }

  if (!best) return null;
  const spread = favoritePerspectiveSpread(best.homeSpread, best.awaySpread, best.homeML, best.awayML);
  const favSide = spread!=null ? (best.homeSpread===spread ? "HOME" : "AWAY") : null;

  let favML = favSide==="HOME" ? best.homeML : best.awayML;
  let dogML = favSide==="HOME" ? best.awayML : best.homeML;
  ({ mlA: favML, mlB: dogML } = maybeBlankML(spread, favML, dogML));

  const out = {
    source: "espnNet",
    provider: best.provider || "ESPN BET (NET)",
    isLive: true,
    lastUpdated: best.lastUpdated || nowIso(),
    spread,
    total: best.total ?? null,
    favML, dogML,
    homeSpread: best.homeSpread ?? null,
    awaySpread: best.awaySpread ?? null
  };
  if (!saneOdds(out)) return null;
  return out;
}

// ---------------------- Adapter: ESPN DOM (table → text) ----------------------
async function adapterEspnDom(eventId) {
  let pw;
  try { pw = await import("playwright"); } catch { return null; }

  const url = gamePageUrl(eventId);
  const browser = await pw.chromium.launch({ headless: true });
  const page = await browser.newPage({ userAgent: "Mozilla/5.0" });

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    for (let y=0; y<6; y++) { await page.mouse.wheel(0, 1000); await page.waitForTimeout(500); }

    const liveRoot = page.locator([
      "section:has-text('Live Odds')",
      "section:has-text('LIVE ODDS')",
      "div:has(h2:has-text('Live Odds'))",
      "div:has(h2:has-text('LIVE ODDS'))",
      "div:has(h3:has-text('Live Odds'))",
      "div:has(h3:has-text('LIVE ODDS'))",
      "a:has-text('All Live Odds on ESPN BET Sportsbook')"
    ].join(",")).first();

    const liveRootCount = await liveRoot.count();
    if (DEBUG) console.log("[dom] liveRoot count:", liveRootCount);
    if (liveRootCount > 0) { try { await liveRoot.scrollIntoViewIfNeeded(); } catch {} }

    let region = liveRoot;
    const role = await liveRoot.evaluate(el => el.tagName).catch(()=>null);
    if (role && /A/i.test(role)) region = liveRoot.locator("xpath=ancestor::*[self::section or self::div][1]");

    const table = region.locator("table").first();
    const hasTable = await table.count().then(c => c>0).catch(()=>false);
    if (DEBUG) console.log("[dom] hasTable:", hasTable);

    const RE_SPREAD = /^[+\-]\d+(?:\.5)?$/;
    const RE_TOTAL  = /^[ou]\d+(?:\.5)?$/i;
    const RE_ML     = /^[+\-]\d{3,4}$/;

    if (hasTable) {
      const headers = await table.locator("thead tr th").allInnerTexts().catch(()=>[]);
      if (DEBUG) console.log("[dom] headers:", headers);
      const idx = (label) => headers.findIndex(h => h.trim().toUpperCase() === label);
      let spreadIdx = idx("SPREAD");
      let totalIdx  = idx("TOTAL");
      let mlIdx     = idx("ML");
      if (mlIdx < 0) mlIdx = headers.findIndex(h => ["MONEYLINE","LINE"].includes(h.trim().toUpperCase()));

      if (spreadIdx >= 0 && totalIdx >= 0 && mlIdx >= 0) {
        const rows = await table.locator("tbody tr").all();
        if (rows.length >= 2) {
          async function firstMatchInCell(row, colIndex, re) {
            const cell = row.locator("td").nth(colIndex);
            const texts = await cell.locator("*").allInnerTexts().catch(() => []);
            const self = await cell.innerText().catch(()=>null);
            if (self) texts.unshift(self);
            const hit = texts.map(t => t.trim()).find(t => {
              if (/off/i.test(t)) return false; // ML OFF → ignore
              return re.test(t);
            });
            return hit || null;
          }

          const awaySpreadTxt = await firstMatchInCell(rows[0], spreadIdx, RE_SPREAD);
          const homeSpreadTxt = await firstMatchInCell(rows[1], spreadIdx, RE_SPREAD);
          const awayTotalTxt  = await firstMatchInCell(rows[0], totalIdx,  RE_TOTAL);
          const awayMLTxt     = await firstMatchInCell(rows[0], mlIdx,     RE_ML);
          const homeMLTxt     = await firstMatchInCell(rows[1], mlIdx,     RE_ML);

          if (DEBUG) console.log("[dom] table parsed", { awaySpreadTxt, homeSpreadTxt, awayTotalTxt, awayMLTxt, homeMLTxt, url });

          const n = (s) => s == null ? null : Number(s.replace(/[ou]/i, ""));
          const awaySpread = awaySpreadTxt ? Number(awaySpreadTxt) : null;
          const homeSpread = homeSpreadTxt ? Number(homeSpreadTxt) : (awaySpread!=null ? -awaySpread : null);
          const total      = n(awayTotalTxt);
          let awayML       = awayMLTxt ? Number(awayMLTxt) : null;
          let homeML       = homeMLTxt ? Number(homeMLTxt) : null;

          const spreadForBlank = isNum(homeSpread) ? homeSpread : (isNum(awaySpread) ? awaySpread : null);
          ({ mlA: homeML, mlB: awayML } = maybeBlankML(spreadForBlank, homeML, awayML));

          await browser.close();

          const gotAny = (awaySpread!=null && homeSpread!=null) || total!=null || homeML!=null || awayML!=null;
          if (gotAny) {
            const spread = favoritePerspectiveSpread(homeSpread, awaySpread, homeML, awayML);
            const favSide = spread!=null ? (homeSpread===spread ? "HOME" : "AWAY") : null;
            const out = {
              source: "espnDomTable",
              provider: "ESPN BET (DOM)",
              isLive: true,
              lastUpdated: "dom",
              spread,
              total: total ?? null,
              favML: favSide==="HOME" ? homeML : awayML,
              dogML: favSide==="HOME" ? awayML : homeML,
              homeSpread, awaySpread
            };
            if (!saneOdds(out)) return null;
            return out;
          }
        }
      }
      if (DEBUG) console.log("[dom] table path had no matches; falling to text mode");
    }

    // TEXT MODE
    let regionText = await region.innerText().catch(()=>null);
    if (!regionText || regionText.length < 20) {
      regionText = await page.innerText("body").catch(()=>null);
      if (DEBUG) console.log("[dom] using full page text fallback");
    }
    await browser.close();
    if (!regionText) return null;

    const spreadVals = [...regionText.matchAll(/(^|\s)([+\-]\d+(?:\.5)?)(?=\s)/g)]
      .map(m => Number(m[2])).filter(v => Math.abs(v) <= 60);
    let chosenAbs = null; const absCount = {};
    for (const v of spreadVals) { const a=Math.abs(v); absCount[a]=(absCount[a]||0)+1; if (absCount[a]>=2) chosenAbs=a; }
    if (chosenAbs == null && spreadVals.length) chosenAbs = Math.abs(spreadVals[spreadVals.length-1]);
    let awaySpread = null, homeSpread = null;
    if (chosenAbs != null) {
      const neg = spreadVals.find(v => v === -chosenAbs);
      const pos = spreadVals.find(v => v ===  chosenAbs);
      if (neg != null && pos != null) { awaySpread = neg; homeSpread = pos; }
      else { const v = spreadVals.find(x => Math.abs(x) === chosenAbs); if (v!=null){ awaySpread=v; homeSpread=-v; } }
    }
    const totalVals = [...regionText.matchAll(/(^|\s)[ou](\d+(?:\.5)?)(?=\s)/gi)].map(m => Number(m[2]));
    let total = null;
    if (totalVals.length) {
      const freq = {}; for (const t of totalVals) freq[t]=(freq[t]||0)+1;
      total = Number(Object.keys(freq).sort((a,b)=>freq[b]-freq[a] || totalVals.lastIndexOf(Number(a)) - totalVals.lastIndexOf(Number(b)))[0]);
    }
    const mlVals = [...regionText.matchAll(/(^|\s)([+\-]\d{3,4})(?=\s)/g)].map(m => Number(m[2]));
    const big = mlVals.filter(v => Math.abs(v) >= 150);
    let awayML = null, homeML = null;
    for (let i=big.length-1;i>=0;i--){ if (big[i] < 0){ awayML = big[i]; break; } }
    for (let i=big.length-1;i>=0;i--){ if (big[i] > 0){ homeML = big[i]; break; } }
    const spreadForBlank = isNum(homeSpread) ? homeSpread : (isNum(awaySpread) ? awaySpread : null);
    ({ mlA: homeML, mlB: awayML } = maybeBlankML(spreadForBlank, homeML, awayML));

    const spread = favoritePerspectiveSpread(homeSpread, awaySpread, homeML, awayML);
    const favSide = spread!=null ? (homeSpread===spread ? "HOME" : "AWAY") : null;
    const out = {
      source: "espnDomText",
      provider: "ESPN BET (DOM)",
      isLive: true,
      lastUpdated: "dom",
      spread,
      total: total ?? null,
      favML: favSide==="HOME" ? homeML : awayML,
      dogML: favSide==="HOME" ? awayML : homeML,
      homeSpread, awaySpread
    };
    if (!saneOdds(out)) return null;
    return out;

  } catch (e) {
    if (DEBUG) console.log("[dom] error:", e.message);
    await browser.close();
    return null;
  }
}

// ---------------------- One fetch cycle (arbiter) ----------------------
async function fetchOnce() {
  const sb = await getScoreboard(DATE);
  const events = sb.events || [];

  // find matchup
  let match = null;
  for (const e of events) {
    const comp = e.competitions?.[0];
    if (!comp) continue;
    const comps = comp.competitors || [];
    const home = comps.find(t => t.homeAway === "home") || comps[1];
    const away = comps.find(t => t.homeAway === "away") || comps[0];
    const homeNames = [home?.team?.name, home?.team?.shortDisplayName, home?.team?.displayName, home?.team?.abbreviation].filter(Boolean);
    const awayNames = [away?.team?.name, away?.team?.shortDisplayName, away?.team?.displayName, away?.team?.abbreviation].filter(Boolean);
    if (awayNames.some(n=>sameTeam(n,AWAY)) && homeNames.some(n=>sameTeam(n,HOME))) { match = { e, comp, home, away }; break; }
  }
  if (!match) {
    console.error(`[error] Matchup not found on ${DATE} [${LEAGUE.toUpperCase()}]: ${AWAY} @ ${HOME}`);
    return null;
  }

  const { e, comp, home, away } = match;
  const eventId = comp.id || e.id;
  const homeAbbr = home?.team?.abbreviation || home?.team?.shortDisplayName || home?.team?.name;
  const awayAbbr = away?.team?.abbreviation || away?.team?.shortDisplayName || away?.team?.name;

  const game = {
    status: comp.status?.type?.name || "",
    clock: comp.status?.displayClock || "",
    period: comp.status?.period ?? "",
    homeScore: Number(home?.score ?? 0),
    awayScore: Number(away?.score ?? 0),
  };

  // Try adapters
  const layers = [];
  const apiOut = await adapterEspnApi(eventId);
  if (apiOut) layers.push(apiOut);

  let netOut=null, domOut=null;
  if (USE_DOM) {
    netOut = await adapterEspnNetwork(eventId); if (netOut) layers.push(netOut);
    domOut = await adapterEspnDom(eventId);     if (domOut) layers.push(domOut);
  }

  // Arbiter: prefer live, then newest, then source priority
  const priority = { espnApi: 2, espnNet: 1, espnDomTable: 0, espnDomText: 0 };
  const best = layers.sort((a,b)=>{
    if ((b.isLive?1:0) !== (a.isLive?1:0)) return (b.isLive?1:0)-(a.isLive?1:0);
    const ta = Date.parse(a.lastUpdated||0)||0, tb = Date.parse(b.lastUpdated||0)||0;
    if (tb !== ta) return tb - ta;
    return (priority[b.source]??-1) - (priority[a.source]??-1);
  })[0] || null;

  if (DEBUG) {
    console.log("[arbiter] candidates:", layers.map(l=>({src:l.source, prov:l.provider, live:l.isLive, t:l.lastUpdated, spread:l.spread, total:l.total, favML:l.favML, dogML:l.dogML})));
    console.log("[arbiter] chosen:", best ? {src:best.source, prov:best.provider} : "none");
  }

  const dateMMDD = mmdd();
  const weekLabel = (sb?.leagues?.[0]?.season?.type?.name) ? sb.leagues[0].season.type.name : (sb?.leagues?.[0]?.season?.year || "");
  const scoreText = `${game.awayScore}-${game.homeScore}`;
  const timeText = `${game.status}${game.clock ? " " + game.clock : ""}${game.period ? " Q" + game.period : ""}`;

  console.log("----- ESPN Live Line -----");
  console.log(`League:  ${LEAGUE.toUpperCase()}`);
  console.log(`Matchup: ${AWAY} @ ${HOME}`);
  console.log(`Score:   ${awayAbbr} ${game.awayScore} - ${homeAbbr} ${game.homeScore}`);
  console.log(`Time:    ${timeText}`);

  if (best) {
    console.log(`Book:    ${best.provider} (${best.isLive ? "LIVE" : "PREGAME"}, updated ${best.lastUpdated || "n/a"})`);
    console.log(`Spread:  ${isNum(best.spread) ? best.spread : "n/a"}  (favorite: ${isNum(best.spread) ? (best.spread<0 ? "AWAY" : "HOME") : "n/a"})`);
    console.log(`Total:   ${isNum(best.total) ? best.total : "n/a"}`);
    console.log(`Fav ML:  ${isNum(best.favML) ? best.favML : "n/a"} | Dog ML: ${isNum(best.dogML) ? best.dogML : "n/a"}`);
  } else {
    console.log("Odds:    n/a (all adapters returned null)");
  }

  const row = [
    dateMMDD, weekLabel, awayAbbr, homeAbbr, scoreText, timeText,
    best?.spread ?? "", best?.total ?? "", best?.favML ?? "", best?.dogML ?? "",
    best?.provider ?? "", best?.lastUpdated ?? "", best?.isLive ? "LIVE" : "PREGAME"
  ];

  console.log("\nCSV:");
  console.log(toCsvRow(row));

  console.log("\nJSON:");
  console.log(JSON.stringify({
    date: dateMMDD, week: weekLabel, away: awayAbbr, home: homeAbbr, score: scoreText, time: timeText,
    provider: best?.provider ?? null, lastUpdated: best?.lastUpdated ?? null, isLive: !!best?.isLive,
    spread: best?.spread ?? null, total: best?.total ?? null, favML: best?.favML ?? null, dogML: best?.dogML ?? null
  }, null, 2));

  return best;
}

// ---------------------- Watch loop ----------------------
let lastKey = null;
async function main() {
  if (WATCH_SEC > 0) {
    if (DEBUG) console.log(`[watch] polling every ${WATCH_SEC}s…`);
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const curr = await fetchOnce();
      const key = JSON.stringify(curr || {});
      if (key !== lastKey) lastKey = key; // fetchOnce printed
      await new Promise(r => setTimeout(r, WATCH_SEC * 1000));
    }
  } else {
    await fetchOnce();
  }
}

main().catch(e => { console.error("[fatal main]", e.message || e); process.exit(1); });
