// --- helpers ---
const ESPN = {
  sb: (sport = 'college-football', dates) =>
    `https://site.api.espn.com/apis/site/v2/sports/football/${sport}/scoreboard${dates ? `?dates=${dates}` : ''}`,
  summary: (eventId, sport = 'college-football') =>
    `https://site.api.espn.com/apis/site/v2/sports/football/${sport}/summary?event=${eventId}`,
  compOdds: (eventId, sport = 'college-football') =>
    `https://sports.core.api.espn.com/v2/sports/football/leagues/${sport}/events/${eventId}/competitions/${eventId}/odds?region=us&lang=en&limit=50`
};

// normalize numbers like "+1300", "-4000", "16.5"
const toNum = v => v == null ? null : Number(String(v).replace(/[,+]/g, '').trim());

// prefer live full-game odds from ESPN BET; fall back to first live book
const pickLiveGameMarket = (oddsArray) => {
  // oddsArray: array of odds objects (expanded below)
  // choose live === true and market === 'game' (not '2h'), ESPN BET first
  const liveGame = oddsArray.filter(o =>
    (o?.isLive === true || /live/i.test(o?.details || '')) &&
    !/2h|second half/i.test(o?.details || '') // keep full-game
  );

  // ESPN BET preferred
  let chosen = liveGame.find(o => /espn bet/i.test(o?.provider?.name || ''));
  if (!chosen) chosen = liveGame[0];
  return chosen || null;
};

// expand core odds feed items (links → objects)
async function expandOddsFeed(oddsFeedUrl, fetchJson) {
  const feed = await fetchJson(oddsFeedUrl);
  const items = feed?.items || [];
  const expanded = [];
  for (const it of items) {
    try {
      const o = await fetchJson(it.$ref || it.href || it);
      expanded.push(o);
    } catch {}
  }
  return expanded;
}

// extract live odds + half score from summary
async function getLiveFromESPN(eventId, fetchJson) {
  // 1) Summary: for drives + scoreboard + a summary-level odds object
  const sum = await fetchJson(ESPN.summary(eventId));
  const comp = sum?.competitions?.[0] || {};
  const home = comp?.competitors?.find(c => c?.homeAway === 'home');
  const away = comp?.competitors?.find(c => c?.homeAway === 'away');

  const halfScore = `${toNum(away?.score) ?? 0}-${toNum(home?.score) ?? 0}`; // K column

  // 2) Odds: use competitions odds collection (richer + live)
  const expanded = await expandOddsFeed(ESPN.compOdds(eventId), fetchJson);
  const chosen = pickLiveGameMarket(expanded);

  // Fallback: try summary.odds[0] if expanded empty
  const fallback = sum?.odds?.[0];

  const src = chosen || fallback;
  if (!src) return { halfScore, live: null };

  const out = {
    awaySpread: toNum(src?.awayTeam?.spread ?? src?.awayTeamOdds?.spread),
    awayML:     toNum(src?.awayTeam?.moneyLine ?? src?.awayTeamOdds?.moneyLine),
    homeSpread: toNum(src?.homeTeam?.spread ?? src?.homeTeamOdds?.spread),
    homeML:     toNum(src?.homeTeam?.moneyLine ?? src?.homeTeamOdds?.moneyLine),
    total:      toNum(src?.overUnder ?? src?.overUnderTotal ?? src?.total)
  };

  // Ensure signs on spreads: away spread positive if dog, home spread negative if favorite
  // (ESPN usually provides correct sign; this is a safety pass)
  if (out.awaySpread != null && out.homeSpread != null) {
    if (out.awaySpread > 0 && out.homeSpread > 0) out.homeSpread = -Math.abs(out.homeSpread);
    if (out.awaySpread < 0 && out.homeSpread < 0) out.awaySpread = Math.abs(out.awaySpread);
  }

  return { halfScore, live: out };
}

// --- usage inside your halftime loop for a given eventId + target sheet row ---
/*
  Assumes:
    - you already matched the sheet row for this event,
    - columns: K=Half Score, L=Live Away Spread, M=Live Away ML,
               N=Live Home Spread, O=Live Home ML, P=Live Total
*/
async function updateSheetWithLiveOdds(eventId, rowIndex, sheetsApi) {
  const fetchJson = async (url) => {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  };

  const { halfScore, live } = await getLiveFromESPN(eventId, fetchJson);

  // Write half score regardless so K is never "5-0" by mistake
  const K = halfScore;

  if (!live) {
    // If no live odds returned, leave L–P untouched (or clear them if you prefer)
    await sheetsApi.updateRange(`K${rowIndex}:K${rowIndex}`, [[K]]);
    return;
  }

  const { awaySpread, awayML, homeSpread, homeML, total } = live;

  // Map exactly to your columns
  const values = [[
    awaySpread,      // L
    awayML,          // M
    homeSpread,      // N
    homeML,          // O
    total            // P
  ]];

  // Two range updates is safer (score + odds)
  await sheetsApi.updateRange(`K${rowIndex}:K${rowIndex}`, [[K]]);
  await sheetsApi.updateRange(`L${rowIndex}:P${rowIndex}`, values);
}
