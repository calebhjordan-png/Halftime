function extractMoneylines(o, awayId, homeId, competitors = []) {
  let awayML = "", homeML = "";

  const numOrBlank = v => {
    if (v === 0) return "0";
    if (v == null) return "";
    const s = String(v).trim();
    const n = parseFloat(s.replace(/[^\d.+-]/g, ""));
    if (!Number.isFinite(n)) return "";
    return s.startsWith("+") ? `+${n}` : `${n}`;
  };

  // 0) PickCenter shape: awayTeamOdds/homeTeamOdds
  //    e.g. { awayTeamOdds: { moneyLine: -380 }, homeTeamOdds: { moneyLine: +290 } }
  if (o && (o.awayTeamOdds || o.homeTeamOdds)) {
    const aObj = o.awayTeamOdds || {};
    const hObj = o.homeTeamOdds || {};
    awayML = awayML || numOrBlank(aObj.moneyLine ?? aObj.moneyline ?? aObj.money_line);
    homeML = homeML || numOrBlank(hObj.moneyLine ?? hObj.moneyline ?? hObj.money_line);
    if (awayML || homeML) return { awayML, homeML };
  }

  // 1) teamOdds: [{ teamId, moneyLine }]
  if (Array.isArray(o?.teamOdds)) {
    for (const t of o.teamOdds) {
      const tid = String(t?.teamId ?? t?.team?.id ?? "");
      const ml  = numOrBlank(t?.moneyLine ?? t?.moneyline ?? t?.money_line);
      if (!ml) continue;
      if (tid === String(awayId)) awayML = awayML || ml;
      if (tid === String(homeId)) homeML = homeML || ml;
    }
    if (awayML || homeML) return { awayML, homeML };
  }

  // 2) nested competitors odds
  if (Array.isArray(o?.competitors)) {
    const findML = c => numOrBlank(c?.moneyLine ?? c?.moneyline ?? c?.odds?.moneyLine ?? c?.odds?.moneyline);
    const aML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(awayId)));
    const hML = findML(o.competitors.find(c => String(c?.id ?? c?.teamId) === String(homeId)));
    awayML = awayML || aML; homeML = homeML || hML;
    if (awayML || homeML) return { awayML, homeML };
  }

  // 3) direct fields
  awayML = awayML || numOrBlank(o?.moneyLineAway ?? o?.awayTeamMoneyLine ?? o?.awayMoneyLine ?? o?.awayMl);
  homeML = homeML || numOrBlank(o?.moneyLineHome ?? o?.homeTeamMoneyLine ?? o?.homeMoneyLine ?? o?.homeMl);
  if (awayML || homeML) return { awayML, homeML };

  // 4) favorite/underdog mapping
  const favId = String(o?.favorite ?? o?.favoriteId ?? o?.favoriteTeamId ?? "");
  const favML = numOrBlank(o?.favoriteMoneyLine);
  const dogML = numOrBlank(o?.underdogMoneyLine);
  if (favId && (favML || dogML)) {
    if (String(awayId) === favId) { awayML = awayML || favML; homeML = homeML || dogML; return { awayML, homeML }; }
    if (String(homeId) === favId) { homeML = homeML || favML; awayML = awayML || dogML; return { awayML, homeML }; }
  }

  // 5) competitors[] { odds: { moneyLine } } variant
  if (Array.isArray(competitors)) {
    for (const c of competitors) {
      const ml = numOrBlank(c?.odds?.moneyLine ?? c?.odds?.moneyline ?? c?.odds?.money_line);
      if (!ml) continue;
      if (c.homeAway === "away") awayML = awayML || ml;
      if (c.homeAway === "home") homeML = homeML || ml;
    }
  }

  return { awayML, homeML };
}
