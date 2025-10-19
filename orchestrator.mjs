// orchestrator.mjs
// Purpose: decide whether to re-queue the CFB/NFL live updater in a 5-minute loop.
// Output: STRICT JSON to STDOUT ONLY -> { shouldLoop: boolean, reason: string }
// Human-readable notes go to STDERR (console.error), so redirection to a file stays valid JSON.

import axios from "axios";
import { DateTime } from "luxon";

const LEAGUE = (process.env.LEAGUE || "college-football").trim();    // "college-football" | "nfl"
const FORCE_FIVE = String(process.env.FORCE_FIVE || "").toLowerCase(); // "true" to force 5-min loop

// ESPN scoreboard URL per league
function scoreboardUrl(yyyyMMdd) {
  const sport = "football";
  const leaguePath = LEAGUE === "nfl" ? "nfl" : "college-football";
  // Example: https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20251019
  return `https://site.api.espn.com/apis/site/v2/sports/${sport}/${leaguePath}/scoreboard?dates=${yyyyMMdd}`;
}

// Decide if any live games exist and whether *any* of them are still prior to 3rd quarter.
function analyzeScoreboard(sb) {
  const events = Array.isArray(sb?.events) ? sb.events : [];
  let inProgress = 0;
  let preThird = 0;

  for (const ev of events) {
    const comp = ev?.competitions?.[0];
    const status = comp?.status || ev?.status || {};
    const type = status?.type || {};
    // ESPN "in" = in-progress
    const isLive = String(type?.state || "").toLowerCase() === "in";
    if (!isLive) continue;
    inProgress += 1;

    // Period/quarter number; ESPN calls it "period"
    const period = Number(status?.period ?? comp?.period ?? ev?.period ?? NaN);
    if (Number.isFinite(period) && period <= 2) preThird += 1;
  }

  return { inProgress, preThird };
}

async function main() {
  try {
    // Eastern date, because your sheets/workflows use ET semantics
    const nowET = DateTime.now().setZone("America/New_York");
    const yyyymmdd = nowET.toFormat("yyyyLLdd");

    if (FORCE_FIVE === "true" || FORCE_FIVE === "1") {
      const payload = { shouldLoop: true, reason: "forced" };
      console.error(`Gate (forced): 5-minute loop enabled.`);
      process.stdout.write(JSON.stringify(payload));
      return;
    }

    const url = scoreboardUrl(yyyymmdd);
    const { data } = await axios.get(url, { timeout: 15000 });
    const { inProgress, preThird } = analyzeScoreboard(data);

    const reason = `${preThird} pre-3rd game(s) out of ${inProgress} in-progress-ish on ${yyyymmdd}.`;
    console.error(`Gate: ${reason}`);

    const shouldLoop = preThird > 0; // loop only if *any* live games are prior to 3rd
    const payload = { shouldLoop, reason };
    process.stdout.write(JSON.stringify(payload));
  } catch (err) {
    const msg = (err?.response?.status ? `HTTP ${err.response.status}` : (err?.message || String(err)));
    console.error(`Gate error: ${msg}`);
    // On error, don’t loop (fail-safe) but surface reason
    const payload = { shouldLoop: false, reason: `error: ${msg}` };
    process.stdout.write(JSON.stringify(payload));
  }
}

main();
