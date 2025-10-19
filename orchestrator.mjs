// orchestrator.mjs
// Decides whether to re-queue (5-min loop) based on ESPN scoreboard.
// Conservative: only loops if at least one game is clearly pre-3rd (Q1, Q2, or Halftime).
// On any fetch/parse error it defaults to HOURLY (no loop).

import axios from "axios";
import { DateTime } from "luxon";

const {
  LEAGUE = "college-football", // "college-football" | "nfl"
  FORCE_FIVE = "",             // "true" to force loop (debug only)
} = process.env;

function log(...a) { console.log(...a); }

function todayKeyET() {
  return DateTime.now().setZone("America/New_York").toFormat("yyyyLLdd"); // e.g., 20251018
}

function leaguePath(league) {
  return league === "nfl" ? "nfl" : "college-football";
}

// True when a status clearly indicates the game is before 3rd quarter.
function isPreThirdFromStatus(statusObj) {
  try {
    const period = Number(statusObj?.period ?? 0); // 0 for not started, 1,2,3,4,...
    const short = String(
      statusObj?.type?.shortDetail ||
      statusObj?.type?.detail ||
      statusObj?.type?.description ||
      ""
    ).toLowerCase();

    // If period field is numeric and < 3 → pre-3rd
    if (period >= 1 && period <= 2) return true;

    // If status is Halftime we still want to loop (we capture “Score” at half).
    if (/\bhalftime\b/.test(short)) return true;

    // If not started yet (period 0), do NOT loop; the hourly pass is enough.
    // “Start of 3rd” / “3:20 - 3rd” / “3rd” etc. → do NOT loop.
    return false;
  } catch {
    return false;
  }
}

async function fetchScoreboard(league, yyyymmdd) {
  const lg = leaguePath(league);
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${yyyymmdd}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

async function decide() {
  if (String(FORCE_FIVE).toLowerCase() === "true") {
    log("Gate: FORCE_FIVE=true → 5-minute loop.");
    return { shouldLoop: true, reason: "forced" };
  }

  try {
    const dateKey = todayKeyET();
    const sb = await fetchScoreboard(LEAGUE, dateKey);
    const events = Array.isArray(sb?.events) ? sb.events : [];
    let preThirdCount = 0;
    let totalLiveish = 0;

    for (const ev of events) {
      const comp = ev?.competitions?.[0];
      const st = comp?.status;
      if (!st) continue;

      const typeName = String(st?.type?.name || "").toLowerCase();
      // Track “in-progress-ish” games for logging.
      if (/in/.test(typeName) || /progress/.test(typeName) || /post/.test(typeName) || /status/.test(typeName)) {
        totalLiveish++;
      }

      if (isPreThirdFromStatus(st)) {
        preThirdCount++;
      }
    }

    const shouldLoop = preThirdCount > 0;
    log(`Gate: ${preThirdCount} pre-3rd game(s) out of ${totalLiveish} in-progress-ish on ${dateKey}.`);
    return { shouldLoop, reason: `${preThirdCount} pre-3rd` };
  } catch (e) {
    // Be conservative: if we can't decide, DO NOT loop (fall back to hourly).
    log("Gate: scoreboard fetch failed → default to hourly (no loop).", e?.message || e);
    return { shouldLoop: false, reason: "fetch-failed" };
  }
}

const res = await decide();
console.log(JSON.stringify(res));
