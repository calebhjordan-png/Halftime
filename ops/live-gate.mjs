// live-gate.mjs
// Emits PURE JSON to STDOUT: { shouldLoop, reason, inProgress, preThird, gameIds }
// Nothing except JSON goes to stdout. Any human logs go to stderr.

import axios from "axios";
import { DateTime } from "luxon";

const {
  LEAGUE = "college-football",               // "college-football" | "nfl"
} = process.env;

// ---- helpers ----
const ET = "America/New_York";
function inDailyWindowET(now = DateTime.now().setZone(ET)) {
  // Active between 10:00–01:59 ET (i.e., 10am–2am next day).
  const h = now.hour;
  return (h >= 10) || (h <= 1);
}

async function fetchScoreboardIds(league) {
  // ESPN “events” list from the scoreboard endpoint
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${league}/scoreboard`;
  const { data } = await axios.get(url, { timeout: 15000 });
  const events = data?.events || [];
  return events.map(e => String(e?.id)).filter(Boolean);
}

async function fetchSummaries(league, ids) {
  const got = [];
  for (const id of ids) {
    try {
      const url = `https://site.api.espn.com/apis/site/v2/sports/football/${league}/summary?event=${id}`;
      const { data } = await axios.get(url, { timeout: 15000 });
      const comp = data?.header?.competitions?.[0];
      const stat = comp?.status?.type?.description || comp?.status?.type?.detail || "";
      // figure quarter number if present
      const qNum = Number(comp?.status?.period ?? 0);
      const short = comp?.status?.type?.shortDetail || stat || "";
      const isInProgress = /in\s*progress|^q[1-4]\b|^1st|^2nd|^3rd|^4th|^ot/i.test(short) || /2nd|3rd|4th|OT/i.test(short);
      const isFinal = /final/i.test(short);
      got.push({ id, short, qNum, isInProgress, isFinal });
    } catch (e) {
      console.error(`summary miss ${id}:`, e?.response?.status || e?.message || e);
    }
  }
  return got;
}

function decideLoop(rows) {
  const inProgress = rows.filter(r => r.isInProgress && !r.isFinal);
  const preThird   = inProgress.filter(r => (r.qNum || 0) < 3);
  if (preThird.length > 0) {
    return { shouldLoop: true, reason: `${preThird.length} pre-3rd`, inProgress: inProgress.length, preThird: preThird.length, gameIds: preThird.map(r => r.id) };
  }
  if (inProgress.length > 0) {
    return { shouldLoop: false, reason: `all ${inProgress.length} in 3Q+`, inProgress: inProgress.length, preThird: 0, gameIds: [] };
  }
  return { shouldLoop: false, reason: "no live games", inProgress: 0, preThird: 0, gameIds: [] };
}

// ---- main ----
(async () => {
  const nowET = DateTime.now().setZone(ET);
  if (!inDailyWindowET(nowET)) {
    // Outside window: never loop
    const out = { shouldLoop: false, reason: "outside 10:00–01:59 ET window", inProgress: 0, preThird: 0, gameIds: [] };
    process.stdout.write(JSON.stringify(out));
    return;
  }

  let ids = [];
  try {
    ids = await fetchScoreboardIds(LEAGUE);
  } catch (e) {
    console.error("scoreboard fetch failed:", e?.message || e);
    const out = { shouldLoop: false, reason: "scoreboard error", inProgress: 0, preThird: 0, gameIds: [] };
    process.stdout.write(JSON.stringify(out));
    return;
  }

  if (!ids.length) {
    const out = { shouldLoop: false, reason: "no events", inProgress: 0, preThird: 0, gameIds: [] };
    process.stdout.write(JSON.stringify(out));
    return;
  }

  const rows = await fetchSummaries(LEAGUE, ids);
  const decision = decideLoop(rows);

  // Emit PURE JSON
  process.stdout.write(JSON.stringify(decision));
})();
