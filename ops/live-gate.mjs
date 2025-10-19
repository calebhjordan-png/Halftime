// live-gate.mjs
// Purpose: tell the workflow whether to run a 5-minute cycle for CFB.
// Decides by looking at ESPN's *scoreboard* (not the sheet).
//
// "FAST=1"  → run every 5 minutes (there is at least one game pre-3rd)
// "FAST=0"  → run on the top-of-hour only
//
// Pre-3rd definition: STATUS_IN_PROGRESS with period 1 or 2, or "Halftime".

import axios from "axios";

// --- config from env (defaults are safe) ---
const LEAGUE = process.env.LEAGUE || "college-football"; // keep CFB here
const DEBUG  = String(process.env.DEBUG_MODE || "") === "1";

// Format an ET date → YYYYMMDD for ESPN scoreboard ?dates= param
function ymdInEastern(d = new Date()) {
  const f = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(d);
  const yy = f.find(p => p.type === "year").value;
  const mm = f.find(p => p.type === "month").value;
  const dd = f.find(p => p.type === "day").value;
  return `${yy}${mm}${dd}`;
}

function isPreThird(status) {
  // Robust detection across ESPN variants
  const type   = status?.type || {};
  const name   = String(type.name || type.state || "").toUpperCase(); // STATUS_IN_PROGRESS etc.
  const desc   = (type.shortDetail || type.detail || type.description || "").toLowerCase();
  const period = Number(status?.period ?? status?.displayPeriod ?? 0);

  if (desc.includes("halftime")) return true;          // halftime still "pre-3rd"
  if (name.includes("IN_PROGRESS") && period > 0 && period < 3) return true;
  return false;
}

async function fetchScoreboard() {
  const ymd = ymdInEastern();
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/scoreboard?dates=${ymd}`;
  if (DEBUG) console.log("🔎 scoreboard:", url);
  const { data } = await axios.get(url, { timeout: 15000 });
  return data;
}

(async () => {
  try {
    const board = await fetchScoreboard();
    const events = Array.isArray(board?.events) ? board.events : [];

    let preThirdCount = 0;
    for (const ev of events) {
      const comp = ev?.competitions?.[0];
      const st   = comp?.status || ev?.status;
      if (isPreThird(st)) preThirdCount++;
    }

    const FAST = preThirdCount > 0 ? "1" : "0";
    console.log(`Gate: pre-3rd games = ${preThirdCount} → FAST=${FAST}`);

    // Emit GitHub Actions output in both modern & legacy styles
    console.log(`::set-output name=FAST::${FAST}`);
    console.log(`FAST=${FAST}`);
  } catch (err) {
    console.log("Gate error (failing open to FAST):", err?.message || err);
    // If the gate can’t decide, fail open so we don’t miss updates.
    console.log("::set-output name=FAST::1");
    console.log("FAST=1");
  }
})();
