// ops/live-gate.mjs
// Prints run_updater=true if ANY game in the league is in progress and <= halftime.
// Uses America/New_York for "today".
import axios from "axios";

const LEAGUE = process.env.LEAGUE || "college-football"; // "college-football" | "nfl"

// YYYYMMDD in ET
const et = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
const d = new Date(et);
const yyyy = d.getFullYear();
const mm = String(d.getMonth() + 1).padStart(2, "0");
const dd = String(d.getDate()).padStart(2, "0");
const dates = `${yyyy}${mm}${dd}`;

const SPORT_PATH = LEAGUE === "nfl" ? "nfl" : "college-football";
const url = `https://site.api.espn.com/apis/site/v2/sports/football/${SPORT_PATH}/scoreboard?dates=${dates}`;

let run = false;

try {
  const { data } = await axios.get(url, { timeout: 15000 });
  const events = data?.events || [];
  for (const ev of events) {
    const comp = ev?.competitions?.[0];
    const st = comp?.status || {};
    const type = st?.type || {};
    const period = Number(st?.period ?? 0);

    // "in" means actively in progress; accept Halftime or Q1/Q2 only
    const inProgress = String(type?.state || "").toLowerCase() === "in";
    const halftime = /half/i.test(type?.shortDetail || type?.detail || "");
    if (inProgress && (period <= 2 || halftime)) { run = true; break; }
  }
} catch (e) {
  // If the gate can't reach ESPN, be safe and DO NOT flip to 5-minute mode
  run = false;
}

const out = String(run);
console.log(`run_updater=${out}`);
if (process.env.GITHUB_OUTPUT) {
  await Bun.write(process.env.GITHUB_OUTPUT, `run_updater=${out}\n`);
} else {
  // Node-only fallback (if not using Bun)
  console.log(`::set-output name=run_updater::${out}`);
}
