// live-gate.mjs — emits a single FAST output for GitHub Actions
// FAST=1  → run 5-min loop (there is at least one game pre-3rd or halftime)
// FAST=0  → skip 5-min loop

const LEAGUE = (process.env.LEAGUE || "college-football").toLowerCase(); // keep default CFB
const DEBUG  = String(process.env.DEBUG_MODE || "") === "1";

// When true, send human logs to stderr and keep stdout machine-clean.
const GHA_JSON_MODE = process.argv.includes("--gha") || process.env.GHA_JSON === "1";

const log  = (...a) => GHA_JSON_MODE ? process.stderr.write(a.join(" ") + "\n") : console.log(...a);
const elog = (...a) => process.stderr.write(a.join(" ") + "\n");

function ymdInEastern(d = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(d);
  const get = t => parts.find(p => p.type === t)?.value ?? "";
  return `${get("year")}${get("month")}${get("day")}`;
}

function isPreThird(status) {
  const type   = status?.type || {};
  const name   = String(type.name || type.state || "").toUpperCase();
  const desc   = String(type.shortDetail || type.detail || type.description || "").toLowerCase();
  const period = Number(status?.period ?? status?.displayPeriod ?? 0);
  if (desc.includes("halftime")) return true;
  return name.includes("IN_PROGRESS") && period > 0 && period < 3;
}

async function fetchScoreboard() {
  const ymd = ymdInEastern();
  const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/scoreboard?dates=${ymd}`;
  if (DEBUG) log("scoreboard:", url);
  const r = await fetch(url, { headers: { "User-Agent": "live-gate/1.0" }, cache: "no-store" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function writeGhaOutput(name, value) {
  const file = process.env.GITHUB_OUTPUT;
  if (file) {
    // Official GA output mechanism
    require("fs").appendFileSync(file, `${name}=${value}\n`, "utf8");
  }
}

(async () => {
  try {
    const board = await fetchScoreboard();
    const events = Array.isArray(board?.events) ? board.events : [];
    let preThird = 0;
    for (const ev of events) {
      const st = ev?.competitions?.[0]?.status || ev?.status;
      if (isPreThird(st)) preThird++;
    }
    const FAST = preThird > 0 ? "1" : "0";

    // Write the GA output file so downstream steps can use `steps.gate.outputs.FAST`
    writeGhaOutput("FAST", FAST);

    // Keep stdout machine-readable only when --gha is set
    if (GHA_JSON_MODE) {
      process.stdout.write(JSON.stringify({ ok: true, FAST }) + "\n");
    } else {
      // Human-friendly logs go to stdout when not in GA JSON mode
      log(`Gate: pre-3rd games = ${preThird} → FAST=${FAST}`);
    }
  } catch (err) {
    // Fail open: if we can’t decide, set FAST=1 so we don’t miss updates
    writeGhaOutput("FAST", "1");
    if (GHA_JSON_MODE) {
      process.stdout.write(JSON.stringify({ ok: false, error: String(err?.message || err), FAST: "1" }) + "\n");
    }
    elog("Gate error (failing open):", err?.message || err);
  }
})();
