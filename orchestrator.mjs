import { google } from "googleapis";

/** ====== CONFIG via GitHub Action env ====== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase(); // "nfl" or "college-football"
const TAB_NAME  = (process.env.TAB_NAME || "NFL").trim();

/** ====== Helpers ====== */
function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw); // raw JSON
  const json = Buffer.from(raw, "base64").toString("utf8"); // Base64
  return JSON.parse(json);
}

function yyyymmddInET(d=new Date()) {
  const et = new Date(d.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const y = et.getFullYear();
  const m = String(et.getMonth()+1).padStart(2,"0");
  const day = String(et.getDate()).padStart(2,"0");
  return `${y}${m}${day}`;
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { "User-Agent": "halftime-bot" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} ${url}`);
  return res.json();
}

/** ESPN Scoreboard endpoint (stable & simple) */
function scoreboardUrl(league, dates) {
  const lg = league === "ncaaf" || league === "college-football" ? "college-football" : "nfl";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}`;
}

/** Prefer ESPN BET in odds array; else fall back to first provider */
function pickOdds(oddsArr=[]) {
  if (!Array.isArray(oddsArr) || oddsArr.length === 0) return null;
  const espnBet =
    oddsArr.find(o => /espn\s*bet/i.test(o.provider?.name || "")) ||
    oddsArr.find(o => /espn bet/i.test(o.provider?.displayName || ""));
  return espnBet || oddsArr[0];
}

/** Normalize an event to your columns */
function toRow(event, weekText) {
  const comp = event.competitions?.[0] || {};
  const status = event.status?.type?.name || comp.status?.type?.name || "";
  const shortStatus = event.status?.type?.shortDetail || comp.status?.type?.shortDetail || "";
  const away = comp.competitors?.find(c => c.homeAway === "away");
  const home = comp.competitors?.find(c => c.homeAway === "home");

  const matchup = `${(away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away")} @ ${(home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home")}`;

  const finalScore = /final/i.test(status)
    ? `${away?.score ?? ""}-${home?.score ?? ""}`
    : "";

  // Pregame odds (closing line just before or at kickoff);
  const o = pickOdds(comp.odds || event.odds || []);

  // Normalize spread & ML so that columns are "Away Spread / Away ML / Home Spread / Home ML / Total"
  // ESPN odds payload often gives the favorite line & details; we map to away/home.
  let awaySpread = "", homeSpread = "", total = "", awayML = "", homeML = "";
  if (o) {
    // spread: favorite team id with spread, "details" like "SEA -3.5" etc, "overUnder"
    total = (o.overUnder ?? o.total) ?? "";
    const favId = o?.favorite ? String(o.favorite) : null;
    const spread = Number.isFinite(o.spread) ? o.spread :
                   (typeof o.spread === "string" ? parseFloat(o.spread) : NaN);

    // Moneylines, if provided per-team:
    // Some payloads: o.moneyLineAway / o.moneyLineHome; others only list favorite/dog ML
    if (o.moneyLineAway !== undefined) awayML = o.moneyLineAway;
    if (o.moneyLineHome !== undefined) homeML = o.moneyLineHome;
    if (!awayML && o.underdogMoneyLine !== undefined && away?.team?.id && favId) {
      awayML = (String(away.team.id) === favId) ? o.favoriteMoneyLine : o.underdogMoneyLine;
      homeML = (String(home?.team?.id) === favId) ? o.favoriteMoneyLine : o.underdogMoneyLine;
    }

    if (!Number.isNaN(spread) && favId && away?.team?.id && home?.team?.id) {
      if (String(away.team.id) === favId) {
        awaySpread = `-${Math.abs(spread)}`;
        homeSpread = `+${Math.abs(spread)}`;
      } else if (String(home.team.id) === favId) {
        homeSpread = `-${Math.abs(spread)}`;
        awaySpread = `+${Math.abs(spread)}`;
      }
    } else if (o.details) {
      // Fallback: parse "SEA -3.5" from details
      const m = o.details.match(/([A-Za-z]+)\s*([+-]?\d+(\.\d+)?)/);
      if (m && away?.team?.abbreviation && home?.team?.abbreviation) {
        const line = parseFloat(m[2]);
        if (m[1].toUpperCase() === (away.team.abbreviation || "").toUpperCase()) {
          awaySpread = (line > 0 ? `+${line}` : `${line}`);
          homeSpread = (line > 0 ? `-${line}` : `+${Math.abs(line)}`);
        } else {
          homeSpread = (line > 0 ? `+${line}` : `${line}`);
          awaySpread = (line > 0 ? `-${line}` : `+${Math.abs(line)}`);
        }
      }
    }
  }

  const dateET = new Date(event.date).toLocaleDateString("en-US", { timeZone: "America/New_York" });

  return [
    dateET,                 // Date
    weekText || "",         // Week
    shortStatus || status,  // Status
    matchup,                // Matchup
    finalScore,             // Final Score
    awaySpread || "",       // Away Spread
    String(awayML || ""),   // Away ML
    homeSpread || "",       // Home Spread
    String(homeML || ""),   // Home ML
    String(total || "")     // Total
  ];
}

/** ====== Main: fetch scoreboard & append new rows (no duplicates) ====== */
async function main() {
  if (!SHEET_ID || !CREDS_RAW) {
    console.error("Missing secrets.");
    process.exit(1);
  }

  // Auth
  const CREDS = parseServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Ensure tab exists
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const tabs = (meta.data.sheets || []).map(s => s.properties?.title);
  if (!tabs.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
  }

  // Read existing rows to avoid duplicates (Date+Matchup key)
  const existingRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A2:D`, // Date (A) + Matchup (D)
  });
  const existing = new Set(
    (existingRes.data.values || []).map(r => `${(r[0]||"").trim()}__${(r[3]||"").trim()}`)
  );

  // Pull scoreboard
  const dates = yyyymmddInET(new Date());
  const url = scoreboardUrl(LEAGUE, dates);
  const data = await fetchJson(url);

  const weekText =
    data?.week?.text ||
    data?.leagues?.[0]?.season?.type?.name ||
    "";

  const rows = [];
  for (const ev of data?.events || []) {
    try {
      const row = toRow(ev, weekText);
      const key = `${row[0]}__${row[3]}`; // Date__Matchup
      if (!existing.has(key)) rows.push(row);
    } catch (e) {
      console.warn("Skip event parse error:", e.message);
    }
  }

  if (rows.length === 0) {
    console.log("No new games to append (or already logged).");
    return;
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: rows },
  });

  console.log(`✅ Appended ${rows.length} row(s) to ${TAB_NAME}.`);
}

main().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});
