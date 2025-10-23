#!/usr/bin/env node
// Basketball.mjs — NBA + College Basketball (CBB) prefill/live/finals in one script.
// Usage examples:
//   node Basketball.mjs --league=nba --mode=prefill --date=20251023
//   node Basketball.mjs --league=cbb --mode=live --span=3          (today ±3 days)
//   node Basketball.mjs --league=nba --mode=finals --date=20251022
//   node Basketball.mjs --mode=week --league=cbb                    (Mon-Sun span)
//
// ENV required:
//   SHEET_ID
//   GOOGLE_SERVICE_ACCOUNT_EMAIL
//   GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY  (escape newlines as \n in repo secrets)

import axios from "axios";
import { google } from "googleapis";
import { DateTime, Interval } from "luxon";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

// -------------------- Config --------------------
const COLUMN_MAP = {
  // Tune these to match your sheet layout
  key: "A",           // Game ID
  date: "B",          // Date (DD/MM/YY)
  time: "C",          // Tip time (local)
  away: "D",
  home: "E",
  status: "F",        // (Scheduled | In-Progress | Final, + clock/period where relevant)
  score: "G",         // "AWY xx - HOME xx"
  line: "H",          // spread
  total: "I",         // over/under
  provider: "J",      // odds provider label
};

const TAB_BY_LEAGUE = {
  nba: "NBA",
  cbb: "CBB",
};

const ESPN_ENDPOINTS = {
  nba: "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
  cbb: "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
};

// How many games to fetch per date (safe high)
const LIMIT = 300;

// -------------------- CLI --------------------
const argv = yargs(hideBin(process.argv))
  .option("league", {
    type: "string",
    choices: ["nba", "cbb"],
    default: "nba",
    describe: "Target league/tab",
  })
  .option("mode", {
    type: "string",
    choices: ["prefill", "live", "finals", "week"],
    default: "prefill",
    describe: "Script mode",
  })
  .option("date", {
    type: "string",
    describe: "Specific ESPN date (YYYYMMDD). Defaults to today.",
  })
  .option("span", {
    type: "number",
    default: 2,
    describe: "In live mode, search window: today ±span days",
  })
  .help()
  .parse();

// -------------------- Google Sheets Auth --------------------
const REQUIRED_ENVS = [
  "SHEET_ID",
  "GOOGLE_SERVICE_ACCOUNT_EMAIL",
  "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY",
];

for (const k of REQUIRED_ENVS) {
  if (!process.env[k]) {
    console.error(`Missing env: ${k}`);
    process.exit(1);
  }
}

const auth = new google.auth.JWT({
  email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
  key: process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, "\n"),
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// -------------------- Helpers --------------------
const toSheetA1 = (colLetter, row) => `${colLetter}${row}`;
const headers = [
  "Game ID",
  "Date",
  "Time",
  "Away",
  "Home",
  "Status",
  "Score",
  "Line",
  "Total",
  "Provider",
];

function toDDMMYY(dtISO) {
  // input ISO string; output DD/MM/YY in America/New_York
  return DateTime.fromISO(dtISO, { zone: "America/New_York" }).toFormat("dd/LL/yy");
}

function toLocalTime(dtISO) {
  return DateTime.fromISO(dtISO, { zone: "America/New_York" }).toFormat("h:mm a");
}

function concatScore(awayScore, homeScore, awayTeam, homeTeam) {
  if (awayScore == null && homeScore == null) return "";
  const a = awayScore ?? "";
  const h = homeScore ?? "";
  return `${awayTeam} ${a} - ${homeTeam} ${h}`.trim();
}

function statusText(event) {
  // ESPN event object -> human status
  const status = event?.status?.type?.name;
  const detail = event?.status?.type?.shortDetail || event?.status?.type?.detail || "";

  if (status === "STATUS_SCHEDULED") return "Scheduled";
  if (status === "STATUS_IN_PROGRESS") return `In-Progress ${detail}`.trim();
  if (status === "STATUS_FINAL") return `Final${detail ? " " + detail : ""}`;
  return detail || status || "";
}

function readOdds(comp) {
  const odds = comp?.odds?.[0];
  if (!odds) return { spread: "", total: "", provider: "" };
  const spread = odds?.spread ?? (odds?.details || ""); // some feeds put it only in 'details'
  const total = odds?.overUnder ?? "";
  const provider = odds?.provider?.name ?? odds?.provider?.id ?? "";
  return { spread: `${spread}`, total: `${total}`, provider: `${provider}` };
}

function pickTeams(event) {
  const comp = event?.competitions?.[0];
  const competitors = comp?.competitors || [];
  const away = competitors.find((c) => c?.homeAway === "away");
  const home = competitors.find((c) => c?.homeAway === "home");

  const awayTeam = away?.team?.shortDisplayName || away?.team?.name || "";
  const homeTeam = home?.team?.shortDisplayName || home?.team?.name || "";

  const awayScore = away?.score ? Number(away.score) : null;
  const homeScore = home?.score ? Number(home.score) : null;

  return { awayTeam, homeTeam, awayScore, homeScore, comp };
}

async function ensureHeaderRow(tab) {
  const range = `${tab}!A1:${String.fromCharCode("A".charCodeAt(0) + headers.length - 1)}1`;
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values: [headers] },
  });
}

async function fetchDay(league, yyyymmdd) {
  const url = `${ESPN_ENDPOINTS[league]}?dates=${yyyymmdd}&limit=${LIMIT}`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data?.events ?? [];
}

function* dateIterator(mode, span, givenDate) {
  if (mode === "week") {
    const now = DateTime.now().setZone("America/New_York");
    const start = now.startOf("week"); // Monday by default; OK for our purposes
    const end = now.endOf("week");
    for (const d of Interval.fromDateTimes(start, end).splitBy({ days: 1 })) {
      yield d.start.toFormat("yyyyLLdd");
    }
    return;
  }

  if (givenDate) {
    yield givenDate;
    return;
  }

  const today = DateTime.now().setZone("America/New_York");
  if (argv.mode === "live") {
    for (let i = -span; i <= span; i++) {
      yield today.plus({ days: i }).toFormat("yyyyLLdd");
    }
  } else {
    yield today.toFormat("yyyyLLdd");
  }
}

async function readAllRows(tab) {
  const range = `${tab}!A2:Z`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.SHEET_ID,
    range,
  });
  const values = res.data.values || [];
  // Map gameId -> row number (starting at 2)
  const map = new Map();
  values.forEach((row, idx) => {
    const id = row[0]; // column A
    if (id) map.set(id, idx + 2);
  });
  return map;
}

function buildRow(event) {
  const id = event?.id;
  const startISO = event?.date;
  const { awayTeam, homeTeam, awayScore, homeScore, comp } = pickTeams(event);
  const stat = statusText(event);
  const { spread, total, provider } = readOdds(comp);

  return {
    id,
    date: startISO ? toDDMMYY(startISO) : "",
    time: startISO ? toLocalTime(startISO) : "",
    away: awayTeam,
    home: homeTeam,
    status: stat,
    score: concatScore(awayScore, homeScore, awayTeam, homeTeam),
    line: spread,
    total,
    provider,
  };
}

function intoRowArray(obj) {
  return [
    obj.id,
    obj.date,
    obj.time,
    obj.away,
    obj.home,
    obj.status,
    obj.score,
    obj.line,
    obj.total,
    obj.provider,
  ];
}

async function batchWriteUpserts(tab, existingMap, rows) {
  const data = [];
  const toAppend = [];

  for (const row of rows) {
    const sheetRow = existingMap.get(row.id);
    if (sheetRow) {
      // Update cells in-place
      const updates = [
        { col: COLUMN_MAP.date, val: row.date },
        { col: COLUMN_MAP.time, val: row.time },
        { col: COLUMN_MAP.away, val: row.away },
        { col: COLUMN_MAP.home, val: row.home },
        { col: COLUMN_MAP.status, val: row.status },
        { col: COLUMN_MAP.score, val: row.score },
        { col: COLUMN_MAP.line, val: row.line },
        { col: COLUMN_MAP.total, val: row.total },
        { col: COLUMN_MAP.provider, val: row.provider },
      ];
      updates.forEach(({ col, val }) => {
        data.push({
          range: `${tab}!${col}${sheetRow}:${col}${sheetRow}`,
          values: [[val]],
        });
      });
    } else {
      toAppend.push(intoRowArray(row));
    }
  }

  if (data.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: process.env.SHEET_ID,
      requestBody: {
        valueInputOption: "USER_ENTERED",
        data,
      },
    });
  }

  if (toAppend.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.SHEET_ID,
      range: `${tab}!A:A`,
      valueInputOption: "USER_ENTERED",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: toAppend },
    });
  }
}

function filterByMode(events, mode) {
  if (mode === "prefill") {
    return events.filter((e) => e?.status?.type?.name === "STATUS_SCHEDULED");
  }
  if (mode === "live") {
    return events.filter((e) => e?.status?.type?.name === "STATUS_IN_PROGRESS");
  }
  if (mode === "finals") {
    return events.filter((e) => e?.status?.type?.name === "STATUS_FINAL");
  }
  // week = all
  return events;
}

// -------------------- Main --------------------
(async function main() {
  const league = argv.league;
  const tab = TAB_BY_LEAGUE[league];

  if (!tab) {
    console.error(`Unknown league: ${league}`);
    process.exit(1);
  }

  await ensureHeaderRow(tab);
  const existingMap = await readAllRows(tab);

  const collected = [];

  for (const yyyymmdd of dateIterator(argv.mode, argv.span, argv.date)) {
    const events = await fetchDay(league, yyyymmdd);
    const filtered = filterByMode(events, argv.mode);
    for (const e of filtered) {
      collected.push(buildRow(e));
    }
  }

  if (collected.length === 0) {
    console.log(JSON.stringify({ ok: true, league, tab, touched: 0 }));
    return;
  }

  await batchWriteUpserts(tab, existingMap, collected);
  console.log(JSON.stringify({ ok: true, league, tab, touched: collected.length }));
})().catch((err) => {
  console.error("Basketball.mjs error:", err?.response?.data || err?.message || err);
  process.exit(1);
});
