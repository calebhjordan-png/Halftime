import { google } from "googleapis";

/** ========== CONFIG ========== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();            // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL")).trim();
const PREFILL_MODE = (process.env.PREFILL_MODE || "week").toLowerCase();  // default to week

if (!SHEET_ID || !CREDS_RAW) {
  console.error("❌ Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/** ========== HELPERS ========== */
function parseSA(raw) {
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const ET_TZ = "America/New_York";

function fmtETDate(dLike) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "numeric", day: "numeric"
  }).format(new Date(dLike));
}
function fmtETKickNoSuffix(dLike) {
  // e.g., "1:00 PM" (no "ET")
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, hour: "numeric", minute: "2-digit", hour12: true
  }).format(new Date(dLike));
}
function yyyymmddET(dateLike) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(new Date(dateLike));
  const g = k => parts.find(p => p.type === k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
}
function addDaysET(base, days) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: ET_TZ, year: "numeric", month: "2-digit", day: "2-digit" })
    .formatToParts(new Date(base));
  const y=+parts.find(p=>p.type==="year").value, m=+parts.find(p=>p.type==="month").value, d=+parts.find(p=>p.type==="day").value;
  const etMid = new Date(Date.UTC(y, m-1, d, 5)); // ~ET midnight
  return new Date(etMid.getTime() + days*86400000);
}
async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "orchestrator/3.6", "Referer": "https://www.espn.com/" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return r.json();
}

const normLeague = l => (l === "ncaaf" || l === "college-football") ? "college-football" : "nfl";
const scoreboardUrl = (l, d) => {
  const lg = normLeague(l);
  const extra = lg === "college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${d}${extra}`;
};
const summaryUrl = (l, id) =>
  `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(l)}/summary?event=${id}`;

const pickOdds = arr =>
  Array.isArray(arr) && arr.length
    ? (arr.find(o => /espn\s*bet/i.test(o.provider?.name || "") ||
      /espn\s*bet/i.test(o.provider?.displayName || "")) || arr[0])
    : null;

const numOrBlank = v => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  const n = parseFloat(s.replace(/[^\d.+-]/g,""));
  return Number.isFinite(n) ? (s.startsWith("+")?`+${n}`:`${n}`) : "";
};

function assignSpreads(odds, away, home) {
  if (!odds) return { awaySpread:"", homeSpread:"", favSide:null };
  const raw = parseFloat(odds.spread ?? odds.details?.match(/([+-]?\d+(\.\d+)?)/)?.[1] ?? NaN);
  if (Number.isNaN(raw)) return { awaySpread:"", homeSpread:"", favSide:null };
  const mag = Math.abs(raw);
  const fav = String(odds.favorite || odds.favoriteTeamId || "");
  const awayId = String(away?.team?.id || "");
  const homeId = String(home?.team?.id || "");
  if (fav === awayId) return { awaySpread:`-${mag}`, homeSpread:`+${mag}`, favSide:"away" };
  if (fav === homeId) return { awaySpread:`+${mag}`, homeSpread:`-${mag}`, favSide:"home" };
  const det=(odds.details||"").toLowerCase();
  const aName=(away?.team?.abbreviation||away?.team?.shortDisplayName||"").toLowerCase();
  const hName=(home?.team?.abbreviation||home?.team?.shortDisplayName||"").toLowerCase();
  if (aName && det.includes(aName)) return { awaySpread:`-${mag}`, homeSpread:`+${mag}`, favSide:"away" };
  if (hName && det.includes(hName)) return { awaySpread:`+${mag}`, homeSpread:`-${mag}`, favSide:"home" };
  return { awaySpread:"", homeSpread:"", favSide:null };
}

const colMap = (hdr = []) =>
  Object.fromEntries(hdr.map((h, i) => [(h || "").trim().toLowerCase(), i]));

/** Map date->"Week N" using each date's own scoreboard week. */
function buildDateToWeekMap(boards) {
  const map = new Map();
  for (const { d, j } of boards) {
    const n = j?.week?.number;
    if (Number.isFinite(n)) map.set(d, `Week ${n}`);
    else {
      // fall back to calendar label if present
      const label = j?.leagues?.[0]?.calendar?.[0]?.label || j?.week?.text || "";
      const m = label && label.match(/week\s*\d+/i);
      map.set(d, m ? m[0].replace(/^\w/, ch => ch.toUpperCase()) : "Week ?");
    }
  }
  return map;
}

/** Rich text underline helper for favorite in Matchup */
function richTextUnderlineForMatchup(matchup, favSide, awayName, homeName) {
  const text = matchup;
  if (!favSide) return { text, runs: [] };
  const favName = favSide === "away" ? (awayName || "") : (homeName || "");
  const start = text.indexOf(favName);
  if (start < 0) return { text, runs: [] };
  const end = start + favName.length;
  return { text, runs: [{ startIndex: start, format: { underline: true } }, { startIndex: end }] };
}

function a1FromColRow(colIndex, rowIndex) {
  // colIndex 0 => A, 1 => B, ...
  const n = colIndex + 1;
  let s = "", x = n;
  while (x > 0) { const m = (x - 1) % 26; s = String.fromCharCode(65 + m) + s; x = Math.floor((x - 1) / 26); }
  return `${s}${rowIndex}`;
}

/** ========== MAIN ========== */
(async () => {
  const CREDS = parseSA(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const sheets = google.sheets({ version: "v4", auth });

  // Get sheet meta to find sheetId (for rich text writes)
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = (meta.data.sheets || []).find(s => s.properties?.title === TAB_NAME);
  if (!sheet) {
    console.error(`❌ Sheet tab "${TAB_NAME}" not found`);
    process.exit(1);
  }
  const sheetId = sheet.properties.sheetId;

  // Read current values / header
  const read = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const vals = read.data.values || [];
  let header = vals[0] || [];
  if (!header.length) {
    header = [
      "Game ID","Date","Week","Status","Matchup","Final Score",
      "Away Spread","Away ML","Home Spread","Home ML","Total",
      "Half Score","Live Away Spread","Live Away ML","Live Home Spread","Live Home ML","Live Total"
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [header] }
    });
  }
  const H = colMap(header);
  const idx = {
    gid: H["game id"] ?? H["id"],
    date: H["date"],
    week: H["week"],
    status: H["status"],
    matchup: H["matchup"],
    final: H["final score"],
    awaySpread: H["away spread"],
    awayML: H["away ml"],
    homeSpread: H["home spread"],
    homeML: H["home ml"],
    total: H["total"]
  };

  /* ---------------- Prefill + Refresh ---------------- */
  if (PREFILL_MODE === "week" || PREFILL_MODE === "today") {
    const dates =
      PREFILL_MODE === "week"
        ? Array.from({ length: 7 }, (_, i) => yyyymmddET(addDaysET(Date.now(), i)))
        : [yyyymmddET(new Date())];

    const boards = (await Promise.allSettled(
      dates.map(d => fetchJson(scoreboardUrl(LEAGUE, d)).then(j => ({ d, j })))
    )).filter(r => r.status === "fulfilled").map(r => r.value);

    const dateToWeek = buildDateToWeekMap(boards);

    // Build de-duped event list
    const seen = new Set();
    const events = [];
    for (const { d, j } of boards) {
      for (const e of j?.events || []) {
        if (!seen.has(e.id)) {
          seen.add(e.id);
          events.push({ e, dKey: d });
        }
      }
    }

    // Map existing occupied rows & existing IDs
    const occupiedRow = new Set();            // rows whose Game ID is non-empty
    const idToRow = new Map();
    vals.slice(1).forEach((r, i) => {
      const rowIdx = i + 2;
      const gid = (r[idx.gid] || "").trim();
      if (gid) {
        occupiedRow.add(rowIdx);
        idToRow.set(gid, rowIdx);
      }
    });

    // Helper to find next free row index (no gaps)
    let cursor = 2;
    function nextFreeRow() {
      while (occupiedRow.has(cursor)) cursor++;
      const out = cursor;
      occupiedRow.add(out);
      cursor++;
      return out;
    }

    // Collect writes
    const valueWrites = [];       // {range, values}
    const richRuns = [];          // rich text requests for matchup underline

    for (const { e, dKey } of events) {
      const comp = e.competitions?.[0] || {};
      const away = comp.competitors?.find(c => c.homeAway === "away");
      const home = comp.competitors?.find(c => c.homeAway === "home");

      const awayName = away?.team?.shortDisplayName || "Away";
      const homeName = home?.team?.shortDisplayName || "Home";
      const matchup = `${awayName} @ ${homeName}`;

      // Odds (scoreboard first)
      let odds = pickOdds(comp.odds || e.odds || null);
      let { awaySpread, homeSpread, favSide } = assignSpreads(odds, away, home);
      let total  = odds?.overUnder ?? odds?.total ?? "";
      let awayML = numOrBlank(odds?.awayTeamOdds?.moneyLine ?? odds?.awayTeamOdds?.moneyline);
      let homeML = numOrBlank(odds?.homeTeamOdds?.moneyLine ?? odds?.homeTeamOdds?.moneyline);

      // Always backfill MLs (and spreads/total if missing) via /summary when missing
      if (!awayML || !homeML || (!awaySpread && !homeSpread) || !total) {
        try {
          const sum = await fetchJson(summaryUrl(LEAGUE, e.id));
          const scomp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
          const sOdds = pickOdds(scomp.odds || sum?.odds || null);
          if (sOdds) {
            if (!awaySpread || !homeSpread) {
              const sSp = assignSpreads(sOdds, away, home);
              awaySpread = awaySpread || sSp.awaySpread;
              homeSpread = homeSpread || sSp.homeSpread;
              favSide = favSide || sSp.favSide;
            }
            total  = total  || sOdds.overUnder || sOdds.total || "";
            awayML = awayML || numOrBlank(sOdds?.awayTeamOdds?.moneyLine ?? sOdds?.awayTeamOdds?.moneyline);
            homeML = homeML || numOrBlank(sOdds?.homeTeamOdds?.moneyLine ?? sOdds?.homeTeamOdds?.moneyline);
          }
        } catch { /* ignore */ }
      }

      const weekLbl = dateToWeek.get(dKey) || "Week ?";
      const status  = fmtETKickNoSuffix(e.date); // no "ET" suffix
      const date    = fmtETDate(e.date);
      const gid     = String(e.id);

      const existingRow = idToRow.get(gid);
      if (!existingRow) {
        // place sequentially in first free row
        const row = nextFreeRow();
        // write raw values
        const rowVals = [];
        rowVals[idx.gid]        = gid;
        rowVals[idx.date]       = date;
        rowVals[idx.week]       = weekLbl;
        rowVals[idx.status]     = status;
        rowVals[idx.matchup]    = matchup;
        rowVals[idx.final]      = "";
        rowVals[idx.awaySpread] = awaySpread;
        rowVals[idx.awayML]     = awayML;
        rowVals[idx.homeSpread] = homeSpread;
        rowVals[idx.homeML]     = homeML;
        rowVals[idx.total]      = String(total || "");

        valueWrites.push({
          range: `${TAB_NAME}!A${row}:${TAB_NAME}!Q${row}`,
          values: [rowVals]
        });

        // underline favorite in matchup (rich text)
        const { text, runs } = richTextUnderlineForMatchup(
          matchup, favSide, awayName, homeName
        );
        if (runs.length) {
          richRuns.push({
            updateCells: {
              rows: [{
                values: [{
                  userEnteredValue: { stringValue: text },
                  textFormatRuns: runs
                }]
              }],
              fields: "userEnteredValue,textFormatRuns",
              range: {
                sheetId,
                startRowIndex: row - 1,
                endRowIndex: row,
                startColumnIndex: idx.matchup,
                endColumnIndex: idx.matchup + 1
              }
            }
          });
        }
      } else {
        // refresh odds/time/week on existing row
        const updates = [];
        const put = (cIndex, val) => {
          if (cIndex == null || val == null || val === "") return;
          updates.push({ range: `${TAB_NAME}!${a1FromColRow(cIndex, existingRow)}`, values: [[val]] });
        };
        put(idx.awaySpread, awaySpread);
        put(idx.homeSpread, homeSpread);
        put(idx.awayML,     awayML);
        put(idx.homeML,     homeML);
        put(idx.total,      String(total || ""));
        put(idx.status,     status);
        put(idx.week,       weekLbl);
        if (updates.length) valueWrites.push(...updates);

        // update favorite underline on matchup
        const { text, runs } = richTextUnderlineForMatchup(
          matchup, favSide, awayName, homeName
        );
        if (runs.length) {
          richRuns.push({
            updateCells: {
              rows: [{
                values: [{
                  userEnteredValue: { stringValue: text },
                  textFormatRuns: runs
                }]
              }],
              fields: "userEnteredValue,textFormatRuns",
              range: {
                sheetId,
                startRowIndex: existingRow - 1,
                endRowIndex: existingRow,
                startColumnIndex: idx.matchup,
                endColumnIndex: idx.matchup + 1
              }
            }
          });
        }
      }
    }

    // Flush raw values first
    if (valueWrites.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: valueWrites }
      });
    }
    // Then apply rich text underlines
    if (richRuns.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { requests: richRuns }
      });
    }

    console.log(`✅ Prefill/refresh done. Wrote ${valueWrites.length} range(s), styled ${richRuns.length} cell(s).`);
  }

  /* ---------------- Finals sweep ---------------- */
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const rows2 = (snap.data.values || []).slice(1);
  const cands = [];
  rows2.forEach((r,i)=>{
    const gid=(r[idx.gid]||"").trim();
    const fin=(r[idx.final]||"").trim();
    if (gid && !fin) cands.push({ row:i+2, gid });
  });
  console.log(`Finals sweep: ${cands.length} candidate row(s) with blank Final Score.`);

  for (const c of cands) {
    try {
      const sum = await fetchJson(summaryUrl(LEAGUE, c.gid));
      const comp = sum?.header?.competitions?.[0] || sum?.competitions?.[0] || {};
      const away = comp.competitors?.find(x=>x.homeAway==="away");
      const home = comp.competitors?.find(x=>x.homeAway==="home");
      const st   = comp?.status?.type || {};
      const isFinal = /FINAL/i.test(st.name || "") || (st.state === "post");
      if (!isFinal) continue;

      const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: {
          valueInputOption: "RAW",
          data: [
            { range: `${TAB_NAME}!${a1FromColRow(idx.final, c.row)}`, values: [[score]] },
            ...(idx.status != null ? [{ range: `${TAB_NAME}!${a1FromColRow(idx.status, c.row)}`, values: [["Final"]] }] : [])
          ]
        }
      });
    } catch (e) {
      console.error(`Finals update failed for ${c.gid}:`, e.message);
    }
  }

  console.log("✅ Finals sweep complete.");
})().catch(e => { console.error("❌ Orchestrator fatal:", e); process.exit(1); });
