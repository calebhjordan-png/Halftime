import { google } from "googleapis";

/* ========================= ENV ========================= */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();           // nfl | college-football
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "nfl" ? "NFL":"CFB")).trim();
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();       // today|week
const TARGET_GAME_ID = (process.env.TARGET_GAME_ID || "").trim();
const GHA_JSON  = (process.argv.includes("--gha") || process.env.GHA_JSON === "1");

if (!SHEET_ID || !CREDS_RAW) {
  const msg = "Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT";
  if (GHA_JSON) { console.log(JSON.stringify({ ok:false, error:msg })); process.exit(0); }
  throw new Error(msg);
}

/* ========================= UTILS ========================= */
function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}
const CREDS = parseServiceAccount(CREDS_RAW);

const auth = new google.auth.GoogleAuth({
  credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

const ET = "America/New_York";
const fmtTimeET = (iso) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET, hour:"numeric", minute:"2-digit" }).format(new Date(iso));
const fmtDateET = (iso) =>
  new Intl.DateTimeFormat("en-US", { timeZone: ET, year:"numeric", month:"2-digit", day:"2-digit" }).format(new Date(iso));

const yyyymmddET = (d=new Date()) => {
  const p = new Intl.DateTimeFormat("en-US",{timeZone:ET,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const g = k => p.find(x=>x.type===k)?.value || "";
  return `${g("year")}${g("month")}${g("day")}`;
};

const log = (...a)=> (GHA_JSON ? process.stderr.write(a.join(" ")+"\n") : console.log(...a));

const normLeague = (x)=> (x==="ncaaf"||x==="college-football") ? "college-football" : "nfl";
const SB_URL = (lg,dates)=>{
  lg = normLeague(lg);
  const extra = lg==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
};
const SUM_URL = (lg,eventId)=> `https://site.api.espn.com/apis/site/v2/sports/football/${normLeague(lg)}/summary?event=${eventId}`;
async function fetchJson(url) {
  log("GET", url);
  const r = await fetch(url, { headers: { "User-Agent":"halftime-orchestrator" } });
  if (!r.ok) throw new Error(`Fetch failed ${r.status}`);
  return r.json();
}

/* ============ SHEET helpers ============ */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","Half A Spread","Half A ML","Half H Spread","Half H ML","Half Total"
];

const colIndex = (headerRow) => {
  const map = {}; headerRow.forEach((h,i)=> map[(h||"").toString().trim().toLowerCase()] = i);
  return map;
};
const A1 = (row, col) => `${TAB_NAME}!${String.fromCharCode(65+col)}${row}:${String.fromCharCode(65+col)}${row}`;

/* ============ Odds helpers ============ */
function pickOdds(arr=[]) {
  if (!Array.isArray(arr)||!arr.length) return null;
  const espnbet = arr.find(o=>/espn\s*bet/i.test(o.provider?.name||"") || /espn bet/i.test(o.provider?.displayName||""));
  return espnbet || arr[0];
}
const nstr = (v)=> v==null ? "" : String(v);
const num = (v)=> {
  if (v==null) return "";
  const t = String(v).trim();
  const m = parseFloat(t.replace(/[^\d.+-]/g,""));
  if (!Number.isFinite(m)) return "";
  return t.startsWith("+") ? `+${m}` : `${m}`;
};

function extractPregameLines(event) {
  const comp = event.competitions?.[0] || {};
  const away = comp.competitors?.find(c=>c.homeAway==="away");
  const home = comp.competitors?.find(c=>c.homeAway==="home");
  const o = pickOdds(comp.odds||event.odds||[]);
  let aSpread="", hSpread="", total="", aML="", hML="", favoriteId="";

  if (o) {
    favoriteId = String(o.favorite || o.favoriteTeamId || "");
    total = nstr(o.overUnder ?? o.total);
    const spread = (typeof o.spread==="string") ? parseFloat(o.spread) : (Number.isFinite(o.spread)?o.spread:NaN);
    if (!Number.isNaN(spread) && favoriteId) {
      if (String(away?.team?.id)===favoriteId) { aSpread = `-${Math.abs(spread)}`; hSpread = `+${Math.abs(spread)}`; }
      else if (String(home?.team?.id)===favoriteId) { hSpread = `-${Math.abs(spread)}`; aSpread = `+${Math.abs(spread)}`; }
    } else if (o.details) {
      const m = o.details.match(/([+-]?\d+(\.\d+)?)/);
      if (m) {
        const line = parseFloat(m[1]);
        aSpread = line>0 ? `+${Math.abs(line)}` : `${line}`;
        hSpread = line>0 ? `-${Math.abs(line)}` : `+${Math.abs(line)}`;
      }
    }
    // many shapes for ML
    const aMl = num(o?.awayTeamOdds?.moneyLine ?? o?.moneyline?.away?.close?.odds ?? o?.moneyline?.away?.open?.odds);
    const hMl = num(o?.homeTeamOdds?.moneyLine ?? o?.moneyline?.home?.close?.odds ?? o?.moneyline?.home?.open?.odds);
    aML = aML || aMl; hML = hML || hMl;
  }

  return { aSpread, hSpread, total:nstr(total), aML, hML, favoriteId };
}

/* ============ Status helpers ============ */
function isLiveLike(evt) {
  const n = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return n.includes("IN_PROGRESS") || n.includes("LIVE") || n.includes("HALFTIME");
}
function isFinal(evt) {
  const n = (evt.status?.type?.name || evt.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return n.includes("FINAL");
}
function scheduledStatusText(evt) {
  return `${fmtDateET(evt.date)} - ${fmtTimeET(evt.date)}`;
}

/* ============ Matchup text format runs ============ */
function buildMatchupTextFormatRuns(text, awayLen, homeLen, underlineFavorite) {
  // text = "Away @ Home"
  // underlineFavorite in {"away","home", ""}

  // Google Sheets TextFormatRuns must start inside string length and be ordered.
  // We write: whole clear, then underline the favorite segment.
  const runs = [
    { startIndex: 0, format: { underline: false, bold: false } },
  ];
  // locate indices safely
  const atIdx = text.indexOf(" @ ");
  const a0 = 0, a1 = Math.max(0, Math.min(text.length, awayLen));
  const h0 = (atIdx >= 0 ? atIdx + 3 : awayLen + 3);
  const h1 = Math.max(h0, Math.min(text.length, h0 + homeLen));

  if (underlineFavorite === "away") {
    runs.push({ startIndex: a0, format: { underline: true, bold: false } });
    runs.push({ startIndex: a1, format: { underline: false, bold: false } });
  } else if (underlineFavorite === "home") {
    runs.push({ startIndex: h0, format: { underline: true, bold: false } });
    runs.push({ startIndex: h1, format: { underline: false, bold: false } });
  }
  return runs;
}

function buildBoldWinnerRuns(text, awayLen, homeLen, awayScore, homeScore) {
  const atIdx = text.indexOf(" @ ");
  const a0 = 0, a1 = Math.max(0, Math.min(text.length, awayLen));
  const h0 = (atIdx >= 0 ? atIdx + 3 : awayLen + 3);
  const h1 = Math.max(h0, Math.min(text.length, h0 + homeLen));
  const runs = [{ startIndex: 0, format: { underline: false, bold: false } }];
  const awayWin = (Number(awayScore)||0) > (Number(homeScore)||0);
  const homeWin = (Number(homeScore)||0) > (Number(awayScore)||0);
  if (awayWin) { runs.push({ startIndex:a0, format:{ underline:false, bold:true } }); runs.push({ startIndex:a1, format:{ underline:false, bold:false } }); }
  if (homeWin) { runs.push({ startIndex:h0, format:{ underline:false, bold:true } }); runs.push({ startIndex:h1, format:{ underline:false, bold:false } }); }
  return runs;
}

/* ============ MAIN ============ */
(async function main() {
  // ensure tab + header
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  let sheetId = meta.data.sheets?.find(s=>s.properties?.title===TAB_NAME)?.properties?.sheetId;
  if (sheetId == null) {
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
    sheetId = add.data.replies?.[0]?.addSheet?.properties?.sheetId;
  }
  const snap = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const values = snap.data.values || [];
  let header = values[0] || [];
  if (!header.length) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [HEADERS] }
    });
    header = HEADERS.slice();
  }
  const h = colIndex(header);
  const rows = values.slice(1);
  const keyToRow = new Map();  // by Game ID
  rows.forEach((r,i)=> { const gid = (r[h["game id"]]||"").toString().trim(); if (gid) keyToRow.set(gid, i+2); });

  // dates to fetch
  const dates = [];
  const today = new Date();
  if (RUN_SCOPE === "today") dates.push(yyyymmddET(today));
  else {
    for (let i=0;i<7;i++) dates.push(yyyymmddET(new Date(today.getTime()+i*86400000)));
  }

  // ESPN data
  let events = [];
  for (const d of dates) {
    const sb = await fetchJson(SB_URL(LEAGUE, d));
    if (Array.isArray(sb?.events)) events.push(...sb.events);
  }
  const forced = (TARGET_GAME_ID ? TARGET_GAME_ID.split(",").map(s=>s.trim()).filter(Boolean) : []);
  if (forced.length) {
    for (const id of forced) {
      try {
        const sum = await fetchJson(SUM_URL(LEAGUE, id));
        if (sum?.header?.competitions?.[0]) {
          const evt = sum.header.competitions[0];
          events.push({ id, competitions:[evt], status: evt.status, date: evt.date });
        }
      } catch {}
    }
  }
  // de-dupe
  const seen = new Set();
  events = events.filter(e=>!seen.has(e.id) && seen.add(e.id));
  log(`Events found: ${events.length}`);

  /* ---- Batch collections ---- */
  const valueWrites = [];
  const cellRequests = []; // for textFormatRuns (underline/bold)

  // process each event
  for (const ev of events) {
    const comp = ev.competitions?.[0] || {};
    const away = comp.competitors?.find(c=>c.homeAway==="away");
    const home = comp.competitors?.find(c=>c.homeAway==="home");

    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const aLen = awayName.length, hLen = homeName.length;

    const gid = String(ev.id);
    let row = keyToRow.get(gid);

    // pregame lines + favorite
    const { aSpread, hSpread, total, aML, hML, favoriteId } = extractPregameLines(ev);
    const favSide = (favoriteId && String(away?.team?.id)===favoriteId) ? "away" :
                    (favoriteId && String(home?.team?.id)===favoriteId) ? "home" : "";

    const scheduled = scheduledStatusText(ev);
    const isLive = isLiveLike(ev);
    const isFin  = isFinal(ev);

    // add row if missing
    if (!row) {
      // append at the next available row (no extra blanks)
      const newRow = [
        gid,
        fmtDateET(ev.date),
        (ev.season?.type?.name || comp.season?.type?.name || comp.week?.text || ""),
        scheduled,                  // initial status (no EDT)
        matchup,
        "",                         // Final Score
        aSpread, aML, hSpread, hML, nstr(total),
        "", "", "", "", "", ""      // half columns (left blank)
      ];
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [newRow] }
      });
      // refresh row index for this id
      const snap2 = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:A` });
      const vals2 = (snap2.data.values||[]).slice(1).map(v=> (v[0]||"").toString().trim());
      const idx = vals2.findIndex(v=>v===gid);
      row = (idx >= 0 ? (idx+2) : undefined);
      if (row) keyToRow.set(gid, row);

      // underline favorite (pregame)
      if (row && favSide) {
        cellRequests.push({
          updateCells: {
            range: { sheetId, startRowIndex: row-1, endRowIndex: row, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
            rows: [{
              values: [{
                userEnteredValue: { stringValue: matchup },
                textFormatRuns: buildMatchupTextFormatRuns(matchup, aLen, hLen, favSide)
              }]
            }],
            fields: "userEnteredValue,textFormatRuns"
          }
        });
      }
      continue;
    }

    // existing row: update PRE-GAME lines if not live yet; lock after kickoff
    const existing = values[row-1] || [];

    // Status rules: never overwrite to a live status; keep scheduled;
    // write Final at the end.
    if (isFin) {
      const finalScore = `${away?.score ?? ""}-${home?.score ?? ""}`;
      if (h["final score"] !== undefined) valueWrites.push({ range: A1(row, h["final score"]), values:[[finalScore]] });
      if (h["status"] !== undefined)      valueWrites.push({ range: A1(row, h["status"]),      values:[["Final"]] });

      // bold winner in matchup
      const text = matchup;
      cellRequests.push({
        updateCells: {
          range: { sheetId, startRowIndex: row-1, endRowIndex: row, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
          rows: [{
            values: [{
              userEnteredValue: { stringValue: text },
              textFormatRuns: buildBoldWinnerRuns(text, aLen, hLen, away?.score, home?.score)
            }]
          }],
          fields: "userEnteredValue,textFormatRuns"
        }
      });
    } else {
      // Pre-kickoff only: update scheduled text + lines; underline favorite
      if (!isLive) {
        if (h["status"] !== undefined) valueWrites.push({ range: A1(row, h["status"]), values:[[scheduled]] });

        // Update lines if changed
        const W = (name, val) => { if (val !== "" && h[name] !== undefined) valueWrites.push({ range: A1(row, h[name]), values:[[val]] }); };
        W("a spread", aSpread); W("a ml", aML); W("h spread", hSpread); W("h ml", hML); W("total", nstr(total));

        if (favSide) {
          cellRequests.push({
            updateCells: {
              range: { sheetId, startRowIndex: row-1, endRowIndex: row, startColumnIndex: h["matchup"], endColumnIndex: h["matchup"]+1 },
              rows: [{
                values: [{
                  userEnteredValue: { stringValue: matchup },
                  textFormatRuns: buildMatchupTextFormatRuns(matchup, aLen, hLen, favSide)
                }]
              }],
              fields: "userEnteredValue,textFormatRuns"
            }
          });
        }
      }
      // if live, do nothing (live-game.mjs owns it)
    }
  }

  if (valueWrites.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: valueWrites }
    });
  }
  if (cellRequests.length) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: cellRequests }
    });
  }

  log("Run complete.");
  if (GHA_JSON) console.log(JSON.stringify({ ok:true, league:normLeague(LEAGUE), tab:TAB_NAME }));
})().catch(err=>{
  if (GHA_JSON) console.log(JSON.stringify({ ok:false, error:String(err?.message||err) }));
  else { console.error(err); process.exit(1); }
});
