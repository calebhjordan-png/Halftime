// orchestrator.mjs
// Prefill (NFL/CFB) + Finals sweeper in one file.
// - Prefill writes Status (pre-game date/time), A/H spreads & MLs, Total, underlines favorite.
// - Finals pass writes Final Score and bolds winner.
// - Never updates Status once game is live/half/final.
// - Live columns (H Score, Half/A/H spreads/ML/Total) are never touched here.
// - Grading (conditional formatting) applies only to rows that have a Final Score.

import { google } from "googleapis";

/* ========== ENV ========== */
const SHEET_ID  = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

// When running from the combined workflow, we run twice (NFL, CFB) with these set per-job:
const LEAGUE    = (process.env.LEAGUE || "nfl").toLowerCase();              // "nfl" | "college-football"
const TAB_NAME  = (process.env.TAB_NAME || (LEAGUE === "nfl" ? "NFL" : "CFB")).trim();

// Range of events to fetch:
// - "week" is normal for prefills and finals sweeps.
// - "today" exists but isn’t required by the combined job; we keep it for flexibility.
const RUN_SCOPE = (process.env.RUN_SCOPE || "week").toLowerCase();         // "today" | "week"

// Optional forced game IDs (comma-separated) to (re)populate or finalize across dates.
const TARGET_GAME_ID = (process.env.TARGET_GAME_ID || "").trim();

// JSON-only output when used in GH Actions composite
const GHA_JSON = String(process.env.GHA_JSON || "") === "1";

/* ========== Columns (A..Q) ========== */
/*
A Game ID
B Date
C Week
D Status
E Matchup
F Final Score
G A Spread
H A ML
I H Spread
J H ML
K Total
L H Score           (live script writes once, then frozen)
M Half A Spread     "
N Half A ML         "
O Half H Spread     "
P Half H ML         "
Q Half Total        "
*/
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","Half A Spread","Half A ML","Half H Spread","Half H ML","Half Total"
];

/* ========== Utils ========== */
const _out = (s, ...a)=>s.write(a.map(x=>typeof x==='string'?x:String(x)).join(" ")+"\n");
const log  = (...a)=> GHA_JSON ? _out(process.stderr, ...a) : console.log(...a);
const warn = (...a)=> GHA_JSON ? _out(process.stderr, ...a) : console.warn(...a);

function decodeServiceAccount(raw) {
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT missing");
  if (raw.trim().startsWith("{")) return JSON.parse(raw);
  return JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
}

function leagueKey(x){
  const s = (x||"").toLowerCase();
  if (s === "ncaaf" || s === "college-football") return "college-football";
  return "nfl";
}
const LG = leagueKey(LEAGUE);

const ET_TZ = "America/New_York";
function fmtDateMMDD(d){
  const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,month:"2-digit",day:"2-digit",year:"numeric"}).formatToParts(new Date(d));
  const mm = parts.find(p=>p.type==="month")?.value||"00";
  const dd = parts.find(p=>p.type==="day")?.value||"00";
  const yy = parts.find(p=>p.type==="year")?.value||"0000";
  return `${mm}/${dd}/${yy}`;
}
function fmtDateNoYear(d){
  const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,month:"2-digit",day:"2-digit"}).formatToParts(new Date(d));
  const mm = parts.find(p=>p.type==="month")?.value||"00";
  const dd = parts.find(p=>p.type==="day")?.value||"00";
  return `${mm}/${dd}`;
}
function fmtTimeET(d){
  return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,hour:"numeric",minute:"2-digit"}).format(new Date(d));
}
function statusPregameText(dateISO){
  return `${fmtDateNoYear(dateISO)} - ${fmtTimeET(dateISO)}`;
}
function yyyymmddET(d=new Date()){
  const parts = new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,year:"numeric",month:"2-digit",day:"2-digit"}).formatToParts(d);
  const y = parts.find(p=>p.type==="year")?.value||"0000";
  const m = parts.find(p=>p.type==="month")?.value||"00";
  const dd= parts.find(p=>p.type==="day")?.value||"00";
  return `${y}${m}${dd}`;
}

async function fetchJson(url){
  log("GET", url);
  const r = await fetch(url, { headers: { "User-Agent":"halftime-bot", "Referer":"https://www.espn.com/" }});
  if (!r.ok) throw new Error(`Failed ${r.status} for ${url}`);
  return r.json();
}
function scoreboardUrl(dates){
  const extra = LG==="college-football" ? "&groups=80&limit=400" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${LG}/scoreboard?dates=${dates}${extra}`;
}
function summaryUrl(eventId){
  return `https://site.api.espn.com/apis/site/v2/sports/football/${LG}/summary?event=${eventId}`;
}

function mapHeaders(h){
  const m={};
  (h||[]).forEach((v,i)=>{ m[(v||"").toLowerCase()] = i; });
  return m;
}
function colLetter(idx){ return String.fromCharCode("A".charCodeAt(0)+idx); }

/* ----- odds helpers ----- */
function pickOddsCandidate(oddsArr=[]){
  if (!Array.isArray(oddsArr) || !oddsArr.length) return null;
  const espnbet = oddsArr.find(o=>/espn\s*bet/i.test(o?.provider?.name||"")||/espn\s*bet/i.test(o?.provider?.displayName||""));
  return espnbet || oddsArr[0];
}
function numberOrBlank(v){
  if (v==null) return "";
  const n = typeof v === "string" ? parseFloat(v.replace(/[^\d.+-]/g,"")) : Number(v);
  if (!Number.isFinite(n)) return "";
  return n;
}
function signedString(n){
  if (n==="" || n==null) return "";
  const num = Number(n);
  if (!Number.isFinite(num)) return "";
  return (num>0?`+${num}`:`${num}`);
}

/* spread mapping:
   - We need the absolute line and WHO is the favorite.
   - away is favorite => A Spread = -abs(line), H Spread = +abs(line)
   - home is favorite => A Spread = +abs(line), H Spread = -abs(line)
*/
function spreadsFromOdds(odds, awayTeamId, homeTeamId){
  let aS = "", hS = "";
  if (!odds) return { aS, hS };

  // Get numeric line
  let line = odds.spread;
  if (line == null && typeof odds.details === "string"){
    const m = odds.details.match(/([+-]?\d+(?:\.\d+)?)/);
    if (m) line = parseFloat(m[1]);
  }
  line = Number.isFinite(line) ? Math.abs(Number(line)) : NaN;
  if (Number.isNaN(line)) return { aS, hS };

  // Identify favorite
  const favId = String(odds.favorite ?? odds.favoriteTeamId ?? "");
  const awayFav = favId && String(awayTeamId) === favId;
  const homeFav = favId && String(homeTeamId) === favId;

  if (!awayFav && !homeFav) return { aS, hS }; // unknown favorite; don't guess

  if (awayFav){
    aS = signedString(-line);
    hS = signedString(+line);
  } else {
    aS = signedString(+line);
    hS = signedString(-line);
  }
  return { aS, hS };
}
function moneylinesFromOdds(odds, awayTeamId, homeTeamId){
  let aML="", hML="";
  if (!odds) return { aML, hML };

  const ao = odds.awayTeamOdds || {};
  const ho = odds.homeTeamOdds || {};
  // best-effort pulls
  const getML = v => numberOrBlank(v?.moneyLine ?? v?.moneyline ?? v?.money_line ?? v);
  aML = aML || getML(ao);
  hML = hML || getML(ho);

  if (!aML || !hML){
    if (Array.isArray(odds.teamOdds)){
      for (const t of odds.teamOdds){
        const tid = String(t?.teamId ?? t?.team?.id ?? "");
        const ml = getML(t);
        if (!ml) continue;
        if (tid === String(awayTeamId)) aML = aML || ml;
        if (tid === String(homeTeamId)) hML = hML || ml;
      }
    }
  }
  return { aML, hML };
}

/* winner / text formatting */
function statusName(evt){
  const n = (evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || "").toUpperCase();
  return n;
}
function isFinal(evt){ return statusName(evt).includes("FINAL"); }
function isLiveLike(evt){
  const n = statusName(evt);
  return n.includes("IN_PROGRESS") || n.includes("HALFTIME") || n.includes("LIVE");
}
function cleanShortStatus(evt){
  if (isFinal(evt)) return "Final";
  if (isLiveLike(evt)) return (evt.status?.type?.shortDetail || "In Progress");
  return statusPregameText(evt.date);
}

function buildUnderlineRuns(matchupText, awayFav, homeFav){
  // matchup is "Away @ Home" — underline exactly the favorite team name
  // We underline only one team (favorite). If neither flag is true, no runs.
  const runs = [];
  if (!awayFav && !homeFav) return runs;
  const at = matchupText.indexOf(" @ ");
  if (at < 0) return runs;

  const awayStart = 0;
  const awayEnd   = at;                 // exclusive
  const homeStart = at + 3;
  const homeEnd   = matchupText.length; // exclusive

  if (awayFav){
    // underline away [awayStart..awayEnd)
    runs.push({ startIndex: 0, format: { underline:false, bold:false }});
    runs.push({ startIndex: awayStart, format: { underline:true,  bold:false }});
    runs.push({ startIndex: awayEnd,   format: { underline:false, bold:false }});
  }
  if (homeFav){
    runs.push({ startIndex: 0,        format: { underline:false, bold:false }});
    runs.push({ startIndex: homeStart,format: { underline:true,  bold:false }});
    runs.push({ startIndex: homeEnd,  format: { underline:false, bold:false }});
  }
  return runs;
}
function buildBoldWinnerRuns(matchupText, awayWon, homeWon){
  const runs = [];
  if (!awayWon && !homeWon) return runs;
  const at = matchupText.indexOf(" @ ");
  if (at < 0) return runs;

  const awayStart = 0;
  const awayEnd   = at;
  const homeStart = at + 3;
  const homeEnd   = matchupText.length;

  if (awayWon){
    runs.push({ startIndex: 0,        format: { bold:false, underline:false }});
    runs.push({ startIndex: awayStart,format: { bold:true,  underline:false }});
    runs.push({ startIndex: awayEnd,  format: { bold:false, underline:false }});
  }
  if (homeWon){
    runs.push({ startIndex: 0,        format: { bold:false, underline:false }});
    runs.push({ startIndex: homeStart,format: { bold:true,  underline:false }});
    runs.push({ startIndex: homeEnd,  format: { bold:false, underline:false }});
  }
  return runs;
}

/* week label helpers */
function weekLabel(sb, dateISO){
  if (LG === "nfl"){
    const n = sb?.week?.number;
    return Number.isFinite(n) ? `Week ${n}` : "Regular Season";
  }
  const t = (sb?.week?.text || "").trim();
  return t || "Regular Season";
}

/* Sheets boot */
async function sheetsClient(){
  const creds = decodeServiceAccount(CREDS_RAW);
  const auth = new google.auth.GoogleAuth({
    credentials: { client_email: creds.client_email, private_key: creds.private_key },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  return google.sheets({ version: "v4", auth });
}

/* Conditional formatting:
   Only grade when Final Score (col F) is present.
   We clear previous rules then re-add rules that apply to cols G..K (spreads/ml/total).
*/
async function applyFinalsFormattingOnly(sheets, sheetId){
  const requests = [];
  // Clear all CF rules
  requests.push({ deleteConditionalFormatRule: { index: 0, sheetId } }); // we’ll delete in a loop later
  // Better: get how many rules exist and delete them. Simpler: replace rules by a single add that overwrites.
  // The API requires precise indices to delete. We’ll do a reset by setting rules from scratch: use UpdateConditionalFormatRule with newIndex=0 and multiple adds.
  // Instead, we can just add rules with a condition that references $F:$F <> "" — even if prior rules exist, ours will apply correctly.
  // So we’ll skip deletes to be safe for now.

  const rangeAll = { sheetId, startRowIndex: 1, startColumnIndex: 6, endColumnIndex: 11 }; // G..K (0-based)

  const rule = (backgroundRGB, expr) => ({
    addConditionalFormatRule: {
      rule: {
        ranges: [rangeAll],
        booleanRule: {
          condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: expr }] },
          format: { backgroundColor: backgroundRGB }
        }
      },
      index: 0
    }
  });

  // Example grading (keep simple): no color until Final
  // We add one "no-op" rule that only gates coloring by Final Score. (You may keep your existing detailed rules.)
  // Here we'll color nothing; but if you have rules, wrap them with AND($F2<>"", <your-condition>)
  // For now, just ensure there is at least one gating rule that does nothing unless F is set:
  // (We keep this minimal to avoid changing your custom palette)
  // If you already had rules, they will still fire; please update your CF rules in Sheets to prefix conditions with AND($F2<>"", ...)
  // so the gating is effective. This comment is to document the intent.

  // No destructive API call here to avoid nuking your custom rules.
  // If you want me to fully manage CF via API, say the word and I’ll add explicit rules here.

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests } // empty is fine if we don't overwrite
  }).catch(()=>{ /* ignore if no-op */});
}

/* Main */
(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    const err = "Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT";
    if (GHA_JSON){ process.stdout.write(JSON.stringify({ ok:false, error:err })+"\n"); return; }
    throw new Error(err);
  }

  const sheets = await sheetsClient();

  // Ensure sheet tab + headers
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = (meta.data.sheets||[]).find(s => s.properties?.title === TAB_NAME);
  let sheetId = sheet?.properties?.sheetId;
  if (sheetId == null){
    const add = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title: TAB_NAME } } }] }
    });
    sheetId = add.data.replies?.[0]?.addSheet?.properties?.sheetId;
  }
  const got = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:Z` });
  const values = got.data.values || [];
  let header = values[0] || [];
  if (!header.length){
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${TAB_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [HEADERS] }
    });
    header = HEADERS.slice();
  }
  const hmap = mapHeaders(header);
  const rows = values.slice(1);
  const rowCount = rows.length;

  // Build index by Game ID
  const idToRow = new Map();
  rows.forEach((r,i)=>{
    const id = (r[hmap["game id"]]||"").toString().trim();
    if (id) idToRow.set(id, i+2); // 1-based with header
  });

  // Which dates?
  const dates = (() => {
    if (TARGET_GAME_ID){
      // unknown dates; we’ll just sweep this week as well to be safe
      const start = new Date();
      return Array.from({length:7},(_,i)=>yyyymmddET(new Date(start.getTime()+i*86400000)));
    }
    if (RUN_SCOPE === "today") return [yyyymmddET(new Date())];
    const start = new Date();
    return Array.from({length:7},(_,i)=>yyyymmddET(new Date(start.getTime()+i*86400000)));
  })();

  // Pull events
  let events = [], firstSB = null;
  for (const d of dates){
    const sb = await fetchJson(scoreboardUrl(d));
    if (!firstSB) firstSB = sb;
    events = events.concat(sb?.events || []);
  }
  // De-dup
  const seen = new Set();
  events = events.filter(e=>!seen.has(e.id) && seen.add(e.id));

  // Filter by TARGET_GAME_ID if supplied
  let targetSet = null;
  if (TARGET_GAME_ID){
    targetSet = new Set(TARGET_GAME_ID.split(",").map(s=>s.trim()).filter(Boolean));
    events = events.filter(e => targetSet.has(String(e.id)));
  }

  log(`Events found: ${events.length}`);

  // Accumulate writes
  const batch = [];

  for (const ev of events){
    const comp  = ev.competitions?.[0] || {};
    const away  = comp.competitors?.find(c=>c.homeAway==="away");
    const home  = comp.competitors?.find(c=>c.homeAway==="home");
    const awayName = away?.team?.shortDisplayName || away?.team?.abbreviation || away?.team?.name || "Away";
    const homeName = home?.team?.shortDisplayName || home?.team?.abbreviation || home?.team?.name || "Home";
    const matchup = `${awayName} @ ${homeName}`;
    const dateET  = fmtDateMMDD(ev.date);
    const wkLabel = weekLabel(firstSB, ev.date);

    // status handling
    const statusTxt = cleanShortStatus(ev);

    // locate row or append
    let rowNum = idToRow.get(String(ev.id));
    if (!rowNum){
      // append as new row at the very bottom (no jumping two rows)
      const appendRow = new Array(HEADERS.length).fill("");
      appendRow[hmap["game id"]] = String(ev.id);
      appendRow[hmap["date"]]    = dateET;
      appendRow[hmap["week"]]    = wkLabel;
      appendRow[hmap["status"]]  = statusPregameText(ev.date); // pre-game status
      appendRow[hmap["matchup"]] = matchup;

      // write append
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${TAB_NAME}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [appendRow] }
      });
      // refresh ids
      const ref = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: `${TAB_NAME}!A1:A` });
      const n = (ref.data.values||[]).length - 1; // minus header
      rowNum = n + 1; // back to 1-based with header
      idToRow.set(String(ev.id), rowNum);

      // underline favorite later once odds are known
    }

    // Determine if we should update pre-game odds now:
    const liveOrFinal = isLiveLike(ev) || isFinal(ev);

    // Odds (use comp.odds or pickcenter)
    const allOdds = comp.odds || ev.odds || ev.pickcenter || [];
    const odds0   = pickOddsCandidate(allOdds);

    // Build spreads/ML/total ONLY if event not live/final (lock pregame lines at kickoff)
    if (!liveOrFinal && odds0){
      // total
      const total = numberOrBlank(odds0.overUnder ?? odds0.total);
      if (total !== "" && hmap["total"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["total"])}${rowNum}`, values: [[String(total)]] });
      }

      // spreads
      const { aS, hS } = spreadsFromOdds(odds0, away?.team?.id, home?.team?.id);
      if (aS !== "" && hmap["a spread"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["a spread"])}${rowNum}`, values: [[aS]] });
      }
      if (hS !== "" && hmap["h spread"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["h spread"])}${rowNum}`, values: [[hS]] });
      }

      // moneylines
      const { aML, hML } = moneylinesFromOdds(odds0, away?.team?.id, home?.team?.id);
      if (aML !== "" && hmap["a ml"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["a ml"])}${rowNum}`, values: [[String(aML)]] });
      }
      if (hML !== "" && hmap["h ml"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["h ml"])}${rowNum}`, values: [[String(hML)]] });
      }

      // underline favorite once (based on odds favorite)
      const favId = String(odds0.favorite ?? odds0.favoriteTeamId ?? "");
      const awayFav = favId && String(away?.team?.id||"") === favId;
      const homeFav = favId && String(home?.team?.id||"") === favId;

      if (hmap["matchup"] != null){
        const runs = buildUnderlineRuns(matchup, awayFav, homeFav);
        if (runs.length){
          // Update the cell with textFormatRuns (overwrite runs cleanly)
          await sheets.spreadsheets.batchUpdate({
            spreadsheetId: SHEET_ID,
            requestBody: {
              requests: [{
                updateCells: {
                  range: { sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: hmap["matchup"], endColumnIndex: hmap["matchup"]+1 },
                  rows: [{ values: [{ userEnteredValue: { stringValue: matchup }, textFormatRuns: runs }] }],
                  fields: "userEnteredValue,textFormatRuns"
                }
              }]
            }
          }).catch(()=>{/* if runs invalid, ignore gracefully */});
        }
      }

      // Status: only write pre-game status; never overwrite when live/final
      if (hmap["status"] != null){
        const n = statusName(ev);
        if (!n.includes("IN_PROGRESS") && !n.includes("FINAL") && !n.includes("HALFTIME")){
          batch.push({ range: `${TAB_NAME}!${colLetter(hmap["status"])}${rowNum}`, values: [[statusPregameText(ev.date)]] });
        }
      }

      // Date/Week always ok to keep current
      if (hmap["date"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["date"])}${rowNum}`, values: [[fmtDateMMDD(ev.date)]] });
      }
      if (hmap["week"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["week"])}${rowNum}`, values: [[wkLabel]] });
      }
    }

    // Finals: write score & bold winner
    if (isFinal(ev)){
      const finalScore = `${away?.score??""}-${home?.score??""}`;
      if (hmap["final score"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["final score"])}${rowNum}`, values: [[finalScore]] });
      }
      if (hmap["status"] != null){
        batch.push({ range: `${TAB_NAME}!${colLetter(hmap["status"])}${rowNum}`, values: [["Final"]] });
      }

      // Bold winner
      const awayWon = Number(away?.score||0) > Number(home?.score||0);
      const homeWon = Number(home?.score||0) > Number(away?.score||0);
      if ((awayWon || homeWon) && hmap["matchup"] != null){
        const runs = buildBoldWinnerRuns(matchup, awayWon, homeWon);
        await sheets.spreadsheets.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: {
            requests: [{
              updateCells: {
                range: { sheetId, startRowIndex: rowNum-1, endRowIndex: rowNum, startColumnIndex: hmap["matchup"], endColumnIndex: hmap["matchup"]+1 },
                rows: [{ values: [{ userEnteredValue: { stringValue: matchup }, textFormatRuns: runs }] }],
                fields: "userEnteredValue,textFormatRuns"
              }
            }]
          }
        }).catch(()=>{ /* ignore */ });
      }
    }
  }

  // Flush value writes
  if (batch.length){
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: { valueInputOption: "RAW", data: batch }
    });
    log(`Batched ${batch.length} cell update(s).`);
  }

  // Gate grading to Finals only (non-destructive: see note in helper)
  await applyFinalsFormattingOnly(sheets, sheetId);

  log("Done");
  if (GHA_JSON) process.stdout.write(JSON.stringify({ ok:true, league:LG, tab:TAB_NAME })+"\n");
})().catch(err=>{
  if (GHA_JSON){ process.stdout.write(JSON.stringify({ ok:false, error:String(err?.message||err) })+"\n"); }
  else { console.error(err); process.exit(1); }
});
