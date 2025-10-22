// Football.mjs
// One script: Prefill + Finals (+ optional halftime live snapshot)
//
// Env (from Football.yml):
//   GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT (json string or base64)
//   LEAGUE  = nfl | college-football
//   TAB_NAME = NFL | CFB
//   GAME_IDS = optional comma list
//   MODE = prefill_and_finals [default] | prefill_only | finals_only | live_only
//   RUN_SCOPE = auto [default] | today | week
//
// Columns (locked):
//   A: Game ID | B: Date | C: Week | D: Status | E: Matchup | F: Final Score
//   G: A Spread | H: A ML | I: H Spread | J: H ML | K: Total
//   L: H Score | M: Half A Spread | N: Half A ML | O: Half H Spread | P: Half H ML | Q: Half Total

import { google } from "googleapis";

// ---------- Config ----------
const SHEET_ID = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
const LEAGUE = normLeague(process.env.LEAGUE || "nfl");
const TAB_NAME = (process.env.TAB_NAME || (LEAGUE==="nfl"?"NFL":"CFB")).trim();
const GAME_IDS = (process.env.GAME_IDS || "").trim();
const MODE = (process.env.MODE || "prefill_and_finals").toLowerCase();
const RUN_SCOPE = (process.env.RUN_SCOPE || "auto").toLowerCase();

const ET_TZ = "America/New_York";
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",
  "A Spread","A ML","H Spread","H ML","Total",
  "H Score","Half A Spread","Half A ML","Half H Spread","Half H ML","Half Total"
];

// ---------- Small utils ----------
const log=(...a)=>console.log(...a);
const warn=(...a)=>console.warn(...a);

function normLeague(x){
  return /college-football|ncaaf/i.test(x) ? "college-football" : "nfl";
}
function fmtET(d, opts){ return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ, ...opts}).format(new Date(d)); }
function statusPregame(d){ return `${fmtET(d,{month:"2-digit",day:"2-digit"})} - ${fmtET(d,{hour:"numeric",minute:"2-digit",hour12:true})}`; }
function yyyymmddInET(d=new Date()){ const p=fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/"); return p[2]+p[0]+p[1]; }
async function fetchJson(u){ const r=await fetch(u,{headers:{'User-Agent':'halftime-bot'}}); if(!r.ok) throw new Error(r.status+" "+u); return r.json(); }
async function fetchText(u){ const r=await fetch(u,{headers:{'User-Agent':'halftime-bot'}}); if(!r.ok) throw new Error(r.status+" "+u); return r.text(); }

function scoreboardUrl(lg,dates){
  const extra = lg==="college-football" ? "&groups=80&limit=300" : "";
  return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`;
}
function summaryUrl(lg,id){ return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`; }
function gameUrl(lg,id){ return `https://www.espn.com/${lg}/game/_/gameId/${id}`; }

function isFinal(evt){ return /(FINAL)/i.test(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || ""); }
function isLive(evt){ return /(IN_PROGRESS|LIVE)/i.test(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || ""); }
function isHalf(evt){ const s=(evt?.status?.type?.shortDetail||"").toUpperCase(); return /HALF/.test(s) || /(Q2).*(0:0?0)/.test(s); }

function mapHeadersToIndex(h){ const m={}; h.forEach((v,i)=>m[(v||"").trim().toLowerCase()]=i); return m; }
function colLetter(i){ return String.fromCharCode(65+i); }

// odds helpers
function pickOdds(arr=[]){
  if(!arr?.length) return null;
  return arr.find(o=>/espn\s*bet/i.test(o.provider?.name||o.provider?.displayName||"")) || arr[0];
}
function resolveFavAndLine(o, awayId, homeId){
  let aSpread="", hSpread="", total = o?.overUnder ?? o?.total ?? "";
  let favId = String(o?.favoriteTeamId ?? o?.favorite ?? "");

  // numeric spread
  const raw = typeof o?.spread==="string" ? parseFloat(o.spread) : (Number.isFinite(o?.spread)?o.spread:NaN);
  if(!Number.isNaN(raw) && favId){
    if(String(awayId)===favId){ aSpread = -Math.abs(raw); hSpread = +Math.abs(raw); }
    else if(String(homeId)===favId){ hSpread = -Math.abs(raw); aSpread = +Math.abs(raw); }
  }else if(o?.details){
    const m=o.details.match(/([+-]?\d+(\.\d+)?)/); if(m){
      const n=parseFloat(m[1]);
      if(n<0){ hSpread=n; aSpread=Math.abs(n); }
      else { aSpread=n; hSpread=-Math.abs(n); }
    }
  }
  return {aSpread:String(aSpread||""), hSpread:String(hSpread||""), total:String(total||""), favId};
}
function resolveMLs(o, awayId, homeId){
  const s=v=>v==null? "": String(v);
  if(o?.awayTeamOdds||o?.homeTeamOdds){
    return {aML:s(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline), hML:s(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline)};
  }
  if(o?.moneyline){
    return {aML:s(o.moneyline?.away?.close?.odds ?? o.moneyline?.away?.open?.odds),
            hML:s(o.moneyline?.home?.close?.odds ?? o.moneyline?.home?.open?.odds)};
  }
  if(Array.isArray(o?.teamOdds)){
    let a="",h="";
    for(const t of o.teamOdds){
      const tid=String(t?.teamId ?? t?.team?.id ?? "");
      const ml=t?.moneyLine ?? t?.moneyline;
      if(ml==null) continue;
      if(tid===String(awayId)) a=String(ml);
      if(tid===String(homeId)) h=String(ml);
    }
    return {aML:a,hML:h};
  }
  const favId=String(o?.favoriteTeamId ?? o?.favorite ?? "");
  const fav=o?.favoriteMoneyLine ?? o?.favoriteMl;
  const dog=o?.underdogMoneyLine ?? o?.dogMl;
  if(favId){
    if(String(awayId)===favId) return {aML:String(fav??""), hML:String(dog??"")};
    if(String(homeId)===favId)  return {hML:String(fav??""), aML:String(dog??"")};
  }
  return {aML:"",hML:""};
}

// matchup formatting
function buildRuns(matchup, underlineAway, boldAway, boldHome){
  const at = matchup.indexOf(" @ ");
  const L = matchup.length;
  if(at<0) return [{startIndex:0,format:{bold:false,underline:false}}];
  const A0=0, H0=at+3;
  const runs=[
    {startIndex:0, format:{bold:false, underline:false}},
    {startIndex:A0, format:{bold:!!boldAway, underline:!!underlineAway}},
    {startIndex:H0, format:{bold:!!boldHome, underline:!underlineAway}},
  ];
  // clamp to valid ascending indices
  return runs
    .map(r=>({...r, startIndex: Math.min(Math.max(r.startIndex,0), Math.max(L-1,0))}))
    .sort((x,y)=>x.startIndex-y.startIndex);
}

// halftime lightweight scrape
async function halftimeSnapshot(gameId){
  try{
    const t=(await fetchText(gameUrl(LEAGUE, gameId))).replace(/\u00a0/g," ").replace(/\s+/g," ");
    const i=t.toUpperCase().indexOf("LIVE ODDS"); if(i<0) return null;
    const sec=t.slice(i, i+2000);

    const spreads=[...sec.matchAll(/([+-]\d+(\.\d+)?)/g)].map(m=>m[1]);
    const liveAwaySpread = spreads?.[2] ?? spreads?.[0] ?? "";
    const liveHomeSpread = spreads?.[3] ?? spreads?.[1] ?? "";

    const o = sec.match(/o\s?(\d+(\.\d+)?)/i)?.[1] || "";
    const u = sec.match(/u\s?(\d+(\.\d+)?)/i)?.[1] || "";
    const liveTotal = o || u || "";

    const mls=[...sec.matchAll(/\s[+-]\d{2,4}\b/g)].map(m=>m[0].trim());
    const liveAwayML = mls?.[0] || "";
    const liveHomeML = mls?.[1] || "";

    const score = t.match(/\b(\d{1,2})\s*-\s*(\d{1,2})\b/);
    const hScore = score ? `${score[1]}-${score[2]}` : "";

    return {liveAwaySpread, liveHomeSpread, liveTotal, liveAwayML, liveHomeML, hScore};
  }catch(e){ warn("halftimeSnapshot:", e.message); return null; }
}

// Sheets helper
class Sheets {
  constructor(auth, spreadsheetId, tab){ this.api=google.sheets({version:"v4",auth}); this.sid=spreadsheetId; this.tab=tab; }
  async ensureHeader(){
    const r=await this.api.spreadsheets.values.get({spreadsheetId:this.sid, range:`${this.tab}!A1:Z`});
    const v=r.data.values||[];
    if(!v[0]?.length){
      await this.api.spreadsheets.values.update({spreadsheetId:this.sid, range:`${this.tab}!A1`, valueInputOption:"RAW", requestBody:{values:[HEADERS]}});
      return HEADERS.slice();
    }
    return v[0];
  }
  async readAll(){ const r=await this.api.spreadsheets.values.get({spreadsheetId:this.sid, range:`${this.tab}!A1:Z`}); return r.data.values||[]; }
  async append(vals){ if(vals.length) await this.api.spreadsheets.values.append({spreadsheetId:this.sid, range:`${this.tab}!A1`, valueInputOption:"RAW", requestBody:{values:vals}}); }
  async batchWrite(data){ if(data.length) await this.api.spreadsheets.values.batchUpdate({spreadsheetId:this.sid, requestBody:{valueInputOption:"RAW", data}}); }
  async sheetId(){ const m=await this.api.spreadsheets.get({spreadsheetId:this.sid}); return (m.data.sheets||[]).find(s=>s.properties?.title===this.tab)?.properties?.sheetId; }
  async updateTextRuns(row, colIdx, text, runs){
    const sheetId=await this.sheetId();
    await this.api.spreadsheets.batchUpdate({
      spreadsheetId:this.sid,
      requestBody:{requests:[{updateCells:{range:{sheetId, startRowIndex:row-1,endRowIndex:row,startColumnIndex:colIdx,endColumnIndex:colIdx+1}, rows:[{values:[{userEnteredValue:{stringValue:text}, textFormatRuns:runs}]}], fields:"userEnteredValue,textFormatRuns"}}]}
    });
  }
  async addGradingCFIfMissing(){
    const sid = await this.sheetId();
    const reqs=[];
    const F=colLetter(5), G=colLetter(6), I=colLetter(8), K=colLetter(10);
    const row=2;
    const green={red:0.85,green:0.95,blue:0.85}, red={red:0.98,green:0.85,blue:0.85};

    const add=(range,formula,bg)=>reqs.push({
      addConditionalFormatRule:{
        rule:{ranges:[{sheetId:sid, startRowIndex:row-1}], booleanRule:{condition:{type:"CUSTOM_FORMULA", values:[{userEnteredValue:formula}]}, format:{backgroundColor:bg}}},
        index:0
      }
    });

    // Away spread hit: away - home + A_spread >= 0
    add(`${this.tab}!${G}${row}:${G}`, `=AND($${F}${row}<>"", VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) - VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) + VALUE($${G}${row}) >= 0)`, green);
    add(`${this.tab}!${G}${row}:${G}`, `=AND($${F}${row}<>"", VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) - VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) + VALUE($${G}${row}) < 0)`, red);
    // Home spread hit: home - away + H_spread >= 0
    add(`${this.tab}!${I}${row}:${I}`, `=AND($${F}${row}<>"", VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) - VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) + VALUE($${I}${row}) >= 0)`, green);
    add(`${this.tab}!${I}${row}:${I}`, `=AND($${F}${row}<>"", VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) - VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) + VALUE($${I}${row}) < 0)`, red);
    // Total: green if points > total (Over). You may invert if you prefer to grade by “posted total” vs result.
    add(`${this.tab}!${K}${row}:${K}`, `=AND($${F}${row}<>"", VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) + VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) > VALUE($${K}${row}))`, green);
    add(`${this.tab}!${K}${row}:${K}`, `=AND($${F}${row}<>"", VALUE(LEFT($${F}${row},FIND("-",$${F}${row})-1)) + VALUE(MID($${F}${row},FIND("-",$${F}${row})+1,99)) <= VALUE($${K}${row}))`, red);

    if(reqs.length){
      try{ await this.api.spreadsheets.batchUpdate({spreadsheetId:this.sid, requestBody:{requests:reqs}}); }catch(_){}
    }
  }
}

// ---------- Main ----------
(async () => {
  if(!SHEET_ID||!CREDS_RAW){ console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT"); process.exit(1); }
  const creds = CREDS_RAW.trim().startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({credentials:{client_email:creds.client_email, private_key:creds.private_key}, scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sh = new Sheets(auth, SHEET_ID, TAB_NAME);

  const header = await sh.ensureHeader();
  const hmap = mapHeadersToIndex(header);
  const all = await sh.readAll();
  const rows = all.slice(1);
  const rowById = new Map();
  rows.forEach((r,i)=>{ const id=(r[hmap["game id"]]||"").toString().trim(); if(id) rowById.set(id, i+2); });

  // dates list
  const today = yyyymmddInET();
  const dates = RUN_SCOPE==="today" ? [today]
              : RUN_SCOPE==="week"  ? Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)))
                                    : Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)));

  // fetch scoreboard
  let events=[]; for(const d of dates){ const sb=await fetchJson(scoreboardUrl(LEAGUE,d)); events.push(...(sb?.events||[])); }

  // filter optional GAME_IDS
  const onlyIds = GAME_IDS ? GAME_IDS.split(",").map(s=>s.trim()) : null;
  if(onlyIds?.length) events = events.filter(e=>onlyIds.includes(String(e.id)));

  log("Events found:", events.length);

  // ---------- PREGAME ----------
  if(MODE==="prefill_only" || MODE==="prefill_and_finals"){
    const toAppend=[];
    const toUpdate=[];
    for(const ev of events){
      const comp=ev.competitions?.[0]||{};
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const awayName=away?.team?.shortDisplayName || away?.team?.abbreviation || "Away";
      const homeName=home?.team?.shortDisplayName || home?.team?.abbreviation || "Home";
      const matchup = `${awayName} @ ${homeName}`;
      const preStatus = statusPregame(ev.date);

      const rowNum = rowById.get(String(ev.id));
      const odds = pickOdds(comp.odds || ev.odds || []);
      let aSpread="",hSpread="",total="",aML="",hML="", favAway=false;

      if(odds){
        const {aSpread:as, hSpread:hs, total:t, favId} = resolveFavAndLine(odds, away?.team?.id, home?.team?.id);
        const mls = resolveMLs(odds, away?.team?.id, home?.team?.id);
        aSpread=as; hSpread=hs; total=t; aML=mls.aML; hML=mls.hML;
        favAway = favId && String(favId)===String(away?.team?.id);
      }

      if(!rowNum){
        // add new row
        toAppend.push([
          String(ev.id), fmtET(ev.date,{year:"numeric",month:"2-digit",day:"2-digit"}), comp.week?.text || ("Week "+(comp.week?.number??"")), preStatus,
          matchup, "", aSpread, aML, hSpread, hML, total, "", "", "", "", "", ""
        ]);
      }else{
        // refresh pregame only if not live/final yet
        const existing = all[rowNum-1]||[];
        const curStatus=(existing[hmap["status"]]||"").toString();
        if(curStatus && (/(Final|Q\d|Half|End of)/i.test(curStatus))) continue;

        const payload=[];
        const set=(name,val)=>{ const idx=hmap[name.toLowerCase()]; if(idx==null) return; payload.push({range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val??""]]}); };

        set("status", preStatus);
        set("matchup", matchup);
        set("a spread", aSpread); set("h spread", hSpread);
        set("a ml", aML); set("h ml", hML); set("total", total);

        if(payload.length) toUpdate.push(...payload);

        // underline favorite now
        try {
          const runs = buildRuns(matchup, favAway, false, false);
          await sh.updateTextRuns(rowNum, hmap["matchup"], matchup, runs);
        } catch(e){ /* ignore formatting fail */ }
      }
    }
    if(toAppend.length) await sh.append(toAppend);
    if(toUpdate.length) await sh.batchWrite(toUpdate);
  }

  // ---------- FINALS ----------
  if(MODE==="finals_only" || MODE==="prefill_and_finals"){
    const batch=[];
    for(const ev of events){
      if(!isFinal(ev)) continue;
      const comp=ev.competitions?.[0]||{};
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
      const finalScore = `${away?.score??""}-${home?.score??""}`;
      const rowNum=rowById.get(String(ev.id)); if(!rowNum) continue;

      const set=(name,val)=>{ const idx=hmap[name.toLowerCase()]; if(idx==null) return; batch.push({range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val??""]]}); };
      set("final score", finalScore);
      set("status","Final");

      // bold winner + keep underline fav based on original odds (best-effort, pick current pickcenter)
      let favAway=false;
      const odds=pickOdds(comp.odds||ev.odds||[]);
      if(odds){
        const {favId}=resolveFavAndLine(odds, away?.team?.id, home?.team?.id);
        favAway = favId && String(favId)===String(away?.team?.id);
      }
      const aPts=Number(away?.score||0), hPts=Number(home?.score||0);
      const boldAway = aPts>hPts, boldHome = hPts>aPts;
      try {
        const runs=buildRuns(matchup, favAway, boldAway, boldHome);
        await sh.updateTextRuns(rowNum, hmap["matchup"], matchup, runs);
      }catch(e){}

    }
    if(batch.length) await sh.batchWrite(batch);
    await sh.addGradingCFIfMissing();
  }

  // ---------- LIVE (halftime snapshot) ----------
  if(MODE==="live_only"){
    const batch=[];
    for(const ev of events){
      if(!isHalf(ev)) continue;
      const rowNum=rowById.get(String(ev.id)); if(!rowNum) continue;

      const snap=await halftimeSnapshot(String(ev.id)); if(!snap) continue;
      const put=(name,val)=>{ const idx=hmap[name.toLowerCase()]; if(idx==null || !val) return; batch.push({range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val]]}); };
      put("h score", snap.hScore);
      put("half a spread", snap.liveAwaySpread);
      put("half h spread", snap.liveHomeSpread);
      put("half a ml", snap.liveAwayML);
      put("half h ml", snap.liveHomeML);
      put("half total", snap.liveTotal);
    }
    if(batch.length) await sh.batchWrite(batch);
  }

  log("Done.");
})().catch(e=>{ console.error("Fatal:", e); process.exit(1); });
