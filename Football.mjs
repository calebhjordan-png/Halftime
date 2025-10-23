import { google } from "googleapis";

/* ========= ENV ========= */
const SHEET_ID = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();

const LEAGUE = normLeague(process.env.LEAGUE || "nfl");               // nfl | college-football
const TAB_NAME = (process.env.TAB_NAME || (LEAGUE==="nfl"?"NFL":"CFB")).trim();
const GAME_IDS = (process.env.GAME_IDS || "").trim();                  // "4017...,4017..."
const MODE = (process.env.MODE || "prefill_and_finals").toLowerCase();// prefill_and_finals | prefill_only | finals_only | live_only
const RUN_SCOPE = (process.env.RUN_SCOPE || "auto").toLowerCase();     // auto|today|week
const REWRITE = String(process.env.REWRITE ?? "1") !== "0";

const ET_TZ = "America/New_York";

/* Headers (A..Q) */
const HEADERS = [
  "Game ID","Date","Week","Status","Matchup","Final Score",   // A..F
  "A Spread","A ML","H Spread","H ML","Total",                 // G..K (graded)
  "H Score","Half A Spread","Half A ML","Half H Spread","Half H ML","Half Total" // L..Q
];

/* ========= helpers ========= */
const log=(...a)=>console.log(...a);
const warn=(...a)=>console.warn(...a);

function normLeague(x){ return /college-football|ncaaf/i.test(x) ? "college-football" : "nfl"; }
function fmtET(d,opts){ return new Intl.DateTimeFormat("en-US",{timeZone:ET_TZ,...opts}).format(new Date(d)); }
function statusPregame(d){ return `${fmtET(d,{month:"2-digit",day:"2-digit"})} - ${fmtET(d,{hour:"numeric",minute:"2-digit",hour12:true})}`; }
function yyyymmddInET(d=new Date()){ const p=fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}).split("/"); return p[2]+p[0]+p[1]; }
function dateOnlyISO(d){ return fmtET(d,{year:"numeric",month:"2-digit",day:"2-digit"}); }

async function fetchJson(url){ const r=await fetch(url,{headers:{'User-Agent':'football-bot'}}); if(!r.ok) throw new Error(`${r.status} ${url}`); return r.json(); }
async function fetchText(url){ const r=await fetch(url,{headers:{'User-Agent':'football-bot'}}); if(!r.ok) throw new Error(`${r.status} ${url}`); return r.text(); }

function scoreboardUrl(lg,dates){ const extra = lg==="college-football" ? "&groups=80&limit=300" : ""; return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/scoreboard?dates=${dates}${extra}`; }
function summaryUrl(lg,id){ return `https://site.api.espn.com/apis/site/v2/sports/football/${lg}/summary?event=${id}`; }
function gameUrl(lg,id){ return `https://www.espn.com/${lg}/game/_/gameId/${id}`; }

function isFinalEvent(evt){ return /(FINAL)/i.test(evt?.status?.type?.name || evt?.competitions?.[0]?.status?.type?.name || ""); }
function isHalf(evt){ const s=(evt?.status?.type?.shortDetail||"").toUpperCase(); return /HALF/.test(s) || /(Q2).*(0:0?0)/.test(s); }

function mapHeadersToIndex(h){ const m={}; h.forEach((v,i)=>m[(v||"").trim().toLowerCase()]=i); return m; }
function colLetter(i){ return String.fromCharCode(65+i); }

function pickOdds(arr=[]){
  if(!arr?.length) return null;
  return arr.find(o=>/espn\s*bet/i.test(o.provider?.name||o.provider?.displayName||"")) || arr[0];
}

function resolveFavAndLine(o, awayId, homeId){
  let aSpread="", hSpread="", total = o?.overUnder ?? o?.total ?? "";
  const favId = String(o?.favoriteTeamId ?? o?.favorite ?? "");
  const raw = typeof o?.spread==="string" ? parseFloat(o.spread) : (Number.isFinite(o?.spread)?o.spread:NaN);
  if(!Number.isNaN(raw) && favId){
    if(String(awayId)===favId){ aSpread = -Math.abs(raw); hSpread = +Math.abs(raw); }
    else if(String(homeId)===favId){ hSpread = -Math.abs(raw); aSpread = +Math.abs(raw); }
  } else if(o?.details){
    const m=o.details.match(/([+-]?\d+(\.\d+)?)/);
    if(m){ const n=parseFloat(m[1]); if(n<0){ hSpread=n; aSpread=Math.abs(n); } else { aSpread=n; hSpread=-Math.abs(n); } }
  }
  return {aSpread:String(aSpread||""), hSpread:String(hSpread||""), total:String(total||""), favId};
}

function resolveMLs(o, awayId, homeId){
  const s=v=>v==null? "": String(v);

  if(o?.awayTeamOdds||o?.homeTeamOdds){
    return {aML:s(o.awayTeamOdds?.moneyLine ?? o.awayTeamOdds?.moneyline),
            hML:s(o.homeTeamOdds?.moneyLine ?? o.homeTeamOdds?.moneyline)};
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

/* Fill gaps from summary (prefer ESPN BET) */
async function fillOddsFromSummaryIfMissing(ev, aSpread, hSpread, total, aML, hML){
  if(aML && hML && (aSpread || hSpread) && total) return {aSpread,hSpread,total,aML,hML,favAway:null,favHome:null};
  try{
    const sum = await fetchJson(summaryUrl(LEAGUE, ev.id));
    const pools=[];
    if(Array.isArray(sum?.header?.competitions?.[0]?.odds)) pools.push(...sum.header.competitions[0].odds);
    if(Array.isArray(sum?.odds)) pools.push(...sum.odds);
    if(Array.isArray(sum?.pickcenter)) pools.push(...sum.pickcenter);
    const chosen = pickOdds(pools);
    if(chosen){
      const ids = {
        away: ev.competitions?.[0]?.competitors?.find(c=>c.homeAway==="away")?.team?.id,
        home: ev.competitions?.[0]?.competitors?.find(c=>c.homeAway==="home")?.team?.id,
      };
      const lines = resolveFavAndLine(chosen, ids.away, ids.home);
      const mls   = resolveMLs(chosen, ids.away, ids.home);
      const favAway = lines.favId && String(lines.favId)===String(ids.away);
      const favHome = lines.favId && String(lines.favId)===String(ids.home);
      return {
        aSpread: aSpread || lines.aSpread,
        hSpread: hSpread || lines.hSpread,
        total:   total   || lines.total,
        aML:     aML     || mls.aML,
        hML:     hML     || mls.hML,
        favAway, favHome
      };
    }
  }catch(e){ /* ignore */ }
  return {aSpread,hSpread,total,aML,hML,favAway:null,favHome:null};
}

/* Safe text runs: underline favorite; later we bold winner */
function runsForMatchup(text, underlineAway=false, boldAway=false, boldHome=false){
  const at=text.indexOf(" @ ");
  const L=text.length;
  if(at<0) return [{startIndex:0,format:{bold:false,underline:false}}];
  const awayStart=0, homeStart=at+3;
  const cl=i=>Math.min(Math.max(i,0), Math.max(L-1,0));
  const awayRun={startIndex:cl(awayStart), format:{bold:!!boldAway, underline:!!underlineAway}};
  const homeRun={startIndex:cl(homeStart), format:{bold:!!boldHome, underline:!underlineAway}};
  return [awayRun, homeRun].sort((a,b)=>a.startIndex-b.startIndex);
}

/* Halftime (kept) */
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

/* ========= Sheets ========= */
class Sheets {
  constructor(auth, id, tab){ this.api=google.sheets({version:"v4",auth}); this.sid=id; this.tab=tab; }
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
      requestBody:{requests:[{updateCells:{range:{sheetId,startRowIndex:row-1,endRowIndex:row,startColumnIndex:colIdx,endColumnIndex:colIdx+1}, rows:[{values:[{userEnteredValue:{stringValue:text}, textFormatRuns:runs}]}], fields:"userEnteredValue,textFormatRuns"}}]}
    });
  }
  async resetConditionalFormatting(){
    const sid = await this.sheetId();
    const reqs=[]; for(let i=0;i<80;i++) reqs.push({deleteConditionalFormatRule:{index:0,sheetId:sid}});
    try { await this.api.spreadsheets.batchUpdate({spreadsheetId:this.sid, requestBody:{requests:reqs}}); } catch(_) {}
  }
  async ensureGradingCF(){
    await this.resetConditionalFormatting();
    const sid = await this.sheetId();
    const green={red:0.85,green:0.95,blue:0.85}, red={red:0.98,green:0.85,blue:0.85};
    const mk = (colIdx, passFormula, failFormula) => ([
      { addConditionalFormatRule:{ index:0, rule:{ ranges:[{sheetId:sid,startRowIndex:1,startColumnIndex:colIdx,endColumnIndex:colIdx+1}], booleanRule:{condition:{type:"CUSTOM_FORMULA", values:[{userEnteredValue:passFormula}]}, format:{backgroundColor:green}}}}},
      { addConditionalFormatRule:{ index:0, rule:{ ranges:[{sheetId:sid,startRowIndex:1,startColumnIndex:colIdx,endColumnIndex:colIdx+1}], booleanRule:{condition:{type:"CUSTOM_FORMULA", values:[{userEnteredValue:failFormula}]}, format:{backgroundColor:red}}}}}
    ]);

    const HAS_FINAL = '$F2<>""';
    const AWAY = 'VALUE(LEFT($F2,FIND("-",$F2)-1))';
    const HOME = 'VALUE(MID($F2,FIND("-",$F2)+1,99))';

    const gPass = `=AND(${HAS_FINAL}, ${AWAY} - ${HOME} + G2 >= 0)`;
    const gFail = `=AND(${HAS_FINAL}, ${AWAY} - ${HOME} + G2 < 0)`;

    const iPass = `=AND(${HAS_FINAL}, ${HOME} - ${AWAY} + I2 >= 0)`;
    const iFail = `=AND(${HAS_FINAL}, ${HOME} - ${AWAY} + I2 < 0)`;

    const hPass = `=AND(${HAS_FINAL}, ${AWAY} > ${HOME}, H2<>"")`;
    const hFail = `=AND(${HAS_FINAL}, ${AWAY} <= ${HOME}, H2<>"")`;

    const jPass = `=AND(${HAS_FINAL}, ${HOME} > ${AWAY}, J2<>"")`;
    const jFail = `=AND(${HAS_FINAL}, ${HOME} <= ${AWAY}, J2<>"")`;

    const kPass = `=AND(${HAS_FINAL}, ${AWAY}+${HOME} > K2)`;
    const kFail = `=AND(${HAS_FINAL}, ${AWAY}+${HOME} < K2)`;

    const reqs=[...mk(6,gPass,gFail), ...mk(7,hPass,hFail), ...mk(8,iPass,iFail), ...mk(9,jPass,jFail), ...mk(10,kPass,kFail)];
    try { await this.api.spreadsheets.batchUpdate({spreadsheetId:this.sid, requestBody:{requests:reqs}}); } catch(e){ warn("ensureGradingCF:", e.message); }
  }
  async clearNonGradedBackgrounds(rowCount){
    const sid = await this.sheetId();
    const reqs = [
      { repeatCell:{ range:{sheetId:sid,startRowIndex:1,endRowIndex:rowCount,startColumnIndex:0,endColumnIndex:6},  cell:{userEnteredFormat:{backgroundColor:null, backgroundColorStyle:null}}, fields:"userEnteredFormat.backgroundColor,userEnteredFormat.backgroundColorStyle" }},
      { repeatCell:{ range:{sheetId:sid,startRowIndex:1,endRowIndex:rowCount,startColumnIndex:11,endColumnIndex:17}, cell:{userEnteredFormat:{backgroundColor:null, backgroundColorStyle:null}}, fields:"userEnteredFormat.backgroundColor,userEnteredFormat.backgroundColorStyle" }},
    ];
    await this.api.spreadsheets.batchUpdate({spreadsheetId:this.sid, requestBody:{requests:reqs}});
  }
}

/* ========= MAIN ========= */
(async () => {
  if(!SHEET_ID||!CREDS_RAW){ console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT"); process.exit(1); }
  const creds = CREDS_RAW.trim().startsWith("{") ? JSON.parse(CREDS_RAW) : JSON.parse(Buffer.from(CREDS_RAW,"base64").toString("utf8"));
  const auth = new google.auth.GoogleAuth({credentials:{client_email:creds.client_email, private_key:creds.private_key}, scopes:["https://www.googleapis.com/auth/spreadsheets"]});
  const sh = new Sheets(auth, SHEET_ID, TAB_NAME);

  const header = await sh.ensureHeader();
  const hmap = mapHeadersToIndex(header);
  const all0 = await sh.readAll();
  const rows0 = all0.slice(1);

  const rowById = new Map();
  rows0.forEach((r,i)=>{ const id=(r[hmap["game id"]]||"").toString().trim(); if(id) rowById.set(id, i+2); });

  const dates = RUN_SCOPE==="today" ? [yyyymmddInET()]
              : RUN_SCOPE==="week"  ? Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)))
                                    : Array.from({length:7},(_,i)=>yyyymmddInET(new Date(Date.now()+i*86400000)));

  let events=[]; for(const d of dates){ const sb=await fetchJson(scoreboardUrl(LEAGUE,d)); events.push(...(sb?.events||[])); }
  const onlyIds = GAME_IDS ? GAME_IDS.split(",").map(s=>s.trim()) : null;
  if(onlyIds?.length) events = events.filter(e=>onlyIds.includes(String(e.id)));
  log("Events found:", events.length);

  /* ---------- PREFILL ---------- */
  if(MODE==="prefill_only" || MODE==="prefill_and_finals"){
    const toAppend=[], toUpdate=[];
    const prefillFormatQueue=[];

    for(const ev of events){
      const comp=ev.competitions?.[0]||{};
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const awayName=away?.team?.shortDisplayName || away?.team?.abbreviation || "Away";
      const homeName=home?.team?.shortDisplayName || home?.team?.abbreviation || "Home";
      const matchup = `${awayName} @ ${homeName}`;
      const preStatus = statusPregame(ev.date);
      const rowNum = rowById.get(String(ev.id));

      let odds = pickOdds(comp.odds || ev.odds || []);
      let aSpread="",hSpread="",total="",aML="",hML="", favAway=null, favHome=null;

      if(odds){
        const lines = resolveFavAndLine(odds, away?.team?.id, home?.team?.id);
        const mls   = resolveMLs(odds, away?.team?.id, home?.team?.id);
        aSpread=lines.aSpread; hSpread=lines.hSpread; total=lines.total; aML=mls.aML; hML=mls.hML;
        favAway = lines.favId && String(lines.favId)===String(away?.team?.id);
        favHome = lines.favId && String(lines.favId)===String(home?.team?.id);
      }
      ({aSpread,hSpread,total,aML,hML,favAway,favHome} = await fillOddsFromSummaryIfMissing(ev, aSpread,hSpread,total,aML,hML));

      if(!rowNum){
        toAppend.push([ String(ev.id), dateOnlyISO(ev.date), comp.week?.text || ("Week "+(comp.week?.number??"")), preStatus,
          matchup, "", aSpread, aML, hSpread, hML, total, "", "", "", "", "", "" ]);
      }else{
        const sheetRow = all0[rowNum-1]||[];
        const curStatus=(sheetRow[hmap["status"]]||"").toString();
        if(curStatus && (/(Final|Q\d|Half|End of)/i.test(curStatus))) {
          // don't overwrite live/final rows
        }else{
          const set=(name,val)=>{ const idx=hmap[name.toLowerCase()]; if(idx==null) return; toUpdate.push({range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val??""]]}); };
          set("status", preStatus);
          set("matchup", matchup);
          set("a spread", aSpread); set("h spread", hSpread);
          set("a ml", aML); set("h ml", hML); set("total", total);
        }
        // queue underline formatting (favorite only if known)
        if(favAway===true || favHome===true){
          prefillFormatQueue.push({rowNum, text:matchup, underlineAway:!!favAway});
        }
      }
    }

    if(toAppend.length) await sh.append(toAppend);

    // refresh index to capture newly appended rows for text formatting
    const allAfterAppend = await sh.readAll();
    const rowsAfterAppend = allAfterAppend.slice(1);
    const rowById2 = new Map();
    rowsAfterAppend.forEach((r,i)=>{ const id=(r[hmap["game id"]]||"").toString().trim(); if(id) rowById2.set(id, i+2); });

    // add underline tasks for newly appended rows
    if(toAppend.length){
      for(const ev of events){
        const rn = rowById2.get(String(ev.id));
        if(!rn) continue;
        const comp=ev.competitions?.[0]||{};
        const away=comp.competitors?.find(c=>c.homeAway==="away");
        const home=comp.competitors?.find(c=>c.homeAway==="home");
        const awayName=away?.team?.shortDisplayName || away?.team?.abbreviation || "Away";
        const homeName=home?.team?.shortDisplayName || home?.team?.abbreviation || "Home";
        const matchup = `${awayName} @ ${homeName}`;

        let favAway=null, favHome=null;
        let odds = pickOdds(comp.odds || ev.odds || []);
        if(odds){
          const lines = resolveFavAndLine(odds, away?.team?.id, home?.team?.id);
          favAway = lines.favId && String(lines.favId)===String(away?.team?.id);
          favHome = lines.favId && String(lines.favId)===String(home?.team?.id);
        }
        if(favAway===null && favHome===null){
          // one more try from summary
          const filled = await fillOddsFromSummaryIfMissing(ev,"","","","","");
          favAway = filled.favAway; favHome = filled.favHome;
        }
        if(favAway===true || favHome===true){
          prefillFormatQueue.push({rowNum:rn, text:matchup, underlineAway:!!favAway});
        }
      }
    }

    if(toUpdate.length) await sh.batchWrite(toUpdate);
    for(const f of prefillFormatQueue){
      try { const runs=runsForMatchup(f.text, f.underlineAway, false, false); await sh.updateTextRuns(f.rowNum, hmap["matchup"], f.text, runs); } catch(_) {}
    }
  }

  /* ---------- FINALS ---------- */
  if(MODE==="finals_only" || MODE==="prefill_and_finals"){
    const batch=[], finalsFormatQueue=[];

    for(const ev of events){
      const comp=ev.competitions?.[0]||{};
      const away=comp.competitors?.find(c=>c.homeAway==="away");
      const home=comp.competitors?.find(c=>c.homeAway==="home");
      const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
      const rowNum=(await locateRow(sh,hmap)).get(String(ev.id)); if(!rowNum) continue;

      const set=(name,val)=>{ const idx=hmap[name.toLowerCase()]; if(idx==null) return; batch.push({range:`${TAB_NAME}!${colLetter(idx)}${rowNum}:${colLetter(idx)}${rowNum}`, values:[[val??""]]}); };

      let finalNow = isFinalEvent(ev);
      let aPts = Number(away?.score||0), hPts = Number(home?.score||0);
      let finalScore = (aPts||hPts) ? `${aPts}-${hPts}` : "";

      const snap = (await sh.readAll())[rowNum-1] || [];
      const sheetFinal = (snap[hmap["final score"]]||"").toString().trim();
      if(sheetFinal && !finalNow){
        finalNow = true; finalScore = sheetFinal;
        const m = sheetFinal.match(/(\d+)\s*-\s*(\d+)/); if(m){ aPts=+m[1]; hPts=+m[2]; }
      }

      if(finalNow){
        if(finalScore) set("final score", finalScore);
        set("status","Final");

        if(REWRITE){
          let odds = pickOdds(comp.odds||ev.odds||[]);
          if(!odds){
            try{
              const sum=await fetchJson(summaryUrl(LEAGUE, ev.id));
              const pool=[]; if(Array.isArray(sum?.odds)) pool.push(...sum.odds); if(Array.isArray(sum?.pickcenter)) pool.push(...sum.pickcenter);
              odds=pickOdds(pool);
            }catch(_){}
          }
          if(odds){
            const lines = resolveFavAndLine(odds, away?.team?.id, home?.team?.id);
            const mls   = resolveMLs(odds, away?.team?.id, home?.team?.id);
            set("a spread", lines.aSpread); set("h spread", lines.hSpread); set("total", lines.total);
            set("a ml", mls.aML); set("h ml", mls.hML);
            const favAway = lines.favId && String(lines.favId)===String(away?.team?.id);
            finalsFormatQueue.push({rowNum, text:matchup, underlineAway:!!favAway, boldAway:aPts>hPts, boldHome:hPts>aPts});
          }else{
            finalsFormatQueue.push({rowNum, text:matchup, underlineAway:false, boldAway:aPts>hPts, boldHome:hPts>aPts});
          }
        }else{
          finalsFormatQueue.push({rowNum, text:matchup, underlineAway:false, boldAway:aPts>hPts, boldHome:hPts>aPts});
        }
      }
    }

    if(batch.length) await sh.batchWrite(batch);
    for(const f of finalsFormatQueue){
      try { const runs=runsForMatchup(f.text, f.underlineAway, f.boldAway, f.boldHome); await sh.updateTextRuns(f.rowNum, hmap["matchup"], f.text, runs); } catch(_) {}
    }

    await sh.ensureGradingCF();

    // Safety: if a row has Final Score but Status not Final, flip it
    const allNow = await sh.readAll();
    const fixPayload=[];
    (allNow.slice(1)).forEach((r,i)=>{
      const st=(r[hmap["status"]]||"").toString().trim();
      const fs=(r[hmap["final score"]]||"").toString().trim();
      if(fs && st!=="Final"){
        const rowNum=i+2;
        fixPayload.push({range:`${TAB_NAME}!${colLetter(hmap["status"])}${rowNum}:${colLetter(hmap["status"])}${rowNum}`, values:[["Final"]]});
      }
    });
    if(fixPayload.length) await sh.batchWrite(fixPayload);

    // Extra sweep: rows with old dates but no final — try Summary to force a finish (e.g., 401772826-type)
    const today = dateOnlyISO(new Date());
    const needsForce=[];
    (allNow.slice(1)).forEach((r,i)=>{
      const dt=(r[hmap["date"]]||"").toString().trim();
      const fs=(r[hmap["final score"]]||"").toString().trim();
      const st=(r[hmap["status"]]||"").toString().trim();
      const id=(r[hmap["game id"]]||"").toString().trim();
      if(id && dt && dt<today && !fs){
        needsForce.push({rowNum:i+2, id});
      }
    });

    const forcePayload=[];
    for(const row of needsForce){
      try{
        const sum = await fetchJson(summaryUrl(LEAGUE, row.id));
        const sAway = sum?.boxscore?.teams?.find(t=>t.homeAway==="away")?.score ?? sum?.header?.competitions?.[0]?.competitors?.find(c=>c.homeAway==="away")?.score;
        const sHome = sum?.boxscore?.teams?.find(t=>t.homeAway==="home")?.score ?? sum?.header?.competitions?.[0]?.competitors?.find(c=>c.homeAway==="home")?.score;
        if(sAway!=null && sHome!=null){
          forcePayload.push({range:`${TAB_NAME}!${colLetter(hmap["final score"])}${row.rowNum}:${colLetter(hmap["final score"])}${row.rowNum}`, values:[[`${sAway}-${sHome}`]]});
          forcePayload.push({range:`${TAB_NAME}!${colLetter(hmap["status"])}${row.rowNum}:${colLetter(hmap["status"])}${row.rowNum}`, values:[["Final"]]});
        }
      }catch(_){}
    }
    if(forcePayload.length) await sh.batchWrite(forcePayload);
  }

  /* ---------- LIVE (optional) ---------- */
  if(MODE==="live_only"){
    const all = await locateRow(sh,hmap);
    const batch=[];
    for(const ev of events){
      if(!isHalf(ev)) continue;
      const rowNum=all.get(String(ev.id)); if(!rowNum) continue;
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

  // cosmetics: keep color only in G..K
  const finalAll = await sh.readAll();
  await sh.clearNonGradedBackgrounds(Math.max(2, finalAll.length));

  log("Done.");
})().catch(e=>{ console.error("Fatal:", e); process.exit(1); });

/* locate rows by Game ID fresh */
async function locateRow(sh,hmap){
  const v=await sh.readAll(); const rows=v.slice(1);
  const map=new Map(); rows.forEach((r,i)=>{ const id=(r[hmap["game id"]]||"").toString().trim(); if(id) map.set(id,i+2); });
  return map;
}
