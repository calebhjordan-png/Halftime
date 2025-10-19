/* live-game.mjs — strict Game ID match (column A) */
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SA_JSON  = process.env.GOOGLE_SERVICE_ACCOUNT;
const LEAGUE   = (process.env.LEAGUE || "nfl").toLowerCase();
const TAB_NAME = process.env.TAB_NAME || (LEAGUE === "college-football" ? "CFB" : "NFL");
const EVENT_ID = String(process.env.TARGET_GAME_ID || "").trim();
const MAX_TOTAL_MIN = Number(process.env.MAX_TOTAL_MIN || "200");
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

if (!SHEET_ID || !SA_JSON || !EVENT_ID) {
  console.error("Missing env: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT, TARGET_GAME_ID");
  process.exit(1);
}

/* --- Helpers --- */
const pick = (o,p)=>p.replace(/\[(\d+)\]/g,'.$1').split('.').reduce((a,k)=>a?.[k],o);
async function fetchJson(url, tries=3) {
  for (let i=1;i<=tries;i++) {
    try {
      const r = await fetch(url,{headers:{"User-Agent":"live-engine/strict-id/1.0"}});
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    } catch(e) {
      if(i===tries) throw e;
      await new Promise(r=>setTimeout(r,500*i));
    }
  }
}
const summaryUrl = id => `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${id}`;

function parseStatus(sum){
  const a = pick(sum,"header.competitions.0.status") || {};
  const b = pick(sum,"competitions.0.status") || {};
  const s = a?.type?.shortDetail ? a : b;
  const type = s.type || {};
  return {
    shortDetail: type.shortDetail || "",
    state: type.state || "",
    period: Number(s.period ?? 0),
    displayClock: s.displayClock ?? "0:00",
    name: (type.name || "").toUpperCase(),
  };
}
function getTeams(sum){
  let away,home;
  let cps = pick(sum,"header.competitions.0.competitors");
  if(!Array.isArray(cps)||cps.length<2) cps = pick(sum,"competitions.0.competitors");
  if(Array.isArray(cps)&&cps.length>=2){
    away = cps.find(c=>c.homeAway==="away");
    home = cps.find(c=>c.homeAway==="home");
  }
  if((!away||!home)&&Array.isArray(pick(sum,"boxscore.teams"))){
    const bs=pick(sum,"boxscore.teams");
    away = bs.find(t=>t.homeAway==="away")||away;
    home = bs.find(t=>t.homeAway==="home")||home;
  }
  const tn=t=>t?.team?.shortDisplayName||t?.team?.abbreviation||t?.team?.displayName||"Team";
  const ts=t=>Number(t?.score??t?.statistics?.find?.(s=>s.name==="points")?.value??0);
  return {awayName:tn(away),homeName:tn(home),awayScore:ts(away),homeScore:ts(home)};
}

/* --- Sheets --- */
async function sheetsClient(){
  const creds=JSON.parse(SA_JSON);
  const jwt=new google.auth.JWT(creds.client_email,null,creds.private_key,["https://www.googleapis.com/auth/spreadsheets"]);
  await jwt.authorize();
  return google.sheets({version:"v4",auth:jwt});
}
function colMap(hdr=[]){const map={};hdr.forEach((h,i)=>map[(h||"").trim().toLowerCase()]=i);return map;}

/* STRICT: find row by column A (Game ID) only */
async function findRowByGameId(sheets, tab, eventId){
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${tab}!A1:A2000`
  });
  const col = res.data.values || [];
  for(let i=1;i<col.length;i++){ // skip header at A1
    const cell = (col[i][0] || "").toString().trim();
    if(cell === String(eventId)) return i; // 0-based index from values; i maps to row i+1
  }
  return -1;
}

async function writeCells(sheets,rowIdx,kv){
  const header=(await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z1`})).data.values?.[0]||[];
  const h=colMap(header);
  const updates=[];
  for(const [name,val] of Object.entries(kv)){
    if(val==null) continue;
    const j=h[name.toLowerCase()]; if(j==null) continue;
    const col=String.fromCharCode("A".charCodeAt(0)+j);
    const row=rowIdx+1; // rowIdx is 0-based for sheet rows; +1 to get real row (we already skipped header in finder)
    updates.push({range:`${TAB_NAME}!${col}${row}`,values:[[val]]});
  }
  if(!updates.length) return 0;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId:SHEET_ID,
    requestBody:{valueInputOption:"USER_ENTERED",data:updates}
  });
  return updates.length;
}

function decideSleep(period,clock){
  let mins=20;
  if(Number(period)===2){
    const m=/(\d{1,2}):(\d{2})/.exec(clock||"");
    if(m){const left=+m[1]+(+m[2])/60;if(left<=10)mins=Math.max(1,Math.ceil(left*2));}
  }
  if(DEBUG_MODE) mins=Math.min(mins,0.2);
  return mins;
}
const msFromMin=m=>Math.max(60_000,Math.round(m*60_000));

/* --- Main loop --- */
(async()=>{
  const sheets=await sheetsClient();
  let totalMin=0;

  for(;;){
    const sum=await fetchJson(summaryUrl(EVENT_ID));
    const st=parseStatus(sum);
    const tm=getTeams(sum);
    const currentStatus=st.shortDetail || st.state || "unknown";

    // find row by Game ID only
    const rowIdx = await findRowByGameId(sheets, TAB_NAME, EVENT_ID);
    if(rowIdx < 0){
      console.log(`No row with Game ID ${EVENT_ID} found in ${TAB_NAME} (column A).`);
      return;
    }

    console.log(`[${currentStatus}] ${tm.awayName} @ ${tm.homeName} Q${st.period||0} (${st.displayClock}) → row ${rowIdx+1}`);

    const isHalf = /HALF/i.test(st.name) ||
                   (st.state==="in" && Number(st.period)===2 && /^0?:?0{1,2}\b/.test(st.displayClock||""));

    if(!isHalf){
      // write rolling Status unless already Half/Final
      const headerRes=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A1:Z1`});
      const h=colMap(headerRes.data.values?.[0]||[]);
      const rowRes=await sheets.spreadsheets.values.get({spreadsheetId:SHEET_ID,range:`${TAB_NAME}!A${rowIdx+1}:Z${rowIdx+1}`});
      const row=rowRes.data.values?.[0]||[];
      const cur=(row[h["status"]]||"").toString().trim();
      if(cur!=="Half" && cur!=="Final"){
        await writeCells(sheets,rowIdx,{"status":currentStatus});
      }
    } else {
      const halfScore = `${tm.awayScore}-${tm.homeScore}`;
      await writeCells(sheets,rowIdx,{"status":"Half","half score":halfScore});
      console.log(`✅ Halftime written (${halfScore})`);
      return;
    }

    const sleepM = decideSleep(st.period,st.displayClock);
    console.log(`Sleeping ${sleepM}m`);
    await new Promise(r=>setTimeout(r,msFromMin(sleepM)));
    totalMin += sleepM;
    if(totalMin >= MAX_TOTAL_MIN){ console.log("⏹ MAX_TOTAL_MIN reached."); return; }
  }
})().catch(e=>{console.error("Fatal:",e);process.exit(1);});
