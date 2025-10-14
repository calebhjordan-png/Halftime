// ...all imports, env, helpers, formatting, scrapeLiveOdds, etc. remain the same...

(async function main(){
  if (!SHEET_ID || !CREDS_RAW){
    console.error("Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT"); process.exit(1);
  }
  const sheets = sheetsClient();

  await resetAndApplyFormatting(sheets);

  const header = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:Z1` })).data.values?.[0]||[];
  const hmap = headerMap(header);
  const list = (await sheets.spreadsheets.values.get({ spreadsheetId:SHEET_ID, range:`${TAB_NAME}!A1:P6000` })).data.values||[];
  const iDate=hmap["date"], iMu=hmap["matchup"], iAwaySp=hmap["away spread"], iHomeSp=hmap["home spread"];

  const today = new Date();
  const todayET = fmtETDate(today);
  const dates = RUN_SCOPE==="week" ? [ yyyymmddET(addDays(today,-1)), yyyymmddET(today) ] : [ yyyymmddET(today) ];

  for (const dateStr of dates){
    const data = await (await fetch(scoreboardUrl(LEAGUE,dateStr))).json();
    for (const ev of (data.events||[])){
      const comp=ev.competitions?.[0]||{};
      const awayC=comp.competitors?.find(c=>c.homeAway==="away");
      const homeC=comp.competitors?.find(c=>c.homeAway==="home");
      const away=awayC?.team?.shortDisplayName||"Away";
      const home=homeC?.team?.shortDisplayName||"Home";
      const matchup=`${away} @ ${home}`;

      // --- robust status detection ---
      const t = ev.status?.type || {};
      const state = String(t.state||"").toLowerCase();          // "pre" | "in" | "post"
      const name  = String(t.name||"").toLowerCase();
      const desc  = String(t.shortDetail||t.detail||t.description||"").toLowerCase();
      const completed = Boolean(t.completed);

      const isFinal = completed || state==="post" || name.includes("final");
      const isHalftime = /halftime/.test(desc);
      const looksLiveByText = /(q1|q2|q3|q4|\b1st\b|\b2nd\b|\b3rd\b|\b4th\b|end of|\d+:\d+)/i.test(desc);
      const isInProg = state==="in" || looksLiveByText || isHalftime;

      const dateET=fmtETDate(ev.date);
      const isToday = dateET === todayET;

      if (MATCHUP_FILTER && matchup.toLowerCase()!==MATCHUP_FILTER.toLowerCase()) continue;

      // find row by Date+Matchup
      let rowNum=0;
      for(let r=1;r<list.length;r++){
        const row=list[r]||[];
        if ((row[iMu]||"")===matchup && (row[iDate]||"")===dateET){ rowNum=r+1; break; }
      }
      if (!rowNum) continue;

      const rowVals=list[rowNum-1]||[];
      const updates=[]; const put=(n,v)=>{ const i=hmap[n]; if(i===undefined||v===undefined||v===null||v==="") return; updates.push({range:`${TAB_NAME}!${colLetter(i)}${rowNum}:${colLetter(i)}${rowNum}`,values:[[v]]}); };

      // Finals: always write
      let winner=null;
      if (isFinal){
        const a=Number(awayC?.score ?? ""), h=Number(homeC?.score ?? "");
        if (!Number.isNaN(a) && !Number.isNaN(h)){ put("final score", `${a}-${h}`); winner = a>h ? "away" : (h>a ? "home" : null); }
        put("status","Final");
      }

      // Live odds if the game is live-ish or today & not final, or FORCE_LIVE
      if ((!isFinal && (isInProg || isToday)) || FORCE_LIVE){
        const live = await scrapeLiveOdds(LEAGUE, ev.id);
        if (live){
          put("live away spread", live.liveAwaySpread);
          put("live home spread", live.liveHomeSpread);
          put("live away ml",    live.liveAwayML);
          put("live home ml",    live.liveHomeML);
          put("live total",      live.liveTotal);
          if (!isFinal) put("status", isHalftime ? "Half" : "Live");
        }
      }

      if (updates.length){
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId:SHEET_ID,
          requestBody:{ valueInputOption:"USER_ENTERED", data:updates }
        });
      }

      // Favorite from spreads; style D with underline+bold
      const fav = favoriteBySpreads(rowVals[iAwaySp], rowVals[iHomeSp]); // function unchanged
      await styleMatchupCell(sheets, rowNum, matchup, fav, winner);      // function unchanged
    }
  }

  log("✅ Orchestrator complete");
})().catch(e=>{ console.error("FATAL", e); process.exit(1); });
