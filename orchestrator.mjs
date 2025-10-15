  let finalsWritten = 0;
  let attempts = 0;

  // repeat fetch until all finals written or max attempts reached
  while (attempts < 2) {   // check twice, 15s apart
    for (const e of events) {
      const comp = e.competitions?.[0] || {};
      const away = comp.competitors?.find(c=>c.homeAway==="away");
      const home = comp.competitors?.find(c=>c.homeAway==="home");
      const matchup = `${away?.team?.shortDisplayName||"Away"} @ ${home?.team?.shortDisplayName||"Home"}`;
      const dateET  = fmtETDate(e.date);

      const statusName = (e.status?.type?.name || comp.status?.type?.name || "").toUpperCase();
      const state      = (e.status?.type?.state || comp.status?.type?.state || "").toLowerCase();
      const isFinal    = /FINAL/.test(statusName) || state === "post";
      if (!isFinal) continue;

      const row = idMap.get(String(e.id)) || keyMap.get(keyOf(dateET, matchup));
      if (!row) continue;

      const score = `${away?.score ?? ""}-${home?.score ?? ""}`;
      const updates = [];
      const add = (name,val)=>{
        const idx = h2[name.toLowerCase()];
        if (idx == null) return;
        const col = String.fromCharCode("A".charCodeAt(0)+idx);
        updates.push({ range:`${TAB_NAME}!${col}${row}`, values:[[val]] });
      };
      add("final score", score);
      add("status", "Final");

      if (updates.length) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SHEET_ID,
          requestBody: { valueInputOption:"RAW", data: updates }
        });
        finalsWritten++;
      }
    }
    if (finalsWritten > 0) break;
    attempts++;
    console.log("No new finals yet, retrying in 15s...");
    await new Promise(r => setTimeout(r, 15000));
  }

  console.log(`✅ Finals written: ${finalsWritten}`);
})();
