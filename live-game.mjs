// ----- ESPN BET fallback scraper (Playwright) -----
import * as playwright from "playwright"; // already in package.json

function normalizeDashes(s){ return String(s).replace(/\u2212|\u2013|\u2014/g,"-").replace(/\uFE63|\uFF0B/g,"+"); }
function tokenizeOdds(rowText){
  let t=normalizeDashes(rowText).replace(/\b[ou](\d+(?:\.\d+)?)/gi,"$1");
  const parts=t.split(/\s+/).filter(Boolean);
  const NUM=/^[-+]?(\d+(\.\d+)?|\.\d+)$/;
  return parts.filter(p=>NUM.test(p));
}
function scrapeRowNumbers(txt){
  const nums=tokenizeOdds(txt).map(Number);
  const spread=nums.find(v=>Math.abs(v)<=60) ?? "";
  const total =nums.find(v=>v>=30 && v<=100) ?? "";
  const ml    =nums.find(v=>Math.abs(v)>=100) ?? "";
  return { spread, total, ml };
}
async function scrapeEspnLiveOdds(gameId, league){
  const lg = league === "college-football" ? "college-football" : "nfl";
  const url=`https://www.espn.com/${lg}/game/_/gameId/${gameId}`;
  const browser=await playwright.chromium.launch({ headless: true });
  const page=await browser.newPage();
  try{
    await page.goto(url, { waitUntil:"domcontentloaded", timeout:60000 });
    // The LIVE ODDS panel
    const section = page.locator("section:has-text('LIVE ODDS'), div:has(h2:has-text('LIVE ODDS'))").first();
    await section.waitFor({ timeout: 8000 });
    const txt=(await section.innerText()).replace(/\u00a0/g," ").replace(/\s+/g," ").trim();

    // Row split by team labels (keeps it robust)
    const allTxt = normalizeDashes(txt);
    const teams = await page.locator("[data-gamepackage-game-information] h2, .ScoreCell__TeamName, .Team__Content").allTextContents().catch(()=>[]);
    // fallback: just split first/second row in the panel
    const rows = allTxt.split(/Note:\s*Odds and lines subject to change\./i)[0].split(/\b(?:Home|Away)\b/i);
    const awayRow = rows[0] || allTxt;  // best effort
    const homeRow = rows[1] || allTxt;

    const a = scrapeRowNumbers(awayRow);
    const h = scrapeRowNumbers(homeRow);

    const total = h.total || a.total || "";
    return {
      liveAwaySpread: a.spread || "",
      liveHomeSpread: h.spread || "",
      liveTotal: total || "",
      liveAwayML: a.ml || "",
      liveHomeML: h.ml || ""
    };
  }catch(e){
    console.log("Scrape fallback failed:", e?.message);
    return null;
  }finally{
    await browser.close();
  }
}
