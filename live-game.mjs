// 001  // live-game.mjs
// 002  // Updates only: Status (D), Half Score (L), Live odds (M..Q).
// 003  // Leaves pregame columns untouched. Supports optional GAME_ID focus.
// 004  // Prefers halftime markets (2H / Second Half) with fallback to generic "live" markets.
// 005
// 006  import axios from "axios";
// 007  import { google } from "googleapis";
// 008
// 009  // ===== ENV =====
/* 010 */ const {
/* 011 */   GOOGLE_SHEET_ID,
/* 012 */   GOOGLE_SERVICE_ACCOUNT,
/* 013 */   LEAGUE = "nfl",                                 // "nfl" | "college-football"
/* 014 */   TAB_NAME = (LEAGUE === "nfl" ? "NFL" : "CFB"),
/* 015 */   GAME_ID = "",                                  // optional: force a single game
/* 016 */   MARKET_PREFERENCE = "2H,Second Half,Halftime,Live",
/* 017 */ } = process.env;
// 018
// 019  for (const k of ["GOOGLE_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT"]) {
/* 020 */   if (!process.env[k]) throw new Error(`Missing required env var: ${k}`);
/* 021 */ }
// 022
// 023  // ===== Google Sheets =====
/* 024 */ const svc = JSON.parse(GOOGLE_SERVICE_ACCOUNT);
/* 025 */ const jwt = new google.auth.JWT(
/* 026 */   svc.client_email,
/* 027 */   undefined,
/* 028 */   svc.private_key,
/* 029 */   ["https://www.googleapis.com/auth/spreadsheets"]
/* 030 */ );
/* 031 */ const sheets = google.sheets({ version: "v4", auth: jwt });
// 032
// 033  // ===== Helpers =====
/* 034 */ function idxToA1(n0) {
/* 035 */   let n = n0 + 1, s = "";
/* 036 */   while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
/* 037 */   return s;
/* 038 */ }
// 039  // Build today's key in US/Eastern to match the sheet
/* 040 */ const todayKey = (() => {
/* 041 */   const d = new Date();
/* 042 */   const parts = new Intl.DateTimeFormat("en-US", {
/* 043 */     timeZone: "America/New_York",
/* 044 */     month: "2-digit", day: "2-digit", year: "2-digit",
/* 045 */   }).formatToParts(d);
/* 046 */   const mm = parts.find(p => p.type === "month")?.value ?? "00";
/* 047 */   const dd = parts.find(p => p.type === "day")?.value ?? "00";
/* 048 */   const yy = parts.find(p => p.type === "year")?.value ?? "00";
/* 049 */   return `${mm}/${dd}/${yy}`;
/* 050 */ })();
// 051
/* 052 */ function shortStatusFromEspn(statusObj) {
/* 053 */   const t = statusObj?.type || {};
/* 054 */   return t.shortDetail || t.detail || t.description || "In Progress";
/* 055 */ }
/* 056 */ function isFinalFromEspn(statusObj) {
/* 057 */   return /final/i.test(String(statusObj?.type?.name || statusObj?.type?.description || ""));
/* 058 */ }
/* 059 */ function looksLiveStatus(s) {
/* 060 */   if (!s) return false;
/* 061 */   const x = s.toLowerCase();
/* 062 */   return /\bhalf\b/.test(x) || /\bin\s*progress\b/.test(x) || /\bq[1-4]\b/.test(x) || /\bot\b/.test(x) || /\blive\b/.test(x);
/* 063 */ }
/* 064 */ function isFinalCell(s) {
/* 065 */   return /^final$/i.test(String(s || ""));
/* 066 */ }
// 067
/* 068 */ function sumFirstTwoPeriods(linescores) {
/* 069 */   if (!Array.isArray(linescores) || linescores.length === 0) return null;
/* 070 */   const take = linescores.slice(0, 2);
/* 071 */   let tot = 0;
/* 072 */   for (const p of take) {
/* 073 */     const v = Number(p?.value ?? p?.score ?? 0);
/* 074 */     if (!Number.isFinite(v)) return null;
/* 075 */     tot += v;
/* 076 */   }
/* 077 */   return tot;
/* 078 */ }
/* 079 */ function parseHalfScore(summary) {
/* 080 */   try {
/* 081 */     const comp = summary?.header?.competitions?.[0];
/* 082 */     const home = comp?.competitors?.find(c => c.homeAway === "home");
/* 083 */     const away = comp?.competitors?.find(c => c.homeAway === "away");
/* 084 */     const hHome = sumFirstTwoPeriods(home?.linescores);
/* 085 */     const hAway = sumFirstTwoPeriods(away?.linescores);
/* 086 */     if (Number.isFinite(hHome) && Number.isFinite(hAway)) {
/* 087 */       return `${hAway}-${hHome}`; // away-first
/* 088 */     }
/* 089 */   } catch {}
/* 090 */   return "";
/* 091 */ }
// 092
/* 093 */ async function espnSummary(gameId) {
/* 094 */   const url = `https://site.api.espn.com/apis/site/v2/sports/football/${LEAGUE}/summary?event=${gameId}`;
* 095 */   const { data } = await axios.get(url, { timeout: 15000 });
/* 096 */   return data;
/* 097 */ }
/* 098 */ async function espnOdds(gameId) {
/* 099 */   const url = `https://sports.core.api.espn.com/v2/sports/football/${LEAGUE}/events/${gameId}/competitions/${gameId}/odds`;
/* 100 */   const { data } = await axios.get(url, { timeout: 15000 });
/* 101 */   return data;
/* 102 */ }
// 103
/* 104 */ function pickMarket(markets, preferenceList) {
/* 105 */   if (!Array.isArray(markets) || markets.length === 0) return null;
/* 106 */   const norm = s => (s || "").toLowerCase();
/* 107 */   const wants = (preferenceList || "")
/* 108 */     .split(",")
/* 109 */     .map(s => s.trim().toLowerCase())
/* 110 */     .filter(Boolean);
/* 111 */   for (const want of wants) {
/* 112 */     const m = markets.find(mk => {
/* 113 */       const a = norm(mk?.name);
/* 114 */       const b = norm(mk?.displayName);
/* 115 */       const c = norm(mk?.period?.displayName || mk?.period?.abbreviation || "");
/* 116 */       return a.includes(want) || b.includes(want) || c.includes(want);
/* 117 */     });
/* 118 */     if (m) return m;
/* 119 */   }
/* 120 */   const fallback = markets.find(mk => {
/* 121 */     const s = (mk?.name || "") + " " + (mk?.displayName || "");
/* 122 */     const t = s.toLowerCase();
/* 123 */     return t.includes("spread") || t.includes("total") || t.includes("line") || t.includes("over") || t.includes("under");
/* 124 */   });
/* 125 */   return fallback || markets[0];
/* 126 */ }
// 127
/* 128 */ function extractLiveNumbers(oddsPayload, preference = MARKET_PREFERENCE) {
/* 129 */   try {
/* 130 */     const markets = oddsPayload?.items || [];
/* 131 */     if (markets.length === 0) return undefined;
/* 132 */     const choose = (typeWords) => {
/* 133 */       const typed = markets.filter(mk => {
/* 134 */         const s = ((mk?.name || "") + " " + (mk?.displayName || "")).toLowerCase();
/* 135 */         return typeWords.some(w => s.includes(w));
/* 136 */       });
/* 137 */       return pickMarket(typed.length ? typed : markets, preference);
/* 138 */     };
/* 139 */     const mSpread = choose(["spread", "line"]);
/* 140 */     const mTotal  = choose(["total", "over", "under"]);
/* 141 */     const firstBook = (m) => m?.books?.[0] || {};
/* 142 */     const sB = firstBook(mSpread);
/* 143 */     const tB = firstBook(mTotal);
/* 144 */     const n = (v) => (v === null || v === undefined || v === "" ? "" : Number(v));
/* 145 */     const aw = sB?.awayTeamOdds || {};
/* 146 */     const hm = sB?.homeTeamOdds || {};
/* 147 */     const spreadAway = n(aw?.current?.spread ?? aw?.spread ?? sB?.current?.spread);
/* 148 */     const spreadHome = n(hm?.current?.spread ?? hm?.spread ?? (spreadAway !== "" ? -spreadAway : ""));
/* 149 */     const mlAway     = n(aw?.current?.moneyLine ?? aw?.moneyLine);
/* 150 */     const mlHome     = n(hm?.current?.moneyLine ?? hm?.moneyLine);
/* 151 */     const total      = n(tB?.current?.total ?? tB?.total);
/* 152 */     const any = [spreadAway, spreadHome, mlAway, mlHome, total].some(v => v !== "");
/* 153 */     return any ? { spreadAway, spreadHome, mlAway, mlHome, total } : undefined;
/* 154 */   } catch {
/* 155 */     return undefined;
/* 156 */   }
/* 157 */ }
// 158
/* 159 */ function makeValue(range, val) { return { range, values: [[val]] }; }
/* 160 */ function a1For(row0, col0, tab = TAB_NAME) {
/* 161 */   const row1 = row0 + 1;
/* 162 */   const colA = idxToA1(col0);
/* 163 */   return `${tab}!${colA}${row1}:${colA}${row1}`;
* 164 */ }
/* 165 */ async function getValues() {
/* 166 */   const range = `${TAB_NAME}!A1:Q2000`;
/* 167 */   const res = await sheets.spreadsheets.values.get({ spreadsheetId: GOOGLE_SHEET_ID, range });
/* 168 */   return res.data.values || [];
/* 169 */ }
// 170
/* 171 */ function mapCols(header) {
/* 172 */   const lower = s => (s || "").trim().toLowerCase();
/* 173 */   const find = (name, fb) => {
/* 174 */     const i = header.findIndex(h => lower(h) === lower(name));
/* 175 */     return i >= 0 ? i : fb;
/* 176 */   };
/* 177 */   return {
/* 178 */     GAME_ID: find("Game ID", 0),
/* 179 */     DATE:    find("Date", 1),
/* 180 */     STATUS:  find("Status", 3),
/* 181 */     HALF:    find("Half Score", 11),
/* 182 */     LA_S:    find("Live Away Spread", 12),
/* 183 */     LA_ML:   find("Live Away ML", 13),
/* 184 */     LH_S:    find("Live Home Spread", 14),
/* 185 */     LH_ML:   find("Live Home ML", 15),
/* 186 */     L_TOT:   find("Live Total", 16),
/* 187 */   };
/* 188 */ }
// 189
/* 190 */ function chooseTargets(rows, col) {
/* 191 */   const targets = [];
/* 192 */   for (let r = 1; r < rows.length; r++) {
/* 193 */     const row = rows[r] || [];
/* 194 */     const id = (row[col.GAME_ID] || "").trim();
/* 195 */     if (!id) continue;
/* 196 */     const dateCell = (row[col.DATE] || "").trim();   // MM/DD/YY
/* 197 */     const status   = (row[col.STATUS] || "").trim();
/* 198 */     if (isFinalCell(status)) continue;
/* 199 */     if (GAME_ID && id === GAME_ID)      { targets.push({ r, id, reason: "GAME_ID" }); continue; }
/* 200 */     if (looksLiveStatus(status))        { targets.push({ r, id, reason: "live-like status" }); continue; }
/* 201 */     if (dateCell === todayKey)          { targets.push({ r, id, reason: "today" }); }
/* 202 */   }
/* 203 */   return targets;
/* 204 */ }
// 205
/* 206 */ async function main() {
/* 207 */   try {
/* 208 */     const ts = new Date().toISOString();
/* 209 */     const values = await getValues();
/* 210 */     if (values.length === 0) { console.log(`[${ts}] Sheet empty—nothing to do.`); return; }
/* 211 */     const col = mapCols(values[0]);
/* 212 */     const targets = chooseTargets(values, col);
/* 213 */     if (targets.length === 0) { console.log(`[${ts}] Nothing to update (no targets${GAME_ID ? " for GAME_ID" : ""}).`); return; }
/* 214 */     const data = [];
/* 215 */     for (const t of targets) {
/* 216 */       const currentStatus = values[t.r]?.[col.STATUS] || "";
/* 217 */       if (isFinalCell(currentStatus)) continue; // defense-in-depth
/* 218 */       // 1) STATUS + HALF
/* 219 */       let statusText = currentStatus;
/* 220 */       try {
/* 221 */         const sum = await espnSummary(t.id);
/* 222 */         const compStatus = sum?.header?.competitions?.[0]?.status;
/* 223 */         const newStatus  = shortStatusFromEspn(compStatus);
/* 224 */         const nowFinal   = isFinalFromEspn(compStatus);
/* 225 */         if (newStatus && newStatus !== currentStatus) {
/* 226 */           statusText = newStatus;
/* 227 */           data.push(makeValue(a1For(t.r, col.STATUS), statusText));
/* 228 */         }
/* 229 */         const hs = parseHalfScore(sum);
/* 230 */         if (hs) data.push(makeValue(a1For(t.r, col.HALF), hs));
/* 231 */         if (nowFinal) continue; // don’t fetch live odds for finals
/* 232 */       } catch (e) {
/* 233 */         if (e?.response?.status !== 404) console.log(`Summary warn ${t.id}:`, e?.message || e);
/* 234 */       }
/* 235 */       // 2) LIVE ODDS (2H preferred)
/* 236 */       try {
/* 237 */         const odds = await espnOdds(t.id);
/* 238 */         const live = extractLiveNumbers(odds, MARKET_PREFERENCE);
/* 239 */         if (live) {
/* 240 */           const w = (c, v) => { if (v !== "" && Number.isFinite(Number(v))) data.push(makeValue(a1For(t.r, c), Number(v))); };
/* 241 */           w(col.LA_S,  live.spreadAway);
/* 242 */           w(col.LA_ML, live.mlAway);
/* 243 */           w(col.LH_S,  live.spreadHome);
/* 244 */           w(col.LH_ML, live.mlHome);
/* 245 */           w(col.L_TOT, live.total);
/* 246 */         } else {
/* 247 */           console.log(`No live market found ${t.id} (pref="${MARKET_PREFERENCE}") — left M..Q as-is.`);
/* 248 */         }
/* 249 */       } catch (e) {
/* 250 */         if (e?.response?.status === 404) console.log(`Odds 404 ${t.id} — markets unavailable, skipping M..Q.`);
/* 251 */         else console.log(`Odds warn ${t.id}:`, e?.message || e);
/* 252 */       }
/* 253 */     }
/* 254 */     if (data.length === 0) { console.log(`[${ts}] Built 0 cell updates across ${targets.length} target(s).`); return; }
/* 255 */     await sheets.spreadsheets.values.batchUpdate({
/* 256 */       spreadsheetId: GOOGLE_SHEET_ID,
/* 257 */       requestBody: { valueInputOption: "USER_ENTERED", data },
/* 258 */     });
/* 259 */     console.log(`[${ts}] Updated ${targets.length} row(s). Wrote ${data.length} precise cell update(s). ` +
/* 260 */       `Targets: ${targets.map(t => `${t.id}(${t.reason})`).join(", ")}`);
/* 261 */   } catch (err) {
/* 262 */     const code = err?.response?.status || err?.code || err?.message || err;
/* 263 */     console.error("Live updater fatal:", "*** code:", code, "***");
/* 264 */     process.exit(1);
/* 265 */   }
/* 266 */ }
/* 267 */ main();
