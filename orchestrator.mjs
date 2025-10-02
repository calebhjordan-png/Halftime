// orchestrator.mjs — TEMP TEST: prove secrets + Google Sheets write.
// It appends a simple row to the "Data" sheet. Once this passes, we can
// swap back in your full halftime/pregame logic confidently.

import { google } from "googleapis";

// ---- Load Secrets from GitHub Actions ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID || "";
const RAW = process.env.GOOGLE_SERVICE_ACCOUNT || "";

console.log("=== DEBUG START ===");
console.log("[dbg] SHEET_ID len:", SHEET_ID.length);
console.log("[dbg] SERVICE_ACCOUNT present:", !!RAW);
let CREDS = {};
try {
  CREDS = JSON.parse(RAW);
  console.log("[dbg] creds keys:", Object.keys(CREDS));
} catch (e) {
  console.error("❌ JSON parse failed for GOOGLE_SERVICE_ACCOUNT:", e.message);
  process.exit(1);
}
console.log("=== DEBUG END ===");

// ---- Authenticate with Google Sheets ----
async function authorize() {
  try {
    const auth = new google.auth.GoogleAuth({
      credentials: {
        client_email: CREDS.client_email,
        private_key: (CREDS.private_key || "").replace(/\\n/g, "\n")
      },
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });
    const sheets = google.sheets({ version: "v4", auth });
    return sheets;
  } catch (err) {
    console.error("❌ Auth error:", err.message);
    process.exit(1);
  }
}

// ---- Append a Test Row ----
(async () => {
  try {
    const sheets = await authorize();

    // Write into the "Data" tab explicitly so we don't hit the wrong sheet.
    const nowET = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
    const row = [nowET, "GitHub Actions test", "Success ✅"];

    const res = await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: "Data!A2",
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [row] }
    });

    console.log("✅ Row appended:", res.data.updates);
    console.log("🎉 Test completed successfully");
  } catch (err) {
    console.error("❌ Write error:", err.message);
    process.exit(1);
  }
})();
