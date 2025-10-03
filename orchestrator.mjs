import { google } from "googleapis";

// ---- Load Secrets ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const CREDS_JSON = process.env.GOOGLE_SERVICE_ACCOUNT;
const CREDS = JSON.parse(Buffer.from(CREDS_JSON, "base64").toString("utf-8"));

// ---- Google Auth ----
const auth = new google.auth.GoogleAuth({
  credentials: CREDS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// ---- Write Test Row ----
async function writeTestRow() {
  try {
    const now = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
    const values = [[now, "✅ GitHub Action Connected!", "No errors"]];
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: "Data!A1",
      valueInputOption: "RAW",
      requestBody: { values },
    });
    console.log("✅ Row added successfully!");
  } catch (err) {
    console.error("❌ Error writing to sheet:", err);
    process.exit(1);
  }
}

await writeTestRow();
