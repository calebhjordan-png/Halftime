import { google } from "googleapis";
import fs from "fs";
import path from "path";

// ---- Secrets (GitHub Actions -> Settings -> Secrets and variables -> Actions) ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const CREDS_JSON = process.env.GOOGLE_SERVICE_ACCOUNT;
const CREDS = JSON.parse(Buffer.from(CREDS_JSON, "base64").toString("utf-8"));

// ---- Authenticate ----
const auth = new google.auth.GoogleAuth({
  credentials: CREDS,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// ---- Example write ----
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
