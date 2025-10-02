// orchestrator.mjs
import { google } from "googleapis";

// ---- Load Secrets from GitHub Actions ----
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const CREDS = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT || "{}");

// ---- Debug Logs ----
console.log("=== DEBUG START ===");
console.log("Loaded Sheet ID:", SHEET_ID);
console.log("Creds keys:", Object.keys(CREDS));
console.log("=== DEBUG END ===");

// ---- Authenticate with Google Sheets ----
async function authorize() {
  try {
    const auth = new google.auth.GoogleAuth({
      credentials: {
        client_email: CREDS.client_email,
        private_key: CREDS.private_key?.replace(/\\n/g, "\n"),
      },
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    const sheets = google.sheets({ version: "v4", auth });
    return sheets;
  } catch (err) {
    console.error("❌ Auth error:", err);
    process.exit(1);
  }
}

// ---- Append a Test Row ----
async function appendTestRow() {
  const sheets = await authorize();
  const now = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
  const row = [now, "GitHub Actions test", "Success ✅"];

  try {
    const res = await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: "A1",
      valueInputOption: "USER_ENTERED",
      requestBody: {
        values: [row],
      },
    });
    console.log("✅ Row appended:", res.data.updates);
  } catch (err) {
    console.error("❌ Write error:", err);
    process.exit(1);
  }
}

// ---- Run ----
appendTestRow()
  .then(() => {
    console.log("🎉 Script completed successfully");
  })
  .catch((err) => {
    console.error("💥 Unexpected error:", err);
    process.exit(1);
  });
