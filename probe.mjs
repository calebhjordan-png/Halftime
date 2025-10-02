// probe.mjs — proves env + Google Sheets auth
import { google } from "googleapis";

const SHEET_ID = process.env.GOOGLE_SHEET_ID || "";
const RAW_CREDS = process.env.GOOGLE_SERVICE_ACCOUNT || "";

console.log("[probe] SHEET_ID len:", SHEET_ID.length);
console.log("[probe] SERVICE_ACCOUNT present:", !!RAW_CREDS);

if (!SHEET_ID || !RAW_CREDS) {
  console.error("[probe] Missing secrets. Check repo Settings → Secrets → Actions and workflow env block.");
  process.exit(2);
}

let CREDS;
try { CREDS = JSON.parse(RAW_CREDS); }
catch (e) {
  console.error("[probe] GOOGLE_SERVICE_ACCOUNT is not valid JSON:", e.message);
  process.exit(3);
}

const key = (CREDS.private_key || "").replace(/\\n/g, "\n");

try {
  const jwt = new google.auth.JWT(
    CREDS.client_email,
    null,
    key,
    ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  );
  const sheets = google.sheets({ version: "v4", auth: jwt });
  const res = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  console.log("[probe] Sheets auth OK. Title:", res.data.properties?.title || "(unknown)");
  process.exit(0);
} catch (e) {
  console.error("[probe] Sheets auth failed:", e.message);
  process.exit(4);
}
