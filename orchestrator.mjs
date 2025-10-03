import { google } from "googleapis";

/** ===== Settings from env ===== */
const SHEET_ID = (process.env.GOOGLE_SHEET_ID || "").trim();
const CREDS_RAW = (process.env.GOOGLE_SERVICE_ACCOUNT || "").trim();
/** Which sheet tab to write to (defaults to NFL). */
const TAB_NAME = (process.env.TAB_NAME || "NFL").trim();

if (!SHEET_ID) {
  console.error("❌ Missing GOOGLE_SHEET_ID");
  process.exit(1);
}
if (!CREDS_RAW) {
  console.error("❌ Missing GOOGLE_SERVICE_ACCOUNT");
  process.exit(1);
}

/** Accept Base64 or raw JSON for the service account. */
function parseServiceAccount(raw) {
  if (raw.startsWith("{")) return JSON.parse(raw); // raw JSON
  const json = Buffer.from(raw, "base64").toString("utf8"); // Base64
  return JSON.parse(json);
}

let CREDS;
try {
  CREDS = parseServiceAccount(CREDS_RAW);
  if (!CREDS.client_email || !CREDS.private_key) {
    throw new Error("Credentials missing client_email or private_key");
  }
  console.log("🔐 Service account:", CREDS.client_email);
} catch (err) {
  console.error("❌ Failed to parse GOOGLE_SERVICE_ACCOUNT:", err.message);
  process.exit(1);
}

/** Auth + client */
const auth = new google.auth.GoogleAuth({
  credentials: { client_email: CREDS.client_email, private_key: CREDS.private_key },
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

/** Ensure the desired tab exists; create it if missing. Returns {title, sheetId}. */
async function ensureTab(spreadsheetId, wantedTitle) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const found = (meta.data.sheets || []).find(
    s => s.properties?.title === wantedTitle
  );
  if (found) {
    console.log(`📑 Using existing tab: ${wantedTitle}`);
    return { title: wantedTitle, sheetId: found.properties.sheetId };
  }

  console.log(`📑 Tab "${wantedTitle}" not found — creating it…`);
  const res = await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{ addSheet: { properties: { title: wantedTitle } } }],
    },
  });
  const sheetId = res.data.replies?.[0]?.addSheet?.properties?.sheetId;
  console.log(`✅ Created tab "${wantedTitle}" (sheetId ${sheetId})`);
  return { title: wantedTitle, sheetId };
}

/** Append a simple test row to confirm connectivity. */
async function appendTestRow(tabTitle) {
  const nowET = new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
  const values = [[nowET, "✅ GitHub Action Connected!", "No errors"]];
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${tabTitle}!A1`,
    valueInputOption: "RAW",
    requestBody: { values },
  });
  console.log(`✅ Row appended to "${tabTitle}"`);
}

(async () => {
  try {
    const { title } = await ensureTab(SHEET_ID, TAB_NAME);
    await appendTestRow(title);
  } catch (err) {
    console.error("❌ Error:", err.message);
    console.error(err);
    process.exit(1);
  }
})();
