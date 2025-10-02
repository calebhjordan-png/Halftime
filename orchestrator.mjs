name: Halftime Bot

on:
  workflow_dispatch:
  # optional auto schedule (UTC): every 15 min Fri–Sun
  schedule:
    - cron: "*/15 12-23 * * 5-7"

jobs:
  run-bot:
    runs-on: ubuntu-latest

    env:
      GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
      GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: 22

      - name: Preflight env
        run: |
          node -e "console.log('SHEET_ID len:', (process.env.GOOGLE_SHEET_ID||'').length);
                   console.log('SERVICE_ACCOUNT present:', !!process.env.GOOGLE_SERVICE_ACCOUNT);"

      - name: Install deps
        run: npm install

      - name: Verify googleapis is resolvable
        run: node --input-type=module -e "import('googleapis').then(()=>console.log('googleapis ok')).catch(e=>{console.error(e);process.exit(1)})"

      - name: Verify Google Sheets auth (read-only)
        run: |
          node --input-type=module -e "
            import { google } from 'googleapis';
            const id = process.env.GOOGLE_SHEET_ID;
            const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT||'{}');
            const key = (creds.private_key||'').replace(/\\n/g,'\n');
            const jwt = new google.auth.JWT(creds.client_email, null, key, ['https://www.googleapis.com/auth/spreadsheets.readonly']);
            const sheets = google.sheets({version:'v4', auth: jwt});
            const res = await sheets.spreadsheets.get({ spreadsheetId: id });
            console.log('Sheets auth OK. Title:', res.data.properties?.title || '(unknown)');"

      - name: Install Playwright (Chromium)
        run: npx playwright install --with-deps chromium

      - name: Run orchestrator
        run: node orchestrator.mjs
