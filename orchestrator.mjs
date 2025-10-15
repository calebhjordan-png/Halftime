name: Orchestrator Finals (sweeper)

on:
  schedule:
    - cron: '*/15 17-23 * * *'  # 1 PM–11 PM UTC
    - cron: '*/15 0-9 * * *'    # 12 AM–9 AM UTC
  workflow_dispatch:

jobs:
  sweep:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        league: [nfl, college-football]
        include:
          - league: nfl
            tab: NFL
          - league: college-football
            tab: CFB

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install deps
        run: npm i googleapis

      - name: Finals sweep (sheet-driven)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: ${{ matrix.league }}
          TAB_NAME: ${{ matrix.tab }}
          PREFILL_MODE: off            # finals only; no date-limited fetches
        run: node orchestrator.mjs
