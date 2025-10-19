name: 🎓 CFB Live

on:
  workflow_dispatch:
  schedule:
    # All times are UTC. These windows map to 10:00–01:59 ET year-round.
    # Top of every hour
    - cron: "0 14-23 * * *"   # 10:00–19:59 ET
    - cron: "0 0-5 * * *"     # 20:00–01:59 ET
    # Every 5 minutes (gated below so we only run the updater when a pre-3rd game is live)
    - cron: "*/5 14-23 * * *"
    - cron: "*/5 0-5 * * *"

permissions:
  contents: read

concurrency:
  group: cfb-live-${{ github.ref_name }}
  cancel-in-progress: false

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install deps
        run: npm i axios googleapis

      # Gate: only allow the */5 runs to call the updater when a game is
      # IN PROGRESS and pre-3rd (Q1, Q2, Halftime).
      - name: Gate for 5-minute cycle
        id: gate
        env:
          LEAGUE: college-football
        run: |
          # Top-of-hour runs ALWAYS proceed.
          now_min=$(date -u +%M)
          if [ "$now_min" != "00" ]; then
            node ops/live-gate.mjs > gate.log || true
            cat gate.log
            v=$(grep -Eo 'run_updater=(true|false)' gate.log | cut -d= -f2)
            echo "run_updater=${v:-false}" >> $GITHUB_OUTPUT
          else
            echo "run_updater=true" >> $GITHUB_OUTPUT
          fi

      - name: Run live updater (CFB)
        if: steps.gate.outputs.run_updater == 'true'
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: "college-football"
          TAB_NAME: "CFB"
        run: node live-game.mjs
