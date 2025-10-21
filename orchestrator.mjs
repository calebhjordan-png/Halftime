name: Orchestrator (Prefill + Finals)

on:
  workflow_dispatch:
    inputs:
      league:
        description: "Which league(s) to run"
        type: choice
        options:
          - both
          - nfl
          - college-football
        default: both
      target_game_ids:
        description: "Optional comma-separated ESPN Game IDs (e.g. 401772816,401772924)"
        required: false
        default: ""

concurrency:
  group: orchestrator-${{ github.ref }}
  cancel-in-progress: false

jobs:
  run-nfl:
    if: ${{ inputs.league == 'both' || inputs.league == 'nfl' }}
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install minimal deps
        run: npm i --no-audit --no-fund axios googleapis

      - name: Orchestrate NFL (prefill + finals)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: nfl
          TAB_NAME: NFL
          # Script decides what to fetch (prefill & finals) for the current week
          RUN_SCOPE: week
          TARGET_GAME_ID: ${{ inputs.target_game_ids }}
          GHA_JSON: "1"
        run: node orchestrator.mjs --gha

  run-cfb:
    if: ${{ inputs.league == 'both' || inputs.league == 'college-football' }}
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install minimal deps
        run: npm i --no-audit --no-fund axios googleapis

      - name: Orchestrate CFB (prefill + finals)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: college-football
          TAB_NAME: CFB
          RUN_SCOPE: week
          TARGET_GAME_ID: ${{ inputs.target_game_ids }}
          GHA_JSON: "1"
        run: node orchestrator.mjs --gha
