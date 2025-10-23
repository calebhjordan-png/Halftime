name: Football (Prefill + Finals + Live)

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
      run_mode:
        description: "Which phase to run"
        type: choice
        options:
          - all
          - prefill
          - finals
          - live
        default: all
      game_ids:
        description: "Comma-separated Game IDs (optional)"
        required: false
        default: ""

jobs:
  nfl:
    if: ${{ inputs.league == 'both' || inputs.league == 'nfl' }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install deps
        run: npm i axios googleapis

      - name: Run Football.mjs (NFL)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: nfl
          TAB_NAME: NFL
          RUN_SCOPE: week
          RUN_MODE: ${{ inputs.run_mode }}
          GAME_IDS: ${{ inputs.game_ids }}
        run: node Football.mjs

  cfb:
    if: ${{ inputs.league == 'both' || inputs.league == 'college-football' }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: "20"

      - name: Install deps
        run: npm i axios googleapis

      - name: Run Football.mjs (CFB)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: college-football
          TAB_NAME: CFB
          RUN_SCOPE: week
          RUN_MODE: ${{ inputs.run_mode }}
          GAME_IDS: ${{ inputs.game_ids }}
        run: node Football.mjs

