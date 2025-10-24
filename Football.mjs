name: Football (Prefill + Finals + Live)

on:
  workflow_dispatch:
    inputs:
      league:
        description: League (both, nfl, college-football)
        type: choice
        options: [both, nfl, college-football]
        default: both
      game_ids:
        description: Optional comma-separated ESPN Game IDs
        required: false
        default: ""
      run_scope:
        description: Range (today|week)
        type: choice
        options: [today, week]
        default: week

concurrency:
  group: football-${{ github.ref }}
  cancel-in-progress: false

jobs:
  run-nfl:
    if: ${{ inputs.league == 'both' || inputs.league == 'nfl' }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm i googleapis axios
      - name: Run Football.mjs (NFL)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: nfl
          TAB_NAME: NFL
          RUN_SCOPE: ${{ inputs.run_scope }}
          GAME_IDS: ${{ inputs.game_ids }}
          WRITE_CHUNK: "100"
          WRITE_CHUNK_SLEEP_MS: "1200"   # 1.2s between batches to stay under per-minute quota
          SKIP_FORMAT: "0"
        run: node Football.mjs

  run-cfb:
    if: ${{ inputs.league == 'both' || inputs.league == 'college-football' }}
    needs: [run-nfl]                     # ensure sequential (never parallel)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: npm i googleapis axios
      - name: Run Football.mjs (CFB)
        env:
          GOOGLE_SHEET_ID: ${{ secrets.GOOGLE_SHEET_ID }}
          GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
          LEAGUE: college-football
          TAB_NAME: CFB
          RUN_SCOPE: ${{ inputs.run_scope }}
          GAME_IDS: ${{ inputs.game_ids }}
          WRITE_CHUNK: "80"              # usually more rows than NFL → smaller chunk
          WRITE_CHUNK_SLEEP_MS: "1500"   # 1.5s between batches (very safe)
          SKIP_FORMAT: "0"
        run: node Football.mjs
