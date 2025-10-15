import os, re, json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials as SAC

import nfl_data_py as nfl

# ------------------------------
# Config from environment
# ------------------------------
SHEET_ID       = os.environ.get("SHEET_ID", "").strip()
SHEET_TAB_NFL  = os.environ.get("SHEET_TAB_NFL", "NFL").strip()
START_CELL     = os.environ.get("START_CELL", "Q2").strip()
SEASON         = int(os.environ.get("SEASON", os.environ.get("SEASONS", "2025")))
LEAGUE         = os.environ.get("LEAGUE", "NFL").upper()

if not SHEET_ID:
    raise RuntimeError("SHEET_ID env variable is required")

START_COL = re.match(r"([A-Z]+)", START_CELL).group(1)
START_ROW = int(re.match(r"[A-Z]+(\d+)", START_CELL).group(1))

# ------------------------------
# Google Sheets auth
# ------------------------------
def open_ws(sheet_id: str, tab: str):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    sa_inline = os.environ.get("GCP_SA_JSON", "").strip()
    if sa_inline:
        sa = json.loads(sa_inline)
        creds = SAC.from_json_keyfile_dict(sa, scope)
    else:
        # fall back to credentials.json in the project folder
        cred_path = os.path.join(os.path.dirname(__file__), "credentials.json")
        if not os.path.exists(cred_path):
            raise RuntimeError("No GCP_SA_JSON and no credentials.json found for Sheets auth.")
        creds = SAC.from_json_keyfile_name(cred_path, scope)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(tab)

# ------------------------------
# Helpers
# ------------------------------
TEAM_FIX = {  # only if your sheet uses long names; otherwise keep empty
    "Football Team": "WAS",
    "Commanders": "WAS",
    "Redskins": "WAS",
    "Patriots": "NE",
    "Giants": "NYG",
    "Jets": "NYJ",
    "49ers": "SF",
}
def norm_team(s: str) -> str:
    s = s.strip()
    s = TEAM_FIX.get(s, s)
    return s.upper()

def parse_matchup_cell(text: str):
    """
    Accepts 'Eagles @ Giants' or 'Eagles at Giants' or 'Eagles vs Giants'
    Returns away, home
    """
    t = text.strip()
    t = t.replace(" at ", " @ ").replace(" vs ", " @ ").replace(" VS ", " @ ")
    if "@" not in t:
        return None, None
    left, right = [x.strip() for x in t.split("@", 1)]
    return norm_team(left), norm_team(right)

def parse_week_cell(text: str):
    m = re.search(r"(\d+)", str(text))
    return int(m.group(1)) if m else None

def letter_to_index(col_letters: str) -> int:
    """Convert 'A'->1, 'B'->2 ... for gspread col math."""
    res = 0
    for ch in col_letters:
        res = res * 26 + (ord(ch) - 64)
    return res

def a1(col: int, row: int) -> str:
    """1-indexed col to letters."""
    s = []
    while col:
        col, r = divmod(col - 1, 26)
        s.append(chr(65 + r))
    return "".join(reversed(s)) + str(row)

# ------------------------------
# Load schedule & PBP
# ------------------------------
def get_schedule_df(season: int) -> pd.DataFrame:
    sch = nfl.import_schedules([season]).copy()
    # Keep columns we need and normalize
    keep = ["game_id","season","week","home_team","away_team","gameday"]
    sch = sch[[c for c in keep if c in sch.columns]].rename(columns={"gameday":"date"})
    # Convert date -> yyyy-mm-dd (string) if available
    if "date" in sch.columns:
        sch["date"] = pd.to_datetime(sch["date"], errors="coerce").dt.date.astype("string")
    return sch

def load_season_pbp(season: int) -> pd.DataFrame:
    # nfl_data_py season parquet (available for completed seasons or when nflverse publishes)
    return nfl.import_pbp_data([season])

def compute_team_split_epa(pbp: pd.DataFrame, half: str):
    """
    Returns DF with columns:
      team, side ('run'/'pass'), split ('H1' or 'FULL'), epa_per_play
    """
    df = pbp.copy()

    # only offensive plays we care about
    df = df[df["play_type"].isin(["run","pass"])]
    # First half filter
    if half == "H1":
        df = df[df["qtr"].isin([1,2])]

    # Map side
    df["side"] = df["play_type"]  # 'run' or 'pass'

    agg = (
        df.groupby(["posteam","side"], dropna=True)["epa"]
          .mean()
          .reset_index()
          .rename(columns={"posteam":"team","epa":"epa_per_play"})
    )
    agg["split"] = "H1" if half == "H1" else "FULL"
    return agg

def add_percentiles(base: pd.DataFrame) -> pd.DataFrame:
    """
    For each split (H1/FULL) and side (run/pass), compute league-wide percentiles.
    """
    out = []
    for sp in ["H1","FULL"]:
        for side in ["run","pass"]:
            blk = base[(base["split"]==sp) & (base["side"]==side)].copy()
            if blk.empty:
                continue
            # rank percentiles (0-100)
            blk["pct"] = blk["epa_per_play"].rank(pct=True) * 100.0
            out.append(blk)
    if out:
        return pd.concat(out, ignore_index=True)
    return base.assign(pct=pd.NA)

# ------------------------------
# Pull sheet rows to evaluate
# ------------------------------
def read_sheet_rows(ws) -> list[dict]:
    """
    Reads the NFL tab and returns a list of dicts with:
      row, date, week, status, matchup, away, home
    Only keeps rows where status is 'Halftime' or 'Final'
    """
    vals = ws.get_all_values()
    # Find relevant columns by header
    headers = [h.strip().lower() for h in vals[0]]
    def col_idx(name):
        for i,h in enumerate(headers):
            if name in h:
                return i
        return None

    col_date   = col_idx("date")
    col_week   = col_idx("week")
    col_status = col_idx("status")
    col_match  = col_idx("matchup")  # D

    rows = []
    for i, row in enumerate(vals[1:], start=2):  # sheet row numbers
        if not row or all(not c for c in row):
            continue
        # status filter
        status = (row[col_status] if col_status is not None and col_status < len(row) else "").strip().lower()
        if status not in ("halftime","final"):
            continue

        week = parse_week_cell(row[col_week]) if col_week is not None else None
        date = row[col_date].strip() if col_date is not None and col_date < len(row) else ""
        matchup_txt = row[col_match] if col_match is not None and col_match < len(row) else ""
        away, home = parse_matchup_cell(matchup_txt)
        if not away or not home or not week:
            continue

        rows.append({
            "row": i,
            "date": date,
            "week": week,
            "status": status,
            "matchup": matchup_txt,
            "away": away,
            "home": home,
        })
    return rows

# ------------------------------
# Main
# ------------------------------
def main():
    if LEAGUE != "NFL":
        print(f"Only NFL supported in this script (got LEAGUE={LEAGUE}).")
        return

    ws = open_ws(SHEET_ID, SHEET_TAB_NFL)
    print(f"✅ Authorized & opened sheet tab: {SHEET_TAB_NFL}")

    targets = read_sheet_rows(ws)
    if not targets:
        print("No Halftime/Final rows found to update; nothing to do.")
        return

    weeks_needed = sorted({t["week"] for t in targets})
    print(f"Found {len(targets)} matchups; weeks present: {weeks_needed}")

    # Schedule to map game_id
    sch = get_schedule_df(SEASON)

    # PBP (league-wide for percentiles)
    print("⏳ Loading play-by-play data (this may take 1-2 minutes)...")
    pbp = load_season_pbp(SEASON)

    # league distributions
    league_h1 = compute_team_split_epa(pbp, "H1")
    league_full = compute_team_split_epa(pbp, "FULL")
    league_all = add_percentiles(pd.concat([league_h1, league_full], ignore_index=True))

    # Prepare records for writing
    records = []

    # columns we’ll fill out (8 columns)
    # Away H1: Run, Pass — Home H1: Run, Pass — Away Full: Run, Pass — Home Full: Run, Pass
    for t in targets:
        # Locate game_id
        g = sch[(sch["season"]==SEASON) &
                (sch["week"]==t["week"]) &
                (sch["home_team"]==t["home"]) &
                (sch["away_team"]==t["away"])]
        if g.empty:
            # try swap (if sheet had “vs” formatting odd)
            g = sch[(sch["season"]==SEASON) &
                    (sch["week"]==t["week"]) &
                    (sch["home_team"]==t["away"]) &
                    (sch["away_team"]==t["home"])]
        if g.empty:
            # nothing to write for this row
            records.append(["","","","","","","",""])
            continue

        game_id = g.iloc[0]["game_id"]
        gp = pbp[pbp["game_id"] == game_id].copy()
        if gp.empty:
            records.append(["","","","","","","",""])
            continue

        def get_val(team: str, sp: str, side: str):
            # compute actual game split
            base = gp[(gp["play_type"].isin(["run","pass"])) & (gp["posteam"]==team)]
            if sp == "H1":
                base = base[base["qtr"].isin([1,2])]
            if base.empty:
                return ""

            num = base.loc[base["play_type"]==side, "epa"].mean()
            if pd.isna(num):
                return ""

            # percentile from league_all
            ref = league_all[(league_all["split"]==sp) &
                             (league_all["side"]==side) &
                             (league_all["team"]==team)]
            if ref.empty:
                # If team didn’t show in league (unlikely), rank against side+split
                block = league_all[(league_all["split"]==sp) & (league_all["side"]==side)]
                if block.empty:
                    pct = None
                else:
                    pct = (block["epa_per_play"] < num).mean() * 100.0
            else:
                pct = float(ref["pct"].iloc[0])

            val = f"{num:.2f}"
            if pct is not None:
                val += f" (p{int(round(pct))})"
            return val

        away = t["away"]; home = t["home"]
        row_vals = [
            get_val(away, "H1",  "run"),
            get_val(away, "H1",  "pass"),
            get_val(home, "H1",  "run"),
            get_val(home, "H1",  "pass"),
            get_val(away, "FULL","run"),
            get_val(away, "FULL","pass"),
            get_val(home, "FULL","run"),
            get_val(home, "FULL","pass"),
        ]
        records.append(row_vals)

    # Write starting at START_CELL
    start_col_idx = letter_to_index(START_COL)
    for r_i, row_vals in enumerate(records, start=START_ROW):
        rng = a1(start_col_idx, r_i) + ":" + a1(start_col_idx + len(row_vals) - 1, r_i)
        ws.update(rng, [row_vals])

    print(f"✅ Wrote {len(records)} rows starting at {START_CELL}.")

if __name__ == "__main__":
    main()
