import os, re, requests, pandas as pd, gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import nfl_data_py as nfl

# ==========================================================
#  ENV + GOOGLE SHEETS SETUP
# ==========================================================
load_dotenv()

def must_env(k):
    v = os.getenv(k)
    if not v:
        raise RuntimeError(f"Missing env var {k}")
    return v

SHEET_ID       = must_env("SHEET_ID")
SHEET_TAB_NFL  = os.getenv("SHEET_TAB_NFL", "NFL")
SEASONS_RAW    = os.getenv("SEASONS", "2025")
SEASONS_LIST   = [int(s.strip()) for s in SEASONS_RAW.split(",") if s.strip()]
START_COL      = os.getenv("START_COL", "Q")
START_ROW      = int(os.getenv("START_ROW", "2"))

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
gc    = gspread.authorize(creds)
ws    = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB_NFL)
print(f"Connected to Google Sheet '{SHEET_TAB_NFL}' (ID: {SHEET_ID[:8]}...)")

# ==========================================================
#  ESPN PLAY-BY-PLAY FETCHER
# ==========================================================
def get_espn_pbp(year:int) -> pd.DataFrame | None:
    """Try ESPN API for play-by-play data for a full season."""
    try:
        print(f"Fetching ESPN PBP for {year} ...")
        sb_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={year}"
        sb = requests.get(sb_url, timeout=15).json()
        events = sb.get("events", [])
        plays_all = []

        for ev in events:
            gid = ev.get("id")
            if not gid: continue
            pbp_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={gid}"
            j = requests.get(pbp_url, timeout=10).json()
            drives = j.get("drives", {}).get("previous", [])
            for d in drives:
                for p in d.get("plays", []):
                    plays_all.append({
                        "game_id": gid,
                        "qtr":     p.get("period", {}).get("number"),
                        "clock":   p.get("clock", {}).get("displayValue"),
                        "text":    p.get("text"),
                        "yards":   p.get("yards", 0),
                        "team":    p.get("team", {}).get("abbreviation"),
                        "type":    p.get("type", {}).get("text"),
                        "scoring": p.get("scoringPlay", False),
                        "epa":     p.get("expectedPointsAdded", 0)
                    })
        if not plays_all:
            print(f"⚠ No plays found for {year} from ESPN.")
            return None
        df = pd.DataFrame(plays_all)
        df["epa"] = pd.to_numeric(df["epa"], errors="coerce").fillna(0.0)
        print(f"✓ ESPN returned {len(df):,} plays for {year}")
        return df
    except Exception as e:
        print(f"⚠ ESPN fetch failed ({type(e).__name__}: {e})")
        return None

# ==========================================================
#  FALLBACK: NFL_DATA_PY
# ==========================================================
def get_nfl_data_py(year:int) -> pd.DataFrame | None:
    try:
        print(f"Trying nfl_data_py for {year} ...")
        df = nfl.import_pbp_data([year])
        df = df[df["posteam"].notna()].copy()
        df.rename(columns={"posteam":"team"}, inplace=True)
        df["epa"] = pd.to_numeric(df["epa"], errors="coerce").fillna(0.0)
        print(f"✓ nfl_data_py returned {len(df):,} plays for {year}")
        return df
    except Exception as e:
        print(f"⚠ nfl_data_py failed ({type(e).__name__}: {e})")
        return None

# ==========================================================
#  MAIN DATA RETRIEVAL
# ==========================================================
frames=[]
for y in SEASONS_LIST:
    df = get_espn_pbp(y)
    if df is None or df.empty:
        df = get_nfl_data_py(y)
    if df is not None and not df.empty:
        frames.append(df)
if not frames:
    raise RuntimeError("No play-by-play data found from ESPN or nfl_data_py.")
pbp = pd.concat(frames, ignore_index=True)
print(f"Total plays gathered: {len(pbp):,}")

# ==========================================================
#  EPA / PLAY BY HALF
# ==========================================================
def qtr_to_half(q):
    try: q=int(q)
    except: return "Other"
    return "1H" if q in (1,2) else "2H" if q in (3,4) else "Other"

pbp["half"] = pbp["qtr"].map(qtr_to_half)
epa_stats = (
    pbp.groupby(["team","half"])["epa"]
        .mean()
        .reset_index()
        .pivot(index="team", columns="half", values="epa")
        .fillna(0.0)
        .reset_index()
)
for c in ("1H","2H"):
    if c not in epa_stats.columns:
        epa_stats[c]=0.0
epa_stats = (
    epa_stats.rename(columns={"team":"Team","1H":"EPA_1H","2H":"EPA_2H"})
              [["Team","EPA_1H","EPA_2H"]]
              .sort_values("Team")
              .reset_index(drop=True)
)
print(epa_stats.head())

# ==========================================================
#  WRITE TO GOOGLE SHEETS
# ==========================================================
values = [epa_stats.columns.tolist()] + epa_stats.values.tolist()
ws.update(range_name=f"{START_COL}{START_ROW}", values=values)
print(f"✅ Updated {SHEET_TAB_NFL} with {len(epa_stats)} teams.")
print("Done.")
