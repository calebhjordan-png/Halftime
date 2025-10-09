import os, re, json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import nflreadpy as nr

# Optional formatting lib
try:
    from gspread_formatting import format_cell_range, CellFormat, Color
    HAS_FMT = True
except Exception:
    HAS_FMT = False

# =========================
#  ENV / CONFIG
# =========================
SHEET_ID      = os.environ["SHEET_ID"]
SHEET_TAB_NFL = os.getenv("SHEET_TAB_NFL", "NFL")
SEASONS       = os.getenv("SEASONS", "2025")
START_COL     = os.getenv("START_COL", "Q")
START_ROW     = int(os.getenv("START_ROW", "2"))

RUN_MODE = os.getenv("RUN_MODE", "HALFTIME_OR_FINAL").upper()  # HALFTIME_ONLY | HALFTIME_OR_FINAL | FINAL_ONLY
MIN_1H_PLAYS = int(os.getenv("MIN_1H_PLAYS", "60"))

HEADERS = [
    "Away 1H Run EPA/play", "Away 1H Pass EPA/play",
    "Home 1H Run EPA/play", "Home 1H Pass EPA/play",
    "Away Full Run EPA/play", "Away Full Pass EPA/play",
    "Home Full Run EPA/play", "Home Full Pass EPA/play",
]

# =========================
#  GOOGLE AUTH
# =========================
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    gcp_sa_json = os.getenv("GCP_SA_JSON", "").strip()
    if gcp_sa_json:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(gcp_sa_json), scope)
        return gspread.authorize(creds)
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    if os.path.exists(cred_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
        return gspread.authorize(creds)
    raise RuntimeError("No Google credentials found. Set GCP_SA_JSON or put credentials.json in project.")

gc = get_gspread_client()
ws = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB_NFL)
print(f"✅ Authorized & opened sheet tab: {SHEET_TAB_NFL}")

# =========================
#  HELPERS
# =========================
TEAM_ABBR = {
    "bills":"BUF","dolphins":"MIA","patriots":"NE","jets":"NYJ",
    "ravens":"BAL","bengals":"CIN","browns":"CLE","steelers":"PIT",
    "texans":"HOU","colts":"IND","jaguars":"JAX","titans":"TEN",
    "broncos":"DEN","chiefs":"KC","raiders":"LV","chargers":"LAC",
    "cowboys":"DAL","giants":"NYG","eagles":"PHI","commanders":"WAS",
    "bears":"CHI","lions":"DET","packers":"GB","vikings":"MIN",
    "falcons":"ATL","panthers":"CAR","saints":"NO","buccaneers":"TB",
    "cardinals":"ARI","rams":"LA","49ers":"SF","seahawks":"SEA",
}
CITY_VARIANTS = {
    "washington":"WAS","footballteam":"WAS","commanders":"WAS",
    "newyorkgiants":"NYG","giants":"NYG",
    "newyorkjets":"NYJ","jets":"NYJ",
    "losangeleschargers":"LAC","chargers":"LAC","sandiegochargers":"LAC",
    "losangelesrams":"LA","rams":"LA","stlouisrams":"LA",
    "lasvegasraiders":"LV","oaklandraiders":"LV","raiders":"LV",
    "tampabaybuccaneers":"TB","buccaneers":"TB","bucs":"TB",
    "sanfrancisco49ers":"SF","49ers":"SF","niners":"SF",
    "jacksonvillejaguars":"JAX","jaguars":"JAX","jags":"JAX",
    "kansascitychiefs":"KC","chiefs":"KC",
}

def to_abbr(name: str) -> str:
    key = re.sub(r"[^a-z]", "", str(name).lower())
    if key in CITY_VARIANTS:
        return CITY_VARIANTS[key]
    return TEAM_ABBR.get(key, "")

def parse_week(s: str):
    if not s: return None
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None

def parse_matchup(s: str):
    parts = re.split(r"\s+@\s+|\s+at\s+", str(s), flags=re.I)
    if len(parts) != 2:
        return None, None
    away_name, home_name = parts[0].strip(), parts[1].strip()
    return to_abbr(away_name), to_abbr(home_name)

def norm_date(x: str | None):
    if not x: return None
    try: return str(pd.to_datetime(x).date())
    except Exception: return None

def col_to_letter(col_idx: int) -> str:
    s = ""
    while col_idx:
        col_idx, r = divmod(col_idx-1, 26)
        s = chr(65+r) + s
    return s

def letter_to_col(s: str) -> int:
    n = 0
    for ch in s.upper():
        n = n*26 + (ord(ch)-64)
    return n

# =========================
#  READ SHEET A/B/D
# =========================
values = ws.get_all_values()
if not values:
    raise SystemExit("Sheet is empty.")

COL_DATE, COL_WEEK, COL_MATCHUP = 0, 1, 3
header, rows = values[0], values[1:]

targets, weeks_in_sheet = [], set()
for r in rows:
    date_str = r[COL_DATE].strip() if len(r) > COL_DATE else ""
    week_str = r[COL_WEEK].strip() if len(r) > COL_WEEK else ""
    matchup  = r[COL_MATCHUP].strip() if len(r) > COL_MATCHUP else ""
    if not matchup:
        targets.append(None)
        continue
    away, home = parse_matchup(matchup)
    week = parse_week(week_str)
    date_norm = norm_date(date_str)
    if week: weeks_in_sheet.add(week)
    targets.append({"away": away, "home": home, "week": week, "date": date_norm})

print(f"Found {sum(t is not None for t in targets)} matchups; weeks present: {sorted(w for w in weeks_in_sheet if w)}")
season_current = int(SEASONS.split(",")[0].strip())

# =========================
#  SCHEDULE
# =========================
sch = nr.load_schedules([season_current]).to_pandas()
schedule = pd.DataFrame({
    "game_id":   sch["game_id"] if "game_id" in sch else sch.get("gameId"),
    "season":    sch["season"]  if "season"  in sch else sch.get("season_year"),
    "week":      sch["week"],
    "home_team": sch["home_team"] if "home_team" in sch else sch.get("home"),
    "away_team": sch["away_team"] if "away_team" in sch else sch.get("away"),
})
if "gameday" in sch:
    schedule["gameday"] = pd.to_datetime(sch["gameday"], errors="coerce").dt.date.astype("string")
else:
    schedule["gameday"] = pd.NaT

# =========================
#  PBP (live-capable)
# =========================
print("⏳ Loading play-by-play data (this may take 1-2 minutes)...")
pbp = nr.load_pbp([season_current]).to_pandas()
pbp = pbp[pbp["play_type"].notna()].copy()

# Ensure rush/pass flags exist
if "rush" not in pbp.columns:
    pbp["rush"] = (pbp["play_type"].str.lower()=="run").astype("int64")
if "pass" not in pbp.columns:
    pbp["pass"] = (pbp["play_type"].str.lower()=="pass").astype("int64")

# Gating for halftime/final
g_max_qtr = pbp.groupby("game_id")["qtr"].max()
h1_mask = pbp["qtr"].isin([1,2])
h1_counts = pbp[h1_mask].groupby("game_id")["play_id"].count()

def is_halftime(gid: str) -> bool:
    return int(g_max_qtr.get(gid, 0)) == 2 and int(h1_counts.get(gid, 0)) >= MIN_1H_PLAYS
def is_final_or_late(gid: str) -> bool:
    return int(g_max_qtr.get(gid, 0)) >= 4
def game_allowed_for_mode(gid: str) -> bool:
    if RUN_MODE == "HALFTIME_ONLY":
        return is_halftime(gid)
    if RUN_MODE == "FINAL_ONLY":
        return is_final_or_late(gid)
    return is_halftime(gid) or is_final_or_late(gid)

# =========================
#  AGG HELPERS
# =========================
def agg_epp(df):
    g = (df.groupby(["game_id","posteam"], dropna=False)
           .agg(plays=("play_id","count"),
                epa=("epa","sum"))
           .reset_index())
    g["epa_per_play"] = g["epa"] / g["plays"]
    g.loc[g["plays"].isna() | (g["plays"]==0), "epa_per_play"] = pd.NA
    return g

# 1H splits
first_half = pbp[h1_mask].copy()
h1_run  = agg_epp(first_half[first_half["rush"]==1]).rename(columns={"epa_per_play":"h1_run_epp"})
h1_pass = agg_epp(first_half[first_half["pass"]==1]).rename(columns={"epa_per_play":"h1_pass_epp"})

# FULL-game-to-date splits (all quarters seen so far)
full_run  = agg_epp(pbp[pbp["rush"]==1]).rename(columns={"epa_per_play":"full_run_epp"})
full_pass = agg_epp(pbp[pbp["pass"]==1]).rename(columns={"epa_per_play":"full_pass_epp"})

# Season-wide percentile distributions
def add_percentile(df, value_col, pct_col_name):
    s = df[value_col].dropna()
    if s.empty:
        df[pct_col_name] = pd.NA
        return df
    # percentile rank across the season so far
    ranks = s.rank(pct=True)
    df = df.merge(ranks.rename(pct_col_name), left_index=True, right_index=True, how="left")
    return df

h1_run  = add_percentile(h1_run,  "h1_run_epp",  "h1_run_pct")
h1_pass = add_percentile(h1_pass, "h1_pass_epp", "h1_pass_pct")
full_run  = add_percentile(full_run,  "full_run_epp",  "full_run_pct")
full_pass = add_percentile(full_pass, "full_pass_epp", "full_pass_pct")

# Merge halves + full for lookup
h1 = (h1_run[["game_id","posteam","h1_run_epp","h1_run_pct"]]
        .merge(h1_pass[["game_id","posteam","h1_pass_epp","h1_pass_pct"]],
               on=["game_id","posteam"], how="outer")
        .rename(columns={"posteam":"team"}))
full = (full_run[["game_id","posteam","full_run_epp","full_run_pct"]]
          .merge(full_pass[["game_id","posteam","full_pass_epp","full_pass_pct"]],
                 on=["game_id","posteam"], how="outer")
          .rename(columns={"posteam":"team"}))

# =========================
#  MATCH ROWS → GIDs
# =========================
def find_game_id(t):
    if t is None or not t["away"] or not t["home"]:
        return None
    cand = schedule[(schedule["home_team"]==t["home"]) & (schedule["away_team"]==t["away"])]
    if t["week"] is not None:
        cand = cand[cand["week"] == t["week"]]
    if t["date"]:
        cand = cand[cand["gameday"] == t["date"]]
    if len(cand)==0:
        return None
    return cand.iloc[0]["game_id"]

def grab(df, col):
    if len(df)==0 or col not in df.columns or pd.isna(df.iloc[0][col]):
        return None
    try:
        return float(df.iloc[0][col])
    except Exception:
        return None

def fmt_val_with_pct(val, pct):
    if val is None or pd.isna(val):
        return ""
    # text: value (percentile)
    pct_txt = f"{int(round((pct or 0)*100)):d}%" if (pct is not None and not pd.isna(pct)) else ""
    if pct_txt:
        return f"{val:.3f} ({pct_txt})"
    return f"{val:.3f}"

# =========================
#  BUILD OUTPUT ROWS (WITH HEADERS)
#  Order:
#   Away H1 Run, Away H1 Pass, Home H1 Run, Home H1 Pass,
#   Away Full Run, Away Full Pass, Home Full Run, Home Full Pass
# =========================
records = []
pct_matrix = []  # 8 percentiles per row for coloring

for t in targets:
    if t is None:
        records.append([""]*8)
        pct_matrix.append([None]*8)
        continue

    gid = find_game_id(t)
    if not gid or not game_allowed_for_mode(gid):
        records.append([""]*8)
        pct_matrix.append([None]*8)
        continue

    h_home = h1[(h1["game_id"]==gid) & (h1["team"]==t["home"])]
    h_away = h1[(h1["game_id"]==gid) & (h1["team"]==t["away"])]
    f_home = full[(full["game_id"]==gid) & (full["team"]==t["home"])]
    f_away = full[(full["game_id"]==gid) & (full["team"]==t["away"])]

    a_h1_run  = grab(h_away, "h1_run_epp");   a_h1_run_pct  = grab(h_away, "h1_run_pct")
    a_h1_pass = grab(h_away, "h1_pass_epp");  a_h1_pass_pct = grab(h_away, "h1_pass_pct")
    h_h1_run  = grab(h_home, "h1_run_epp");   h_h1_run_pct  = grab(h_home, "h1_run_pct")
    h_h1_pass = grab(h_home, "h1_pass_epp");  h_h1_pass_pct = grab(h_home, "h1_pass_pct")

    a_full_run  = grab(f_away, "full_run_epp");   a_full_run_pct  = grab(f_away, "full_run_pct")
    a_full_pass = grab(f_away, "full_pass_epp");  a_full_pass_pct = grab(f_away, "full_pass_pct")
    h_full_run  = grab(f_home, "full_run_epp");   h_full_run_pct  = grab(f_home, "full_run_pct")
    h_full_pass = grab(f_home, "full_pass_epp");  h_full_pass_pct = grab(f_home, "full_pass_pct")

    row_vals = [
        fmt_val_with_pct(a_h1_run,  a_h1_run_pct),
        fmt_val_with_pct(a_h1_pass, a_h1_pass_pct),
        fmt_val_with_pct(h_h1_run,  h_h1_run_pct),
        fmt_val_with_pct(h_h1_pass, h_h1_pass_pct),
        fmt_val_with_pct(a_full_run,  a_full_run_pct),
        fmt_val_with_pct(a_full_pass, a_full_pass_pct),
        fmt_val_with_pct(h_full_run,  h_full_run_pct),
        fmt_val_with_pct(h_full_pass, h_full_pass_pct),
    ]
    records.append(row_vals)
    pct_matrix.append([
        a_h1_run_pct, a_h1_pass_pct, h_h1_run_pct, h_h1_pass_pct,
        a_full_run_pct, a_full_pass_pct, h_full_run_pct, h_full_pass_pct
    ])

# =========================
#  WRITE (headers + data) with named args to avoid deprecation warning
# =========================
ws.update(range_name=f"{START_COL}1", values=[HEADERS])
start_col_idx = letter_to_col(START_COL)
end_col_idx = start_col_idx + len(HEADERS) - 1
end_col_letter = col_to_letter(end_col_idx)
end_row = START_ROW + len(records) - 1
rng = f"{START_COL}{START_ROW}:{end_col_letter}{end_row}"
ws.update(range_name=rng, values=records)
print(f"✅ Wrote {len(records)} rows to {rng}.")

# =========================
#  COLORING by percentile (more extreme)
#   - ~3rd pct ≈ dark red, ~97th pct ≈ strong green
#   - Nonlinear contrast (sigmoid-ish) so middles stay pale and tails pop
# =========================
def clamp01(x): return max(0.0, min(1.0, float(x)))
def contrast_curve(p):
    # Increase contrast around the middle. Adjust factor to taste (1.8 = punchy)
    from math import tanh
    return 0.5 + 0.5 * tanh((p - 0.5) * 3.6)

def pct_to_color(p):
    if p is None or pd.isna(p):
        return Color(1,1,1)  # white for missing/blank
    p = clamp01(p)
    pc = contrast_curve(p)  # boosted
    # Interpolate 0..0.5 red->white, 0.5..1 white->green (with stronger endpoints)
    if pc <= 0.5:
        t = pc / 0.5
        # from dark red (0.85,0.18,0.18) to white (1,1,1)
        r = 0.85 + (1.0 - 0.85)*t
        g = 0.18 + (1.0 - 0.18)*t
        b = 0.18 + (1.0 - 0.18)*t
    else:
        t = (pc - 0.5) / 0.5
        # from white (1,1,1) to strong green (0.20,0.92,0.20)
        r = 1.0 + (0.20 - 1.0)*t
        g = 1.0 + (0.92 - 1.0)*t
        b = 1.0 + (0.20 - 1.0)*t
    return Color(r, g, b)

if HAS_FMT and records:
    for i, pcts in enumerate(pct_matrix):
        row_num = START_ROW + i
        for j, p in enumerate(pcts):
            col_letter = col_to_letter(start_col_idx + j)
            a1 = f"{col_letter}{row_num}:{col_letter}{row_num}"
            try:
                format_cell_range(ws, a1, CellFormat(backgroundColor=pct_to_color(p)))
            except Exception:
                pass
else:
    print("ℹ️ Install 'gspread-formatting' for percentile-based coloring: pip install gspread-formatting")
