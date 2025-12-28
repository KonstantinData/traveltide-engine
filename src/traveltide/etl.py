import pandas as pd
import glob
from pathlib import Path
import os

# --- KONFIGURATION ---
DATA_DIR = Path("data")
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

def find_latest_file(pattern: str):
    search_path = BRONZE_DIR / pattern
    files = glob.glob(str(search_path))
    return sorted(files)[-1] if files else None

def clean_hotel_data(hotels_df: pd.DataFrame) -> pd.DataFrame:
    """Fixes negative nights logic."""
    print("   🧹 Cleaning Hotel data...")
    hotels_df['check_in_time'] = pd.to_datetime(hotels_df['check_in_time'], format='mixed')
    hotels_df['check_out_time'] = pd.to_datetime(hotels_df['check_out_time'], format='mixed')

    hotels_df['nights_actual'] = (
        hotels_df['check_out_time'].dt.normalize() -
        hotels_df['check_in_time'].dt.normalize()
    ).dt.days

    valid_hotels = hotels_df[hotels_df['nights_actual'] > 0].copy()
    valid_hotels['nights'] = valid_hotels['nights_actual']
    return valid_hotels.drop(columns=['nights_actual'])

def load_bronze_data():
    """Loads raw data from Bronze layer."""
    print(f"📂 Loading Bronze data from {BRONZE_DIR}...")
    files_to_load = {"users": "*users*.csv", "sessions": "*sessions*.csv",
                     "flights": "*flights*.csv", "hotels": "*hotels*.csv"}

    dfs = {}
    for name, pattern in files_to_load.items():
        file_path = find_latest_file(pattern)
        if file_path:
            print(f"   Found {name}: {Path(file_path).name}")
            dfs[name] = pd.read_csv(file_path)
        else:
            dfs[name] = None

    if dfs["users"] is None or dfs["sessions"] is None:
        raise FileNotFoundError("❌ Critical: 'users' or 'sessions' file missing!")
    return dfs["users"], dfs["sessions"], dfs["flights"], dfs["hotels"]

def process_bronze_to_silver(users, sessions, flights, hotels):
    """
    Applies strategic cohort filtering aligned with business goals (Retention).

    Cohort Definition:
    1. Timeframe: Active after Jan 4, 2023 (Current Campaign)
    2. Behavior: "High Potential Users" defined as:
       - Returning Users (Sessions > 1)
       OR
       - Converting Users (At least 1 Booking)

    Reasoning: Analyzing one-time visitors without bookings adds noise.
    We focus on users who have demonstrated intent or value.
    """
    print("\n⚙️ Processing Bronze -> Silver (Applying Strategic Cohort Rules)...")

    # 1. Prepare Dates
    users['sign_up_date'] = pd.to_datetime(users['sign_up_date'], format='mixed')
    sessions['session_start'] = pd.to_datetime(sessions['session_start'], format='mixed')

    # 2. Filter Sessions Timeframe (> Jan 4, 2023)
    start_date_filter = pd.Timestamp("2023-01-04").tz_localize(None)
    sessions['session_start'] = sessions['session_start'].dt.tz_localize(None)

    active_sessions = sessions[sessions['session_start'] >= start_date_filter].copy()
    print(f"   Sessions in analysis period: {len(active_sessions)}")

    # 3. Apply Strategic Filter (The "Mastery" Logic)

    # A. Count Sessions per User
    user_stats = active_sessions.groupby('user_id').agg({
        'session_id': 'count',
        'flight_booked': 'sum',
        'hotel_booked': 'sum'
    })

    # B. Define the Filter Condition
    # Keep if: More than 1 session OR at least 1 booking
    strategic_mask = (user_stats['session_id'] > 1) | \
                     (user_stats['flight_booked'] > 0) | \
                     (user_stats['hotel_booked'] > 0)

    active_user_ids = user_stats[strategic_mask].index

    # 4. Apply Filter to Tables
    cohort_sessions = active_sessions[active_sessions['user_id'].isin(active_user_ids)].copy()
    cohort_users = users[users['user_id'].isin(active_user_ids)].copy()

    print(f"   📉 Strategic Filter Applied:")
    print(f"      - Excluded 'One-Hit-Wonders' (1 session, 0 bookings)")
    print(f"      - Kept 'Returning Users' & 'Buyers'")
    print(f"      - Final Cohort Size: {len(cohort_users)} Users")

    # Filter Dependent Tables
    if flights is not None:
        flights = flights[flights['trip_id'].isin(cohort_sessions['trip_id'])].copy()

    if hotels is not None:
        hotels = clean_hotel_data(hotels)
        hotels = hotels[hotels['trip_id'].isin(cohort_sessions['trip_id'])].copy()

    # Save
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    cohort_users.to_parquet(SILVER_DIR / "users.parquet", index=False)
    cohort_sessions.to_parquet(SILVER_DIR / "sessions.parquet", index=False)
    if flights is not None: flights.to_parquet(SILVER_DIR / "flights.parquet", index=False)
    if hotels is not None: hotels.to_parquet(SILVER_DIR / "hotels.parquet", index=False)

    print(f"✅ Silver data saved.")
    return cohort_users, cohort_sessions, flights, hotels

def load_silver_data():
    try:
        u = pd.read_parquet(SILVER_DIR / "users.parquet")
        s = pd.read_parquet(SILVER_DIR / "sessions.parquet")
        f = pd.read_parquet(SILVER_DIR / "flights.parquet") if (SILVER_DIR / "flights.parquet").exists() else None
        h = pd.read_parquet(SILVER_DIR / "hotels.parquet") if (SILVER_DIR / "hotels.parquet").exists() else None
        return u, s, f, h
    except FileNotFoundError:
        print("❌ Silver data not found.")
        raise