import pandas as pd
import numpy as np

def handle_outliers(df, cols, method='percentile', lower=0.01, upper=0.99):
    """
    Caps outliers using the percentile method (Winsorization).
    """
    df_clean = df.copy()
    for col in cols:
        if col in df_clean.columns:
            l_bound = df_clean[col].quantile(lower)
            u_bound = df_clean[col].quantile(upper)
            df_clean[col] = np.clip(df_clean[col], l_bound, u_bound)
    return df_clean

def engineer_features(users, sessions, flights, hotels):
    print("   Feature Engineering started...")

    # 1. Base Features
    features = users[['user_id', 'has_children', 'married', 'home_airport_lat', 'home_airport_lon']].copy()

    # 2. Session Metrics
    if sessions is not None:
        session_stats = sessions.groupby('user_id').agg({
            'session_id': 'count',
            'page_clicks': 'mean',
            'cancellation': 'sum',
            'flight_discount': 'mean',
            'flight_booked': 'sum',
            'hotel_booked': 'sum'
        }).reset_index()

        session_stats.rename(columns={
            'session_id': 'n_sessions',
            'page_clicks': 'avg_clicks',
            'flight_booked': 'total_flights',
            'cancellation': 'total_cancellations'
        }, inplace=True)
        features = features.merge(session_stats, on='user_id', how='left')

    # 3. Flight Metrics
    if flights is not None and sessions is not None:
        sess_flights = sessions.merge(flights, on='trip_id', how='inner')
        flight_agg = sess_flights.groupby('user_id').agg({
            'checked_bags': 'mean',
            'base_fare_usd': 'mean',
            'seats': 'mean'
        }).reset_index()
        flight_agg.rename(columns={'base_fare_usd': 'avg_flight_fare'}, inplace=True)
        features = features.merge(flight_agg, on='user_id', how='left')

    # 4. Hotel Metrics
    if hotels is not None and sessions is not None:
        sess_hotels = sessions.merge(hotels, on='trip_id', how='inner')
        hotel_agg = sess_hotels.groupby('user_id').agg({
            'nights': 'mean',
            'hotel_per_room_usd': 'mean'
        }).reset_index()
        hotel_agg.rename(columns={'hotel_per_room_usd': 'avg_hotel_price'}, inplace=True)
        features = features.merge(hotel_agg, on='user_id', how='left')

    features = features.fillna(0)

    # 5. Outlier Handling (The "Pro" Touch)
    numeric_cols = ['avg_clicks', 'avg_flight_fare', 'avg_hotel_price', 'nights']
    print(f"   ⚖️ Handling outliers for columns: {numeric_cols}")
    features = handle_outliers(features, numeric_cols)

    # 6. Derived Features
    features['cancellation_rate'] = features.apply(
        lambda row: row['total_cancellations'] / row['n_sessions'] if row['n_sessions'] > 0 else 0, axis=1
    )

    print(f"   ✅ Features generated for {len(features)} users.")
    return features