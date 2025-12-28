import pandas as pd
from src.traveltide.features import engineer_features

def test_engineer_features_calculations():
    # Setup Mock Data
    users = pd.DataFrame({'user_id': [1], 'has_children': [True], 'married': [True]})
    sessions = pd.DataFrame({
        'user_id': [1, 1],
        'page_clicks': [10, 20],
        'flight_booked': [1, 0],
        'cancellation': [1, 0],
        'flight_discount': [True, False]
    })

    # Execute
    result = engineer_features(users, sessions)

    # Assert
    assert result.iloc[0]['avg_clicks'] == 15.0
    assert result.iloc[0]['total_flights'] == 1
    assert result.iloc[0]['cancellation_rate'] == 0.5
    assert 'has_children' in result.columns