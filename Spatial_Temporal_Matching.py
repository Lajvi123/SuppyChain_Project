import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree
from datetime import timedelta
import os

# Reload the uploaded datasets
weather_df = pd.read_csv("Datasets/Weather_Data.csv")
shipment_df = pd.read_csv("Datasets/Shipment_Route_Data.csv")
conflict_df = pd.read_csv("Datasets/Geopolitical_Data.csv")

# Convert dates
shipment_df["shipment_date"] = pd.to_datetime(shipment_df["shipment_date"])
conflict_df["date"] = pd.to_datetime(conflict_df["date"])
weather_df["YEARMONTH"] = pd.to_datetime(weather_df["YEARMONTH"].astype(str), format='%Y%m')

# Convert lat/lon to radians for BallTree
def to_radians(df, lat_col, lon_col):
    return np.radians(df[[lat_col, lon_col]].values)

# Parameters (loosened for more matches)
SPATIAL_RADIUS_KM = 200  # Increased from 100
TEMPORAL_WINDOW_DAYS = 10  # Increased from 7
EARTH_RADIUS_KM = 6371

# Build BallTrees
conflict_tree = BallTree(to_radians(conflict_df, "latitude", "longitude"), metric='haversine')
weather_tree = BallTree(to_radians(weather_df, "LATITUDE", "LONGITUDE"), metric='haversine')

# Matching
matched_data = []
for _, shipment in shipment_df.iterrows():
    ship_point_rad = np.radians([[shipment["origin_lat"], shipment["origin_lon"]]])
    ship_date = shipment["shipment_date"]
    ship_month = ship_date.to_period("M").to_timestamp()

    # Conflict Matching
    conflict_indices = conflict_tree.query_radius(ship_point_rad, r=SPATIAL_RADIUS_KM / EARTH_RADIUS_KM)[0]
    matched_conflicts = conflict_df.iloc[conflict_indices]
    matched_conflicts = matched_conflicts[
        (matched_conflicts["date"] >= ship_date - timedelta(days=TEMPORAL_WINDOW_DAYS)) &
        (matched_conflicts["date"] <= ship_date + timedelta(days=TEMPORAL_WINDOW_DAYS))
    ]

    # Weather Matching
    weather_indices = weather_tree.query_radius(ship_point_rad, r=SPATIAL_RADIUS_KM / EARTH_RADIUS_KM)[0]
    matched_weather = weather_df.iloc[weather_indices]
    matched_weather = matched_weather[
        matched_weather["YEARMONTH"].dt.to_period("M") == ship_month.to_period("M")
    ]

    for _, conflict in matched_conflicts.iterrows():
        for _, weather in matched_weather.iterrows():
            combined = {
                **shipment.to_dict(),
                "conflict_type": conflict["event_type"],
                "conflict_severity": conflict["severity"],
                "conflict_date": conflict["date"],
                "conflict_region": conflict["region"],
                "weather_location": weather["LOCATION"],
                "weather_range": weather["RANGE"],
                "weather_azimuth": weather["AZIMUTH"]
            }
            matched_data.append(combined)

# Create merged DataFrame
merged_df = pd.DataFrame(matched_data)
merged_df.to_csv("Datasets/SupplyChain_Data.csv", index=False)
print("Merged dataset saved to 'Datasets/SupplyChain_Data.csv'")
print("Shape:", merged_df.shape)