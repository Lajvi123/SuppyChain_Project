import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.neighbors import BallTree

# Load datasets
weather_df = pd.read_csv("Datasets/Weather_Data.csv")
shipment_df = pd.read_csv("Datasets/Shipment_Route_Data.csv")
conflict_df = pd.read_csv("Datasets/Geopolitical_Data.csv")

# Convert dates
shipment_df["shipment_date"] = pd.to_datetime(shipment_df["shipment_date"])
conflict_df["date"] = pd.to_datetime(conflict_df["date"])
weather_df["YEARMONTH"] = pd.to_datetime(weather_df["YEARMONTH"].astype(str), format="%Y%m")

# Parameters
SPATIAL_RADIUS_KM = 50
TEMPORAL_WINDOW_DAYS = 3
EARTH_RADIUS_KM = 6371.0

def to_radians(df, lat_col, lon_col):
    return np.radians(df[[lat_col, lon_col]].values)

# Build BallTree for conflicts
conflict_coords_rad = to_radians(conflict_df, "latitude", "longitude")
conflict_tree = BallTree(conflict_coords_rad, metric="haversine")

# Build BallTree for weather
weather_coords_rad = to_radians(weather_df, "LATITUDE", "LONGITUDE")
weather_tree = BallTree(weather_coords_rad, metric="haversine")

# Merge logic
merged_records = []

for _, shipment in shipment_df.iterrows():
    ship_point_rad = np.radians([[shipment["origin_lat"], shipment["origin_lon"]]])
    ship_date = shipment["shipment_date"]
    ship_month = ship_date.to_period("M").to_timestamp()

    # Conflict proximity
    conflict_indices = conflict_tree.query_radius(ship_point_rad, r=SPATIAL_RADIUS_KM / EARTH_RADIUS_KM)[0]
    relevant_conflicts = conflict_df.iloc[conflict_indices]
    relevant_conflicts = relevant_conflicts[
        (relevant_conflicts["date"] >= ship_date - timedelta(days=TEMPORAL_WINDOW_DAYS)) &
        (relevant_conflicts["date"] <= ship_date + timedelta(days=TEMPORAL_WINDOW_DAYS))
    ]
    # Weather proximity
    same_month_weather = weather_df[
        weather_df["YEARMONTH"].dt.to_period("M") == ship_month.to_period("M")
    ]

    if not same_month_weather.empty:
        same_month_weather_coords = to_radians(same_month_weather, "LATITUDE", "LONGITUDE")
        weather_subtree = BallTree(same_month_weather_coords, metric="haversine")
        weather_indices = weather_subtree.query_radius(ship_point_rad, r=SPATIAL_RADIUS_KM / EARTH_RADIUS_KM)[0]
        relevant_weather = same_month_weather.iloc[weather_indices]
    else:
        relevant_weather = pd.DataFrame()

    # Combine all matches
    for _, conflict in relevant_conflicts.iterrows():
        for _, weather in relevant_weather.iterrows():
            merged_record = {
                **shipment.to_dict(),
                "conflict_type": conflict["event_type"],
                "conflict_severity": conflict["severity"],
                "conflict_date": conflict["date"],
                "conflict_region": conflict["region"],
                "weather_location": weather["LOCATION"],
                "weather_range": weather["RANGE"],
                "weather_azimuth": weather["AZIMUTH"]
            }
            merged_records.append(merged_record)

# Final DataFrame
df_merged = pd.DataFrame(merged_records)
df_merged.to_csv("Datasets/SupplyChain_Data.csv", index=False)
print("Merged dataset saved to 'Datasets/SupplyChain_Data.csv'")
print("Shape:", df_merged.shape)