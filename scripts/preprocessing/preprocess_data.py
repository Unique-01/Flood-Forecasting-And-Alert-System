import pandas as pd
import sqlite3
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split

conn = sqlite3.connect("data/flood_data.db")

try:
    weather_df = pd.read_sql("SELECT * FROM weather", conn)
    flood_df = pd.read_sql("SELECT * FROM historical_floods", conn)

    print("Weather Data Sample:\n", weather_df.head())
    print(f"Weather Records: {len(weather_df)}")
    print("Weather Cities:", ", ".join(weather_df["city"].unique()))
    print(f"Weather Date Range: {weather_df['timestamp'].min()} to {weather_df['timestamp'].max()}")

    print("\nFlood Data Sample:\n", flood_df.head())
    print(f"Flood Records: {len(flood_df)}")
    print("Flood Locations:", ", ".join(flood_df["location"].unique()))
    print(f"Flood Date Range: {flood_df['date'].min()} to {flood_df['date'].max()}")

    # Standardize dates and cities
    weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"], errors="coerce").dt.date
    flood_df["date"] = pd.to_datetime(flood_df["date"], errors="coerce").dt.date
    weather_df["city"] = weather_df["city"].replace({
        "Port Harcourt": "Rivers",
        "Makurdi": "Benue",
        "Yenagoa": "Bayelsa"
    })

    # Filter floods to 2014–2023
    flood_df = flood_df[flood_df["date"] >= pd.to_datetime("2014-01-01").date()]
    print(f"Filtered Flood Records (2014–2023): {len(flood_df)}")

    # Merge
    data = flood_df.merge(
        weather_df,
        left_on=["location", "date"],
        right_on=["city", "timestamp"],
        how="left"
    )
    data["severity"] = data["severity"].fillna("No Flood")
    print(f"\nMerged Records: {len(data)}")
    print("Merged Data Sample:\n", data.head())

    # Impute missing weather data
    data[["temperature", "humidity", "precipitation"]] = data[["temperature", "humidity", "precipitation"]].fillna({
        "temperature": 25.0,  # Average for Nigeria
        "humidity": 80.0,
        "precipitation": 0.0
    })

except Exception as e:
    print(f"Error merging data: {e}")
    data = flood_df.copy()
    data["severity"] = data["severity"].fillna("No Flood")
    data["temperature"] = 25.0
    data["humidity"] = 80.0
    data["precipitation"] = 0.0
    print(f"Fallback to flood data only: {len(data)} records")

# Normalize features
scaler = MinMaxScaler()
data[["temperature", "humidity", "precipitation"]] = scaler.fit_transform(
    data[["temperature", "humidity", "precipitation"]]
)

# Create flood risk label
data["flood_risk"] = data["severity"].apply(lambda x: 1 if x != "No Flood" else 0)

# Split
train, test = train_test_split(data, test_size=0.2, random_state=42)

# Save
train.to_csv("data/train_data.csv", index=False)
test.to_csv("data/test_data.csv", index=False)

conn.close()
print(f"Data preprocessing complete: {len(train)} training rows, {len(test)} test rows")
