import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def extract_weather_features(db_path):
    """
    Aggregate weather metrics (7-day and 30-day windows) per city.
    Args:
        db_path (str): SQLite database path.
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Create table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_features (
                city TEXT,
                window_start_date TEXT,
                avg_precipitation_7d REAL,
                avg_temperature_7d REAL,
                avg_humidity_7d INTEGER,
                avg_precipitation_30d REAL,
                feature_date TEXT,
                PRIMARY KEY (city, window_start_date)
            );
        """)
        
        # Query weather data
        df = pd.read_sql("SELECT city, timestamp, temperature, humidity, precipitation FROM weather;", conn)
        
        # Parse timestamps
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", errors="coerce")
        if df["timestamp"].isna().any():
            print(f"Warning: {df['timestamp'].isna().sum()} invalid timestamps. Dropping rows.")
            df = df.dropna(subset=["timestamp"])
        
        # Fill NaN in data columns
        df[["temperature", "humidity", "precipitation"]] = df[["temperature", "humidity", "precipitation"]].fillna(0)
        if df["humidity"].isna().any():
            print(f"Warning: {df['humidity'].isna().sum()} NaN values in humidity after fill.")
        
        # Initialize results
        results = []
        
        # Process each city
        for city in df["city"].unique():
            city_df = df[df["city"] == city].sort_values("timestamp")
            start_date = city_df["timestamp"].min().floor("D")
            end_date = city_df["timestamp"].max().floor("D")
            
            current_date = start_date
            while current_date <= end_date:
                # 7-day window
                window_7d = city_df[
                    (city_df["timestamp"] >= current_date) &
                    (city_df["timestamp"] < current_date + timedelta(days=7))
                ]
                # 30-day window
                window_30d = city_df[
                    (city_df["timestamp"] >= current_date) &
                    (city_df["timestamp"] < current_date + timedelta(days=30))
                ]
                
                # Skip empty windows
                if window_7d.empty:
                    print(f"Skipping empty 7-day window for {city} at {current_date}")
                    current_date += timedelta(days=7)
                    continue
                
                # Aggregate
                record = {
                    "city": city,
                    "window_start_date": current_date.strftime("%Y-%m-%d"),
                    "avg_precipitation_7d": window_7d["precipitation"].mean() or 0,
                    "avg_temperature_7d": window_7d["temperature"].mean() or 0,
                    "avg_humidity_7d": int(window_7d["humidity"].mean() or 0),
                    "avg_precipitation_30d": window_30d["precipitation"].mean() or 0,
                    "feature_date": datetime.now().strftime("%Y-%m-%d")
                }
                results.append(record)
                
                current_date += timedelta(days=7)
            
        # Save to database
        results_df = pd.DataFrame(results)
        results_df.fillna(0, inplace=True)  # Ensure no NaN in final results
        for _, row in results_df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO weather_features
                (city, window_start_date, avg_precipitation_7d, avg_temperature_7d, avg_humidity_7d, avg_precipitation_30d, feature_date)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (
                row["city"],
                row["window_start_date"],
                row["avg_precipitation_7d"],
                row["avg_temperature_7d"],
                row["avg_humidity_7d"],
                row["avg_precipitation_30d"],
                row["feature_date"]
            ))
        
        conn.commit()
        conn.close()
        print("Extracted weather features.")
        
        # Preview
        engine = create_engine(f"sqlite:///{db_path}")
        preview = pd.read_sql("SELECT * FROM weather_features;", engine)
        print("Weather Features Preview:\n", preview.head())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_file = "data/flood_data.db"
    extract_weather_features(db_file)