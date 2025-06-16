import pandas as pd
import sqlite3

# Train/test schemas
try:
    train = pd.read_csv("data/train_data.csv")
    test = pd.read_csv("data/test_data.csv")
    print("Train Data Schema:\n", train.dtypes)
    print("Train Columns:", train.columns.tolist())
    print("Train City/Location Preview:\n", train[["city", "location"]].head() if "city" in train.columns else train[["location"]].head())
    print("\nTest Data Schema:\n", test.dtypes)
    print("Test Columns:", test.columns.tolist())
    print("Test City/Location Preview:\n", test[["city", "location"]].head() if "city" in test.columns else test[["location"]].head())
except Exception as e:
    print(f"Error: {e}")

# Weather features schema
try:
    conn = sqlite3.connect("data/flood_data.db")
    weather = pd.read_sql("SELECT city, window_start_date, avg_precipitation_7d FROM weather_features;", conn)
    print("\nWeather Features Schema:\n", weather.dtypes)
    print("Weather Columns:", weather.columns.tolist())
    print("Weather City/Date Preview:\n", weather[["city", "window_start_date"]].head())
    conn.close()
except Exception as e:
    print(f"Error: {e}")
