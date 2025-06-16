import pandas as pd
import sqlite3

conn = sqlite3.connect("data/flood_data.db")

# Full weather_features with all columns
weather_features = pd.read_sql("SELECT * FROM weather_features;", conn)
print("Full Weather Features Preview:\n", weather_features.head())
print("Weather Features Row Count:", len(weather_features))
print("Weather Features by City:\n", weather_features["city"].value_counts())

# Check weather table for all states
weather = pd.read_sql("SELECT city, COUNT(*) as count FROM weather GROUP BY city;", conn)
print("Weather Table Record Counts:\n", weather)

conn.close()