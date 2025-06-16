import pandas as pd
import sqlite3
import os
from pathlib import Path


current_file_path = Path(__file__).resolve()
parent_path = current_file_path.parents[2]

# Paths
db_path = parent_path/'data/processed/flood_data.db'
train_data_path = parent_path/'data/processed/train_data_with_features.csv'

# Load data
df = pd.read_csv(train_data_path)

# Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create socioeconomic table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS socioeconomic (
        state TEXT,
        landuse_type TEXT,
        area_sqm REAL
    )
''')
# Populate (example: filter relevant columns)
socio_df = df[['location', 'landuse_type', 'area_residential']].rename(columns={
    'location': 'state', 'area_residential': 'area_sqm'
})
socio_df.to_sql('socioeconomic', conn, if_exists='replace', index=False)

# Create sentinel_features table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sentinel_features (
        region TEXT,
        week_start_date TEXT,
        image_count INTEGER
    )
''')
# Populate
sentinel_df = df[['location', 'date', 'images_2025-06-02']].rename(columns={
    'location': 'region', 'date': 'week_start_date', 'images_2025-06-02': 'image_count'
})
sentinel_df.to_sql('sentinel_features', conn, if_exists='replace', index=False)

# Create weather_features table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_features (
        city TEXT,
        window_start_date TEXT,
        avg_precipitation_7d REAL,
        avg_temperature_7d REAL,
        avg_humidity_7d REAL,
        avg_precipitation_30d REAL
    )
''')
# Populate
weather_df = df[['location', 'date', 'avg_precipitation_7d', 'avg_temperature_7d', 'avg_humidity_7d', 'avg_precipitation_30d']].rename(columns={
    'location': 'city', 'date': 'window_start_date'
})
weather_df.to_sql('weather_features', conn, if_exists='replace', index=False)

# Commit and close
conn.commit()
conn.close()
print("Database populated successfully.")