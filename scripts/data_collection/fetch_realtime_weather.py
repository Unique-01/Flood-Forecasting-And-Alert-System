import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import time

load_dotenv()
api_key = os.getenv("OPENWEATHERMAP_API_KEY")
if not api_key:
    print("Error: OPENWEATHERMAP_API_KEY not set in .env")
    exit(1)

cities = [
    {"api_name": "Lagos,NG", "db_name": "Lagos"},
    {"api_name": "Port Harcourt,NG", "db_name": "Rivers"},
    {"api_name": "Makurdi,NG", "db_name": "Benue"},
    {"api_name": "Yenagoa,NG", "db_name": "Bayelsa"}
]

data = []
for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city['api_name']}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        data.append({
            "city": city["db_name"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": json_data["main"]["temp"],
            "humidity": json_data["main"]["humidity"],
            "precipitation": json_data.get("rain", {}).get("1h", 0)
        })
        print(f"Fetched real-time data for {city['db_name']}")
    except requests.RequestException as e:
        print(f"Error fetching data for {city['api_name']}: {e}")
    time.sleep(1)

# Append to CSV
weather_df = pd.DataFrame(data)
output_file = "data/raw_weather_realtime.csv"
if os.path.exists(output_file):
    existing_df = pd.read_csv(output_file)
    weather_df = pd.concat([existing_df, weather_df], ignore_index=True)
weather_df.to_csv(output_file, index=False)
print(f"Real-time weather appended to {output_file}: {len(weather_df)} records")
