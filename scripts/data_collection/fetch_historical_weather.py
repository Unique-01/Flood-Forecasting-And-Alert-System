import pandas as pd
from meteostat import Stations, Daily
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_fixed

# Define cities and coordinates
cities = [
    {"name": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"name": "Rivers", "lat": 4.8156, "lon": 7.0498},  # Port Harcourt
    {"name": "Benue", "lat": 7.7322, "lon": 8.5391},  # Makurdi
    {"name": "Bayelsa", "lat": 4.9211, "lon": 6.2642}  # Yenagoa
]

# Date range: 2014–2023
start_date = datetime(2014, 1, 1)
end_date = datetime(2023, 12, 31)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_station_data(station_id, city_name):
    weather = Daily(station_id, start_date, end_date)
    return weather.fetch()

data = []
used_stations = set()
for city in cities:
    # Find up to 10 nearby stations
    stations = Stations()
    stations = stations.nearby(city["lat"], city["lon"]).fetch(10)
    if stations.empty:
        print(f"No stations found for {city['name']}")
        continue

    # Filter stations within 50 km
    stations = stations[stations["distance"] <= 50]
    if stations.empty:
        print(f"No stations within 50 km for {city['name']}")
        continue

    for station_id, station in stations.iterrows():
        if station_id in used_stations:
            print(f"Skipping station {station_id} for {city['name']} (already used)")
            continue
        print(f"Trying station {station_id} for {city['name']} (distance: {station['distance']:.1f} km)")
        try:
            weather = fetch_station_data(station_id, city["name"])
            if not weather.empty:
                weather["city"] = city["name"]
                weather["timestamp"] = weather.index
                available_cols = [col for col in ["tavg", "prcp", "rhum"] if col in weather.columns]
                weather = weather[["city", "timestamp"] + available_cols]
                weather.columns = ["city", "timestamp"] + [
                    {"tavg": "temperature", "prcp": "precipitation", "rhum": "humidity"}[col] for col in available_cols
                ]
                data.append(weather)
                used_stations.add(station_id)
                print(f"Fetched {len(weather)} records for {city['name']} from station {station_id}")
                break
            else:
                print(f"No data for {city['name']} from station {station_id}")
        except Exception as e:
            print(f"Error fetching data for {city['name']} from station {station_id}: {e}")

# Combine and save
if data:
    weather_df = pd.concat(data, ignore_index=True)
    weather_df.to_csv("data/raw_weather_historical.csv", index=False)
    print(f"Historical weather saved to flood-system/data/raw_weather_historical.csv: {len(weather_df)} records")
else:
    print("No historical weather data fetched")