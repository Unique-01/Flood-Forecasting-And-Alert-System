import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
api_key = os.getenv("OPENWEATHERMAP_API_KEY")
cities = ["Lagos,NG", "Port Harcourt,NG", "Makurdi,NG", "Yenagoa,NG"]

data = []
for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        data.append({
            "city": city.split(",")[0],
            "timestamp": datetime.now(),
            "temperature": json_data["main"]["temp"],
            "humidity": json_data["main"]["humidity"],
            "precipitation": json_data.get("rain", {}).get("1h", 0)
        })
    except requests.RequestException as e:
        print(f"Error fetching data for {city}: {e}")

weather_df = pd.DataFrame(data)
weather_df.to_csv("data/raw_weather.csv", index=False)
print("Weather data saved to flood-system/data/raw_weather.csv")