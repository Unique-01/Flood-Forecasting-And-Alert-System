import xarray as xr
import pandas as pd
import os
import traceback
import numpy as np

# Define cities and coordinates
cities = [
    {"name": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"name": "Rivers", "lat": 4.8156, "lon": 7.0498},
    {"name": "Benue", "lat": 7.7322, "lon": 8.5391},
    {"name": "Bayelsa", "lat": 4.9211, "lon": 6.2642}
]

# File paths
accum_file = "data/data_stream-oper_stepType-accum.nc"
instant_file = "data/data_stream-oper_stepType-instant.nc"

# Validate files
for file in [accum_file, instant_file]:
    if not os.path.exists(file):
        print(f"Error: {file} does not exist. Extract zip to data/")
        exit(1)
    file_size = os.path.getsize(file)
    if file_size < 10 * 1024 * 1024:  # Less than 10 MB
        print(f"Error: {file} is too small ({file_size / 1024 / 1024:.2f} MB), likely corrupted")
        # exit(1)
    print(f"File size for {file}: {file_size / 1024 / 1024:.2f} MB")

# Process NetCDF
try:
    for engine in ["netcdf4", "h5netcdf"]:
        try:
            ds_accum = xr.open_dataset(accum_file, engine=engine)
            ds_instant = xr.open_dataset(instant_file, engine=engine)
            print(f"Opened NetCDF files with {engine} backend")
            break
        except Exception as e:
            print(f"Failed with {engine}: {e}")
            if engine == "h5netcdf":
                print(traceback.format_exc())
                exit(1)
except Exception as e:
    print(f"Error opening NetCDF files: {e}")
    print(traceback.format_exc())
    exit(1)

# Validate variables
required_vars = {"accum": ["tp"], "instant": ["t2m", "d2m"]}
try:
    for var in required_vars["accum"]:
        if var not in ds_accum.variables:
            raise KeyError(f"Missing variable '{var}' in {accum_file}")
    for var in required_vars["instant"]:
        if var not in ds_instant.variables:
            raise KeyError(f"Missing variable '{var}' in {instant_file}")
except KeyError as e:
    print(f"Error: {e}")
    exit(1)

# Calculate relative humidity from t2m and d2m
def calculate_rh(t2m, d2m):
    # t2m, d2m in Kelvin; returns RH in %
    t_c = t2m - 273.15
    d_c = d2m - 273.15
    e = 6.1078 * 10 ** (7.5 * d_c / (237.3 + d_c))  # Vapor pressure
    es = 6.1078 * 10 ** (7.5 * t_c / (237.3 + t_c))  # Saturation vapor pressure
    rh = 100 * (e / es)
    return np.clip(rh, 0, 100)

try:
    data = []
    for city in cities:
        accum_data = ds_accum.sel(
            latitude=city["lat"],
            longitude=city["lon"],
            method="nearest"
        )
        instant_data = ds_instant.sel(
            latitude=city["lat"],
            longitude=city["lon"],
            method="nearest"
        )
        # Ensure time alignment
        time = instant_data["valid_time"].values
        df = pd.DataFrame({
            "city": city["name"],
            "timestamp": time,
            "temperature": instant_data["t2m"].values - 273.15,  # Kelvin to Celsius
            "precipitation": accum_data["tp"].values * 1000,  # m to mm
            "humidity": calculate_rh(instant_data["t2m"].values, instant_data["d2m"].values)
        })
        data.append(df)
    ds_accum.close()
    ds_instant.close()
except Exception as e:
    print(f"Error extracting city data: {e}")
    print(traceback.format_exc())
    exit(1)

# Combine and save
weather_df = pd.concat(data, ignore_index=True)
weather_df.to_csv("data/raw_weather_historical.csv", index=False)
print(f"Historical weather saved to data/raw_weather_historical.csv: {len(weather_df)} records")
