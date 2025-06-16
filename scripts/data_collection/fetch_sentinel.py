import ee
import pandas as pd
from datetime import datetime, timedelta
from decouple import config

# Initialize with project ID
try:
    ee.Initialize(project=config("GOOGLE_EARTH_ENGINE_PROJECT_ID"))  
except Exception as e:
    print(f"Earth Engine initialization error: {e}")
    print("Run 'earthengine authenticate' if not authenticated.")
    exit(1)

# Define regions (bounding boxes for state capitals)
regions = {
    "Lagos": ee.Geometry.Rectangle([3.1, 6.4, 3.5, 6.7]),
    "Port Harcourt": ee.Geometry.Rectangle([6.9, 4.7, 7.1, 4.9]),
    "Makurdi": ee.Geometry.Rectangle([8.4, 7.6, 8.6, 7.8]),
    "Yenagoa": ee.Geometry.Rectangle([6.2, 4.8, 6.4, 5.0])
}

# Fetch Sentinel-1 data (last 7 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
data = []

for region_name, region in regions.items():
    try:
        collection = (ee.ImageCollection('COPERNICUS/S1_GRD')
                      .filterBounds(region)
                      .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                      .filter(ee.Filter.eq('instrumentMode', 'IW')))
        image = collection.first()
        if not image:
            print(f"No Sentinel-1 images found for {region_name}")
            continue
        metadata = {
            "image_id": image.get("system:id").getInfo(),
            "date": image.get("system:time_start").getInfo(),
            "region": region_name
        }
        data.append(metadata)
    except Exception as e:
        print(f"Error fetching Sentinel-1 data for {region_name}: {e}")

# Save to CSV
if data:
    pd.DataFrame(data).to_csv("data/sentinel_metadata.csv", index=False)
    print("Sentinel-1 metadata saved to flood-system/data/sentinel_metadata.csv")
else:
    print("No Sentinel-1 data collected.")