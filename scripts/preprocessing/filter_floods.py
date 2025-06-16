import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Input and output files
raw_file = "data/historical_floods_raw.csv"
shapefile = "data/nigeria_states.geojson"  # Or .shp
output_file = "data/historical_floods.csv"

try:
    # Load raw CSV
    df = pd.read_csv(raw_file, encoding="utf-8", low_memory=False)
    
    # Inspect columns
    print("Columns:", df.columns.tolist())
    print("Data types:\n", df.dtypes)
    
    # Filter for Nigeria
    df["Country"] = df["Country"].astype(str).fillna("")
    nigeria_floods = df[df["Country"].str.contains("Nigeria", case=False, na=False)]

    # Create geometry from long/lat
    nigeria_floods = nigeria_floods[nigeria_floods["long"].notnull() & nigeria_floods["lat"].notnull()]
    geometry = [Point(xy) for xy in zip(nigeria_floods["long"], nigeria_floods["lat"])]
    gdf_floods = gpd.GeoDataFrame(nigeria_floods, geometry=geometry, crs="EPSG:4326")

    # Load Nigeria states shapefile
    gdf_states = gpd.read_file(shapefile)
    gdf_states = gdf_states.to_crs("EPSG:4326")

    # Spatial join to get state names
    gdf_joined = gpd.sjoin(gdf_floods, gdf_states, how="left", predicate="intersects")
    
    # Select and rename columns
    columns_map = {
        "Began": "date",
        "Country": "country",
        "NAME_1": "location",  # Adjust to match shapefile's state name column
        "Severity": "severity"
    }
    available_columns = {k: v for k, v in columns_map.items() if k in gdf_joined.columns}
    filtered_df = gdf_joined[list(available_columns.keys())].copy()
    filtered_df.rename(columns=available_columns, inplace=True)

    # Standardize data
    if "location" in filtered_df.columns:
        filtered_df["location"] = filtered_df["location"].astype(str).fillna("Unknown")
    if "severity" in filtered_df.columns:
        filtered_df["severity"] = filtered_df["severity"].astype(str).fillna("Unknown").str.title()
    if "date" in filtered_df.columns:
        filtered_df["date"] = pd.to_datetime(filtered_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Save filtered data
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered floods saved to {output_file}: {len(filtered_df)} records")

except Exception as e:
    print(f"Error filtering floods: {e}")