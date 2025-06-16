import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Input and output files
raw_file = "data/historical_floods_raw.csv"
shapefile = "data/nigeria_states.geojson"
temp_file = "data/historical_floods_all_mapped.csv"
output_file = "data/historical_floods.csv"

try:
    # Load raw CSV
    df = pd.read_csv(raw_file, encoding="utf-8", low_memory=False)
    print("Dartmouth Columns:", df.columns.tolist())
    print("Dartmouth Sample:\n", df.head())
    print(f"Initial Dartmouth Records: {len(df)}")

    # Filter for Nigeria
    df["Country"] = df["Country"].astype(str).fillna("")
    nigeria_floods = df[df["Country"].str.contains("Nigeria", case=False, na=False)]
    print(f"Nigeria Floods: {len(nigeria_floods)}")

    # Create geometry from long/lat where available
    valid_coords = nigeria_floods[nigeria_floods["long"].notnull() & nigeria_floods["lat"].notnull()]
    print(f"Records with valid coordinates: {len(valid_coords)}")
    geometry = [Point(xy) for xy in zip(valid_coords["long"], valid_coords["lat"])]
    gdf_floods = gpd.GeoDataFrame(valid_coords, geometry=geometry, crs="EPSG:4326")

    # Load Nigeria states shapefile
    gdf_states = gpd.read_file(shapefile)
    gdf_states = gdf_states.to_crs("EPSG:4326")
    print("Shapefile State Names:", gdf_states["NAME_1"].unique())

    # Spatial join to get state names
    gdf_joined = gpd.sjoin(gdf_floods, gdf_states, how="left", predicate="intersects")

    # Combine with events lacking coordinates
    invalid_coords = nigeria_floods[nigeria_floods["long"].isnull() | nigeria_floods["lat"].isnull()]
    invalid_coords["NAME_1"] = "Unknown"
    combined_df = pd.concat([gdf_joined, invalid_coords], ignore_index=True)

    # Save all mapped floods for inspection
    combined_df[["Began", "Country", "NAME_1", "Severity", "long", "lat"]].to_csv(temp_file, index=False)
    print(f"All mapped Nigeria floods saved to {temp_file}: {len(combined_df)} records")

    # Select and rename columns
    columns_map = {
        "Began": "date",
        "Country": "country",
        "NAME_1": "location",
        "Severity": "severity"
    }
    available_columns = {k: v for k, v in columns_map.items() if k in combined_df.columns}
    filtered_df = combined_df[list(available_columns.keys())].copy()
    filtered_df.rename(columns=available_columns, inplace=True)

    # Standardize data
    filtered_df["location"] = filtered_df["location"].astype(str).fillna("Unknown")
    filtered_df["severity"] = filtered_df["severity"].astype(str).replace({
        "1.0": "Low",
        "1.5": "Medium",
        "2.0": "High"
    }).fillna("Unknown").str.title()
    filtered_df["date"] = pd.to_datetime(filtered_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Filter for target states
    target_states = ["Lagos", "Rivers", "Benue", "Bayelsa"]
    filtered_df = filtered_df[filtered_df["location"].isin(target_states)].copy()
    print(f"Records after state filter: {len(filtered_df)}")

    # Save filtered data
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered Dartmouth floods saved to {output_file}: {len(filtered_df)} records")

    # Check for 2012 and later
    if not filtered_df[filtered_df["date"].str.contains("2012", na=False)].empty:
        print("2012 flood data found in output.")
    else:
        print("Warning: No 2012 flood data found. Check {temp_file} for mapped locations.")

except Exception as e:
    print(f"Error filtering Dartmouth data: {e}")
