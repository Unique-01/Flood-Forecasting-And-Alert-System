import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Input and output files
gfm_file = "data/global_flood_monitor.csv"  # Your GFM data file
geonames_file = "data/geonames_ng.txt"  # GeoNames data for Nigeria
shapefile = "data/nigeria_states.geojson"  # Nigeria states shapefile
output_file = "data/gfm_floods.csv"  # Output file

# Admin1 code to state name mapping
admin1_map = {
    "05": "Lagos",
    "50": "Rivers",
    "26": "Benue",
    "52": "Bayelsa"
}

try:
    # Load GFM CSV
    gfm_df = pd.read_csv(gfm_file, encoding="utf-8", low_memory=False)
    print("GFM Columns:", gfm_df.columns.tolist())
    print("GFM Sample:\n", gfm_df.head())
    print(f"Initial GFM Records: {len(gfm_df)}")
    
    if gfm_df.empty:
        print("Error: GFM data is empty.")
        gfm_df.to_csv(output_file, index=False)
        exit()

    # Remove 'g-' prefix from location_ID
    gfm_df["location_ID"] = gfm_df["location_ID"].astype(str).str.replace("g-", "", regex=False)
    
    # Convert start to datetime
    gfm_df["start"] = pd.to_datetime(gfm_df["start"], errors="coerce")
    
    # Check for invalid dates
    invalid_dates = gfm_df["start"].isna().sum()
    print(f"Records with invalid start dates: {invalid_dates}")
    
    # Check available years
    years = gfm_df["start"].dt.year.unique()
    print("GFM Years:", years)
    
    # Filter for 2014–2023
    floods = gfm_df[gfm_df["start"].dt.year.between(2014, 2023)].copy()
    print(f"Records after year filter (2014–2023): {len(floods)}")
    if floods.empty:
        print("No floods found in GFM data for 2014–2023.")
        floods.to_csv(output_file, index=False)
        exit()

    # Load GeoNames data
    geonames_df = pd.read_csv(geonames_file, sep="\t", header=None, 
                              usecols=[0, 1, 4, 5, 10], 
                              names=["geonameid", "name", "latitude", "longitude", "admin1_code"],
                              encoding="utf-8")
    geonames_df["geonameid"] = geonames_df["geonameid"].astype(str)
    print(f"GeoNames Records: {len(geonames_df)}")
    
    # Merge GFM with GeoNames
    merged_df = floods.merge(geonames_df, left_on="location_ID", right_on="geonameid", how="left")
    print(f"Records after GeoNames merge: {len(merged_df)}")
    
    # Map admin1_code to state names
    merged_df["location"] = merged_df["admin1_code"].map(admin1_map).fillna("Unknown")
    
    # Check unmapped locations
    unmapped_count = (merged_df["location"] == "Unknown").sum()
    print(f"Unmapped locations (before spatial join): {unmapped_count}")
    
    # Spatial join for unmapped locations
    unmapped = merged_df[merged_df["location"] == "Unknown"]
    if not unmapped.empty:
        unmapped = unmapped[unmapped["latitude"].notnull() & unmapped["longitude"].notnull()]
        print(f"Unmapped records with valid coordinates: {len(unmapped)}")
        if not unmapped.empty:
            geometry = [Point(xy) for xy in zip(unmapped["longitude"], unmapped["latitude"])]
            gdf_unmapped = gpd.GeoDataFrame(unmapped, geometry=geometry, crs="EPSG:4326")
            
            # Load Nigeria states shapefile
            gdf_states = gpd.read_file(shapefile)
            gdf_states = gdf_states.to_crs("EPSG:4326")
            
            # Spatial join
            gdf_joined = gpd.sjoin(gdf_unmapped, gdf_states, how="left", predicate="intersects")
            merged_df.loc[merged_df["location"] == "Unknown", "location"] = gdf_joined["NAME_1"].fillna("Unknown")
    
    # Assign severity based on duration
    merged_df["end"] = pd.to_datetime(merged_df["end"], errors="coerce")
    merged_df["duration"] = (merged_df["end"] - merged_df["start"]).dt.days
    merged_df["severity"] = merged_df["duration"].apply(
        lambda x: "High" if pd.notna(x) and x > 5 else "Moderate" if pd.notna(x) else "Unknown"
    )
    
    # Select and rename columns
    output_df = merged_df[["start", "location", "severity"]].copy()
    output_df["country"] = "Nigeria"
    output_df.rename(columns={"start": "date"}, inplace=True)
    
    # Standardize date
    output_df["date"] = pd.to_datetime(output_df["date"]).dt.strftime("%Y-%m-%d")
    
    # Filter for target states
    target_states = list(admin1_map.values())
    output_df = output_df[output_df["location"].isin(target_states)].copy()
    print(f"Records after location filter: {len(output_df)}")
    
    # Save
    output_df.to_csv(output_file, index=False)
    print(f"GFM data saved to {output_file}: {len(output_df)} records")
    
    # Summary statistics
    print("Severity Distribution:\n", output_df["severity"].value_counts())
    print("Records by State:\n", output_df["location"].value_counts())
    print(f"Date Range: {output_df['date'].min()} to {output_df['date'].max()}")

except Exception as e:
    print(f"Error processing GFM data: {e}")