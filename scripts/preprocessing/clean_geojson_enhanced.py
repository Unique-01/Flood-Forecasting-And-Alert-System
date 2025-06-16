import geopandas as gpd
from shapely.validation import make_valid
import os
# import ogr

# Set OGR to reject unclosed rings
os.environ["OGR_GEOMETRY_ACCEPT_UNCLOSED_RING"] = "NO"

# Directory for GeoJSON files
data_dir = "data/"

# States to process
states = ["rivers", "benue"]  # Focus on problematic states

for state in states:
    input_file = f"{data_dir}{state}_landuse.geojson"
    output_file = f"{data_dir}{state}_landuse_cleaned.geojson"
    
    try:
        # Read GeoJSON with Pyogrio to enforce strict geometry checking
        gdf = gpd.read_file(input_file, engine="pyogrio")
        print(f"Original {state.capitalize()}: {len(gdf)} features")

        # Filter out invalid or empty geometries
        gdf = gdf[gdf["geometry"].notnull() & gdf["geometry"].is_valid]

        # Clean remaining geometries
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: make_valid(geom) if geom else None)

        # Remove any remaining invalid or empty geometries
        gdf = gdf[gdf["geometry"].notnull() & gdf["geometry"].is_valid]

        # Save cleaned GeoJSON
        gdf.to_file(output_file, driver="GeoJSON", engine="pyogrio")
        print(f"Cleaned {state.capitalize()}: {len(gdf)} features saved to {output_file}")

    except Exception as e:
        print(f"Error processing {state.capitalize()}: {e}")

# Verify all cleaned files
for state in ["lagos", "rivers", "benue", "bayelsa"]:
    try:
        gdf = gpd.read_file(f"{data_dir}{state}_landuse_cleaned.geojson")
        print(f"Verified {state.capitalize()}: {len(gdf)} features")
    except Exception as e:
        print(f"Verification failed for {state.capitalize()}: {e}")