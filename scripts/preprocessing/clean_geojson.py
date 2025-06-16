import geopandas as gpd
import pandas as pd
import os

def clean_geojson(state, input_path, output_path):
    """
    Clean GeoJSON by removing invalid or empty geometries.
    Args:
        state (str): State name (e.g., 'lagos').
        input_path (str): Input GeoJSON path.
        output_path (str): Output cleaned GeoJSON path.
    Returns:
        int: Number of valid features.
    """
    try:
        gdf = gpd.read_file(input_path)
        gdf = gdf.to_crs(epsg=4326)
        
        # Remove empty geometries
        initial_count = len(gdf)
        gdf = gdf[~gdf["geometry"].is_empty]
        print(f"Removed {initial_count - len(gdf)} empty geometries for {state}.")
        
        # Fix invalid geometries
        gdf["geometry"] = gdf["geometry"].buffer(0)  # Fix self-intersections
        gdf = gdf[gdf["geometry"].is_valid]
        print(f"Kept {len(gdf)} valid geometries after fixing for {state}.")
        
        # Save cleaned GeoJSON
        gdf.to_file(output_path, driver="GeoJSON")
        print(f"Saved cleaned GeoJSON for {state} to {output_path}")
        
        return len(gdf)
    except Exception as e:
        print(f"Error cleaning {state} GeoJSON: {e}")
        return 0

if __name__ == "__main__":
    states = ["lagos", "rivers", "benue", "bayelsa"]
    for state in states:
        input_file = f"data/{'lagos_landuse_cleaned.geojson' if state == 'lagos' else f'{state}_landuse_cleaned.geojson'}"
        output_file = f"data/{state}_landuse_cleaned_valid.geojson"
        if not os.path.exists(input_file):
            print(f"Input file {input_file} not found.")
        else:
            num_features = clean_geojson(state, input_file, output_file)
            print(f"Processed {num_features} features for {state}.")