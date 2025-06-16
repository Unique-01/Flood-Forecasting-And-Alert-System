import geopandas as gpd
import os

def correct_geojson(input_path, output_path):
    """
    Filter GeoJSON features for Lagos bounds.
    Args:
        input_path (str): Input GeoJSON file.
        output_path (str): Output corrected GeoJSON file.
    Returns:
        int: Number of filtered features.
    """
    try:
        gdf = gpd.read_file(input_path)
        gdf = gdf.to_crs(epsg=4326)
        lagos_bounds = gdf.cx[3.0:3.6, 6.0:6.8]
        if lagos_bounds.empty:
            raise ValueError("No features in Lagos bounds.")
        lagos_bounds.to_file(output_path, driver="GeoJSON")
        print(f"Saved {len(lagos_bounds)} features to {output_path}")
        return len(lagos_bounds)
    except Exception as e:
        print(f"Error: {e}")
        return 0

if __name__ == "__main__":
    input_file = "data/lagos_landuse_cleaned.geojson"
    output_file = "data/lagos_landuse_corrected.geojson"
    if not os.path.exists(input_file):
        print(f"Input file {input_file} not found.")
    else:
        num_features = correct_geojson(input_file, output_file)
        print(f"Processed {num_features} features.")