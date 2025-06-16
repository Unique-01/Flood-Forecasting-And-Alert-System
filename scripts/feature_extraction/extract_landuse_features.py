import geopandas as gpd
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import os

def extract_land_use_features(db_path):
    """
    Compute area of each land use type per state and update socioeconomic table.
    Args:
        db_path (str): SQLite database path.
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Check if area_sqm exists
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(socioeconomic);")
        columns = [col[1] for col in cursor.fetchall()]
        if "area_sqm" not in columns:
            conn.execute("ALTER TABLE socioeconomic ADD COLUMN area_sqm REAL;")
            print("Added area_sqm column to socioeconomic table.")
        
        states = ["lagos", "rivers", "benue", "bayelsa"]
        for state in states:
            try:
                # Use cleaned GeoJSONs
                file_path = f"data/{state}_landuse_cleaned_valid.geojson"
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"GeoJSON file {file_path} not found.")
                gdf = gpd.read_file(file_path)
                gdf = gdf.to_crs(gdf.estimate_utm_crs())  # Project to UTM
                gdf["area_sqm"] = gdf["geometry"].area
                
                # Aggregate by landuse
                areas = gdf.groupby("landuse")["area_sqm"].sum().reset_index()
                areas["state"] = state.title()
                
                # Update socioeconomic table
                for _, row in areas.iterrows():
                    conn.execute("""
                        UPDATE socioeconomic
                        SET area_sqm = ?
                        WHERE state = ? AND landuse_type = ?;
                    """, (row["area_sqm"], row["state"], row["landuse"]))
                
                print(f"Updated socioeconomic areas for {state}")
            except Exception as e:
                print(f"Error processing {state}: {e}")
        
        conn.commit()
        conn.close()
        
        # Preview
        engine = create_engine(f"sqlite:///{db_path}")
        preview = pd.read_sql("SELECT * FROM socioeconomic;", engine)
        print("Socioeconomic Preview:\n", preview)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_file = "data/flood_data.db"
    extract_land_use_features(db_file)