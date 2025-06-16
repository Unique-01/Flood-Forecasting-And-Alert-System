import sqlite3
import pandas as pd

def check_database(db_path):
    """
    Check land_use and sentinel_metadata tables in database.
    Args:
        db_path (str): SQLite database path.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite")
        
        # Check land_use table
        land_use_count = pd.read_sql("SELECT COUNT(*) as count FROM land_use;", conn).iloc[0]["count"]
        land_use_sample = pd.read_sql("SELECT id, landuse FROM land_use LIMIT 5;", conn)
        print(f"Land Use Table: {land_use_count} rows")
        print("Sample:\n", land_use_sample)
        
        # Check sentinel_metadata table
        sentinel_count = pd.read_sql("SELECT COUNT(*) as count FROM sentinel_metadata;", conn).iloc[0]["count"]
        sentinel_sample = pd.read_sql("SELECT image_id, date, region FROM sentinel_metadata LIMIT 5;", conn)
        print(f"Sentinel Metadata Table: {sentinel_count} rows")
        print("Sample:\n", sentinel_sample)
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

def initialize_sentinel_metadata(db_path, csv_path):
    """
    Initialize sentinel_metadata table from CSV if empty.
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_csv(csv_path)
        
        # Deduplicate entries
        df = df.drop_duplicates(subset=["image_id"])
        
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO sentinel_metadata (image_id, date, region)
                VALUES (?, ?, ?);
            """, (row["image_id"], row["date"], row["region"]))
        
        conn.commit()
        conn.close()
        print("Initialized sentinel_metadata table.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_file = "data/flood_data.db"
    sentinel_csv = "data/sentinel_metadata.csv"
    
    print("Checking database...")
    check_database(db_file)
    
    # Reinitialize sentinel_metadata if needed
    sentinel_count = pd.read_sql("SELECT COUNT(*) as count FROM sentinel_metadata;", sqlite3.connect(db_file)).iloc[0]["count"]
    if sentinel_count == 0:
        print("Sentinel metadata table empty. Initializing...")
        initialize_sentinel_metadata(db_file, sentinel_csv)