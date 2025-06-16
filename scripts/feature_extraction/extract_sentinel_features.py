import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from datetime import datetime

def extract_sentinel_features(db_path):
    """
    Count Sentinel images per region and week, store in database.
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Create table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sentinel_features (
                region TEXT,
                week_start_date TEXT,
                image_count INTEGER,
                feature_date TEXT,
                PRIMARY KEY (region, week_start_date)
            );
        """)
        
        # Query and deduplicate
        df = pd.read_sql("SELECT image_id, date, region FROM sentinel_metadata;", conn)
        df = df.drop_duplicates(subset=["image_id"])
        df["date"] = pd.to_datetime(df["date"], unit="ms", errors="coerce")
        df["week_start_date"] = df["date"].dt.to_period("W").dt.start_time
        
        # Aggregate
        counts = df.groupby(["region", "week_start_date"]).size().reset_index(name="image_count")
        counts["feature_date"] = datetime.now().strftime("%Y-%m-%d")
        counts["week_start_date"] = counts["week_start_date"].dt.strftime("%Y-%m-%d")
        
        # Insert
        for _, row in counts.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO sentinel_features (region, week_start_date, image_count, feature_date)
                VALUES (?, ?, ?, ?);
            """, (row["region"], row["week_start_date"], row["image_count"], row["feature_date"]))
        
        conn.commit()
        conn.close()
        print("Extracted Sentinel features.")
        
        # Preview
        engine = create_engine(f"sqlite:///{db_path}")
        preview = pd.read_sql("SELECT * FROM sentinel_features;", engine)
        print("Sentinel Features Preview:\n", preview)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_file = "data/flood_data.db"
    extract_sentinel_features(db_file)