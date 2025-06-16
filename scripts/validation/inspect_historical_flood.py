import pandas as pd
import sqlite3

conn = sqlite3.connect("data/flood_data.db")
floods = pd.read_sql("SELECT * FROM historical_floods;", conn)

print("Historical Floods Preview:\n", floods.head())
print("Historical Floods Columns:", floods.columns.tolist())
print("Severity Value Counts:\n", floods["severity"].value_counts() if "severity" in floods.columns else "No severity column")

conn.close()
