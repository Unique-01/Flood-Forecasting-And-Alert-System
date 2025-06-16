import pandas as pd

# Input and output files
dartmouth_file = "data/historical_floods.csv"
gfm_file = "data/gfm_floods.csv"
output_file = "data/historical_floods_merged.csv"

try:
    # Load datasets
    dartmouth_df = pd.read_csv(dartmouth_file)
    gfm_df = pd.read_csv(gfm_file)

    # Standardize columns
    dartmouth_df = dartmouth_df[["date", "country", "location", "severity"]].copy()
    gfm_df = gfm_df[["date", "country", "location", "severity"]].copy()

    # Normalize severity
    severity_map = {
        "Low": "Low",
        "Medium": "Medium",
        "High": "High",
        "Moderate": "Medium",  # Map GFM's Moderate to Medium
        "Unknown": "Unknown"
    }
    dartmouth_df["severity"] = dartmouth_df["severity"].map(severity_map).fillna("Unknown")
    gfm_df["severity"] = gfm_df["severity"].map(severity_map).fillna("Unknown")

    # Concatenate
    merged_df = pd.concat([dartmouth_df, gfm_df], ignore_index=True)

    # Remove duplicates (same date and location)
    merged_df.drop_duplicates(subset=["date", "location"], keep="first", inplace=True)

    # Save
    merged_df.to_csv(output_file, index=False)
    print(f"Merged floods saved to {output_file}: {len(merged_df)} records")

    # Summarize
    merged_df["date"] = pd.to_datetime(merged_df["date"], errors="coerce")
    merged_df["year"] = merged_df["date"].dt.year
    print("\nMerged Floods by Year:\n", merged_df["year"].value_counts().sort_index())
    print("\nMerged Floods by State:\n", merged_df["location"].value_counts())

except Exception as e:
    print(f"Error merging floods: {e}")