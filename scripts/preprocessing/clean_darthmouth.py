import pandas as pd

# Input and output files
input_file = "data/historical_floods.csv"
output_file = "data/historical_floods_cleaned.csv"

try:
    # Load CSV
    df = pd.read_csv(input_file)
    
    # Clean location
    df["location"] = df["location"].astype(str).fillna("Unknown")
    
    # Filter for target states (optional: comment out to keep all states)
    target_states = ["Lagos", "Rivers", "Benue", "Bayelsa"]
    df = df[df["location"].isin(target_states + ["Unknown"])].copy()
    
    # Standardize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"Cleaned Dartmouth floods saved to {output_file}: {len(df)} records")
    
except Exception as e:
    print(f"Error cleaning Dartmouth data: {e}")