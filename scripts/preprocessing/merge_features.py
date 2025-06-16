import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Enable future pandas behavior
pd.set_option('future.no_silent_downcasting', True)

def find_closest_weather_date(target_date, city, weather):
    """Find the closest weather window_start_date for a city."""
    try:
        target_date = pd.to_datetime(target_date, errors='coerce')
        if pd.isna(target_date):
            print(f"Invalid target date for {city}: {target_date}")
            return None
        city_weather = weather[weather["city"] == city]
        if city_weather.empty:
            print(f"No weather data for city: {city}")
            return None
        weather_dates = pd.to_datetime(city_weather["window_start_date"], errors='coerce').dropna()
        if len(weather_dates) == 0:
            print(f"No valid weather dates for city: {city}")
            return None
        diff = np.abs((weather_dates - target_date).dt.days)
        idx = diff.idxmin()
        closest_date = weather_dates.loc[idx]
        print(f"Closest date for {target_date} in {city}: {closest_date} ({diff.loc[idx]} days)")
        return closest_date
    except Exception as e:
        print(f"Error in find_closest_weather_date for {target_date}, {city}: {e}")
        return None

def merge_features(db_path, train_path, test_path, output_train, output_test):
    """Merge socioeconomic, Sentinel, and weather features with train/test data."""
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        
        # Load socioeconomic features
        socio = pd.read_sql("SELECT state, landuse_type, area_sqm FROM socioeconomic;", engine)
        print("Socioeconomic Preview:\n", socio.head())
        socio_pivot = socio.pivot(index="state", columns="landuse_type", values="area_sqm").fillna(0)
        socio_pivot.columns = [f"area_{col.lower().replace(' ', '_')}" for col in socio_pivot.columns]
        socio_pivot.reset_index(inplace=True)
        socio_pivot["state"] = socio_pivot["state"].str.lower()
        print("Socioeconomic Pivot Preview:\n", socio_pivot.head())
        
        # Load Sentinel features
        sentinel = pd.read_sql("SELECT region, week_start_date, image_count FROM sentinel_features;", engine)
        print("Sentinel Preview:\n", sentinel.head())
        sentinel_pivot = sentinel.pivot(index="region", columns="week_start_date", values="image_count").fillna(0)
        sentinel_pivot.columns = [f"images_{col}" for col in sentinel_pivot.columns]
        sentinel_pivot.reset_index(inplace=True)
        sentinel_pivot["region"] = sentinel_pivot["region"].str.lower()
        print("Sentinel Pivot Preview:\n", sentinel_pivot.head())
        
        # Load weather features
        weather = pd.read_sql("SELECT city, window_start_date, avg_precipitation_7d, avg_temperature_7d, avg_humidity_7d, avg_precipitation_30d FROM weather_features;", engine)
        print("Weather Preview:\n", weather.head())
        weather["city"] = weather["city"].str.lower()
        weather["window_start_date"] = pd.to_datetime(weather["window_start_date"], errors='coerce')
        print("Unique Weather Cities:", weather["city"].unique())
        print("Unique Weather Dates:", weather["window_start_date"].dropna().unique())
        
        # Compute mean weather features per city
        weather_means = weather.groupby("city")[["avg_precipitation_7d", "avg_temperature_7d", "avg_humidity_7d", "avg_precipitation_30d"]].mean().reset_index()
        print("Weather Means Preview:\n", weather_means)
        
        # Load train/test data
        train_df = pd.read_csv(train_path)
        test_df = pd.read_csv(test_path)
        print("Train Preview:\n", train_df[["date", "location", "severity"]].head())
        print("Test Preview:\n", test_df[["date", "location", "severity"]].head())
        train_df["location"] = train_df["location"].str.lower()
        test_df["location"] = test_df["location"].str.lower()
        train_df["date"] = pd.to_datetime(train_df["date"], errors='coerce')
        test_df["date"] = pd.to_datetime(test_df["date"], errors='coerce')
        print("Unique Train Locations:", train_df["location"].unique())
        
        # Merge socioeconomic and sentinel
        for df, df_type in [(train_df, "train_df"), (test_df, "test_df")]:
            # Merge socioeconomic
            merged_df = df.merge(socio_pivot, left_on="location", right_on="state", how="left")
            merged_df.drop(columns=["state"], errors='ignore', inplace=True)
            # Merge Sentinel
            merged_df = merged_df.merge(sentinel_pivot, left_on="location", right_on="region", how="left")
            merged_df.drop(columns=["region"], errors='ignore', inplace=True)
            # Update original DataFrame
            if df_type == "train_df":
                train_df = merged_df
            else:
                test_df = merged_df
            print(f"{df_type} After Socio/Sentinel Merge:\n", train_df[["date", "location", "area_residential", "images_2025-06-02"]].head())
        
        # Weather columns to merge
        weather_columns = ["avg_precipitation_7d", "avg_temperature_7d", "avg_humidity_7d", "avg_precipitation_30d"]
        
        # Merge weather for train and test
        for df, df_type in [(train_df, "train_df"), (test_df, "test_df")]:
            # Compute closest weather date
            df["closest_weather_date"] = df.apply(
                lambda row: find_closest_weather_date(row["date"], row["location"], weather), axis=1
            )
            print(f"Sample Closest Dates for {df_type}:\n", df[["date", "location", "closest_weather_date"]].head())
            
            # Initialize weather columns
            for col in weather_columns:
                df[col] = np.nan
            
            # Date-based merge
            if df["closest_weather_date"].notna().any():
                merge_df = df.merge(
                    weather[["city", "window_start_date"] + weather_columns],
                    left_on=["location", "closest_weather_date"],
                    right_on=["city", "window_start_date"],
                    how="left",
                    suffixes=('', '_weather')
                )
                # Update weather columns with merged values
                for col in weather_columns:
                    df[col] = merge_df[col].fillna(df[col])
                # Drop merge artifacts
                df.drop(columns=["window_start_date", "city"], errors='ignore', inplace=True)
            else:
                print(f"No valid closest_weather_date for {df_type}, skipping date-based merge")
            print(f"{df_type} After Date-Based Weather Merge:\n", df[["date", "location"] + weather_columns].head())
            
            # Fallback: Merge mean weather features
            merge_df = df.merge(
                weather_means[["city"] + weather_columns],
                left_on="location",
                right_on="city",
                how="left",
                suffixes=('', '_mean')
            )
            # Fill NaNs with mean values
            for col in weather_columns:
                df[col] = df[col].fillna(merge_df[f"{col}_mean"])
                df.drop(columns=[f"{col}_mean"], errors='ignore', inplace=True)
            df.drop(columns=["city"], errors='ignore', inplace=True)
            print(f"{df_type} After Fallback Weather Merge:\n", df[["date", "location"] + weather_columns].head())
            
            # Drop closest_weather_date
            df.drop(columns=["closest_weather_date"], errors='ignore', inplace=True)
            
            # Validate weather columns
            print(f"{df_type} Weather Validation After Merge:\n", df[weather_columns].describe())
        
        # Final cleanup
        for df, df_type in [(train_df, "train_df"), (test_df, "test_df")]:
            # Drop any residual merge artifacts
            columns_to_drop = [col for col in ["city_x", "city_y", "state", "region", "window_start_date", "closest_weather_date"] if col in df.columns]
            if columns_to_drop:
                df.drop(columns=columns_to_drop, inplace=True)
                print(f"Dropped residual columns in {df_type}: {columns_to_drop}")
            # Fill NaNs for non-weather columns
            non_weather_columns = [col for col in df.columns if col not in weather_columns]
            df[non_weather_columns] = df[non_weather_columns].fillna(0)
            # Ensure weather columns exist
            for col in weather_columns:
                if col not in df.columns:
                    df[col] = np.nan
            print(f"{df_type} Final Columns ({len(df.columns)}):", df.columns.tolist())
            print(f"{df_type} Final Weather Validation:\n", df[weather_columns].describe())
        
        # Save output
        train_df.to_csv(output_train, index=False)
        test_df.to_csv(output_test, index=False)
        print(f"Saved train features to {output_train}")
        print(f"Saved test features to {output_test}")
        
        # Final preview
        print("Train Features Preview:\n", train_df.head())
        print("Test Features Preview:\n", test_df.head())
        print("Train Columns:", train_df.columns.tolist())
        print("Test Columns:", test_df.columns.tolist())
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    db_file = "data/flood_data.db"
    train_file = "data/train_data.csv"
    test_file = "data/test_data.csv"
    output_train = "data/train_data_with_features.csv"
    output_test = "data/test_data_with_features.csv"
    
    merge_features(db_file, train_file, test_file, output_train, output_test)
