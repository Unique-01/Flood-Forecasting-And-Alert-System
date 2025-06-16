import pandas as pd

try:
    train = pd.read_csv("data/train_data_with_features.csv")
    test = pd.read_csv("data/test_data_with_features.csv")

    print("Train Data Preview:\n", train.head())
    print("Test Data Preview:\n", test.head())
    print("Train Columns:", train.columns.tolist())
    print("Test Columns:", test.columns.tolist())
except Exception as e:
    print(f"Error: {e}")

