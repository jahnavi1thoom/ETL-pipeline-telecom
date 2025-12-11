import os
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    # Load the Telco raw data
    source_path = r"C:\Users\jahna\Downloads\WA_Fn-UseC_-Telco-Customer-Churn.csv"

    df = pd.read_csv(source_path)

    # Save raw data inside ETL folder
    raw_path = os.path.join(data_dir, "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    df.to_csv(raw_path, index=False)

    print(f"✅ Data extracted and saved at: {raw_path}")
    return raw_path

if __name__ == "__main__":
    extract_data()
