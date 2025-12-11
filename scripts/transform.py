import os
import pandas as pd
import numpy as np

def transform_data(raw_path):
    # Base project directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Staged directory
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Load raw dataset
    df = pd.read_csv(raw_path)

    
    # 1️⃣ Handle Missing Values

    # Convert TotalCharges → numeric (spaces → NaN)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    # Fill numeric missing values with median
    num_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median())

    # Fill categorical with “Unknown”
    cat_cols = df.select_dtypes(include=["object"]).columns
    df[cat_cols] = df[cat_cols].fillna("Unknown")

    
    # 2️⃣ Feature Engineering
    

    # tenure_group
    bins = [-1, 12, 36, 60, np.inf]
    labels = ["New", "Regular", "Loyal", "Champion"]
    df["tenure_group"] = pd.cut(df["tenure"], bins=bins, labels=labels)

    # monthly_charge_segment
    df["monthly_charge_segment"] = np.where(
        df["MonthlyCharges"] < 30, "Low",
        np.where(df["MonthlyCharges"] <= 70, "Medium", "High")
    )

    # has_internet_service
    df["InternetService_norm"] = df["InternetService"].astype(str).str.lower().str.strip()
    df["has_internet_service"] = df["InternetService_norm"].apply(
        lambda x: 1 if x in ["dsl", "fiber optic", "fiberoptic", "fiber"] else 0
    )

    # is_multi_line_user
    df["is_multi_line_user"] = df["MultipleLines"].astype(str).str.lower().eq("yes").astype(int)

    # contract_type_code
    contract_map = {
        "month-to-month": 0,
        "one year": 1,
        "two year": 2
    }

    df["Contract_norm"] = df["Contract"].astype(str).str.lower().str.strip()
    df["contract_type_code"] = df["Contract_norm"].map(contract_map).fillna(-1).astype(int)

    # clean helper cols
    df.drop(columns=["InternetService_norm", "Contract_norm"], inplace=True, errors="ignore")

    
    # 3️⃣ Drop Unnecessary Fields
    
    df.drop(columns=["customerID", "gender"], inplace=True, errors="ignore")

    
    # 4️⃣ Save to Staged Folder
    
    staged_path = os.path.join(staged_dir, "churn_staged.csv")
    df.to_csv(staged_path, index=False)

    print(f"✅ Transformed data saved at: {staged_path}")
    return staged_path


# Run transform after extract
if __name__ == "__main__":
    from extract import extract_data
    raw_file = extract_data()   
    transform_data(raw_file)
