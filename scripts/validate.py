
import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv



# Initialize Supabase client

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
       
        print("⚠️  SUPABASE_URL or SUPABASE_KEY not set - skipping Supabase checks.")
        return None

    return create_client(url, key)



# Fetch data count from Supabase

def supabase_row_count(table_name="churn_data"):
    supabase = get_supabase_client()
    if supabase is None:
        print("ℹ️  Supabase client not available - cannot fetch remote row count.")
        return None

    try:
        response = supabase.table(table_name).select("id", count="exact").execute()
        # response may be an object or dict depending on supabase client version
        if hasattr(response, "count"):
            return response.count
        if isinstance(response, dict) and response.get("count") is not None:
            return response.get("count")
        
        if isinstance(response, dict) and isinstance(response.get("data"), list):
            return len(response.get("data"))
    except Exception as e:
        print(f"⚠️  Error fetching Supabase row count: {e}")
        return None



# VALIDATION LOGIC

def run_validation():

    print("\n===============================")
    print("🔍 STARTING DATA VALIDATION")
    print("===============================\n")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_path = os.path.join(base_dir, "data", "staged", "churn_staged.csv")

    if not os.path.exists(staged_path):
        print("❌ ERROR: Transformed file not found. Run transform.py first.")
        return

    df = pd.read_csv(staged_path)

    validation_results = {}

   
    # 1️⃣ No missing values in required columns
   
    required_cols = ["tenure", "MonthlyCharges", "TotalCharges"]

    missing_check = df[required_cols].isnull().sum()
    validation_results["missing_values"] = missing_check.sum() == 0

    print("📌 Missing value check:")
    print(missing_check, "\n")

    
    # 2️⃣ Row count equals original rows
    local_row_count = len(df)
    validation_results["local_row_count"] = local_row_count

    print(f"📌 Local row count: {local_row_count}")

    
    # 3️⃣ Row count = Supabase row count
    sb_count = supabase_row_count()
    validation_results["supabase_row_count"] = sb_count

    print(f"📌 Supabase row count: {sb_count}")

    validation_results["row_count_match"] = (local_row_count == sb_count)

    # 4️⃣ All tenure_group categories exist
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    actual_tenure_groups = set(df["tenure_group"].unique())

    validation_results["tenure_group_ok"] = (
        expected_tenure_groups == actual_tenure_groups
    )

    print(f"📌 Tenure groups found: {actual_tenure_groups}")

    # 5️⃣ All monthly_charge_segment categories exist
    expected_segments = {"Low", "Medium", "High"}
    actual_segments = set(df["monthly_charge_segment"].unique())

    validation_results["monthly_segment_ok"] = (
        expected_segments == actual_segments
    )

    print(f"📌 Charge segments found: {actual_segments}")

    # 6️⃣ Contract code must be only {0,1,2}
    actual_codes = set(df["contract_type_code"].unique())
    validation_results["contract_code_ok"] = actual_codes.issubset({0, 1, 2})

    print(f"📌 Contract codes found: {actual_codes}\n")

    # 📊 Final Summary
    print("\n===============================")
    print("📊 VALIDATION SUMMARY")
    print("===============================\n")

    print(f"Missing values OK: {validation_results['missing_values']}")
    print(f"Local row count: {local_row_count}")
    print(f"Supabase row count: {sb_count}")
    print(f"Row count match: {validation_results['row_count_match']}")
    print(f"Tenure groups OK: {validation_results['tenure_group_ok']}")
    print(f"Charge segments OK: {validation_results['monthly_segment_ok']}")
    print(f"Contract codes valid: {validation_results['contract_code_ok']}")

    print("\n===============================")
    print("🎉 VALIDATION COMPLETE")
    print("===============================\n")


if __name__ == "__main__":
    run_validation()
