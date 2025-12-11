# ===========================
# load.py
# ===========================
# Purpose: Load transformed Titanic dataset into Supabase using Supabase client
 
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import re
import time
 
# Initialize Supabase client
def get_supabase_client():
    """Initialize and return Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        # Do not raise here so script can run locally without Supabase configured.
        print("⚠️  SUPABASE_URL or SUPABASE_KEY not set in environment. Running in local fallback mode.")
        return None

    return create_client(url, key)
 
# # ------------------------------------------------------
# # Step 1: Create table if not exists
# # ------------------------------------------------------
def create_table_if_not_exists():
    """
    Ensures the titanic_data table exists in Supabase.
    """
    try:
        supabase = get_supabase_client()

        if supabase is None:
            print("ℹ️  Supabase not configured — skipping remote table creation.")
            return

        # Try to create the table using raw SQL
        create_table_sql = """
       

CREATE TABLE public.churn_data (
    id BIGSERIAL PRIMARY KEY,

    -- Original dataset columns
    SeniorCitizen INTEGER,
    Partner TEXT,
    Dependents TEXT,
    tenure INTEGER,
    PhoneService TEXT,
    MultipleLines TEXT,
    InternetService TEXT,
    OnlineSecurity TEXT,
    OnlineBackup TEXT,
    DeviceProtection TEXT,
    TechSupport TEXT,
    StreamingTV TEXT,
    StreamingMovies TEXT,
    Contract TEXT,
    PaperlessBilling TEXT,
    PaymentMethod TEXT,
    MonthlyCharges DOUBLE PRECISION,
    TotalCharges DOUBLE PRECISION,
    Churn TEXT,

    -- Engineered features
    tenure_group TEXT,
    monthly_charge_segment TEXT,
    has_internet_service INTEGER,
    is_multi_line_user INTEGER,
    contract_type_code INTEGER
);

 
        """
       
        try:
            # Execute raw SQL to create table (may not be available depending on Supabase setup)
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("✅ Table 'churn_data' created or already exists")
        except Exception as e:
            # Non-fatal: table creation may not be allowed via RPC; we'll continue and let inserts fail gracefully.
            print(f"ℹ️  Note creating table: {e}")
            print("ℹ️  Table creation skipped — will try inserts (they may fail if table doesn't exist)")
 
    except Exception as e:
        print(f"⚠️  Error checking/creating table: {e}")
        print("ℹ️  Trying to continue with data insertion...")
 
# ------------------------------------------------------
# Step 2: Load CSV data into Supabase table
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "churn_data", batch_size: int = 200, max_retries: int = 3, backoff_factor: float = 2.0):
    """
    Load a transformed CSV into a Supabase table.
 
    Args:
        staged_path (str): Path to the transformed CSV file.
        table_name (str): Supabase table name. Default is 'churn_data'.
    """
    # Convert to absolute path
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
   
    print(f"🔍 Looking for data file at: {staged_path}")
   
    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return
 
    try:
        # Initialize Supabase client
        supabase = get_supabase_client()

        # Read the CSV
        df = pd.read_csv(staged_path)
        # Normalize column names to snake_case lower-case to match PostgREST/Postgres naming
        def _normalize_col(c: str) -> str:
            # Add underscore between camelCase boundaries, replace non-alphanum with underscore, lowercase
            s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', str(c))
            s = re.sub(r'[^0-9a-zA-Z_]+', '_', s)
            return s.strip('_').lower()

        original_cols = list(df.columns)
        normalized = [_normalize_col(c) for c in original_cols]

        # Map normalized names to the DB's expected identifier names.
        # Reason: your CREATE TABLE used unquoted CamelCase identifiers (e.g. SeniorCitizen) which
        # Postgres stores as lowercase without underscores (seniorcitizen). To match that, for any
        # original header that contained uppercase letters we will remove underscores from the normalized name.
        mapped = []
        for orig, norm in zip(original_cols, normalized):
            if any(ch.isupper() for ch in str(orig)):
                # remove underscores to match unquoted CamelCase->lowercase concatenation in Postgres
                db_name = norm.replace("_", "")
            else:
                db_name = norm
            mapped.append(db_name)

        df.columns = mapped
        # Print a short mapping sample for debugging
        sample_map = {o: n for o, n in zip(original_cols[:12], df.columns[:12])}
        print(f"🔁 Header mapping sample (orig -> db): {sample_map}")
        total_rows = len(df)

        print(f"📊 Loading {total_rows} rows into '{table_name}'...")

        # If Supabase not configured, fall back to writing a local copy and exit successfully.
        if supabase is None:
            out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "loaded"))
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, os.path.basename(staged_path).replace('.csv', '_localcopy.csv'))
            df.to_csv(out_path, index=False)
            print(f"✅ Supabase not configured. Wrote local copy to: {out_path}")
            return

        # Process in batches
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size].copy()
            # Convert NaN to None for proper NULL handling
            batch = batch.where(pd.notnull(batch), None)
            records = batch.to_dict('records')

            # If Supabase not configured, write a local copy and exit early (already handled above) — keep defensive check
            if supabase is None:
                out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "loaded"))
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, os.path.basename(staged_path).replace('.csv', '_localcopy.csv'))
                df.to_csv(out_path, index=False)
                print(f"✅ Supabase not configured. Wrote local copy to: {out_path}")
                return

            attempt = 0
            while attempt <= max_retries:
                try:
                    response = supabase.table(table_name).insert(records).execute()
                    # Handle supabase-py response / PostgREST errors
                    if isinstance(response, dict) and response.get('error'):
                        err = response.get('error')
                        err_str = str(err)
                        print(f"⚠️  Insert error in batch {i//batch_size + 1} (attempt {attempt+1}): {err_str}")
                        # Schema issues should abort and write local copy
                        if 'Could not find' in err_str or 'PGRST' in err_str or 'column' in err_str:
                            print("ℹ️  Detected remote schema issue — writing local copy instead and aborting remote inserts.")
                            out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "loaded"))
                            os.makedirs(out_dir, exist_ok=True)
                            out_path = os.path.join(out_dir, os.path.basename(staged_path).replace('.csv', '_localcopy.csv'))
                            df.to_csv(out_path, index=False)
                            print(f"✅ Wrote local copy to: {out_path}")
                            return
                        # otherwise retry
                        attempt += 1
                        if attempt > max_retries:
                            print(f"❌ Failed to insert batch {i//batch_size + 1} after {max_retries} retries. Skipping batch.")
                            break
                        wait = backoff_factor ** attempt
                        print(f"🔁 Retrying batch {i//batch_size + 1} after {wait:.1f}s...")
                        time.sleep(wait)
                        continue
                    else:
                        end = min(i + batch_size, total_rows)
                        print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
                        break

                except Exception as e:
                    err_str = str(e)
                    print(f"⚠️  Exception inserting batch {i//batch_size + 1} (attempt {attempt+1}): {err_str}")
                    # If schema-related, abort to local copy
                    if 'Could not find' in err_str or 'PGRST' in err_str or 'column' in err_str:
                        print("ℹ️  Detected remote schema issue during insert — writing local copy instead and aborting remote inserts.")
                        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "loaded"))
                        os.makedirs(out_dir, exist_ok=True)
                        out_path = os.path.join(out_dir, os.path.basename(staged_path).replace('.csv', '_localcopy.csv'))
                        df.to_csv(out_path, index=False)
                        print(f"✅ Wrote local copy to: {out_path}")
                        return

                    attempt += 1
                    if attempt > max_retries:
                        print(f"❌ Failed to insert batch {i//batch_size + 1} after {max_retries} retries due to exceptions. Skipping batch.")
                        break
                    wait = backoff_factor ** attempt
                    print(f"🔁 Retrying batch {i//batch_size + 1} after {wait:.1f}s due to exception...")
                    time.sleep(wait)
                    continue

        print(f"🎯 Finished loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
 
# ------------------------------------------------------
# Step 3: Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    # Path relative to the script location
    staged_csv_path = os.path.join("..", "data", "staged", "churn_staged.csv")
    create_table_if_not_exists()  # Ensure table exists
    load_to_supabase(staged_csv_path)
 