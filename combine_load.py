from sqlalchemy import create_engine
import os
import pandas as pd
from utils.pkey_gen import custom_job_id

# List of source names (same as your main script)
sources = ["jobnetmm", "jobsdbth", "jobsdbsg", "founditsg", "jobstreetmalay"]

# Read the database URL from environment (injected by GitHub Actions)
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set. Make sure it is configured in GitHub Secrets.")

# Create engine
engine = create_engine(DATABASE_URL)

# Collect all transformed dataframes
dfs = []

for source in sources:
    table_name = f"{source}_transformed"
    try:
        df = pd.read_sql_table(table_name, con=engine)
        dfs.append(df)
        print(f"Loaded {table_name} with shape {df.shape}")
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")

# Combine and write
if dfs:
    full_df = pd.concat(dfs, ignore_index=True)
    print(f"\nCombined DataFrame shape: {full_df.shape}")

    # Generate custom job IDs
    full_df = custom_job_id(full_df)
    print("🔧 Custom job IDs generated.")
    print(full_df[['job_id']].head())

    try:
        full_df.to_sql("IT", schema="IT_jobs", con=engine, if_exists='replace', index=False)
        print("Data loaded into IT_jobs.IT successfully.")
    except Exception as e:
        print(f"Error writing combined data to IT_jobs.IT: {e}")
    finally:
        engine.dispose()
else:
    print("No dataframes loaded — skipping combine operation.")
