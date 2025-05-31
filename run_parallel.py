from sqlalchemy import create_engine
import subprocess
import sys
import os
import pandas as pd
from utils.pkey_gen import custom_job_id

sources = ["jobnetmm", "jobsdbth", "jobsdbsg", "founditsg", "jobstreetmalay"]
procs = []

for source in sources:
    print(f"🚀 Starting ETL for {source} ...")
    # Log output to separate files for each source
    procs.append(subprocess.Popen(
        [sys.executable, "main.py", "--source", source]
    ))

# Wait for all subprocesses to complete
for proc in procs:
    proc.wait()

print("Combining data from all sources...")

dfs = []
url = os.getenv("DATABASE_URL")
engine = create_engine(url)

for source in sources:
    table_name = f"{source}_transformed"
    try:
        df = pd.read_sql_table(table_name, con=engine)
        dfs.append(df)
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")

## Combine all DataFrames into one
if dfs:
    full_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined DataFrame shape: {full_df.shape}")
    full_df = custom_job_id(full_df)
    print("Custom job IDs generated.")
    print(full_df['job_id'].head())
    print(full_df.columns)

    try:
        full_df.to_sql("IT", schema="IT_jobs", con=engine, if_exists='replace', index=False)
        print("Data loaded into IT_jobs table.")
    except Exception as e:
        print(f"Error loading data into IT_jobs: {e}")
    finally:
        engine.dispose()

else:
    print("No data to combine. Please check the individual source tables.")
# Clean up subprocesses
for proc in procs:
    if proc.poll() is None:  # If the process is still running
        proc.terminate()  # Terminate the process
        print(f"Terminated process {proc.pid} for source {source}.")