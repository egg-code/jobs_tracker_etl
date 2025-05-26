import subprocess
import sys

sources = ["jobnetmm", "jobsdbsg", "jobsdbth", "founditsg", "jobstreetmalay"]
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

print("✅ All ETL processes completed.")