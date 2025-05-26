import subprocess
import sys

sources = ["jobnetmm", "jobsdbsg", "jobsdbth", "founditsg", "jobstreetmalay"]
procs = []

for source in sources:
    print(f"🚀 Starting ETL for {source} ...")
    # Log output to separate files for each source
    logfile = open(f"{source}_etl.log", "w")
    procs.append(subprocess.Popen(
        [sys.executable, "main.py", "--source", source],
        stdout=logfile,
        stderr=logfile
    ))

# Wait for all subprocesses to complete
for proc in procs:
    proc.wait()

print("✅ All ETL processes completed.")