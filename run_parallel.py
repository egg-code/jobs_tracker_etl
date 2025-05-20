import subprocess
import sys

sources = ["jobnetmm", "jobsdbsg", "jobsdbth", "founditsg", "jobstreetmalay"]

procs = []
for source in sources:
    print(f"Running {source} extraction...")
    procs.append(subprocess.Popen([sys.executable, "main.py", "--source", source]))

for proc in procs:
    proc.wait()

print("All extractions completed.")