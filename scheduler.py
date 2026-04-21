import time
import subprocess

while True:
    print("Running pipeline...")
    subprocess.run(["python", "run_pipeline.py"])

    print("Sleeping for 60 seconds...")
    time.sleep(60)