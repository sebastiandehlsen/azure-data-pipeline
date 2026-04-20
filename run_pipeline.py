from pipeline import run_transformation
from sql_step import run_sql
import subprocess


def run_upload():
    subprocess.run(["python", "upload_to_azure.py"], check=True)


if __name__ == "__main__":
    print("Starting pipeline...")

    run_transformation()
    print("Transformation done")

    run_sql()
    print("SQL aggregation done")

    run_upload()
    print("Upload done")

    print("Pipeline completed")