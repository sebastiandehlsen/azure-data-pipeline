import json
from logger import get_logger
from tasks import Task
from pipeline import run_transformation
from sql_step import run_sql
from upload_to_azure import run_upload
from datetime import datetime


run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

logger = get_logger()

with open("config.json") as f:
    config = json.load(f)

max_retries = config["max_retries"]


def main():
    logger.info("Pipeline started")

    tasks = [
    Task("transform_data", lambda: run_transformation(run_id), retries=max_retries),
    Task("aggregate_with_sql", lambda: run_sql(run_id), retries=max_retries),
    Task("upload_to_azure", lambda: run_upload(run_id), retries=max_retries),
]

    for task in tasks:
        task.run()

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()