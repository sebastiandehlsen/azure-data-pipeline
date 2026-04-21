import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from logger import get_logger

logger = get_logger()

load_dotenv()

with open("config.json") as f:
    config = json.load(f)

connection_string = os.getenv("AZURE_CONNECTION_STRING")

if not connection_string:
    raise ValueError("AZURE_CONNECTION_STRING is not set")

container_name = config["container_name"]
file_path = config["aggregated_path"]
blob_name = "product_sales_summary.csv"


def run_upload(run_id):
    logger.info("Connecting to Azure Blob Storage")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    file_path = f"output/{run_id}/product_sales_summary.csv"
    blob_name = f"{run_id}/product_sales_summary.csv"

    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
        logger.info(f"Created container: {container_name}")
    except Exception:
        logger.info(f"Using existing container: {container_name}")

    logger.info(f"Uploading {file_path} to Azure Blob Storage")
    with open(file_path, "rb") as data:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        blob_client.upload_blob(data, overwrite=True)

    logger.info("Upload complete")