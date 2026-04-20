import os
from azure.storage.blob import BlobServiceClient

connection_string = os.getenv("AZURE_CONNECTION_STRING")

if not connection_string:
    raise ValueError("AZURE_CONNECTION_STRING is not set")

container_name = "data"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_client = blob_service_client.get_container_client(container_name)
try:
    container_client.create_container()
except Exception:
    pass

file_path = "output/transformed_sales.csv"
blob_name = "product_sales_summary.csv"

with open(file_path, "rb") as data:
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)

print("Upload complete!")