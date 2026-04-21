from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import json
from logger import get_logger

logger = get_logger()

with open("config.json") as f:
    config = json.load(f)

input_path = config["input_path"]
# output_path = config["transformed_path"]


def run_transformation(run_id):
    spark = SparkSession.builder.appName("Sales Data Pipeline").getOrCreate()

    logger.info("Loading raw sales data")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    logger.info("Applying transformation: total = price * quantity")
    df = df.withColumn("total", col("price") * col("quantity"))


    folder_path = f"output/{run_id}"
    os.makedirs(folder_path, exist_ok=True)

    output_path = f"{folder_path}/transformed_sales.csv"

    df.toPandas().to_csv(output_path, index=False)

    logger.info(f"Saved transformed data to {output_path}")

    spark.stop()