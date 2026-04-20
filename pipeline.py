from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


def run_transformation():
    spark = SparkSession.builder.appName("Sales Data Pipeline").getOrCreate()

    # Load data
    df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

    print("Original data:")
    df.show()

    # Transformation: total per row
    df = df.withColumn("total", col("price") * col("quantity"))

    print("Transformed data:")
    df.show()

    # Save transformed data (IKKE aggregated længere)
    os.makedirs("output", exist_ok=True)
    df.toPandas().to_csv("output/transformed_sales.csv", index=False)

    print("Saved transformed file")

    spark.stop()