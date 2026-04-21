from pyspark.sql import SparkSession
import json
from logger import get_logger

logger = get_logger()

with open("config.json") as f:
    config = json.load(f)

input_path = config["transformed_path"]
output_path = config["aggregated_path"]


def run_sql(run_id):

    input_path = f"output/{run_id}/transformed_sales.csv"
    output_path = f"output/{run_id}/product_sales_summary.csv"

    spark = SparkSession.builder.appName("SQL Layer").getOrCreate()

    logger.info("Loading transformed dataset for SQL aggregation")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df.createOrReplaceTempView("sales")

    logger.info("Running SQL aggregation")
    df_agg = spark.sql("""
        SELECT
            product,
            SUM(quantity) AS total_quantity,
            SUM(total) AS total_revenue
        FROM sales
        GROUP BY product
    """)

    df_agg.toPandas().to_csv(output_path, index=False)

    logger.info(f"Saved aggregated data to {output_path}")

    spark.stop()