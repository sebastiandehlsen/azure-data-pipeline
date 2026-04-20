from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import os

spark = SparkSession.builder.appName("Sales Data Pipeline").getOrCreate()

# Load data
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

print("Original data:")
df.show()

# Transformation 1: total per row
df = df.withColumn("total", col("price") * col("quantity"))

# Transformation 2: aggregate per product
df_agg = df.groupBy("product").agg(
    _sum("quantity").alias("total_quantity"),
    _sum("total").alias("total_revenue")
)

print("Aggregated data:")
df_agg.show()

# Save output
os.makedirs("output", exist_ok=True)
df_agg.toPandas().to_csv("output/product_sales_summary.csv", index=False)

print("Saved aggregated file to output/product_sales_summary.csv")

spark.stop()