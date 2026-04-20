from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum


def run_sql():
    spark = SparkSession.builder.appName("SQL Layer").getOrCreate()

    df = spark.read.csv("output/transformed_sales.csv", header=True, inferSchema=True)

    df.createOrReplaceTempView("sales")

    df_agg = spark.sql("""
        SELECT 
            product,
            SUM(quantity) as total_quantity,
            SUM(total) as total_revenue
        FROM sales
        GROUP BY product
    """)

    print("Aggregated data:")
    df_agg.show()

    df_agg.toPandas().to_csv("output/product_sales_summary.csv", index=False)

    print("Saved aggregated file")

    spark.stop()