from pathlib import Path
from pyspark.sql import functions as F
from spark_session import create_spark_session

ROOT_PATH=Path(__file__).resolve().parent.parent
SILVER_DATA=ROOT_PATH/"data"/"silver"/"sales"
GOLD_DATA=ROOT_PATH/"data"/"gold"/"province_sales"

def run_gold(spark):
    silver_df = spark.read.format("delta").load(str(SILVER_DATA))

    completed_orders = silver_df.filter(
        F.col("status") == "Completed"
    )

    grouped_by_province = (
        completed_orders
        .groupBy("province")
        .agg(
            F.count("*").alias("completed_order_count"),
            F.sum("amount").alias("total_sales_amount"),
            F.avg("amount")
             .cast("decimal(12,2)")
             .alias("average_sales_amount")
        )
    )
    grouped_by_province.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(GOLD_DATA))
    print("\nGOLD PROVINCE SALES:")
    grouped_by_province.explain("formatted")
    grouped_by_province.show(truncate=False)


if __name__=="__main__":
    spark=create_spark_session()
    run_gold(spark)
    spark.stop()
