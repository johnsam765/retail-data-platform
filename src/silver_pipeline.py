from spark_session import create_spark_session
from pathlib import Path
from pyspark.sql import functions as F

ROOT=Path(__file__).resolve().parent.parent
SALES_BRONZE_PATH=ROOT/"data"/"bronze"/"sales"
CUSTOMER_BRONZE_PATH=ROOT/"data"/"bronze"/"customers"
SILVER_PATH=ROOT/"data"/"silver"/"sales"
QUARANTINE_PATH=ROOT/"data"/"quarantine"

def run_silver(spark):
    sales_df= (
    load_bronze_data(spark,SALES_BRONZE_PATH)
    .withColumn("parsed_order_date",F.expr("try_cast(order_date as date)"))
    .withColumn("parsed_amount",F.expr("try_cast(amount as decimal(12,2))"))
    .withColumn("rejection_reason",F.when(F.col("parsed_order_date").isNull(),F.lit("INVALID_ORDER_DATE"))
    .when(F.col("parsed_amount").isNull(),F.lit("INVALID_AMOUNT"))))
    valid_df=sales_df.filter(F.col("rejection_reason").isNull())
    quarantine_df=sales_df.filter(F.col("rejection_reason").isNotNull())
    print("\nVALID:")
    valid_df.show(truncate=False)
    print("\nQUARANTINE:")
    quarantine_df.select(
    "order_id",
    "order_date",
    "amount",
    "rejection_reason"
    ).show(truncate=False) 
    sales_df.show(truncate=False)
    sales_df.printSchema()


def load_bronze_data(spark,path):
    return spark.read.format("delta").load(str(path))


if __name__=="__main__":
    spark=create_spark_session()
    run_silver(spark)
    spark.stop()
