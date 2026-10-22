from spark_session import create_spark_session
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from run_identity import get_source_batch_id

ROOT=Path(__file__).resolve().parent.parent
SALES_BRONZE_PATH=ROOT/"data"/"bronze"/"sales"
CUSTOMER_BRONZE_PATH=ROOT/"data"/"bronze"/"customers"
SILVER_PATH=ROOT/"data"/"silver"/"sales"
QUARANTINE_PATH=ROOT/"data"/"quarantine"/"sales"

def run_silver(spark):
    source_batch_id = get_source_batch_id(
        spark,
        SALES_BRONZE_PATH
    )
    sales_df= (
    load_bronze_data(spark,SALES_BRONZE_PATH)
    .withColumn("parsed_order_date",F.expr("try_cast(order_date as date)"))
    .withColumn("parsed_amount",F.expr("try_cast(amount as decimal(12,2))"))
    .withColumn("_row_id",
    F.monotonically_increasing_id()))
    order_window=Window.partitionBy("order_id").orderBy(F.col("ingestion_timestamp").desc(),F.col("_row_id").desc())
    sales_df=sales_df.withColumn("_order_rank",F.row_number().over(order_window))
    customer_df=(
    load_bronze_data(spark,CUSTOMER_BRONZE_PATH)
    .select("customer_id","customer_name","province")
    .dropDuplicates(["customer_id"]).withColumn("_customer_found",F.lit(1)))
    joined_df=sales_df.join(customer_df,on="customer_id",how="left")
    validated_df=(
    joined_df.withColumn("rejection_reason",
    F.when(
            F.col("_order_rank") > 1,
            F.lit("DUPLICATE_ORDER_ID")
        )
    .when(F.col("parsed_order_date").isNull(),F.lit("INVALID_ORDER_DATE"))
    .when(F.col("parsed_amount").isNull(),F.lit("INVALID_AMOUNT"))
    .when(F.col("_customer_found").isNull(),F.lit("CUSTOMER_NOT_FOUND"))))
    valid_df=validated_df.filter(F.col("rejection_reason").isNull())
    quarantine_df=validated_df.filter(F.col("rejection_reason").isNotNull())
    silver_final_df = valid_df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "province",
    F.col("parsed_order_date").alias("order_date"),
    "status",
    F.col("parsed_amount").alias("amount"),
    "ingestion_timestamp",
    "source_path",
    "pipeline_run_id"
)

    quarantine_final_df = quarantine_df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "province",
    "order_date",
    "amount",
    "parsed_order_date",
    "parsed_amount",
    "rejection_reason",
    "ingestion_timestamp",
    "source_path",
    "pipeline_run_id"
)
    silver_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(SILVER_PATH))

    quarantine_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(QUARANTINE_PATH))

    print("\nFINAL SILVER:")
    silver_final_df.show(truncate=False)

    print("\nFINAL QUARANTINE:")
    quarantine_final_df.show(truncate=False)

    print("\nCOUNTS:")
    bronze_count = sales_df.count()
    silver_count = silver_final_df.count()
    quarantine_count = quarantine_final_df.count()

    processed_count = silver_count + quarantine_count
    reconciliation_passed = (bronze_count == processed_count)
    if not reconciliation_passed:
        raise RuntimeError(f"Reconciliation failed: Bronze={bronze_count}, "
        f"Silver={silver_count}, "
        f"Quarantine={quarantine_count}")
    print("RECONCILIATION PASSED")
    return {
    "source_batch_id": source_batch_id,
    "bronze_count": bronze_count,
    "silver_count": silver_count,
    "quarantine_count": quarantine_count,
    "reconciliation_passed": reconciliation_passed
    }

def load_bronze_data(spark,path):
    return spark.read.format("delta").load(str(path))

if __name__=="__main__":
    spark=create_spark_session()
    run_silver(spark)
    spark.stop()
