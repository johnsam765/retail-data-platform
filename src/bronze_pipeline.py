from spark_session import create_spark_session
from pathlib import Path
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime,timezone


ROOT=Path(__file__).resolve().parent.parent
SALES_RAW=ROOT/"data"/"raw"/"sales.csv"
CUSTOMER_RAW=ROOT/"data"/"raw"/"customer.csv"
SALES_BRONZE_PATH=ROOT/"data"/"bronze"/"sales"
CUSTOMER_BRONZE_PATH=ROOT/"data"/"bronze"/"customers"
sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", StringType(), True),
])

customer_schema=StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("province", StringType(), True)
])


def load_csv_to_bronze(spark:SparkSession,source_schema:StructType,source:Path,destination:Path,pipeline_run_id:str)->None :
    bronze_df1=spark.read.option("header",True).schema(source_schema).csv(str(source))
    bronze_final=(bronze_df1.withColumn("pipeline_run_id",F.lit(pipeline_run_id))
    .withColumn("ingestion_timestamp",F.current_timestamp())
    .withColumn("source_path",F.lit(source.name)))
    print(f"\nSOURCE: {source.name}")
    print("\nSCHEMA:")
    bronze_final.printSchema()
    print("\nPARTITIONS:")
    print(bronze_final.rdd.getNumPartitions())
    bronze_final.write.format("delta").mode("append").save(str(destination))
    return 

if __name__== "__main__":
    spark=create_spark_session()
    pipeline_run_id=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")+"_"+uuid4().hex[:8]
    load_csv_to_bronze(
        spark,
        sales_schema,
        SALES_RAW,
        SALES_BRONZE_PATH,
        pipeline_run_id,
    )
    load_csv_to_bronze(
        spark,
        customer_schema,
        CUSTOMER_RAW,
        CUSTOMER_BRONZE_PATH,
        pipeline_run_id,
    )

    spark.stop()
