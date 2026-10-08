from pathlib import Path

from spark_session import create_spark_session


ROOT = Path(__file__).resolve().parent.parent

SALES_BRONZE_PATH = ROOT / "data" / "bronze" / "sales"
CUSTOMER_BRONZE_PATH = ROOT / "data" / "bronze" / "customers"


if __name__ == "__main__":
    spark = create_spark_session()

    sales_df = (
        spark.read
        .format("delta")
        .load(str(SALES_BRONZE_PATH))
    )

    customers_df = (
        spark.read
        .format("delta")
        .load(str(CUSTOMER_BRONZE_PATH))
    )

    print("\nSALES BRONZE")
    sales_df.show(truncate=False)

    print("\nCUSTOMERS BRONZE")
    customers_df.show(truncate=False)

    print("\nSALES PARTITIONS")
    print(sales_df.rdd.getNumPartitions())

    print("\nCUSTOMER PARTITIONS")
    print(customers_df.rdd.getNumPartitions())

    spark.stop()
