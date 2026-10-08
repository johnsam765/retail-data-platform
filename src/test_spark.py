from spark_session import create_spark_session

spark=create_spark_session()

print("Spark version",spark.version)
print("Default parallelism:", spark.sparkContext.defaultParallelism)

spark.stop()
