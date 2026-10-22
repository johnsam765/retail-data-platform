from pyspark.sql import functions as F
from uuid import uuid4


def generate_pipeline_run_id():
    return str(uuid4())

def get_source_batch_id(spark,bronze_path):
    batch_ids=(spark.read.format("delta").load(str(bronze_path))
    .select(F.col("pipeline_run_id").alias("source_batch_id"))
    .distinct().limit(2).collect())
    if len(batch_ids) != 1:
        raise RuntimeError(
            f"Expected exactly one source batch ID, "
            f"found {len(batch_ids)} distinct values"
        )
    source_batch_id=batch_ids[0]["source_batch_id"]
    if source_batch_id is None:
        raise RuntimeError("Source batch ID cannot be NULL")
    return source_batch_id
