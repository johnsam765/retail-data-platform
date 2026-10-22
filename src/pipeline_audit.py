from pathlib import Path
from pyspark.sql.types import(
TimestampType,
BooleanType,
LongType,
StringType,
StructType,
StructField)
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent.parent
AUDIT_PATH=ROOT/"data"/"audit"/"orchestrator_runs"

AUDIT_SCHEMA=(StructType([
StructField("pipeline_name",StringType(),False),
StructField("pipeline_run_id",StringType(),False),
StructField("source_batch_id",StringType(),True),
StructField("status",StringType(),False),
StructField("failed_stage",StringType(),True),
StructField("error_message",StringType(),True),
StructField("bronze_count",LongType(),True),
StructField("silver_count",LongType(),True),
StructField("quarantine_count",LongType(),True),
StructField("gold_count",LongType(),True),
StructField("reconciliation_passed",BooleanType(),True),
StructField("started_at",TimestampType(),False),
StructField("completed_at",TimestampType(),False),
]))


def write_pipeline_audit(spark,audit_data):
    audit_df=spark.createDataFrame([audit_data],schema=AUDIT_SCHEMA)
    audit_df.write.format("delta").mode("append").save(str(AUDIT_PATH))
