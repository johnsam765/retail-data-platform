from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
AUDIT_PATH = ROOT / "data" / "audit" / "pipeline_runs"

def write_audit_record(
    spark,
    pipeline_run_id,
    bronze_count,
    silver_count,
    quarantine_count,
    reconciliation_passed,
    status
):
    audit_data = [{
        "pipeline_run_id": pipeline_run_id,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "reconciliation_passed": reconciliation_passed,
        "status": status,
        "audit_timestamp": datetime.now(timezone.utc)
    }]

    audit_df = spark.createDataFrame(audit_data)

    audit_df.show(truncate=False)

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .save(str(AUDIT_PATH))
