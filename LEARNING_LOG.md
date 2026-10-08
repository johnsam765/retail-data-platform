## Capstone Day 1 — Bronze Ingestion and Spark Execution

### Project
Started a new production-style retail data platform on Ubuntu.

Implemented reusable Bronze ingestion for:
- sales.csv
- customer.csv

### Bronze design
Bronze preserves source values using explicit string-based schemas where later
validation is required.

Added lineage metadata:
- pipeline_run_id
- ingestion_timestamp
- source_path

Both datasets belonging to the same pipeline execution share the same
pipeline_run_id.

### Spark concepts reinforced
- DataFrame transformations are lazily evaluated.
- withColumn() is normally a narrow transformation.
- Actions such as show() and write() trigger execution.
- Multiple actions can cause lineage recomputation.
- One partition generally corresponds to one task within a stage.
- Number of partitions determines available task-level parallelism.
- Executor cores/task slots determine how many tasks can run simultaneously.
- More available cores do not help when the DataFrame has too few partitions.

### Delta Lake
A Delta table consists of:
- Parquet data files containing the records.
- _delta_log containing transactional table metadata and version history.

Spark must be configured with Delta support to read/write Delta tables.

### Issue discovered
The CSV header was accidentally ingested as a data row because the reader
configuration used the wrong header option.

Correct:
.option("header", True)

This reinforced that successful pipeline execution does not guarantee correct
data.
