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

## Capstone Day 2 — Silver Parsing and Initial Data Quality

### Silver input
- Silver reads the persisted Bronze Delta table.
- Bronze business fields such as order_date and amount are intentionally stored
  as strings so the original source values are preserved.

### Safe parsing
Created typed Silver columns:
- parsed_order_date -> DATE
- parsed_amount -> DECIMAL(12,2)

Used try_cast so malformed values become NULL instead of failing the pipeline.

Examples:
- "2026-10-01" -> valid DATE
- "bad-date" -> NULL
- "500.00" -> DECIMAL 500.00
- "abc" -> NULL

### Data-quality rules
Created rejection_reason:
- NULL parsed_order_date -> INVALID_ORDER_DATE
- NULL parsed_amount -> INVALID_AMOUNT

Split the DataFrame into:
- valid_df: rejection_reason IS NULL
- quarantine_df: rejection_reason IS NOT NULL

Current results:
- O001 -> valid
- O002 -> valid
- O003 -> valid
- O004 -> INVALID_ORDER_DATE
- O005 -> INVALID_AMOUNT

Cancelled is a valid business status and is not automatically a data-quality
failure.

### DataFrame lesson
select() does not permanently remove columns from the original DataFrame.

quarantine_df still contains all columns. Using:

quarantine_df.select(...).show()

only creates a projection for that operation/display.

### Current state
Silver and Quarantine have not yet been persisted to Delta.
More validation rules will be added before the final write.
