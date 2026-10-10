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

## Capstone Day 3 — Customer Join, Deduplication and Silver Persistence

### Customer enrichment
- Read the Bronze customer Delta table.
- Customer lookup grain should be one row per customer_id.
- Duplicate customer_id values can cause join fan-out and inflate downstream Gold aggregates.
- Used a LEFT JOIN because Sales is the driving dataset.
- An INNER JOIN could silently remove sales rows whose customer_id does not exist.

### Customer validation
Added a technical marker:

_customer_found = 1

After a LEFT JOIN:
- matched customer -> _customer_found = 1
- unmatched customer -> _customer_found = NULL

This allows CUSTOMER_NOT_FOUND to be detected independently from a missing
province or customer_name.

### Duplicate order handling
Used a Window:

partitionBy(order_id)
orderBy(ingestion_timestamp DESC, _row_id DESC)

Added ROW_NUMBER to rank duplicate orders.

- _order_rank = 1 -> preferred/current row
- _order_rank > 1 -> DUPLICATE_ORDER_ID

ingestion_timestamp is the primary ordering field.
_row_id is currently a technical tie-breaker only.

A proper production source would ideally provide:
- updated_at
- event timestamp
- sequence number
- source version
- Kafka offset / CDC sequence

### Validation priority
Current primary rejection priority:

1. DUPLICATE_ORDER_ID
2. INVALID_ORDER_DATE
3. INVALID_AMOUNT
4. CUSTOMER_NOT_FOUND

Because chained when() conditions return the first match, a row with multiple
problems currently stores only one primary rejection reason.

### Final Silver
Persisted valid records to Silver Delta using trusted business columns.

Current Silver:
- O001
- O002
- O003

O003 is Cancelled but is still valid Silver data.

### Quarantine
Persisted rejected records to Quarantine Delta while preserving:
- original raw values
- parsed values
- customer enrichment
- rejection reason
- pipeline lineage metadata

Current Quarantine:
- O004 -> INVALID_ORDER_DATE
- O005 -> INVALID_AMOUNT

### Reconciliation
Current counts:

Bronze Sales = 5
Silver = 3
Quarantine = 2

3 + 2 = 5

Reconciliation checks whether all incoming records were accounted for.

It can help identify:
- silently dropped rows
- accidental duplication
- join fan-out
- incorrect filters
- records that were never classified

### Reconciliation vs Audit
Reconciliation answers:

"Does the data balance?"

Audit answers:

"What happened during this pipeline run?"

The reconciliation result can later be stored as one field in the audit record.
