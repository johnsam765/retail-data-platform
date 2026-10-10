## Next Session — Capstone Day 2

- Verify corrected Bronze row counts
- Inspect Delta transaction history
- Begin Silver layer
- Parse order_date safely
- Parse amount safely using try_cast
- Define validation rules
- Separate valid rows from invalid rows
- Start Quarantine
- Connect Silver transformations to Spark execution plans

## Next Session — Silver Part 2

- Read Bronze customers
- Join sales with customers using customer_id
- Understand why LEFT JOIN is used instead of INNER JOIN
- Detect CUSTOMER_NOT_FOUND
- Add customer information such as province
- Preserve existing INVALID_ORDER_DATE / INVALID_AMOUNT logic
- Review validation-rule priority
- Begin final Silver vs Quarantine column design

After that:
- duplicate order detection
- final Silver/Quarantine Delta writes
- reconciliation checks

## Next Session — Gold and Pipeline Reconciliation

### Main build
- Turn the row-count reconciliation into actual pipeline logic
- Fail the pipeline when Bronze != Silver + Quarantine
- Build the Gold sales summary
- Include only Completed sales
- Group by province
- Calculate:
  - completed order count
  - total sales amount
  - average sales amount

### Spark learning
- Understand why groupBy causes a shuffle
- Inspect the Gold execution plan
- Connect shuffle partitions to stages and tasks

### Reliability
- Read Silver and Quarantine from persisted Delta tables
- Verify persisted counts
- Begin designing the pipeline audit record

### Upcoming
- Audit SUCCESS / FAILED
- exception handling
- pipeline_run_id based auditing
- idempotency and input fingerprinting
- incremental processing
- Delta MERGE
