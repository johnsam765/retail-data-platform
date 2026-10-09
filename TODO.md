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
