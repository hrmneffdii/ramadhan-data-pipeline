# DATA ENGINEERING CHECKLIST

## 1. Architecture & Data Modeling

### 🔹 Layering

* [ ] Staging layer is separated from the warehouse layer
* [ ] Dimension tables exist (e.g., `dim_date`, `dim_keyword`)
* [ ] Fact table with clearly defined grain
* [ ] Mart layer for aggregation
* [ ] No business logic inside the staging layer

### 🔹 Grain Clarity

* [ ] Fact table grain is explicitly defined

  > 1 row = 1 keyword per 1 date

* [ ] Composite Primary Key matches the defined grain

### 🔹 Constraint Discipline

* [ ] Primary Key enforced on fact table
* [ ] UNIQUE constraint enforced in staging
* [ ] UNIQUE constraint enforced in dimension tables
* [ ] Foreign key relationships are active

## 2. Load Strategy

### 🔹 Incremental Load

* [ ] Loads only new data
* [ ] Safe to run multiple times (idempotent)
* [ ] Does not create duplicate fact records

### 🔹 Backfill

* [ ] Can load historical date ranges
* [ ] Safe when overlapping with existing data
* [ ] Does not corrupt previously loaded data

### 🔹 Full Rebuild

* [ ] Can rebuild the entire warehouse
* [ ] Leaves no legacy artifacts behind
* [ ] Incremental load remains safe after rebuild

## 3. Idempotency & Safety

* [ ] Fact table uses UPSERT / `ON CONFLICT` strategy
* [ ] Pipeline can be safely re-run without errors
* [ ] Constraints are never disabled to “avoid errors”
* [ ] Modeling phase is wrapped inside a single atomic transaction

## 4. Data Mart Strategy

* [ ] Mart is built from the fact table (not staging)
* [ ] Aggregations properly join with `dim_date`
* [ ] Mart does not produce duplicates when re-run
* [ ] Primary Key in mart remains enforced
* [ ] Does not use `replace` in a way that drops schema constraints

## 5. Engineering Mindset

* [ ] Tested incremental load twice
* [ ] Tested incremental → backfill → incremental
* [ ] Tested full rebuild → incremental
* [ ] Tested overlapping date ranges
* [ ] Validated duplicates using SQL checks

## 6. Monitoring & Logging

* [ ] `pipeline_run_log` table exists
* [ ] Run status recorded (Success / Failed)
* [ ] Execution duration tracked
* [ ] Error messages stored as strings
* [ ] Rows loaded are recorded

## 7. Data Quality Checks

* [ ] Duplicate checks in staging
* [ ] NULL checks on critical columns
* [ ] Referential integrity validation
* [ ] Row count consistency checks
* [ ] Data quality results logged in a dedicated table

## 8. Performance Awareness (Even at Local Scale)

* [ ] Indexes on foreign keys
* [ ] Indexes on staging filter columns
* [ ] Avoid loading entire dataset into pandas when SQL can handle it
* [ ] Aggregations performed inside the database

## 9. Code Quality

* [ ] No hardcoded table names
* [ ] Configuration separated from logic
* [ ] Modular function design
* [ ] No commit inside every function
* [ ] Single transaction for modeling phase
* [ ] Clear separation between extract / transform / load / modeling

## 10. Advanced (Almost Mid Bonus)

* [ ] Watermark table implemented (optional)
* [ ] Late-arriving data handling considered
* [ ] SCD (Slowly Changing Dimension) awareness
* [ ] Able to explain architectural trade-offs
* [ ] README clearly explains architecture decisions

