CRM 360 — End-to-End Data Engineering Pipeline
A production-style Customer 360 data pipeline built to demonstrate modern data engineering practices across ingestion, transformation, testing, and ML-readiness.
Architecture
Synthetic CRM Data (Faker)
        ↓
PySpark (local/EMR-ready)
  - Schema enforcement
  - Fuzzy customer deduplication (Levenshtein blocking)
  - 90-day rolling spend windows
  - Nested JSON explosion (support tickets)
  - Quarantine pattern for bad records
        ↓
Snowflake (CRM360 database)
  RAW schema         ← source data as-is
  STAGING schema     ← typed, renamed, tested
  INTERMEDIATE schema← business logic, joins
  MARTS schema       ← analyst and ML-ready outputs
        ↓
dbt Core (transformation layer)
  - 4 staging models
  - 3 intermediate models
  - 4 mart models (dim, 2 facts, feature store)
  - 36 automated tests
  - Full data lineage documentation
Stack
LayerTechnologyData GenerationPython (Faker)ProcessingPySpark (local[*], EMR-ready)Cloud StorageAWS S3 (designed — local for dev)WarehouseSnowflakeTransformationdbt Core + dbt-snowflakeOrchestrationApache Airflow (in progress)CI/CDGitHub Actions (in progress)IaCTerraform (in progress)VisualizationTableau (in progress)
dbt Project Structure
crm360_dbt/
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_loyalty_events.sql
│   │   ├── stg_support_ticket_messages.sql
│   │   └── stg_transactions_quarantine.sql
│   ├── intermediate/
│   │   ├── int_customer_transactions.sql
│   │   ├── int_customer_loyalty.sql
│   │   └── int_customer_support.sql
│   └── marts/
│       ├── dim_customer.sql
│       ├── fact_transactions.sql
│       ├── fact_support.sql
│       └── feature_store_customer.sql
├── macros/
│   └── generate_schema_name.sql
└── packages.yml
Key Engineering Decisions
SCD2 on dim_customer — loyalty tier changes tracked historically using valid_from/valid_to/is_current with surrogate keys. Fact tables join on customer_id + event_timestamp between valid_from and valid_to for point-in-time correct tier attribution.
Incremental fact_transactions — 500K+ transactions use incremental materialization with a 3-day lookback window and merge strategy to handle late-arriving events without duplicates.
Quarantine pattern — bad records (malformed timestamps, null foreign keys) surfaced in stg_transactions_quarantine rather than silently dropped. Mirrors production reconciliation frameworks used in CRM environments.
run_started_at over current_timestamp — all _loaded_at columns use dbt's built-in run_started_at variable for consistent timestamps across a pipeline run, preventing incremental filter skew on micro-timestamp variance.
Feature store ML-readiness — feature_store_customer uses sentinel value 999 for days_since_last_transaction (never-transacted customers) rather than NULL, ensuring compatibility across ML frameworks that handle NULL differently.
Anti-join pattern — customers without loyalty events identified via left join + where null rather than NOT IN, avoiding silent empty result sets caused by NULLs in the subquery.
Schema separation — RAW, STAGING, INTERMEDIATE, and MARTS are separate Snowflake schemas with different intended access levels. Analysts get read-only access to MARTS only.
Data Scale
TableRowsCustomers (with fuzzy duplicates)103,000Transactions (clean)492,561Transactions (quarantined)7,439Support ticket messages45,074Loyalty events64,306dim_customer (SCD2)131,483fact_transactions492,561fact_support15,000feature_store_customer103,000
Testing
36 automated dbt tests covering:

Primary key uniqueness on all fact and dimension tables
Not-null constraints on all critical columns
Accepted values on categorical fields (loyalty_tier, channel)
Not-null coverage on all ML signal columns in feature store

Run all tests:
bashcd crm360_dbt && dbt test
Known Limitations
See notes/dedup_findings.md for a detailed write-up of the fuzzy deduplication approach, what went wrong, and how it would be fixed in production using Splink probabilistic record linkage.
Setup
bash# Install dependencies
pip install dbt-core dbt-snowflake pyspark faker

# Generate synthetic data
python scripts/generate_data.py

# Run PySpark cleaning job
python scripts/spark_clean_transform.py

# Install dbt packages
cd crm360_dbt && dbt deps

# Verify Snowflake connection
dbt debug

# Run full pipeline
dbt run

# Run all tests
dbt test

# Generate and serve documentation
dbt docs generate && dbt docs serve
