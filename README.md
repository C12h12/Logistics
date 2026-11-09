# Logistics Shipment Data Platform

End-to-end logistics data pipeline on Databricks using the medallion architecture (Bronze -> Silver -> Gold) on Unity Catalog. It ingests shipment orders, tracking events, delivery attempts and master data, applies data quality rules and CDC, builds a star schema for analytics, and validates the results automatically.

## Architecture

```
CSV files (UC Volume)  ----\
                            >-- Bronze --> Silver (clean + DQ + CDC + lifecycle) --> Gold (star schema + aggregates) --> Validation
Azure SQL master data ----/
```

* **Catalog:** `logistics`
* **Schemas:** `logistics.bronze`, `logistics.silver`, `logistics.gold`
* **Raw files:** `/Volumes/logistics/bronze/volume/`
  * `transaction_data/shipment_orders/` - 12 batch CSV files of shipment orders
  * `events_data/` - `shipment_tracking_events.csv`, `delivery_attempts.csv`
  * `master_data/azure_sql_seed/` - carrier, route and warehouse master CSVs plus the T-SQL seed script
* **External source:** Azure SQL database (`dbo.carrier_master`, `dbo.warehouse_master`, `dbo.route_master`), credentials read from the secret scope `azure-sql-scope`

## Repository Structure

```
Logistics/
|-- notebooks/
|   |-- 01_source_profiling           Profiles the raw source files (schema, nulls, duplicates)
|   |-- 02_bronze_ingestion           Loads order/event CSVs into logistics.bronze
|   |-- 03_master_ingestion           Seeds and reads Azure SQL master tables into logistics.bronze
|   |-- 04_silver_cleaning            Trims, standardises and casts types -> silver.clean_* tables
|   |-- 05_data_quality_quarantine    Rules engine: valid rows -> silver.<dataset>, rejects -> silver.quarantine_<dataset>
|   |-- 06_shipment_cdc               Shipment status CDC -> silver_shipment_status_history / silver_shipment_current
|   |-- 07_event_lifecycle            Shipment lifecycle stages and exceptions
|   |-- 08_gold_layer                 Gold aggregate tables built from the star schema
|   |-- 09_reconciliation_test        Row-count and DQ reconciliation across bronze/silver/gold
|   |-- 10_late_delivery_baseline     Baseline ML model for late/failed delivery prediction
|   |-- dimensions and facts          Builds the gold star schema (dims + facts)
|   |-- old gold layer                Previous gold layer version (kept for reference)
|-- tests/
|   |-- checkpoint_recovery.py        Structured Streaming checkpoint recovery demo (no duplicates on rerun)
|   |-- failure_recovery_test         Simulates a task failure (widget simulate_failure=true) to test job retries
|   |-- optimize_benchmark.py         Benchmarks OPTIMIZE + ZORDER BY carrier_id on a gold fact table
|-- testing_validation                Final validation task of the production job
|-- README.md
```

## Data Layers

### Bronze (`logistics.bronze`)
Raw data as received, all columns as strings, plus metadata columns (`_source_system`, `_source_table`, `_ingestion_timestamp`).

| Table | Source |
| --- | --- |
| `shipment_orders` | Order CSV batches (10,930 rows) |
| `shipment_tracking_events` | Tracking event CSV |
| `delivery_attempts` | Delivery attempt CSV |
| `carrier_master`, `warehouse_master`, `route_master` | Azure SQL |

### Silver (`logistics.silver`)
* `clean_<dataset>` - cleaned and typed staging tables (duplicates are intentionally kept here); type-cast failures are recorded in `_cast_errors`.
* `<dataset>` - final validated tables after the DQ rules (e.g. `shipment_orders`, 10,844 rows).
* `quarantine_<dataset>` - rows that failed ERROR-level rules, with `rejection_reasons` (e.g. duplicates, sentinel weight 999999.99, invalid/null status).
* `dq_issue_log`, `dq_quality_report` - data quality audit outputs. WARNING-level issues are kept in the data and flagged in `_dq_warnings`.
* `silver_shipment_status_history`, `silver_shipment_current` - CDC outputs, one row per shipment, with `cdc_operation` = UPDATE / NO_CHANGE / NO_EVENT.
* `silver_shipment_lifecycle`, `silver_lifecycle_exceptions`, `silver_shipment_lifecycle_summary` - lifecycle stages and exceptions.

### Gold (`logistics.gold`)
Star schema with SHA-256 surrogate keys (`*_sk = sha2(natural key, 256)`). Each dimension has an `UNKNOWN` member (`sha2('UNKNOWN', 256)`) for unmatched keys.

| Table | Type | Grain |
| --- | --- | --- |
| `dim_carrier` | Dimension | One carrier (+ unknown member) |
| `dim_warehouse` | Dimension | One warehouse (+ unknown member) |
| `fact_shipment` | Fact | One shipment (10,844 rows) |
| `fact_delivery_attempt` | Fact | One delivery attempt (278 rows) |
| `fact_route` | Fact | One route (135 rows), measures distance_km, planned_hours, avg_speed_kmph |

Aggregate tables built by `08_gold_layer`:
`gold_shipments_by_status`, `gold_carrier_performance`, `gold_daily_shipments`, `gold_destination_city_summary`, `gold_service_level_summary`, `gold_delivery_attempt_summary`, `gold_city_delivery_success`, `gold_status_transitions`, `gold_cdc_operation_summary`.

## Orchestration

The Lakeflow Job **`logistics_production_pipeline`** runs the pipeline end to end (parameter `run_date` defaults to the job start date, max 1 concurrent run, email notifications on start/success/failure):

```
bronze_ingestion (02, 2 retries) --\
                                     >--> silver_cleaning (04) --> data_quality (05) --> shipment_cdc (06)
master_ingestion (03) -------------/        --> event_lifecycle (07) --> gold_layer (08) --> test_validation
```

## How to Run

1. Upload the raw CSV files to `/Volumes/logistics/bronze/volume/` using the folder layout above.
2. Create the secret scope `azure-sql-scope` with keys `sql-user` and `sql-pwd` for the Azure SQL database.
3. Run the notebooks in order (`01` -> `08`, then `dimensions and facts` before `08_gold_layer` if the star schema is not built yet), or trigger the `logistics_production_pipeline` job.
4. Run `testing_validation` and `09_reconciliation_test` to verify results; both should report PASS.

## Data Quality Notes

* Duplicate orders, sentinel weights (999999.99) and invalid/null statuses are quarantined, not dropped silently.
* Known source issues kept as warnings: order `origin_warehouse_id` values (`ORIGIN_*`) do not match `warehouse_master.warehouse_id` (`WAREHO*`), so shipments map to the unknown warehouse; tracking events and delivery attempts are timestamped before shipment creation.
* Every tracked shipment currently has a single tracking event, so lifecycle durations are limited.

## Known Issues

* `tests/optimize_benchmark.py` still targets `logistics.gold.fact_shipment_activity`, which no longer exists; point it at `logistics.gold.fact_shipment` before running.
* Writes to Azure SQL from serverless compute must use the `sqlserver` connector; the database may need a retry on first connection while it resumes from auto-pause.

## Tech Stack

Databricks (serverless), Unity Catalog, Delta Lake, PySpark, Structured Streaming, Lakeflow Jobs, Azure SQL Database.
