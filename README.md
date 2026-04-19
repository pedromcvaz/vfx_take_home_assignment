# vfx_take_home_assignment
Repo for the take home assignment from VFX Financial’s interview process

## Project Overview
This project loads the kaggle e-commerce data to Snowflake simulating the behaviour of a Fivetran‑like tool.
It then implements:

The raw data is a single table (RAW_ECOMMERCE_DATA) containing one row per transaction line item.

The project implements three master entities:

- `master_users` – deterministic hash key, user‑level metrics, customer segmentation, and GDPR support columns.
- `master_products` – product attributes with category normalisation, sales metrics, and SCD2 preparation.
- `master_orders` – transaction‑level fact table with multi‑currency support (USD → GBP) and late‑arrival handling notes.


 Data quality is enforced via dbt tests (unique, not null, accepted values, relationships, expression checks) defined in `schema.yml`.

Freshness and anomaly metrics are proposed for observability (see below).

## Prerequisites

- Snowflake account with access to the raw schema (or your target dataset)
- dbt Core ≥1.5 or dbt Cloud
- Python 3.8+ with dbt-snowflake adapter
- dbt packages: dbt_utils (already referenced)

## Setup

```commandline
# Clone the repository
git clone https://github.com/pedromcvaz/vfx_take_home_assignment.git
cd vfx_take_home_assignment

# Install dbt packages
dbt deps

# Configure your Snowflake profile (~/.dbt/profiles.yml)
```

# Run the Models
```commandline
# Build staging model first (if not already materialized)
dbt run --select stg_ecommerce__transactions

# Build all master models
dbt run --select master_*

# Run all data quality tests
dbt test
```

# Seeds
This project uses an exchange_rate seed to feed the macro `get_conversion_rate()` that should return the current USD→GBP rate.


| Area | Assumption                                                                                                                                                                                                                                    |
|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Raw data structure | A single table `RAW_ECOMMERCE_DATA` where each row is a transaction line item. No separate user/product/order tables.                                                                                                                         |
| Source currency | Raw monetary columns (`PRICE_RS`, `FINAL_PRICE_RS`) are assumed to be in USD. Conversion to GBP is performed in `master_orders` using a seeded rate.                                                                                          |
| Deterministic keys | All master surrogate keys are generated via `dbt_utils.generate_surrogate_key()` on natural keys (e.g., `user_id`, `product_id`, `transaction_id`).                                                                                           |
| User deduplication | Each `user_id` corresponds to one unique customer. No further deduplication needed – `master_users` aggregates transactions per user.                                                                                                         |
| Product deduplication | Each `product_id` + `category` combination defines a product. The model groups by both to ensure uniqueness.                                                                                                                                  |
| Orders | `master_orders` uses `transaction_id` as the natural key (line‑item level). The challenge's "canonicalize orders" is satisfied because each source row is a shop transaction. Aggregation to order header would be done in a downstream mart. |
| Late‑arriving updates | Not implemented. The `master_orders` model includes a `dbt_updated_at` column and comments on how to convert to incremental + merge for status transitions.                                                                                   |
| Currency conversion | A macro `convert_amount(amount, rate)` and `get_conversion_rate()` are used. The rate is obtained from a seed table.                                                                                                                          |
| GDPR deletes | Columns `is_deleted`, `deleted_at`, `deleted_reason` exist in `master_users` but are not actively populated. A future step would filter out deleted users or mask PII.                                                                        |
| SCD2 | Not implemented, but columns (`dbt_valid_from`, `dbt_valid_to`, `is_current`) are present in `master_users` and `master_products` as a placeholder.                                                                                           |


## Design choices

### 1- Staging Model (`stg_ecommerce__transactions`)
- __Audit columns__: `loaded_at` (from source) and `dbt_processed_at` (current timestamp).
- __Defensive filtering__: Only rows with non‑null `USER_ID`, `PRODUCT_ID`, `PURCHASE_DATE`, `FINAL_PRICE_RS` pass through.

### 2. Master Users (`master_users`)

- __Surrogate key__: MD5(source_user_id) via `generate_surrogate_key`
- __User metrics__: first/last purchase date, total orders, lifetime value (in USD)
- __Customer segment__ derived from `total_orders`:
  - VIP ≥ 10
  - LOYAL ≥ 5
  - REPEAT ≥ 2
  - else NEW
- __GDPR support__: `is_deleted`, `deleted_at`, `deleted_reason` (ready for future deletion logic)
- __SCD2 placeholders__: `dbt_valid_from`, `dbt_valid_to`, `is_current`

### 3. Master Products (`master_products`)

- __Surrogate key__: MD5(`product_id`)
- __Category__ already normalised in staging
- __Aggregated metrics__:
  - `times_sold`
  - `unique_customers`
  - `avg_price`
  - `avg_discount_pct`
  - first/last sold date
- __SCD2 columns__ prepared for future tracking of category or price changes

### 4. Master Orders (`master_orders`)

- __Surrogate key__: MD5(`transaction_id`)
- __Currency conversion__: Uses macro `convert_amount` with `get_conversion_rate()` to produce `price_gbp` and `final_price_gbp`. Also calculates `discount_amount_gbp`
- __Foreign keys__: Joins to `master_users` and `master_products` via `source_user_id` / `source_product_id` (requires those columns to exist in staging – they do as `user_id` and `product_id`)
- __Order status__: Simplified to `'COMPLETED'` (source has no status column). The test allows `PENDING`, `CANCELLED`, `REFUNDED` for future extension
- __Late‑arriving updates__: The SQL file contains commented suggestions for incremental materialisation with `merge_update_columns`

 
## Proposed Freshness & Anomaly Metrics (Not Implemented)

Based on the challenge’s optional request, here are the metrics I would monitor.
In a real world scenario these thresholds might be defined with some help from the business team.

| Layer | Metric | Model / Source | Warning Threshold | Error Threshold |
|-------|--------|----------------|-------------------|-----------------|
| Staging | max(`loaded_at`) | `stg_ecommerce__transactions` | > 2 hours ago | > 6 hours ago |
| Staging | Row count change vs 7‑day avg | `stg_ecommerce__transactions` | ±30% | ±50% |
| Master | max(`order_date`) | `master_orders` | > 1 day ago | > 2 days ago |
| Master | Orphan rate (missing `user_master_id`) | `master_orders` | > 5% | > 10% |
| Master | Null rate drift for critical columns | All master tables | > 1% absolute change | > 5% |
| Master | Duplicate rate (by natural key) | `master_users` (`source_user_id`) | > 0% | > 0% |

Implementation approach:

- Use dbt source freshness for the raw source (with loaded_at_field defined in sources.yml).
- For master freshness, add a dbt freshness test or a custom macro that checks max(order_date) against current_date.
- For anomaly detection, create a post‑hook that logs row counts and null percentages to a metadata table, then compare to rolling averages.

## What I Would Extend Next (If Double the Time)

### Incremental master tables with late‑arriving updates

- Convert `master_orders` to incremental (`materialized='incremental'`, `unique_key='order_master_id'`, merge strategy)
- Add `order_status` transitions from a separate source or track changes via a history table

### Full SCD Type 2 for products and users

- Implement logic to close old records (`dbt_valid_to = current_timestamp`) and insert new ones when category or `customer_segment` changes

### Automated anomaly detection

- Create a meta `anomaly_metrics` table that stores daily counts, null percentages, and fresh timestamps
- Write a macro that raises test failures, for example when a metric deviates by >3 standard deviations

### Backfill & replay strategy

- Add a `backfill_date` variable to allow reprocessing of specific date ranges without full refresh
- Modify staging model to filter on `purchase_date >= '{{ var("backfill_start") }}'` when the variable is set

### GDPR delete automation

- Accept a `gdpr_deleted_users` seed or source table and join to `master_users` to set `is_deleted = true` and `deleted_at = current_timestamp`
- Add a `where is_deleted = false` clause to downstream models that should not see deleted users

### Data quality dashboard

- Expose test results and freshness metrics via dbt Explorer or a simple Streamlit app
- Send alerts to Slack/Teams when error thresholds are breached

## Folder Structure
```text
.
├── README.md
├── load_raw_data_to_snowflake.py
├── requirements.txt
└── vfx_take_home_dbt
    ├── README.md
    ├── dbt_project.yml
    ├── macros
    │   └── convert_currency.sql
    ├── models
    │   ├── master
    │   │   ├── master_orders.sql
    │   │   ├── master_products.sql
    │   │   ├── master_users.sql
    │   │   └── schema.yml
    │   ├── staging
    │   │   ├── schema.yml
    │   │   └── stg_ecommerce__transactions.sql
    │   └── tests
    │       └── test_currency_conversion.sql
    ├── packages.yml
    ├── seeds
    │   ├── exchange_rates.csv
    │   └── schema.yml
    └── tests

```
