# Portfolio FAQ

## Why this dataset?

The PKDD'99 Czech bank dataset is one of the very few public datasets with a real bank ledger: accounts, clients, loans, cards and over a million transactions with running balances. It is messy in realistic ways: padded and blank codes, missing demographics, no posting time and non-chronological ids. That made it a good stand-in for the platform data an analytics engineer at a bank would model.

## Why DuckDB instead of Snowflake?

DuckDB lets anyone clone the repo and run the whole warehouse on a laptop or in CI for free. The dbt project uses dbt's cross-database macros (dateadd, date_trunc, datediff, last_day) and a recursive CTE for the calendar, so the same SQL compiles on Snowflake. The profile includes a Snowflake target that reads credentials from environment variables. It is not exercised in CI.

## Why both Airflow and Dagster?

To show the same pipeline in the two most common orchestration styles. Airflow schedules tasks: a monthly DAG with catchup replays 2016 to 2018 as monthly refreshes. Dagster models data assets: raw tables, every dbt model and the QC step are assets with lineage, and the quality gate is a set of blocking asset checks. Both call the same Python step functions, so neither has its own copy of the logic.

## Why write custom generic tests instead of using dbt_utils?

The project needs no package downloads, so it builds anywhere. The three generic tests (`expression_is_true`, `unique_combination_of_columns`, `reconciles_with`) are short and show how dbt tests work under the hood. `reconciles_with` is the important one: it compares a sum or count between two relations, overall or per group, with a tolerance. That is how every layer reconciles to the layer before it.

## How do you know the incremental models are right?

`tests/test_incremental_equivalence.py` loads two month-end extracts, builds the second one incrementally, fingerprints the incremental tables (row count plus an order-independent hash of every column), then rebuilds from scratch and asserts the fingerprints are identical. The refresh-over-refresh QC also confirms that closed months never change between replayed refreshes.

## What would you do next?

- Add a mart-level check of each published table against its source in the gate, not only in dbt tests.
- Replace the fixed 25% volume band with a seasonal baseline.
- Add SCD type 2 snapshots once a source with changing attributes is available. This dataset is a single extract, so nothing changes over time.
- Run the Snowflake target in CI against a trial account.
