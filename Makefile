.PHONY: setup run replay test test-all lint docs airflow dagster clean

setup:            ## install the environment
	uv sync --all-extras

run:              ## one full build: extract, load, gate, dbt, QC
	uv run python -m pipeline.run --full-refresh

replay:           ## twelve monthly refreshes of 2018, as a live feed would arrive
	uv run python -m pipeline.run --replay 2018-01-31 2018-12-31 --full-refresh

test:             ## fast unit tests
	uv run pytest -m "not integration" -q

test-all:         ## unit tests plus the incremental-equals-full-rebuild check
	uv run pytest -q

lint:
	uv run ruff check . && uv run ruff format --check .
	uv run sqlfluff lint transform/models transform/tests/assert_*.sql

docs:             ## dbt docs with lineage at http://localhost:8081
	uv run dbt docs generate --project-dir transform --profiles-dir transform
	uv run dbt docs serve --project-dir transform --profiles-dir transform --port 8081

airflow:          ## Airflow at http://localhost:8080 (needs Docker)
	docker compose -f orchestration/airflow/docker-compose.yml up --build

dagster:          ## Dagster at http://localhost:3000
	cd orchestration/dagster && PYTHONPATH=. uv run dagster dev -m bank_dagster.definitions

clean:
	rm -rf data/warehouse.duckdb reports logs transform/target transform/logs
