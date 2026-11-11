"""Dagster version of the pipeline: the same steps as software-defined assets.

Where Airflow schedules tasks, Dagster models the data: every raw table and every dbt
model is an asset with lineage, and the quality gate is a set of blocking asset checks on
the raw tables, so a failed check stops downstream materialisation.

    dagster dev -m bank_dagster.definitions      (from orchestration/dagster)

No `from __future__ import annotations` here: Dagster reads the Config type hints at runtime.
"""

import datetime as dt
import os
import sys
from pathlib import Path

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Config,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline import ingest, load, refresh_compare
from pipeline.config import DBT_PROJECT_DIR, SOURCES, WAREHOUSE_PATH
from pipeline.quality_gate import CHECKS

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

RAW_KEYS = {src.name: AssetKey(["bank", src.name]) for src in SOURCES}  # = dbt source keys


class RefreshConfig(Config):
    as_of: str = "2018-12-31"  # month-end extract date


@asset(group_name="extract", description="Source files downloaded and checksum-verified.")
def landing_files() -> MaterializeResult:
    checksums = ingest.run()
    return MaterializeResult(metadata={"files": len(checksums)})


@multi_asset(
    specs=[AssetSpec(key, deps=[landing_files], group_name="raw") for key in RAW_KEYS.values()],
    description="Raw tables loaded untouched into the warehouse `raw` schema.",
)
def raw_tables(context: AssetExecutionContext, config: RefreshConfig):
    run_id = context.run_id[:12]
    load.run(as_of=dt.date.fromisoformat(config.as_of), run_id=run_id)
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        for name, key in RAW_KEYS.items():
            rows = con.execute(f'select count(*) from raw."{name}"').fetchone()[0]
            yield MaterializeResult(asset_key=key, metadata={"rows": rows, "run_id": run_id})


def _gate_check(check):
    @asset_check(
        name=f"gate_{check.check_id.lower()}",
        asset=RAW_KEYS["transaction"],
        blocking=check.severity == "BLOCK",
        description=f"{check.name}. {check.description}",
    )
    def _check(config: RefreshConfig) -> AssetCheckResult:
        with duckdb.connect(str(WAREHOUSE_PATH)) as con:
            row = con.execute(check.sql.replace("{as_of}", config.as_of)).fetchone()
        observed, passed = row if row else ("no rows to check", check.severity == "ALERT")
        return AssetCheckResult(
            passed=bool(passed),
            severity=AssetCheckSeverity.ERROR
            if check.severity == "BLOCK"
            else AssetCheckSeverity.WARN,
            metadata={"observed": str(observed)},
        )

    return _check


gate_checks = [_gate_check(c) for c in CHECKS]


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def bank_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--indirect-selection", "cautious"], context=context).stream()


@asset(
    deps=[AssetKey("mart_monthly_kpis")],
    group_name="qc",
    description="Refresh-over-refresh comparison of the published KPIs.",
)
def refresh_qc(context: AssetExecutionContext, config: RefreshConfig) -> MaterializeResult:
    findings = refresh_compare.run(context.run_id[:12], dt.date.fromisoformat(config.as_of))
    return MaterializeResult(
        metadata={
            "findings": len(findings),
            "restated": sum(1 for f in findings if f[0] in ("RESTATED", "DROPPED")),
            "report": str(Path(refresh_compare.REPORT_DIR) / f"qc_{context.run_id[:12]}.md"),
        }
    )


# DuckDB allows one writer per file, so steps run one at a time in a single process.
monthly_refresh = define_asset_job(
    "monthly_refresh", selection="*", executor_def=in_process_executor
)

# Use the dbt installed next to this interpreter, not whatever `dbt` is first on PATH.
DBT_EXECUTABLE = str(Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt"))

defs = Definitions(
    assets=[landing_files, raw_tables, bank_dbt_models, refresh_qc],
    asset_checks=gate_checks,
    jobs=[monthly_refresh],
    schedules=[ScheduleDefinition(job=monthly_refresh, cron_schedule="0 6 1 * *")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE)},
)
