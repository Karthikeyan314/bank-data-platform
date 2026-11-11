"""Monthly refresh of the bank data platform.

One DAG run per month-end. With catchup on, Airflow backfills 2016-2018 as a series of
monthly refreshes: each run extracts the source as of its month-end, so the incremental
models, the quality gate and the refresh-over-refresh QC run exactly as on a live feed.

    ingest -> load_raw -> quality_gate -> dbt_staging -> dbt_marts -> refresh_qc

The pipeline runs in its own virtualenv inside the image (BANK_PYTHON), so dbt's
dependencies never clash with Airflow's. A BLOCK in the gate fails its task and Airflow
skips everything downstream: the marts keep last month's build.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

BANK_PYTHON = os.environ.get("BANK_PYTHON", "/opt/bank/.venv/bin/python")
BANK_HOME = os.environ.get("BANK_HOME", "/opt/bank")
AS_OF = "{{ (data_interval_end - macros.timedelta(days=1)) | ds }}"
RUN_ID = "{{ run_id | replace(':', '') | replace('+', '') }}"


def alert_on_failure(context) -> None:
    """Send the failure through the pipeline's own alert channel (Slack, email or log)."""
    ti = context["task_instance"]
    message = f"{ti.dag_id}.{ti.task_id} failed for {context['run_id']} (try {ti.try_number})"
    subprocess.run(
        [
            BANK_PYTHON,
            "-c",
            "import sys; from pipeline import notify; "
            "notify.send('Airflow task failed', sys.argv[1], level='error', run_id=sys.argv[2])",
            message,
            context["run_id"],
        ],
        cwd=BANK_HOME,
        check=False,
        timeout=60,
    )


def step(task_id: str, command: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {BANK_HOME} && {BANK_PYTHON} -m {command}",
        **kwargs,
    )


@dag(
    dag_id="bank_monthly_refresh",
    schedule="@monthly",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2018, 12, 31),
    catchup=True,
    max_active_runs=1,  # refreshes are sequential: each builds on the last
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_on_failure,
    },
    tags=["bank", "dbt", "quality"],
    doc_md=__doc__,
)
def bank_monthly_refresh():
    ingest = step("ingest", "pipeline.ingest")
    load_raw = step("load_raw", f"pipeline.load --as-of {AS_OF} --run-id {RUN_ID}")
    # Not retried: a failed check is a data problem, and retrying would only page twice.
    gate = step(
        "quality_gate",
        f"pipeline.quality_gate --as-of {AS_OF} --run-id {RUN_ID}",
        retries=0,
    )
    dbt_staging = step(
        "dbt_staging", 'pipeline.dbt_step --select "resource_type:seed staging intermediate"'
    )
    dbt_marts = step("dbt_marts", "pipeline.dbt_step --select marts")
    refresh_qc = step("refresh_qc", f"pipeline.refresh_compare --run-id {RUN_ID} --as-of {AS_OF}")

    ingest >> load_raw >> gate >> dbt_staging >> dbt_marts >> refresh_qc


bank_monthly_refresh()
