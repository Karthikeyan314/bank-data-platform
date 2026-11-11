"""Integration test: incremental builds must equal a full rebuild, row for row.

Replays two month-end extracts (full refresh, then incremental), fingerprints every
incremental model, rebuilds from scratch at the same as-of date and compares. A
difference means the lookback window or the balance anchor in the incremental logic is
wrong. Runs against a throwaway warehouse, so it never touches data/warehouse.duckdb.

    pytest -m integration
"""

from __future__ import annotations

import datetime as dt
import importlib
import os

import duckdb
import pytest

INCREMENTAL_MODELS = ("intermediate.int_transactions_sequenced", "core.fct_transactions")


def fingerprint(path: str, table: str) -> tuple:
    """Row count plus an order-independent hash of every column except load metadata."""
    # Same connection settings as dbt-duckdb, which keeps its connection open in-process.
    with duckdb.connect(path) as con:
        cols = [r[0] for r in con.execute(f"describe {table}").fetchall() if r[0] != "_loaded_at"]
        row = ", ".join(cols)
        return con.execute(
            f"select count(*), sum(hash(struct_pack({row}))) from {table}"
        ).fetchone()


@pytest.mark.integration
def test_incremental_matches_full_refresh(tmp_path, monkeypatch):
    warehouse = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("BANK_WAREHOUSE", str(warehouse))
    os.environ["BANK_WAREHOUSE"] = str(warehouse)

    import pipeline.config

    importlib.reload(pipeline.config)
    from pipeline import dbt_step, ingest, load

    for mod in (load, dbt_step):
        importlib.reload(mod)

    ingest.run()
    load.run(as_of=dt.date(2016, 5, 31))
    dbt_step.build("resource_type:seed staging intermediate marts", full_refresh=True)
    load.run(as_of=dt.date(2016, 6, 30))
    dbt_step.build("staging intermediate marts")
    incremental = {t: fingerprint(str(warehouse), t) for t in INCREMENTAL_MODELS}

    dbt_step.build("staging intermediate marts", full_refresh=True)
    full = {t: fingerprint(str(warehouse), t) for t in INCREMENTAL_MODELS}

    assert incremental == full
