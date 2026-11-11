"""Run dbt programmatically, so every orchestrator calls dbt the same way.

Usage:
    python -m pipeline.dbt_step --select "staging intermediate"
    python -m pipeline.dbt_step --select marts --full-refresh
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from dbt.cli.main import dbtRunner

from pipeline.config import DBT_PROJECT_DIR, WAREHOUSE_PATH

log = logging.getLogger("dbt_step")


def build(select: str | None = None, full_refresh: bool = False) -> None:
    os.environ.setdefault("BANK_WAREHOUSE", str(WAREHOUSE_PATH))
    args = [
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        # Only run tests whose every parent is in this step. The default ("eager") would
        # run a mart's reconciliation test during the staging step, against last run's mart.
        "--indirect-selection",
        "cautious",
    ]
    if select:
        args += ["--select", select]
    if full_refresh:
        args.append("--full-refresh")
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt build failed (select={select!r})")
    log.info("dbt build ok (select=%s)", select or "all")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--select", default=None)
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()
    try:
        build(args.select, args.full_refresh)
    except RuntimeError as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
