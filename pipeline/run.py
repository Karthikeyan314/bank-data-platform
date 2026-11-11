"""Run the whole pipeline once, in order, without an orchestrator.

    extract -> load raw -> quality gate -> dbt staging + intermediate -> dbt marts -> refresh QC

Airflow and Dagster call the same step functions; this module is the reference order and
what CI runs. A BLOCK in the gate stops the run before any model is rebuilt.

Usage:
    python -m pipeline.run                       # full history
    python -m pipeline.run --as-of 2016-06-30    # simulated extract
    python -m pipeline.run --replay 2016-01-31 2016-06-30   # one refresh per month-end
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import logging
import sys

from pipeline import dbt_step, ingest, load, notify, quality_gate, refresh_compare

log = logging.getLogger("run")


def run_once(as_of: dt.date | None, full_refresh: bool = False) -> str:
    ingest.run()
    run_id = load.run(as_of=as_of)
    gate_date = as_of or dt.date(2018, 12, 31)
    if not quality_gate.run(gate_date, run_id=run_id):
        raise SystemExit(2)
    try:
        dbt_step.build("resource_type:seed staging intermediate", full_refresh=full_refresh)
        dbt_step.build("marts", full_refresh=full_refresh)
    except RuntimeError as exc:
        notify.send("dbt build failed", str(exc), level="error", run_id=run_id)
        raise
    refresh_compare.run(run_id, as_of)
    notify.send("Pipeline run succeeded", f"as of {as_of or 'full history'}", run_id=run_id)
    return run_id


def month_ends(start: dt.date, end: dt.date) -> list[dt.date]:
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(dt.date(y, m, calendar.monthrange(y, m)[1]))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", type=dt.date.fromisoformat)
    parser.add_argument("--replay", nargs=2, type=dt.date.fromisoformat, metavar=("FROM", "TO"))
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    if args.replay:
        for i, as_of in enumerate(month_ends(*args.replay)):
            log.info("=== replay refresh as of %s ===", as_of)
            run_once(as_of, full_refresh=args.full_refresh and i == 0)
    else:
        run_once(args.as_of, full_refresh=args.full_refresh)


if __name__ == "__main__":
    sys.exit(main())
