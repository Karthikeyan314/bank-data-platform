"""Quality gate: checks the raw layer before any model is rebuilt.

A load that finishes is not a load that is right. Each check is either:

* BLOCK - the data is wrong in a way no model can fix. The gate exits non-zero, the
  orchestrator skips every downstream task, and the marts keep the last good build:
  stale, but not wrong.
* ALERT - something moved that a person should look at, but the data is usable. The
  result is recorded and an alert is sent; the run continues.

Every result is stored in `dq.check_results`, so any morning can be replayed. To let one
run through on purpose, insert a row into `dq.gate_bypass` for the check and as-of date.

Usage:
    python -m pipeline.quality_gate --as-of 2016-06-30
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from dataclasses import dataclass

import duckdb

from pipeline import notify
from pipeline.config import WAREHOUSE_PATH

log = logging.getLogger("quality_gate")

VOLUME_BAND = 0.25  # latest closed month may sit within +/-25% of the trailing median
FRESHNESS_ALERT_DAYS = 4


@dataclass(frozen=True)
class Check:
    check_id: str
    name: str
    severity: str  # BLOCK or ALERT
    sql: str  # returns (observed, passed) as one row
    description: str


CHECKS: tuple[Check, ...] = (
    Check(
        "G01",
        "Every raw table has rows",
        "BLOCK",
        """
        select min(row_count)::varchar, min(row_count) > 0
        from raw._load_audit where run_id = (select run_id from raw._load_audit
                                            order by loaded_at desc limit 1)
        """,
        "An empty table means the extract or the load failed silently.",
    ),
    Check(
        "G02",
        "Transaction ids are unique",
        "BLOCK",
        """
        select (count(*) - count(distinct trans_id))::varchar,
               count(*) = count(distinct trans_id)
        from raw."transaction"
        """,
        "A duplicated posting would double money in every downstream total.",
    ),
    Check(
        "G03",
        "Dates and amounts parse",
        "BLOCK",
        """
        select count(*)::varchar, count(*) = 0
        from raw."transaction"
        where try_cast(trans_date as date) is null
           or try_cast(amount as decimal(14, 2)) is null
           or try_cast(balance as decimal(14, 2)) is null
        """,
        "Unparseable values mean the file layout changed upstream.",
    ),
    Check(
        "G04",
        "Every posting has an account",
        "BLOCK",
        """
        select count(*)::varchar, count(*) = 0
        from raw."transaction" t
        left join raw.account a on t.account_id = a.account_id
        where a.account_id is null
        """,
        "Orphan postings would silently drop out of every account-level mart.",
    ),
    Check(
        "G05",
        "No closed month is empty",
        "BLOCK",
        """
        with m as (
            select date_trunc('month', trans_date::date) as month_start, count(*) as n
            from raw."transaction" group by 1
        ),
        expected as (
            select unnest(generate_series(min(month_start), max(month_start), interval 1 month))
                   as month_start
            from m
        )
        select count(*)::varchar, count(*) = 0
        from expected e left join m using (month_start)
        where m.n is null
        """,
        "A gap in the monthly series means a partition was missed.",
    ),
    Check(
        "G06",
        "Feed is current",
        "ALERT",
        f"""
        select (date '{{as_of}}' - max(trans_date::date))::varchar || ' days',
               date '{{as_of}}' - max(trans_date::date) <= {FRESHNESS_ALERT_DAYS}
        from raw."transaction"
        """,
        "Latest posting should be within a few days of the extract date.",
    ),
    Check(
        "G07",
        "Volume band",
        "ALERT",
        f"""
        with m as (
            select date_trunc('month', trans_date::date) as month_start, count(*) as n
            from raw."transaction"
            where trans_date::date < date_trunc('month', date '{{as_of}}')
            group by 1
        ),
        latest as (select * from m order by month_start desc limit 1),
        trail as (
            select median(n) as med from (
                select n from m where month_start < (select month_start from latest)
                order by month_start desc limit 6
            )
        )
        select latest.n::varchar || ' vs median ' || coalesce(trail.med, latest.n)::varchar,
               trail.med is null
               or abs(latest.n - trail.med) <= {VOLUME_BAND} * trail.med
        from latest, trail
        """,
        "Latest closed month within 25% of the median of the six months before it.",
    ),
    Check(
        "G08",
        "Blank category share",
        "ALERT",
        """
        select
            round(100.0 * count_if(nullif(trim(category), '') is null) / count(*), 1)::varchar
                || '%',
            count_if(nullif(trim(category), '') is null) <= 0.6 * count(*)
        from raw."transaction"
        """,
        "Uncategorised postings normally run near 51%; a jump means a mapping broke upstream.",
    ),
)


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists dq")
    con.execute(
        """
        create table if not exists dq.check_results (
            run_id varchar, as_of_date date, check_id varchar, check_name varchar,
            severity varchar, observed varchar, passed boolean, bypassed boolean,
            checked_at timestamp
        )
        """
    )
    con.execute(
        """
        create table if not exists dq.gate_bypass (
            check_id varchar, as_of_date date, reason varchar, approved_by varchar
        )
        """
    )


def run(as_of: dt.date, run_id: str = "manual") -> bool:
    """Run every check; return True when no BLOCK check failed."""
    checked_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    blocked: list[str] = []
    alerts: list[str] = []
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        ensure_tables(con)
        bypass = {
            row[0]
            for row in con.execute(
                "select check_id from dq.gate_bypass where as_of_date = ?", [as_of]
            ).fetchall()
        }
        for check in CHECKS:
            row = con.execute(check.sql.replace("{as_of}", as_of.isoformat())).fetchone()
            if row is None:
                # Nothing to measure yet (e.g. no closed month). A BLOCK check that cannot
                # verify the data has not verified it; an ALERT check has nothing to report.
                row = ("no rows to check", check.severity == "ALERT")
            observed, passed = row
            bypassed = (not passed) and check.check_id in bypass
            con.execute(
                "insert into dq.check_results values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    as_of,
                    check.check_id,
                    check.name,
                    check.severity,
                    observed,
                    passed,
                    bypassed,
                    checked_at,
                ],
            )
            status = "PASS" if passed else ("BYPASSED" if bypassed else "FAIL")
            log.info(
                "%s %-5s %-30s %-8s %s",
                check.check_id,
                check.severity,
                check.name,
                status,
                observed,
            )
            if not passed and not bypassed:
                line = f"{check.check_id} {check.name}: {observed}. {check.description}"
                (blocked if check.severity == "BLOCK" else alerts).append(line)

    if alerts:
        notify.send("Quality gate alert", "\n".join(alerts), level="warning", run_id=run_id)
    if blocked:
        notify.send(
            "Quality gate BLOCKED the pipeline",
            "Downstream models were not rebuilt; marts keep the last good build.\n"
            + "\n".join(blocked),
            level="error",
            run_id=run_id,
        )
        return False
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.date(2018, 12, 31))
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()
    sys.exit(0 if run(args.as_of, args.run_id) else 1)


if __name__ == "__main__":
    main()
