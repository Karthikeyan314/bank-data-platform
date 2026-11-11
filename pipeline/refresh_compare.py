"""Refresh-over-refresh QC: compare this refresh's published KPIs with the last one.

Tests prove a build is internally consistent. They cannot tell you that the numbers the
board saw last month have changed. After every successful build this step:

1. snapshots `reporting.mart_monthly_kpis` into `qc.kpi_snapshots` under the run id;
2. compares it with the previous snapshot, metric by metric, month by month:
   * RESTATED - a month present in both refreshes changed. Closed months should be frozen,
     so any movement is reported with its before and after values;
   * DROPPED  - a month in the previous refresh is missing from this one;
   * OUT_OF_BAND - the newest month moved more than the band against the month before it;
3. writes the differences to `qc.refresh_diffs` and a Markdown report to `reports/qc/`,
   and alerts when anything needs a person.

Usage:
    python -m pipeline.refresh_compare --run-id <id> --as-of 2016-06-30
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging

import duckdb

from pipeline import notify
from pipeline.config import REPO_ROOT, WAREHOUSE_PATH

log = logging.getLogger("refresh_compare")

REPORT_DIR = REPO_ROOT / "reports" / "qc"
METRICS = (
    "open_accounts",
    "active_accounts",
    "new_accounts",
    "total_deposits",
    "credits",
    "debits",
    "transaction_count",
    "overdrawn_accounts",
    "interest_credited",
    "loans_granted",
    "principal_granted",
)
# Money is compared to the cent, counts exactly.
TOLERANCE = 0.005
MONTH_OVER_MONTH_BAND = 0.25
BAND_METRICS = ("active_accounts", "transaction_count", "credits", "debits")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists qc")
    con.execute(
        """
        create table if not exists qc.kpi_snapshots as
        select cast(null as varchar) as run_id, cast(null as date) as as_of_date,
               cast(null as timestamp) as captured_at, *
        from reporting.mart_monthly_kpis where false
        """
    )
    con.execute(
        """
        create table if not exists qc.refresh_diffs (
            run_id varchar, previous_run_id varchar, finding varchar, month_start date,
            metric varchar, previous_value double, current_value double, change_pct double
        )
        """
    )


def run(run_id: str, as_of: dt.date | None = None) -> list[tuple]:
    captured_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        ensure_tables(con)
        con.execute("delete from qc.kpi_snapshots where run_id = ?", [run_id])
        con.execute(
            "insert into qc.kpi_snapshots select ?, ?, ?, * from reporting.mart_monthly_kpis",
            [run_id, as_of, captured_at],
        )
        prev = con.execute(
            """
            select run_id from qc.kpi_snapshots where run_id <> ?
            group by run_id order by max(captured_at) desc limit 1
            """,
            [run_id],
        ).fetchone()

        findings: list[tuple] = []
        previous_run = prev[0] if prev else None
        if previous_run:
            unpivot = " union all ".join(
                f"select month_start, '{m}' as metric, cast({m} as double) as value "
                f"from qc.kpi_snapshots where run_id = $run"
                for m in METRICS
            )
            findings += con.execute(
                f"""
                with cur as ({unpivot.replace("$run", "$cur")}),
                     prv as ({unpivot.replace("$run", "$prv")})
                select
                    case when c.value is null then 'DROPPED' else 'RESTATED' end,
                    p.month_start, p.metric, p.value, c.value,
                    case when p.value <> 0 then (c.value - p.value) / abs(p.value) end
                from prv p
                left join cur c on p.month_start = c.month_start and p.metric = c.metric
                where c.value is null or abs(c.value - p.value) > {TOLERANCE}
                order by p.month_start, p.metric
                """,
                {"cur": run_id, "prv": previous_run},
            ).fetchall()

        band_sql = " union all ".join(
            f"""
            select month_start, '{m}' as metric, prev_{m} as prev_value, {m} as value
            from (select month_start, {m}, lag({m}) over (order by month_start) as prev_{m}
                  from qc.kpi_snapshots where run_id = $cur)
            qualify month_start = max(month_start) over ()
            """
            for m in BAND_METRICS
        )
        findings += con.execute(
            f"""
            select 'OUT_OF_BAND', month_start, metric, prev_value, value,
                   (value - prev_value) / abs(prev_value)
            from ({band_sql})
            where prev_value is not null and prev_value <> 0
              and abs(value - prev_value) > {MONTH_OVER_MONTH_BAND} * abs(prev_value)
            """,
            {"cur": run_id},
        ).fetchall()

        con.execute("delete from qc.refresh_diffs where run_id = ?", [run_id])
        if findings:
            con.executemany(
                "insert into qc.refresh_diffs values (?, ?, ?, ?, ?, ?, ?, ?)",
                [(run_id, previous_run, *f) for f in findings],
            )
        months = con.execute(
            "select count(*), max(month_start) from qc.kpi_snapshots where run_id = ?", [run_id]
        ).fetchone()

    write_report(run_id, previous_run, as_of, months, findings)
    summarise(run_id, previous_run, findings)
    return findings


def write_report(run_id, previous_run, as_of, months, findings) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Refresh QC: run {run_id}",
        "",
        f"- As of: {as_of or 'full history'}",
        f"- Compared with: {previous_run or 'nothing (first refresh)'}",
        f"- Months published: {months[0]} (latest {months[1]})",
        f"- Findings: {len(findings)}",
        "",
    ]
    if findings:
        lines += [
            "| Finding | Month | Metric | Previous | Current | Change |",
            "|---|---|---|---:|---:|---:|",
        ]
        for kind, month, metric, before, after, pct in findings:
            pct_txt = f"{pct:+.2%}" if pct is not None else "n/a"
            lines.append(
                f"| {kind} | {month} | {metric} | {fmt(before)} | {fmt(after)} | {pct_txt} |"
            )
    else:
        lines.append("No restated, dropped or out-of-band numbers.")
    (REPORT_DIR / f"qc_{run_id}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def fmt(value) -> str:
    return "missing" if value is None else f"{value:,.2f}"


def summarise(run_id, previous_run, findings) -> None:
    restated = [f for f in findings if f[0] in ("RESTATED", "DROPPED")]
    band = [f for f in findings if f[0] == "OUT_OF_BAND"]
    log.info(
        "refresh QC %s vs %s: %s restated/dropped, %s out of band",
        run_id,
        previous_run,
        len(restated),
        len(band),
    )
    if restated:
        months = sorted({str(f[1]) for f in restated})
        notify.send(
            "Refresh QC: published numbers changed",
            f"{len(restated)} metric values moved in {len(months)} month(s) already published "
            f"({', '.join(months[:6])}{'...' if len(months) > 6 else ''}). "
            f"See reports/qc/qc_{run_id}.md.",
            level="warning",
            run_id=run_id,
        )
    if band:
        notify.send(
            "Refresh QC: newest month outside its band",
            "; ".join(f"{f[2]} {f[5]:+.1%}" for f in band),
            level="warning",
            run_id=run_id,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", type=dt.date.fromisoformat, default=None)
    args = parser.parse_args()
    run(args.run_id, args.as_of)


if __name__ == "__main__":
    main()
