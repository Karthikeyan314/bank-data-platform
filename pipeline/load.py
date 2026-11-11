"""Load: land the verified files in the warehouse `raw` schema, exactly as received.

Raw tables keep every column as text, so a bad value in the source never breaks the
load; typing and cleaning happen in dbt staging, where failures are tested and visible.

The source is a static 1993-1998 extract. To exercise incremental models, freshness
checks and refresh-over-refresh QC the way a live feed would, the loader can replay it:
`--as-of 2016-06-30` emits only the rows whose business date is on or before that day,
like a source system extracted on that date. Without `--as-of` the full history loads.

Usage:
    python -m pipeline.load                    # full history
    python -m pipeline.load --as-of 2016-06-30 # simulated extract as of a date
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import uuid

import duckdb

from pipeline.config import CHECKSUM_LOCK, LANDING_DIR, SOURCES, WAREHOUSE_PATH

log = logging.getLogger("load")

COLUMNS: dict[str, list[str]] = {
    "account": ["account_id", "district_id", "create_date", "frequency"],
    "client": ["client_id", "birth_date", "gender", "district_id"],
    "disposition": ["disp_id", "client_id", "account_id", "disp_type"],
    "permanent_order": ["order_id", "account_id", "bank_to", "account_to", "amount", "category"],
    "transaction": [
        "trans_id",
        "account_id",
        "trans_date",
        "amount",
        "balance",
        "trans_type",
        "operation",
        "category",
        "other_bank_id",
        "other_account_id",
    ],
    "loan": ["loan_id", "account_id", "granted_date", "amount", "duration", "payments", "status"],
    "card": ["card_id", "disp_id", "card_type", "issued_date"],
    "district": [
        "district_id",
        "district_name",
        "region",
        "num_inhabitants",
        "num_municipalities_lt500",
        "num_municipalities_500_1999",
        "num_municipalities_2000_9999",
        "num_municipalities_ge10000",
        "num_cities",
        "ratio_urban",
        "average_salary",
        "unemployment_rate_95",
        "unemployment_rate_96",
        "num_entrepreneurs_per_1000",
        "num_crimes_95",
        "num_crimes_96",
    ],
}

# Business date that decides whether a row "exists yet" in a replayed extract.
# Districts are reference data and always load in full. Tables that hang off an account
# (dispositions, standing orders) and clients exist once their account does.
# Known limit: loan status is the status at the end of the source period, so a replayed
# extract sees each loan's final status from the day it is granted.
EVENT_DATE: dict[str, str] = {
    "account": "create_date",
    "transaction": "trans_date",
    "loan": "granted_date",
    "card": "issued_date",
}
ACCOUNT_CHILDREN = {"disposition", "permanent_order"}


def ensure_audit(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw")
    con.execute(
        """
        create table if not exists raw._load_audit (
            run_id varchar, table_name varchar, source_file varchar, source_sha256 varchar,
            as_of_date date, row_count bigint, loaded_at timestamp
        )
        """
    )


def run(as_of: dt.date | None = None, run_id: str | None = None) -> str:
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    checksums = json.loads(CHECKSUM_LOCK.read_text())["files"]
    run_id = run_id or uuid.uuid4().hex[:12]
    loaded_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)

    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        ensure_audit(con)
        for src in SOURCES:
            path = (LANDING_DIR / src.file).as_posix()
            cols = COLUMNS[src.name]
            col_spec = ", ".join(f"'{c}': 'VARCHAR'" for c in cols)
            reader = (
                f"read_csv('{path}', delim='\t', header=false, quote='', escape='', "
                f"columns={{{col_spec}}}, null_padding=true, strict_mode=false)"
            )
            where = ""
            if as_of and src.name in EVENT_DATE:
                where = f"where {EVENT_DATE[src.name]} <= '{as_of.isoformat()}'"
            elif as_of and src.name in ACCOUNT_CHILDREN:
                # raw.account was replaced earlier in this run, so it holds the as-of accounts.
                where = "where account_id in (select account_id from raw.account)"
            elif as_of and src.name == "client":
                # A client enters the extract with their first account.
                dispositions = (
                    f"read_csv('{(LANDING_DIR / 'fin_disp.tsv').as_posix()}', delim='\t', "
                    "header=false, columns={'disp_id': 'VARCHAR', 'client_id': 'VARCHAR', "
                    "'account_id': 'VARCHAR', 'disp_type': 'VARCHAR'})"
                )
                where = (
                    f"where client_id in (select client_id from {dispositions} "
                    "where account_id in (select account_id from raw.account))"
                )
            con.execute(
                f"""
                create or replace table raw."{src.name}" as
                select *,
                       '{src.file}' as _source_file,
                       '{run_id}' as _run_id,
                       timestamp '{loaded_at.isoformat(sep=" ")}' as _loaded_at
                from {reader}
                {where}
                """
            )
            rows = con.execute(f'select count(*) from raw."{src.name}"').fetchone()[0]
            if as_of is None and rows != src.expected_rows:
                raise SystemExit(
                    f"raw.{src.name}: loaded {rows:,} rows, source documents {src.expected_rows:,}"
                )
            con.execute(
                "insert into raw._load_audit values (?, ?, ?, ?, ?, ?, ?)",
                [run_id, src.name, src.file, checksums[src.file], as_of, rows, loaded_at],
            )
            log.info("raw.%-16s %10s rows", src.name, f"{rows:,}")
    log.info("load run %s complete (as of %s)", run_id, as_of or "full history")
    return run_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--as-of", type=dt.date.fromisoformat, default=None)
    parser.add_argument("--run-id", default=None, help="defaults to a random id")
    args = parser.parse_args()
    run(as_of=args.as_of, run_id=args.run_id)


if __name__ == "__main__":
    main()
