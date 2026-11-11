"""The refresh-over-refresh QC must catch a published month that changed between refreshes."""

from __future__ import annotations

import duckdb

from pipeline import refresh_compare

COLUMNS = (
    "month_start date, open_accounts bigint, active_accounts bigint, new_accounts bigint, "
    "total_deposits decimal(18,2), credits decimal(18,2), debits decimal(18,2), "
    "transaction_count bigint, overdrawn_accounts bigint, interest_credited decimal(18,2), "
    "loans_granted bigint, principal_granted decimal(18,2)"
)


def publish(db, rows):
    with duckdb.connect(str(db)) as con:
        con.execute("create schema if not exists reporting")
        con.execute(f"create or replace table reporting.mart_monthly_kpis ({COLUMNS})")
        con.executemany(
            "insert into reporting.mart_monthly_kpis values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def month(m, deposits, transaction_count=1000):
    return (
        f"2016-0{m}-01",
        100,
        90,
        5,
        deposits,
        500.0,
        400.0,
        transaction_count,
        3,
        10.0,
        1,
        900.0,
    )


def test_restated_closed_month_and_band_breach_are_reported(tmp_path, monkeypatch):
    db = tmp_path / "w.duckdb"
    monkeypatch.setattr(refresh_compare, "WAREHOUSE_PATH", db)
    monkeypatch.setattr(refresh_compare, "REPORT_DIR", tmp_path / "qc")
    alerts = []
    monkeypatch.setattr(
        refresh_compare.notify, "send", lambda subject, *a, **k: alerts.append(subject)
    )

    publish(db, [month(1, 10_000.00), month(2, 11_000.00)])
    assert refresh_compare.run("run1") == []

    # Next refresh: January's deposits moved by 50.00, and March doubles its transactions.
    publish(
        db, [month(1, 10_050.00), month(2, 11_000.00), month(3, 11_500.00, transaction_count=2000)]
    )
    findings = refresh_compare.run("run2")

    kinds = {(f[0], str(f[1]), f[2]) for f in findings}
    assert ("RESTATED", "2016-01-01", "total_deposits") in kinds
    assert ("OUT_OF_BAND", "2016-03-01", "transaction_count") in kinds
    assert not any(f[1].month == 2 and f[0] == "RESTATED" for f in findings)
    assert "Refresh QC: published numbers changed" in alerts
    assert (tmp_path / "qc" / "qc_run2.md").read_text().count("RESTATED") == 1
