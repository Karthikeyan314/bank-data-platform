"""Fast unit tests for the Python pipeline steps. No network, no real warehouse."""

from __future__ import annotations

import datetime as dt
import json

import duckdb
import pytest

from pipeline import ingest, notify, quality_gate, run


def test_month_ends_cover_each_month_once():
    ends = run.month_ends(dt.date(2015, 11, 3), dt.date(2016, 2, 1))
    assert ends == [
        dt.date(2015, 11, 30),
        dt.date(2015, 12, 31),
        dt.date(2016, 1, 31),
        dt.date(2016, 2, 29),  # leap year
    ]


def test_ingest_rejects_a_file_whose_checksum_changed(tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    landing.mkdir()
    lock = tmp_path / "sources.lock.json"
    for src in ingest.SOURCES:
        (landing / src.file).write_text("original")
    good = ingest.sha256_of(landing / ingest.SOURCES[0].file)
    lock.write_text(
        json.dumps(
            {
                "commit": ingest.SOURCE_COMMIT,
                "files": {s.file: good for s in ingest.SOURCES},
            }
        )
    )
    monkeypatch.setattr(ingest, "LANDING_DIR", landing)
    monkeypatch.setattr(ingest, "CHECKSUM_LOCK", lock)

    ingest.run()  # all files match the lock

    tampered = landing / ingest.SOURCES[3].file
    tampered.write_text("changed upstream")
    with pytest.raises(SystemExit, match="Checksum mismatch"):
        ingest.run()
    assert not tampered.exists(), "a bad file must not stay in the landing zone"


def test_notify_without_channels_only_writes_the_log(tmp_path, monkeypatch):
    for var in ("SLACK_WEBHOOK_URL", "SMTP_HOST", "ALERT_EMAIL_TO"):
        monkeypatch.delenv(var, raising=False)
    log_file = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(notify, "ALERT_LOG", log_file)

    notify.send("subject", "body", level="warning", run_id="r1")

    record = json.loads(log_file.read_text().splitlines()[0])
    assert record["channels"] == [] and record["errors"] == []
    assert record["level"] == "warning" and record["run_id"] == "r1"


def _tiny_raw(path, trans_ids):
    with duckdb.connect(str(path)) as con:
        con.execute("create schema raw")
        con.execute("create table raw.account as select '1' as account_id")
        rows = ", ".join(
            f"('{t}', '1', '2016-01-{d:02d}', '10.00', '10.00', 'HH')"
            for t, d in zip(trans_ids, range(1, 32), strict=False)
        )
        con.execute(
            'create table raw."transaction" (trans_id varchar, account_id varchar, '
            "trans_date varchar, amount varchar, balance varchar, category varchar)"
        )
        con.execute(f'insert into raw."transaction" values {rows}')
        con.execute(
            "create table raw._load_audit as select 'run1' as run_id, 'transaction' as table_name,"
            " 3 as row_count, now() as loaded_at"
        )


def test_gate_blocks_duplicate_transaction_ids(tmp_path, monkeypatch):
    db = tmp_path / "w.duckdb"
    _tiny_raw(db, ["1", "2", "2"])
    monkeypatch.setattr(quality_gate, "WAREHOUSE_PATH", db)
    sent = []
    monkeypatch.setattr(quality_gate.notify, "send", lambda *a, **k: sent.append((a, k)))

    assert quality_gate.run(dt.date(2016, 1, 31), run_id="t") is False
    with duckdb.connect(str(db)) as con:
        failed = con.execute(
            "select check_id from dq.check_results where not passed and severity = 'BLOCK'"
        ).fetchall()
    assert ("G02",) in failed
    assert any("BLOCKED" in a[0] for a, _ in sent)


def test_gate_bypass_lets_a_known_failure_through(tmp_path, monkeypatch):
    db = tmp_path / "w.duckdb"
    _tiny_raw(db, ["1", "2", "2"])
    with duckdb.connect(str(db)) as con:
        quality_gate.ensure_tables(con)
        con.execute(
            "insert into dq.gate_bypass values ('G02', date '2016-01-31', 'known dup', 'qa')"
        )
    monkeypatch.setattr(quality_gate, "WAREHOUSE_PATH", db)
    monkeypatch.setattr(quality_gate.notify, "send", lambda *a, **k: None)

    assert quality_gate.run(dt.date(2016, 1, 31), run_id="t") is True
