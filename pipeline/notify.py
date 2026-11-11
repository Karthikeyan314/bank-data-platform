"""Alerts: every run outcome and failed check reaches a person, not just a log.

Channels are configured by environment variable only:

* SLACK_WEBHOOK_URL               post to a Slack incoming webhook
* SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_EMAIL_TO, ALERT_EMAIL_FROM
                                  send an email

With nothing configured, alerts are only appended to `logs/alerts.jsonl`, so local runs
and CI never message anyone. A failed send is logged and recorded, never raised: an alert
problem must not take the pipeline down with it.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import smtplib
from email.message import EmailMessage

import httpx

from pipeline.config import REPO_ROOT

log = logging.getLogger("notify")
ALERT_LOG = REPO_ROOT / "logs" / "alerts.jsonl"


def send(subject: str, body: str, level: str = "info", run_id: str | None = None) -> None:
    record = {
        "sent_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "level": level,
        "run_id": run_id,
        "subject": subject,
        "body": body,
        "channels": [],
        "errors": [],
    }
    text = f"[{level.upper()}] {subject}\n{body}"

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            httpx.post(webhook, json={"text": text}, timeout=10).raise_for_status()
            record["channels"].append("slack")
        except httpx.HTTPError as exc:
            record["errors"].append(f"slack: {exc}")

    if os.environ.get("SMTP_HOST") and os.environ.get("ALERT_EMAIL_TO"):
        try:
            msg = EmailMessage()
            msg["Subject"] = f"[bank-data-platform] {subject}"
            msg["From"] = os.environ.get("ALERT_EMAIL_FROM", "pipeline@localhost")
            msg["To"] = os.environ["ALERT_EMAIL_TO"]
            msg.set_content(text)
            port = int(os.environ.get("SMTP_PORT", "587"))
            with smtplib.SMTP(os.environ["SMTP_HOST"], port, timeout=10) as smtp:
                smtp.starttls()
                if os.environ.get("SMTP_USER"):
                    smtp.login(os.environ["SMTP_USER"], os.environ.get("SMTP_PASSWORD", ""))
                smtp.send_message(msg)
            record["channels"].append("email")
        except (OSError, smtplib.SMTPException) as exc:
            record["errors"].append(f"email: {exc}")

    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
    for err in record["errors"]:
        log.error("alert delivery failed: %s", err)
    log.log(
        logging.ERROR
        if level == "error"
        else logging.WARNING
        if level == "warning"
        else logging.INFO,
        "alert: %s",
        subject,
    )
