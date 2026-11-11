"""Build the chatbot's knowledge base from the repository itself.

Every chunk is generated from a file in this repo (README sections, dbt model and test
descriptions, model SQL notes, quality-gate checks, orchestration docs) plus the hand-
written files in chatbot/knowledge/. Rebuilding after a change keeps the bot's answers in
step with the code, and every chunk records the file it came from so answers can cite it.

    python chatbot/build_knowledge.py     # writes chatbot/knowledge/chunks.jsonl
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "chatbot" / "knowledge" / "chunks.jsonl"
HANDWRITTEN = REPO / "chatbot" / "knowledge"
MAX_CHARS = 1800


def chunk_markdown(text: str, source: str, title_prefix: str = "") -> list[dict]:
    """Split Markdown on ## and ### headings; long sections are split on paragraphs."""
    chunks, heading, buf = [], title_prefix or source, []
    for line in text.splitlines():
        m = re.match(r"^(#{1,3})\s+(.*)", line)
        if m and buf:
            chunks += _flush(heading, "\n".join(buf), source)
            buf = []
        if m:
            heading = f"{title_prefix}{m.group(2)}" if title_prefix else m.group(2)
        buf.append(line)
    if buf:
        chunks += _flush(heading, "\n".join(buf), source)
    return chunks


def _flush(title: str, body: str, source: str) -> list[dict]:
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S).strip()
    # Unfilled template sections stay out, so the bot says "I don't know" instead.
    if len(body) < 40 or "TODO:" in body:
        return []
    parts, cur = [], ""
    for para in body.split("\n\n"):
        if cur and len(cur) + len(para) > MAX_CHARS:
            parts.append(cur)
            cur = ""
        cur = f"{cur}\n\n{para}" if cur else para
    parts.append(cur)
    return [{"title": title, "source": source, "text": p.strip()} for p in parts]


def dbt_models() -> list[dict]:
    """One chunk per dbt model: description, columns, tests, and the SQL header note."""
    chunks = []
    for yml in sorted((REPO / "transform" / "models").rglob("*.yml")):
        doc = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
        for model in doc.get("models", []):
            name = model["name"]
            sql = next((REPO / "transform" / "models").rglob(f"{name}.sql"), None)
            lines = [f"dbt model `{name}` (layer: {yml.parent.name})."]
            if model.get("description"):
                lines.append(" ".join(model["description"].split()))
            if sql:
                note = re.search(r"\{#(.*?)#\}", sql.read_text(encoding="utf-8"), re.S)
                if note:
                    lines.append("Design note: " + " ".join(note.group(1).split()))
                cfg = re.search(r"materialized='(\w+)'", sql.read_text(encoding="utf-8"))
                lines.append(f"Materialization: {cfg.group(1) if cfg else 'per folder default'}.")
            if (model.get("config") or {}).get("contract", {}).get("enforced"):
                lines.append("Contract: enforced (column names and types are fixed).")
            tests = [_test_name(t) for t in model.get("data_tests", [])]
            for col in model.get("columns", []):
                for t in col.get("data_tests", []) or []:
                    tests.append(f"{col['name']}: {_test_name(t)}")
                if col.get("description"):
                    lines.append(f"Column {col['name']}: {col['description']}")
            if tests:
                lines.append("Tests: " + "; ".join(tests) + ".")
            source = str((sql or yml).relative_to(REPO)).replace("\\", "/")
            chunks.append({"title": f"Model {name}", "source": source, "text": "\n".join(lines)})
        for ut in doc.get("unit_tests", []):
            chunks.append(
                {
                    "title": f"Unit test {ut['name']}",
                    "source": str(yml.relative_to(REPO)).replace("\\", "/"),
                    "text": f"dbt unit test `{ut['name']}` on `{ut['model']}`. "
                    + " ".join((ut.get("description") or "").split()),
                }
            )
    return chunks


def _test_name(test) -> str:
    if isinstance(test, str):
        return test
    ((kind, spec),) = test.items()
    return (spec or {}).get("name", kind)


def singular_tests() -> list[dict]:
    chunks = []
    for sql in sorted((REPO / "transform" / "tests").glob("*.sql")):
        note = re.search(r"\{#(.*?)#\}", sql.read_text(encoding="utf-8"), re.S)
        if note:
            chunks.append(
                {
                    "title": f"Singular test {sql.stem}",
                    "source": str(sql.relative_to(REPO)).replace("\\", "/"),
                    "text": f"dbt singular test `{sql.stem}`. " + " ".join(note.group(1).split()),
                }
            )
    return chunks


def gate_checks() -> list[dict]:
    sys.path.insert(0, str(REPO))
    from pipeline.quality_gate import CHECKS

    lines = [f"{c.check_id} {c.name} ({c.severity}): {c.description}" for c in CHECKS]
    return [
        {
            "title": "Quality gate checks",
            "source": "pipeline/quality_gate.py",
            "text": "The quality gate runs on the raw layer before any dbt model is rebuilt. "
            "BLOCK checks stop the pipeline so marts keep the last good build; ALERT checks "
            "notify and let the run continue.\n" + "\n".join(lines),
        }
    ]


def module_docstrings() -> list[dict]:
    chunks = []
    files = sorted((REPO / "pipeline").glob("*.py")) + [
        REPO / "orchestration" / "airflow" / "dags" / "bank_monthly_refresh.py",
        REPO / "orchestration" / "dagster" / "bank_dagster" / "definitions.py",
    ]
    for py in files:
        doc = ast.get_docstring(ast.parse(py.read_text(encoding="utf-8")))
        if doc and len(doc) > 80:
            rel = str(py.relative_to(REPO)).replace("\\", "/")
            chunks += _flush(f"Code: {rel}", doc, rel)
    return chunks


def main() -> None:
    chunks = chunk_markdown((REPO / "README.md").read_text(encoding="utf-8"), "README.md")
    chunks += dbt_models() + singular_tests() + gate_checks() + module_docstrings()
    # The assistant's own README, so it can explain how it works (minus Space front matter).
    own = (REPO / "chatbot" / "README.md").read_text(encoding="utf-8")
    own = re.sub(r"\A---\n.*?\n---\n", "", own, flags=re.S)
    chunks += chunk_markdown(own, "chatbot/README.md", title_prefix="Chatbot: ")
    for md in sorted(HANDWRITTEN.glob("*.md")):
        chunks += chunk_markdown(
            md.read_text(encoding="utf-8"), f"chatbot/knowledge/{md.name}", title_prefix=""
        )
    for i, c in enumerate(chunks):
        c["id"] = i
    OUT.write_text(
        "\n".join(json.dumps(c, ensure_ascii=False) for c in chunks) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(chunks)} chunks to {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
