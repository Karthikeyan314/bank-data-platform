"""Chatbot: retrieval finds the right passage, prompts stay grounded, the KB is current."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "chatbot"))

from retrieval import BM25Index, tokenize  # noqa: E402

st = pytest.importorskip("streamlit")
import app  # noqa: E402


@pytest.fixture(scope="module")
def index() -> BM25Index:
    return BM25Index.from_file()


def test_snake_case_identifiers_are_split():
    assert tokenize("fct_account_monthly") == ["fct_account_monthly", "fct", "account", "monthly"]


@pytest.mark.parametrize(
    ("question", "expected_in_top3"),
    [
        ("What was the hardest SQL problem?", "ledger"),
        ("How does the quality gate decide to block?", "gate"),
        ("Is fct_transactions incremental?", "fct_transactions"),
        ("Why both Airflow and Dagster?", "airflow"),
        ("how do you know incremental builds are right", "incremental"),
    ],
)
def test_questions_retrieve_the_relevant_passage(index, question, expected_in_top3):
    top = index.search(question, k=3)
    blob = " ".join(f"{h.chunk['title']} {h.chunk['text']}" for h in top).lower()
    assert expected_in_top3 in blob


def test_unrelated_question_finds_little(index):
    assert len(index.search("zzqx flibbertigibbet", k=3)) == 0


def test_prompt_carries_rules_context_and_trimmed_history(index):
    hits = index.search("quality gate", k=2)
    history = [{"role": "user", "content": f"q{i}"} for i in range(20)]
    msgs = app.build_messages("What blocks a run?", hits, history)
    assert msgs[0]["role"] == "system" and "Answer only from the CONTEXT" in msgs[0]["content"]
    assert "CONTEXT:" in msgs[-1]["content"] and "What blocks a run?" in msgs[-1]["content"]
    assert len(msgs) == 1 + 2 * app.MAX_HISTORY_TURNS + 1


def test_retrieval_only_answer_works_without_a_token(index):
    text = app.retrieval_only_answer(index.search("balance continuity", k=3))
    assert "HF_TOKEN" in text and "[" in text
    assert "couldn't find" in app.retrieval_only_answer([])


def test_knowledge_base_is_up_to_date():
    """CI fails if the docs changed but chunks.jsonl was not rebuilt."""
    kb = REPO / "chatbot" / "knowledge" / "chunks.jsonl"
    before = kb.read_text(encoding="utf-8")
    subprocess.run(
        [sys.executable, str(REPO / "chatbot" / "build_knowledge.py")],
        check=True,
        capture_output=True,
    )
    after = kb.read_text(encoding="utf-8")
    assert before == after, "run: python chatbot/build_knowledge.py"
    assert all(json.loads(line)["source"] for line in after.splitlines())
