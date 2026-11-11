"""Lexical retrieval (BM25) over the knowledge chunks.

The corpus is ~60 short, technical chunks full of exact identifiers (model names, check
ids, column names), where keyword matching beats embeddings and needs no model download,
vector store or GPU. Pure Python, so the Space starts in seconds.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CHUNKS = Path(__file__).parent / "knowledge" / "chunks.jsonl"

# One string reads better than 41 literals.
STOPWORDS = frozenset(
    "a an and are as at be by can do does for from has have how i in is it its of on or "  # noqa: SIM905
    "that the this to was what when where which who why will with you your me my".split()
)


def tokenize(text: str) -> list[str]:
    """Lower-case words; snake_case identifiers also yield their parts."""
    tokens = []
    for word in re.findall(r"[a-z0-9_]+", text.lower()):
        if word in STOPWORDS:
            continue
        tokens.append(word)
        if "_" in word:
            tokens += [p for p in word.split("_") if p and p not in STOPWORDS]
    return tokens


@dataclass(frozen=True)
class Hit:
    chunk: dict
    score: float


class BM25Index:
    def __init__(self, chunks: list[dict], k1: float = 1.4, b: float = 0.75):
        self.chunks = chunks
        self.k1, self.b = k1, b
        # Titles are short and precise, so they count twice.
        self.docs = [tokenize(f"{c['title']} {c['title']} {c['text']}") for c in chunks]
        self.tf = [Counter(d) for d in self.docs]
        self.avg_len = sum(map(len, self.docs)) / max(len(self.docs), 1)
        df = Counter(t for d in self.docs for t in set(d))
        n = len(self.docs)
        self.idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}

    @classmethod
    def from_file(cls, path: Path = DEFAULT_CHUNKS) -> BM25Index:
        lines = path.read_text(encoding="utf-8").splitlines()
        return cls([json.loads(line) for line in lines if line.strip()])

    def search(self, query: str, k: int = 5) -> list[Hit]:
        terms = tokenize(query)
        scores = []
        for i, tf in enumerate(self.tf):
            length = len(self.docs[i])
            s = 0.0
            for t in terms:
                if t in tf:
                    f = tf[t]
                    s += (
                        self.idf[t]
                        * f
                        * (self.k1 + 1)
                        / (f + self.k1 * (1 - self.b + self.b * length / self.avg_len))
                    )
            if s > 0:
                scores.append((s, i))
        scores.sort(reverse=True)
        return [Hit(self.chunks[i], s) for s, i in scores[:k]]
