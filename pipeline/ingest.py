"""Extract: download the source files and verify them against a checksum lock.

Each file is fetched from a pinned commit, streamed to a temp file and only moved
into the landing zone once its SHA-256 matches the lock. A changed upstream file
therefore fails loudly here instead of silently changing every number downstream.

Usage:
    python -m pipeline.ingest            # download what is missing, verify all
    python -m pipeline.ingest --relock   # accept current upstream bytes as the new lock
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

import httpx

from pipeline.config import CHECKSUM_LOCK, LANDING_DIR, SOURCE_BASE_URL, SOURCE_COMMIT, SOURCES

log = logging.getLogger("ingest")

MAX_ATTEMPTS = 4


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, target: Path) -> None:
    """Stream a URL to disk with retries and exponential backoff."""
    tmp = target.with_suffix(target.suffix + ".part")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with httpx.stream("GET", url, timeout=60, follow_redirects=True) as resp:
                resp.raise_for_status()
                with tmp.open("wb") as fh:
                    for chunk in resp.iter_bytes(1 << 20):
                        fh.write(chunk)
            tmp.replace(target)
            return
        except httpx.HTTPError as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            wait = 2**attempt
            log.warning("attempt %s for %s failed (%s); retrying in %ss", attempt, url, exc, wait)
            time.sleep(wait)


def load_lock() -> dict:
    if CHECKSUM_LOCK.exists():
        return json.loads(CHECKSUM_LOCK.read_text())
    return {}


def run(relock: bool = False) -> dict[str, str]:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    lock = load_lock()
    if lock and lock.get("commit") != SOURCE_COMMIT and not relock:
        raise SystemExit(
            f"Lock was written for commit {lock.get('commit')}, config pins {SOURCE_COMMIT}. "
            "Run with --relock after reviewing the upstream change."
        )
    expected = lock.get("files", {})
    actual: dict[str, str] = {}

    for src in SOURCES:
        target = LANDING_DIR / src.file
        if not target.exists():
            log.info("downloading %s", src.file)
            download(f"{SOURCE_BASE_URL}/{src.file}", target)
        checksum = sha256_of(target)
        actual[src.file] = checksum
        if not relock and src.file in expected and expected[src.file] != checksum:
            target.unlink()
            raise SystemExit(
                f"Checksum mismatch for {src.file}: expected {expected[src.file]}, got {checksum}. "
                "The file was removed from landing; investigate the upstream change "
                "before relocking."
            )
        log.info("verified %s (%s)", src.file, checksum[:12])

    if relock or not expected:
        CHECKSUM_LOCK.write_text(
            json.dumps({"commit": SOURCE_COMMIT, "files": actual}, indent=2) + "\n"
        )
        log.info("wrote checksum lock %s", CHECKSUM_LOCK.name)
    return actual


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--relock", action="store_true", help="rewrite the checksum lock")
    args = parser.parse_args()
    run(relock=args.relock)


if __name__ == "__main__":
    sys.exit(main())
