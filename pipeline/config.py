"""Shared paths and source definitions for the pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("BANK_DATA_DIR", REPO_ROOT / "data"))
LANDING_DIR = DATA_DIR / "landing"
WAREHOUSE_PATH = Path(os.environ.get("BANK_WAREHOUSE", DATA_DIR / "warehouse.duckdb"))
DBT_PROJECT_DIR = REPO_ROOT / "transform"
CHECKSUM_LOCK = REPO_ROOT / "pipeline" / "sources.lock.json"

# The original PKDD'99 host (sorry.vse.cz) is offline. This mirror keeps all eight
# tables and row counts; it shifts dates by +20 years, divides amounts by 10 and
# translates Czech codes to English abbreviations. Pinning a commit makes every
# download byte-identical, which the checksum lock then enforces.
SOURCE_REPO = "dnoeth/1999_Czech_financial_dataset_Teradata"
SOURCE_COMMIT = "0dd97f4b794102fc434587fc67c41cb3d0cd6c97"
SOURCE_BASE_URL = f"https://raw.githubusercontent.com/{SOURCE_REPO}/{SOURCE_COMMIT}"


@dataclass(frozen=True)
class SourceTable:
    name: str  # raw table name in the warehouse
    file: str  # file name in the source repo
    expected_rows: int  # row count documented by the dataset authors


SOURCES: tuple[SourceTable, ...] = (
    SourceTable("account", "fin_account.tsv", 4_500),
    SourceTable("client", "fin_client.tsv", 5_369),
    SourceTable("disposition", "fin_disp.tsv", 5_369),
    SourceTable("permanent_order", "fin_order.tsv", 6_471),
    SourceTable("transaction", "fin_trans.tsv", 1_056_320),
    SourceTable("loan", "fin_loan.tsv", 682),
    SourceTable("card", "fin_card.tsv", 892),
    SourceTable("district", "fin_district.tsv", 77),
)
