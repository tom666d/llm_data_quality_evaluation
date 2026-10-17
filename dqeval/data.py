"""
data.py
Loads the pre-injection reference data and adds the synthetic fields the
evaluation needs (spec §3).

The reference file is not committed. Extract it from the team repository at
the commit the course project was submitted with:

    git -C <team-repo> show b01ef2e:data/LendingClub_100k.csv > data/LendingClub_100k.csv
"""

import hashlib
from pathlib import Path

import pandas as pd

PROJECT_ROOT     = Path(__file__).resolve().parent.parent
REFERENCE_PATH   = PROJECT_ROOT / "data" / "LendingClub_100k.csv"
# SHA-256 of data/LendingClub_100k.csv at commit b01ef2e of the team repository
REFERENCE_SHA256 = "c72a9cbf265557db58fd5e64fadb5c4a1ee5e656e87c87605b35ca1b1ca270b5"

# Reference columns kept for the evaluation (the source file has 151)
COLUMNS = [
    "loan_amnt", "funded_amnt", "funded_amnt_inv", "term", "int_rate", "installment",
    "grade", "sub_grade", "emp_length", "home_ownership", "annual_inc",
    "verification_status", "issue_d", "loan_status", "purpose", "addr_state",
    "dti", "revol_util", "tot_cur_bal",
]
# Columns a system under test may see: identity, the reference columns, and the synthetic fields
VISIBLE_COLUMNS = ["id", *COLUMNS, "source_system", "ingested_at"]

# ── Synthetic stream layout ───────────────────────────────────────────────────
# Rows are replayed in file order. Row ranges are [start, end) over row_idx.
WINDOW_ROWS     = 2_000
SPLITS          = {"dev": (0, 50_000), "S1": (50_000, 75_000), "S2": (75_000, 100_000)}
SOURCES         = ["A", "B", "C", "D"]
STREAM_START    = pd.Timestamp("2026-01-05 00:00:00")
SECONDS_PER_ROW = 3   # one 2,000-row window spans 100 minutes


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_reference(path: Path = REFERENCE_PATH, verify: bool = True) -> pd.DataFrame:
    """Reads the reference CSV, checks its hash, and returns it enriched (see enrich())."""
    if verify and sha256(path) != REFERENCE_SHA256:
        raise ValueError(f"{path} does not match the reference SHA-256; re-extract it from b01ef2e")
    df = pd.read_csv(path, usecols=["id", *COLUMNS], low_memory=False)
    return enrich(df[["id", *COLUMNS]])


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the synthetic fields, deterministically:
      row_idx        position in the replayed stream (file order)
      source_system  upstream feed A-D, from id mod 4
      ingested_at    STREAM_START + row_idx * SECONDS_PER_ROW
      window         row_idx // WINDOW_ROWS
      split          dev / S1 / S2, from row_idx
    row_idx, window and split are evaluation metadata, not visible to systems.
    """
    df = df.reset_index(drop=True).copy()
    df["row_idx"]       = range(len(df))
    df["source_system"] = [SOURCES[i % len(SOURCES)] for i in df["id"]]
    df["ingested_at"]   = STREAM_START + pd.to_timedelta(df["row_idx"] * SECONDS_PER_ROW, unit="s")
    df["window"]        = df["row_idx"] // WINDOW_ROWS
    df["split"]         = None
    for name, (start, end) in SPLITS.items():
        df.loc[(df["row_idx"] >= start) & (df["row_idx"] < end), "split"] = name
    return df


def split_windows(split: str) -> list[int]:
    """Window indices that lie entirely inside a split."""
    start, end = SPLITS[split]
    return list(range(start // WINDOW_ROWS, end // WINDOW_ROWS))
