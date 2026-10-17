"""
pyspark_runner.py
Runs a generated PySpark snippet against copies of input tables, in a fresh
subprocess (spec §7.1):
  - each table starts from its own copy, so one run cannot affect the next
  - the subprocess environment has no API keys or other secrets
  - the subprocess is killed after TIMEOUT_SECONDS

The snippet sees one variable, df (a Spark DataFrame), and must leave the
result in df. Tables are exchanged as Parquet files in a temporary directory.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

WORKER          = Path(__file__).resolve().with_name("_pyspark_worker.py")
TIMEOUT_SECONDS = 120
SECRET_MARKERS  = ("KEY", "TOKEN", "SECRET", "PASSWORD")
# Imports any PySpark pipeline already has; prepended so a fix is judged on what it does, not on a missing import
PREAMBLE = ("from pyspark.sql import functions as F\n"
            "from pyspark.sql.functions import (col, when, lit, expr, coalesce, mean, trim, upper, lower, length,\n"
            "    substring, concat, regexp_replace, regexp_extract, to_date, date_format, isnan)\n")


def clean_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not any(m in k.upper() for m in SECRET_MARKERS)}
    env["PYSPARK_PYTHON"] = env["PYSPARK_DRIVER_PYTHON"] = sys.executable   # workers must match the driver
    env["TZ"] = "UTC"                       # naive timestamps must round-trip unchanged
    return env


def run_code(code: str, tables: dict[str, pd.DataFrame],
             timeout: int = TIMEOUT_SECONDS) -> tuple[dict[str, pd.DataFrame] | None, str | None]:
    """Returns ({name: output table}, None) on success, or (None, error message)."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for name, table in tables.items():
            table.to_parquet(tmp / f"{name}.in.parquet", index=False)
        (tmp / "candidate.py").write_text(PREAMBLE + code)
        try:
            proc = subprocess.run([sys.executable, str(WORKER), str(tmp), *tables],
                                  cwd=tmp, env=clean_env(), capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return None, f"timeout after {timeout}s"
        if (tmp / "error.txt").exists():
            return None, (tmp / "error.txt").read_text().strip()
        if proc.returncode != 0:
            tail = [line for line in proc.stderr.strip().splitlines() if line.strip()]
            return None, f"worker exited with {proc.returncode}: {tail[-1] if tail else 'no output'}"
        return {name: pd.read_parquet(tmp / f"{name}.out.parquet") for name in tables}, None
