"""
incidents.py
Incident format, injector, and the author-side consistency checks (spec §5).

An incident is data, not code: one window, one target column, a case type,
and SQL expressions for the corruption. Injection runs in DuckDB. Ground truth
is computed by diffing the injected window against the reference, so a
corruption that leaves a value unchanged is not counted as injected.

Incident JSON (a file holds a list of these):
{
  "incident_id":   "dev-01",
  "split":         "dev",
  "window":        3,                        # global window index, must lie in the split
  "column":        "annual_inc",
  "case_type":     "unrecoverable",          # recoverable | representation | unrecoverable | normal
  "corruption":    {"scope": "source_system = 'B'", "value": "annual_inc / 12"},   # null for normal
  "population":    null,                     # optional: fill the window with split rows matching this predicate
  "recovery":      null,                     # recoverable: SQL rebuilding the reference value from visible columns
  "equivalence":   null,                     # representation: SQL over `out` and `ref`, true when the meaning matches
  "canonical":     null,                     # representation: SQL over `out`, true for the column's established encoding
  "counterexamples": "annual_inc > 250000",  # clean split rows that a rule scoped too broadly would wrongly change
  "description":   "..."                     # free text, scorer-only
}
"""

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from . import data

CASE_TYPES = ("recoverable", "representation", "unrecoverable", "normal")
EXPECTED_DECISION = {
    "recoverable":    "RESTORE",
    "representation": "STANDARDIZE",
    "unrecoverable":  "QUARANTINE",
    "normal":         "KEEP",
}
MIN_COUNTEREXAMPLES = 10
MAX_COUNTEREXAMPLES = 500
NUMERIC_TOLERANCE   = 0.01   # recovery proofs may differ by rounding, e.g. to cents


@dataclass(frozen=True)
class Incident:
    incident_id:      str
    split:            str
    window:           int
    column:           str
    case_type:        str
    corruption_scope: str | None = None
    corruption_value: str | None = None
    population:       str | None = None
    recovery:         str | None = None
    equivalence:      str | None = None
    canonical:        str | None = None
    counterexamples:  str | None = None
    description:      str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "Incident":
        corruption = d.get("corruption") or {}
        return cls(
            incident_id=d["incident_id"], split=d["split"], window=int(d["window"]),
            column=d["column"], case_type=d["case_type"],
            corruption_scope=corruption.get("scope"), corruption_value=corruption.get("value"),
            population=d.get("population"), recovery=d.get("recovery"),
            equivalence=d.get("equivalence"), canonical=d.get("canonical"),
            counterexamples=d.get("counterexamples"), description=d.get("description", ""),
        )


@dataclass
class Injected:
    incident:        Incident
    clean:           pd.DataFrame   # the window before injection
    window:          pd.DataFrame   # the window after injection (what systems see, minus metadata)
    truth:           pd.DataFrame   # one row per injected cell: id, ref, injected
    counterexamples: pd.DataFrame   # clean split rows outside the window


def load_incidents(path: Path) -> list[Incident]:
    with open(path, "r", encoding="utf-8") as f:
        return [Incident.from_dict(d) for d in json.load(f)]


def connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with file and network access disabled."""
    con = duckdb.connect()
    con.execute("SET enable_external_access = false")
    return con


def build_window(ref: pd.DataFrame, inc: Incident, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    The clean window: the natural rows of the window, or, when the incident sets
    a population, the first WINDOW_ROWS split rows matching it, re-stamped with
    the window's positions and timestamps so the stream stays ordered.
    """
    natural = ref[ref["window"] == inc.window].reset_index(drop=True)
    if not inc.population:
        return natural

    con.register("split_rows", ref[ref["split"] == inc.split])
    drawn = con.execute(
        f"SELECT * FROM split_rows WHERE ({inc.population}) ORDER BY row_idx LIMIT {data.WINDOW_ROWS}"
    ).df()
    if len(drawn) < data.WINDOW_ROWS:
        raise ValueError(f"population matches only {len(drawn)} rows; need {data.WINDOW_ROWS}")
    for col in ("row_idx", "ingested_at", "window"):
        drawn[col] = natural[col].values
    return drawn


def inject(ref: pd.DataFrame, inc: Incident) -> Injected:
    con   = connect()
    clean = build_window(ref, inc, con)
    con.register("clean_df", clean)
    con.execute("CREATE TABLE win AS SELECT * FROM clean_df")
    if inc.corruption_scope is not None:
        con.execute(f"UPDATE win SET {inc.column} = ({inc.corruption_value}) WHERE ({inc.corruption_scope})")
    window = con.execute("SELECT * FROM win ORDER BY row_idx").df()

    # Ground truth = cells whose value actually changed
    col   = inc.column
    truth = con.execute(f"""
        SELECT w.id, c.{col} AS ref, w.{col} AS injected
        FROM win w JOIN clean_df c USING (id)
        WHERE w.{col} IS DISTINCT FROM c.{col}
        ORDER BY w.row_idx
    """).df()

    counterexamples = pd.DataFrame(columns=ref.columns)
    if inc.counterexamples:
        con.register("split_rows", ref[ref["split"] == inc.split])
        counterexamples = con.execute(f"""
            SELECT * FROM split_rows
            WHERE id NOT IN (SELECT id FROM clean_df) AND ({inc.counterexamples})
            ORDER BY row_idx LIMIT {MAX_COUNTEREXAMPLES}
        """).df()
    return Injected(inc, clean, window, truth, counterexamples)


# ── Author-side consistency checks ────────────────────────────────────────────
def check_definition(inc: Incident) -> list[str]:
    """Problems detectable without running the incident."""
    problems = []
    if inc.split not in data.SPLITS:
        problems.append(f"unknown split {inc.split!r}")
    elif inc.window not in data.split_windows(inc.split):
        problems.append(f"window {inc.window} is not inside split {inc.split}")
    if inc.column not in data.COLUMNS:
        problems.append(f"unknown column {inc.column!r}")
    if inc.case_type not in CASE_TYPES:
        problems.append(f"unknown case_type {inc.case_type!r}")
    has_corruption = inc.corruption_scope is not None and inc.corruption_value is not None
    if inc.case_type == "normal" and has_corruption:
        problems.append("a normal incident must not have a corruption")
    if inc.case_type != "normal" and not has_corruption:
        problems.append("corruption.scope and corruption.value are required")
    if inc.case_type == "recoverable" and not inc.recovery:
        problems.append("a recoverable incident needs a recovery expression")
    if inc.case_type == "representation" and not (inc.equivalence and inc.canonical):
        problems.append("a representation incident needs equivalence and canonical expressions")
    if not inc.counterexamples:
        problems.append("counterexamples predicate is required")
    return problems


def check_injected(inj: Injected) -> list[str]:
    """Problems that need the injected window: empty corruptions and failed proofs."""
    inc, problems = inj.incident, []
    n = len(inj.truth)
    if inc.case_type == "normal" and n:
        problems.append(f"normal incident changed {n} cells")
    if inc.case_type != "normal" and not n:
        problems.append("corruption changed no cells")
    if len(inj.counterexamples) < MIN_COUNTEREXAMPLES:
        problems.append(f"only {len(inj.counterexamples)} counterexamples; need {MIN_COUNTEREXAMPLES}")
    if not n:
        return problems

    con = connect()
    con.register("win", inj.window)
    con.register("truth", inj.truth)
    if inc.case_type == "recoverable":
        rebuilt = con.execute(f"""
            SELECT t.ref, ({inc.recovery}) AS rebuilt
            FROM win JOIN truth t USING (id)
        """).df()
        if pd.api.types.is_numeric_dtype(rebuilt["ref"]):
            ok = (rebuilt["ref"] - pd.to_numeric(rebuilt["rebuilt"], errors="coerce")).abs() <= NUMERIC_TOLERANCE
        else:
            ok = rebuilt["ref"].astype(str) == rebuilt["rebuilt"].astype(str)
        if not ok.all():
            problems.append(f"recovery rebuilds only {int(ok.sum())}/{n} injected cells")

    if inc.case_type == "representation":
        not_equiv = con.execute(
            f"SELECT count(*) FROM (SELECT injected AS out, ref FROM truth) WHERE NOT ({inc.equivalence})"
        ).fetchone()[0]
        if not_equiv:
            problems.append(f"{not_equiv} injected values do not keep the reference meaning")
        con.register("clean_df", inj.clean)
        bad_ref = con.execute(
            f"SELECT count(*) FROM (SELECT {inc.column} AS out FROM clean_df) "
            f"WHERE out IS NOT NULL AND NOT ({inc.canonical})"
        ).fetchone()[0]
        if bad_ref:
            problems.append(f"{bad_ref} reference values are not canonical")
        still_canonical = con.execute(
            f"SELECT count(*) FROM (SELECT injected AS out FROM truth) WHERE ({inc.canonical})"
        ).fetchone()[0]
        if still_canonical:
            problems.append(f"{still_canonical} injected values are still canonical")
    return problems
