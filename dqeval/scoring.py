"""
scoring.py
Scores one rule candidate on one injected incident (spec §7.1, §7.2). This is
the only evaluation module that reads ground truth.

  Level 1  executes       passes the contract and applies without error
  Level 2  fixes window   meets the case type's requirement on the injected cells,
                          changes no clean cell, and quarantines at most 1% of clean rows
  Level 3  safe to reuse  on the counterexamples and on the regression slice,
                          changes no cell and quarantines at most 1% of rows
                          (each set checked on its own)

A run succeeds only if all three levels hold.
"""

from dataclasses import asdict, dataclass

import pandas as pd

from . import contract, data
from .contract import QUARANTINE_COLUMN, RuleCandidate
from .incidents import NUMERIC_TOLERANCE, Injected, connect

FIX_SHARE            = 0.95   # share of injected cells that must be handled
MAX_QUARANTINE_SHARE = 0.01   # share of clean rows that may be quarantined
REGRESSION_ROWS      = 5_000


@dataclass
class Score:
    executes:      bool
    fixes_window:  bool
    safe_to_reuse: bool
    success:       bool
    error:         str | None = None
    decision:      str | None = None
    # level 2 detail
    injected_cells:         int = 0
    handled_cells:          int = 0
    clean_rows:             int = 0
    clean_cells_changed:    int = 0
    clean_rows_quarantined: int = 0
    # level 3 detail
    counterexample_rows:             int = 0
    counterexample_cells_changed:    int = 0
    counterexample_rows_quarantined: int = 0
    regression_rows:                 int = 0
    regression_cells_changed:        int = 0
    regression_rows_quarantined:     int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def failed(error: str, candidate: RuleCandidate | None = None) -> Score:
    return Score(False, False, False, False, error=error, decision=candidate.decision if candidate else None)


def same_values(a: pd.Series, b: pd.Series) -> pd.Series:
    """Cell-wise equality that treats two nulls as equal and numbers within NUMERIC_TOLERANCE as equal."""
    a, b = a.reset_index(drop=True), b.reset_index(drop=True)
    both_null, present = a.isna() & b.isna(), a.notna() & b.notna()
    if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
        equal = (a - b).abs() <= NUMERIC_TOLERANCE
    else:
        equal = a.astype(str) == b.astype(str)
    return both_null | (present & equal)


def regression_slice(ref: pd.DataFrame, inj: Injected) -> pd.DataFrame:
    """
    REGRESSION_ROWS clean rows of the incident's split from the windows after the
    incident's window (nearest first), topped up from the windows before it, without
    the incident's own rows. Later windows are what a reused rule would run on next.
    """
    inc     = inj.incident
    windows = data.split_windows(inc.split)
    order   = [w for w in windows if w > inc.window] + [w for w in reversed(windows) if w < inc.window]
    rank    = {w: i for i, w in enumerate(order)}
    rows    = ref[ref["window"].isin(order) & ~ref["id"].isin(inj.window["id"])]
    rows    = rows.assign(_rank=rows["window"].map(rank)).sort_values(["_rank", "row_idx"])
    return rows.drop(columns="_rank").head(REGRESSION_ROWS)


def changes(before: pd.DataFrame, after: pd.DataFrame, column: str) -> tuple[pd.Series, pd.Series]:
    """Per row of `before`: whether the candidate changed the column, and whether it quarantined the row."""
    merged = before[["id", column]].merge(after[["id", column, QUARANTINE_COLUMN]], on="id", suffixes=("", "_out"))
    return ~same_values(merged[column], merged[f"{column}_out"]), merged[QUARANTINE_COLUMN].astype(bool)


def standardized(inj: Injected, out_values: pd.Series, ref_values: pd.Series) -> pd.Series:
    """Representation incidents: the output is canonical and means the same as the reference."""
    inc = inj.incident
    con = connect()
    con.register("pairs", pd.DataFrame({"out": out_values.values, "ref": ref_values.values}))
    flags = con.execute(f"""
        SELECT coalesce(TRY({inc.canonical}), false) AS canonical,
               coalesce(TRY({inc.equivalence}), false) AS equivalent
        FROM pairs
    """).df()
    same = same_values(out_values, ref_values)
    return (flags["canonical"] & (same | flags["equivalent"])).set_axis(out_values.index)


def score(ref: pd.DataFrame, inj: Injected, candidate: RuleCandidate | None, error: str | None = None) -> Score:
    """Scores one candidate. Pass candidate=None with an error when the system produced nothing usable."""
    inc = inj.incident
    if inc.case_type != "normal" and inj.truth.empty:
        # otherwise "0 handled >= 95% of 0 injected" would hold for any candidate
        raise ValueError(f"incident {inc.incident_id} injected no cells; it should not have passed the checker")
    if candidate is None:
        return failed(error or "no candidate")
    problems = contract.validate(candidate, inc.column)
    if problems:
        return failed("; ".join(problems), candidate)

    visible = data.VISIBLE_COLUMNS
    regression = regression_slice(ref, inj)
    try:
        out_window = contract.apply(inj.window[visible], candidate)
        out_cx     = contract.apply(inj.counterexamples[visible], candidate)
        out_reg    = contract.apply(regression[visible], candidate)
    except Exception as e:
        return failed(f"{type(e).__name__}: {str(e).splitlines()[0]}", candidate)

    # ── Level 2: the incident window, compared with what the system saw and with the reference
    col = inc.column
    rows = (inj.window[["id", col]].rename(columns={col: "seen"})
            .merge(inj.clean[["id", col]].rename(columns={col: "ref"}), on="id")
            .merge(out_window[["id", col, QUARANTINE_COLUMN]].rename(columns={col: "out"}), on="id"))
    injected    = rows["id"].isin(inj.truth["id"])
    clean       = ~injected
    changed     = ~same_values(rows["out"], rows["seen"])
    quarantined = rows[QUARANTINE_COLUMN].astype(bool)

    if inc.case_type == "recoverable":
        handled = injected & same_values(rows["out"], rows["ref"])
    elif inc.case_type == "representation":
        handled = injected & standardized(inj, rows["out"], rows["ref"])
    elif inc.case_type == "unrecoverable":
        handled = injected & quarantined & ~changed
    else:
        handled = pd.Series(False, index=rows.index)

    n_injected, n_clean = int(injected.sum()), int(clean.sum())
    clean_changed       = int((clean & changed).sum())
    clean_quarantined   = int((clean & quarantined).sum())
    enough_handled      = inc.case_type == "normal" or handled.sum() >= FIX_SHARE * n_injected
    fixes_window = (enough_handled and clean_changed == 0
                    and clean_quarantined <= MAX_QUARANTINE_SHARE * n_clean)

    # ── Level 3: clean look-alikes and the regression slice, each on its own
    cx_changed, cx_quarantined   = changes(inj.counterexamples, out_cx, col)
    reg_changed, reg_quarantined = changes(regression, out_reg, col)
    safe_to_reuse = (not cx_changed.any() and not reg_changed.any()
                     and cx_quarantined.sum() <= MAX_QUARANTINE_SHARE * len(cx_quarantined)
                     and reg_quarantined.sum() <= MAX_QUARANTINE_SHARE * len(reg_quarantined))

    return Score(
        executes=True, fixes_window=bool(fixes_window), safe_to_reuse=bool(safe_to_reuse),
        success=bool(fixes_window and safe_to_reuse), decision=candidate.decision,
        injected_cells=n_injected, handled_cells=int(handled.sum()), clean_rows=n_clean,
        clean_cells_changed=clean_changed, clean_rows_quarantined=clean_quarantined,
        counterexample_rows=len(cx_changed), counterexample_cells_changed=int(cx_changed.sum()),
        counterexample_rows_quarantined=int(cx_quarantined.sum()),
        regression_rows=len(reg_changed), regression_cells_changed=int(reg_changed.sum()),
        regression_rows_quarantined=int(reg_quarantined.sum()),
    )
