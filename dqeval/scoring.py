"""
scoring.py
Scores one system output on one injected incident (spec §7.1, §7.2). This is
the only evaluation module that reads ground truth.

  Level 1  executes       the candidate passes the contract (or the code runs) and
                          produces the window, counterexample, and regression outputs
  Level 2  fixes window   meets the case type's requirement on the injected cells,
                          harms no clean data, and quarantines at most 1% of clean rows
  Level 3  safe to reuse  on the counterexamples and on the regression slice,
                          harms nothing and quarantines at most 1% of rows
                          (each set checked on its own)

"Harm" is any change to a clean cell of the target column, any change to another
visible column, or a dropped clean row. A dropped injected row is not handled.
A new boolean column ending in _quarantine is a quarantine flag; other new
columns are ignored. Contract candidates (score) and PySpark code (score_code)
go through the same checks (score_outputs).

A run succeeds only if all three levels hold.
"""

from dataclasses import asdict, dataclass

import pandas as pd

from . import contract, data, pyspark_runner
from .contract import RuleCandidate
from .incidents import EXPECTED_DECISION, NUMERIC_TOLERANCE, Injected, connect, referenced_columns

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
    # reporting only, not part of success:
    #   decision_matches  the decision is the one spec §5.1 names for the case type
    #   scope_uses_time   the scope reads ingested_at; such a rule is a one-off fix for a time range,
    #                     and level 3 says little about it because the check rows lie outside that range
    decision_matches: bool = False
    scope_uses_time:  bool = False
    # level 2 detail
    injected_cells:         int = 0
    handled_cells:          int = 0
    clean_rows:             int = 0
    clean_cells_changed:    int = 0     # rows harmed: a clean cell changed, another column changed, or a clean row dropped
    clean_rows_quarantined: int = 0
    rows_dropped:           int = 0
    # level 3 detail (rows harmed / quarantined)
    counterexample_rows:             int = 0
    counterexample_cells_changed:    int = 0
    counterexample_rows_quarantined: int = 0
    regression_rows:                 int = 0
    regression_cells_changed:        int = 0
    regression_rows_quarantined:     int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def failed(error: str, decision: str | None = None) -> Score:
    return Score(False, False, False, False, error=error, decision=decision)


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


def quarantine_flags(out: pd.DataFrame) -> pd.Series:
    """The contract's _quarantine column, or any boolean column a code arm added that ends in _quarantine."""
    names = [c for c in out.columns if c.endswith("_quarantine") and c not in data.VISIBLE_COLUMNS]
    if not names:
        return pd.Series(False, index=out.index)
    return out[names].fillna(False).astype(bool).any(axis=1)


def effects(before: pd.DataFrame, after: pd.DataFrame, column: str) -> pd.DataFrame:
    """Per row of `before`, joined on id: the output value, and whether the row's target changed,
    another visible column changed, the row was dropped, or the row was quarantined."""
    after = after.drop_duplicates("id")
    kept  = after[[c for c in after.columns if c in data.VISIBLE_COLUMNS]].assign(_flag=quarantine_flags(after).values)
    rows  = before[data.VISIBLE_COLUMNS].merge(kept, on="id", how="left", suffixes=("", "_out"), indicator=True)
    dropped = rows["_merge"] == "left_only"

    other_changed = pd.Series(False, index=rows.index)
    for c in data.VISIBLE_COLUMNS:
        if c in ("id", column):
            continue
        other_changed |= ~same_values(rows[c], rows[f"{c}_out"]) if f"{c}_out" in rows else True
    return pd.DataFrame({
        "id":            rows["id"],
        "out":           rows[f"{column}_out"],
        "changed":       ~same_values(rows[column], rows[f"{column}_out"]) & ~dropped,
        "other_changed": other_changed & ~dropped,
        "dropped":       dropped,
        "quarantined":   rows["_flag"].eq(True) & ~dropped,      # dropped rows have no flag (NaN)
    })


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


def score_outputs(inj: Injected, regression: pd.DataFrame, out_window: pd.DataFrame,
                  out_cx: pd.DataFrame, out_reg: pd.DataFrame,
                  decision: str | None = None, scope_uses_time: bool = False) -> Score:
    inc, col = inj.incident, inj.incident.column

    # ── Level 2: the incident window, compared with what the system saw and with the reference
    eff      = effects(inj.window, out_window, col)
    ref_vals = eff[["id"]].merge(inj.clean[["id", col]], on="id", how="left")[col]
    injected = eff["id"].isin(inj.truth["id"])
    clean    = ~injected

    if inc.case_type == "recoverable":
        handled = injected & ~eff["dropped"] & same_values(eff["out"], ref_vals)
    elif inc.case_type == "representation":
        handled = injected & ~eff["dropped"] & standardized(inj, eff["out"], ref_vals)
    elif inc.case_type == "unrecoverable":
        handled = injected & eff["quarantined"] & ~eff["changed"]
    else:
        handled = pd.Series(False, index=eff.index)

    harmed = (clean & (eff["changed"] | eff["other_changed"] | eff["dropped"])) | (injected & eff["other_changed"])
    n_injected, n_clean = int(injected.sum()), int(clean.sum())
    clean_quarantined   = int((clean & eff["quarantined"]).sum())
    enough_handled      = inc.case_type == "normal" or handled.sum() >= FIX_SHARE * n_injected
    fixes_window        = enough_handled and not harmed.any() and clean_quarantined <= MAX_QUARANTINE_SHARE * n_clean

    # ── Level 3: clean look-alikes and the regression slice, each on its own
    def reuse(before: pd.DataFrame, after: pd.DataFrame) -> tuple[int, int, int]:
        e = effects(before, after, col)
        return len(e), int((e["changed"] | e["other_changed"] | e["dropped"]).sum()), int(e["quarantined"].sum())

    cx_rows, cx_harmed, cx_quarantined     = reuse(inj.counterexamples, out_cx)
    reg_rows, reg_harmed, reg_quarantined  = reuse(regression, out_reg)
    safe_to_reuse = (cx_harmed == 0 and reg_harmed == 0
                     and cx_quarantined <= MAX_QUARANTINE_SHARE * cx_rows
                     and reg_quarantined <= MAX_QUARANTINE_SHARE * reg_rows)

    return Score(
        executes=True, fixes_window=bool(fixes_window), safe_to_reuse=bool(safe_to_reuse),
        success=bool(fixes_window and safe_to_reuse), decision=decision,
        decision_matches=decision == EXPECTED_DECISION[inc.case_type], scope_uses_time=scope_uses_time,
        injected_cells=n_injected, handled_cells=int(handled.sum()), clean_rows=n_clean,
        clean_cells_changed=int(harmed.sum()), clean_rows_quarantined=clean_quarantined,
        rows_dropped=int(eff["dropped"].sum()),
        counterexample_rows=cx_rows, counterexample_cells_changed=cx_harmed,
        counterexample_rows_quarantined=cx_quarantined,
        regression_rows=reg_rows, regression_cells_changed=reg_harmed, regression_rows_quarantined=reg_quarantined,
    )


def check_incident(inj: Injected) -> None:
    if inj.incident.case_type != "normal" and inj.truth.empty:
        # otherwise "0 handled >= 95% of 0 injected" would hold for any output
        raise ValueError(f"incident {inj.incident.incident_id} injected no cells; it should not have passed the checker")


def score(ref: pd.DataFrame, inj: Injected, candidate: RuleCandidate | None, error: str | None = None) -> Score:
    """Scores a contract candidate. Pass candidate=None with an error when the system produced nothing usable."""
    check_incident(inj)
    if candidate is None:
        return failed(error or "no candidate")
    problems = contract.validate(candidate, inj.incident.column)
    if problems:
        return failed("; ".join(problems), candidate.decision)

    visible, regression = data.VISIBLE_COLUMNS, regression_slice(ref, inj)
    try:
        out_window = contract.apply(inj.window[visible], candidate)
        out_cx     = contract.apply(inj.counterexamples[visible], candidate)
        out_reg    = contract.apply(regression[visible], candidate)
    except Exception as e:
        return failed(f"{type(e).__name__}: {str(e).splitlines()[0]}", candidate.decision)
    uses_time = bool(candidate.scope) and "ingested_at" in referenced_columns(candidate.scope)
    return score_outputs(inj, regression, out_window, out_cx, out_reg, candidate.decision, uses_time)


def score_code(ref: pd.DataFrame, inj: Injected, code: str | None, error: str | None = None) -> Score:
    """Scores PySpark code by running it on copies of the window, counterexamples, and regression slice."""
    check_incident(inj)
    if not code:
        return failed(error or "no code")
    visible, regression = data.VISIBLE_COLUMNS, regression_slice(ref, inj)
    outputs, err = pyspark_runner.run_code(code, {"window": inj.window[visible],
                                                  "counterexamples": inj.counterexamples[visible],
                                                  "regression": regression[visible]})
    if err:
        return failed(err)
    if any("id" not in out.columns for out in outputs.values()):
        return failed("the output lost the id column")
    return score_outputs(inj, regression, outputs["window"], outputs["counterexamples"], outputs["regression"])
