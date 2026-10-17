"""
monitors.py
Generic monitors that turn a window into alerts (spec §4).

Each window is compared with a baseline built from nearby clean reference
windows of the same split. An alert says which column and which statistic
moved, and by how much. It never says how the data was corrupted, which rows
are affected, or what the scope is.
"""

import re

import pandas as pd

from . import data

BASELINE_WINDOWS   = 5      # clean reference windows a window is compared with
NULL_RATE_PP       = 0.10   # null rate rises by at least 10 percentage points
NUMERIC_RATIO      = 3.0    # a quantile changes by at least 3x
NUMERIC_QUANTILES  = {"p5": 0.05, "median": 0.50, "p95": 0.95}
NEW_CATEGORY_SHARE = 0.05   # unseen values make up at least 5% of non-null rows
PATTERN_SHARE_PP   = 0.10   # a character-class pattern's share changes by at least 10 pp


def pattern(value: str) -> str:
    """Character-class signature: 'Dec-2015' -> 'Aaa-9999', '01/2020' -> '99/9999'."""
    return re.sub(r"[0-9]", "9", re.sub(r"[a-z]", "a", re.sub(r"[A-Z]", "A", value)))


def fold_change(new: float, old: float) -> float:
    """How many times larger or smaller `new` is than `old`; inf when only one side is zero or negative."""
    if new == old:
        return 1.0
    if new <= 0 or old <= 0:
        return float("inf")
    return max(new / old, old / new)


def baseline_windows(window: int) -> list[int]:
    """The BASELINE_WINDOWS windows just before `window` in its split, topped up from the ones after it."""
    split   = next(s for s in data.SPLITS if window in data.split_windows(s))
    windows = data.split_windows(split)
    before  = [w for w in windows if w < window][-BASELINE_WINDOWS:]
    after   = [w for w in windows if w > window][: BASELINE_WINDOWS - len(before)]
    return before + after


def run_monitors(window: pd.DataFrame, baseline: pd.DataFrame) -> list[dict]:
    """All alerts for one window, at most one per (column, monitor)."""
    bounds = {"window_start": str(window["ingested_at"].min()), "window_end": str(window["ingested_at"].max())}
    alerts = []

    def alert(column, monitor, baseline_value, window_value, detail=None):
        alerts.append({"column": column, "monitor": monitor, "baseline": baseline_value,
                       "window": window_value, "detail": detail, **bounds})

    for col in data.COLUMNS:
        w, b = window[col], baseline[col]

        null_w, null_b = float(w.isna().mean()), float(b.isna().mean())
        if null_w - null_b >= NULL_RATE_PP:
            alert(col, "null_rate", round(null_b, 4), round(null_w, 4))

        w, b = w.dropna(), b.dropna()
        if w.empty or b.empty:
            continue

        if pd.api.types.is_numeric_dtype(b):
            # report the quantile that moved the most
            changes = {name: (fold_change(w.quantile(q), b.quantile(q)), b.quantile(q), w.quantile(q))
                       for name, q in NUMERIC_QUANTILES.items()}
            name, (ratio, q_b, q_w) = max(changes.items(), key=lambda kv: kv[1][0])
            if ratio >= NUMERIC_RATIO:
                alert(col, "numeric_shift", round(float(q_b), 4), round(float(q_w), 4), detail=name)
            continue

        w, b = w.astype(str), b.astype(str)
        unseen = float((~w.isin(set(b))).mean())
        if unseen >= NEW_CATEGORY_SHARE:
            alert(col, "new_categories", 0.0, round(unseen, 4))

        share_w = w.map(pattern).value_counts(normalize=True)
        share_b = b.map(pattern).value_counts(normalize=True)
        shift   = share_w.sub(share_b, fill_value=0)
        # report the pattern that grew the most; fall back to the one that shrank the most
        top = shift.idxmax() if shift.max() >= PATTERN_SHARE_PP else (
              shift.idxmin() if -shift.min() >= PATTERN_SHARE_PP else None)
        if top is not None:
            alert(col, "string_pattern", round(float(share_b.get(top, 0.0)), 4),
                  round(float(share_w.get(top, 0.0)), 4), detail=top)
    return alerts


def incident_alerts(ref: pd.DataFrame, inj) -> tuple[list[dict], list[dict]]:
    """
    Returns (alerts, caused) for an injected incident:
      alerts  every alert on the incident's column; this is what systems receive
      caused  the subset that does not fire on the same window without the incident,
              i.e. alerts the incident is responsible for (the authoring checker needs one)
    """
    column   = inj.incident.column
    baseline = ref[ref["window"].isin(baseline_windows(inj.incident.window))]
    natural  = ref[ref["window"] == inj.incident.window]

    alerts     = [a for a in run_monitors(inj.window, baseline) if a["column"] == column]
    background = {a["monitor"] for a in run_monitors(natural, baseline) if a["column"] == column}
    caused     = [a for a in alerts if a["monitor"] not in background]
    return alerts, caused
