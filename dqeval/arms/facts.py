"""
facts.py
A compact text summary of a Case for the LLM arms: what moved, where (feeds and
time), which other columns match the alerted one, and a few unusual rows.
Observations only; interpreting them is the model's job.
"""

import pandas as pd

from .. import data
from . import Case


def _numeric(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series)


def unusual_rows(case: Case) -> pd.Series:
    """Rows whose alerted value is null, outside the baseline's range, or never seen in the baseline."""
    col, w, b = case.column, case.window, case.baseline
    if _numeric(b[col]):
        lo, hi = b[col].quantile([0.001, 0.999])
        flagged = w[col].isna() | (w[col] < lo) | (w[col] > hi)
    else:
        flagged = w[col].isna() | ~w[col].astype(str).isin(set(b[col].dropna().astype(str)))
    return flagged & ~(w[col].isna() & (b[col].isna().mean() > 0.2))   # ignore nulls if they are normal here


def _profile(values: pd.Series) -> str:
    if _numeric(values):
        q = values.quantile([0.05, 0.5, 0.95])
        return f"p5={q.iloc[0]:,.10g} median={q.iloc[1]:,.10g} p95={q.iloc[2]:,.10g}"
    top = values.astype(str).value_counts(normalize=True).head(3)
    return ", ".join(f"{v!r} {s:.0%}" for v, s in top.items())


def describe(case: Case) -> str:
    col, w, b = case.column, case.window, case.baseline
    kind = "numeric" if _numeric(b[col]) else "text"
    lines = [f"Alerted column: {col} ({kind})"]
    for a in case.alerts:
        detail = f", detail {a['detail']}" if a.get("detail") else ""
        lines.append(f"Alert: {a['monitor']} moved from {a['baseline']} (baseline) to {a['window']} (window){detail}")
    lines.append(f"Window: {len(w)} rows from {case.window_start} to {case.window_end}. "
                 f"Baseline: {len(b)} clean rows from nearby windows.")
    lines.append(f"Null rate: window {w[col].isna().mean():.1%}, baseline {b[col].isna().mean():.1%}")
    lines.append(f"Values: window {_profile(w[col].dropna())} | baseline {_profile(b[col].dropna())}")

    flagged = unusual_rows(case)
    lines.append(f"Unusual rows (null, out of the baseline range, or unseen): {int(flagged.sum())} of {len(w)}")
    if flagged.any():
        top = w.loc[flagged, col].astype(str).value_counts().head(5)
        lines.append("Most common unusual values: " + ", ".join(f"{v!r} x{n}" for v, n in top.items()))

    lines.append("Unusual share by feed: " + ", ".join(
        f"{feed} {flagged[w['source_system'] == feed].mean():.0%}" for feed in sorted(w["source_system"].unique())))
    order = w["ingested_at"].rank(method="first")
    quarters = pd.cut(order, 4, labels=["1st", "2nd", "3rd", "4th"])
    lines.append("Unusual share by quarter of the window (in time order): " + ", ".join(
        f"{q} {flagged[quarters == q].mean():.0%}" for q in ["1st", "2nd", "3rd", "4th"]))

    matches = []
    for other in data.VISIBLE_COLUMNS:
        if other in (col, "id", "ingested_at") or _numeric(b[other]) != _numeric(b[col]):
            continue
        both = b[col].notna() & b[other].notna()
        if both.sum() >= 100:
            same = (b.loc[both, col] - b.loc[both, other]).abs() <= 0.01 if _numeric(b[col]) else \
                   b.loc[both, col].astype(str) == b.loc[both, other].astype(str)
            if same.mean() >= 0.9:
                matches.append(f"{other} ({same.mean():.0%})")
    lines.append("Baseline columns equal to it: " + (", ".join(matches) if matches else "none"))

    sample = w[flagged] if flagged.any() else w
    lines.append("Sample rows from the window (CSV):")
    lines.append(sample.head(5).to_csv(index=False).strip())
    return "\n".join(lines)
