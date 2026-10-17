"""
rules.py
Arm A2: a deterministic rule library (spec §6.2), developed on the dev set only.

It reads facts from the alert window and the clean baseline, the same kind of
facts the prototype's evidence step computes, and picks one decision. Nothing
names a specific column or incident.

  null_rate          restore from a column the target derives from; otherwise
                     quarantine the new nulls, narrowed to where they appeared
  numeric_shift      quarantine a dominant out-of-range value; rescale a segment
                     whose values match the baseline after a common unit factor;
                     keep a shift spread across the whole window (a real population)
  new_categories,
  string_pattern     keep if every value is already known; restore a placeholder
                     from a column it derives from; standardize dates or categories
                     written another way; quarantine dates outside the baseline
                     range and anything else unseen

Wherever the rule needs a segment, it narrows the scope to the feeds, and the
start time, where the problem concentrates. A time-scoped rule is also capped at
the end of the window: it only knows the incident through this window.
"""

import math
import re
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd

from ..contract import RuleCandidate
from ..incidents import connect
from . import Arm, Case, Proposal, QuarantineAll

EQUAL_SHARE     = 0.99    # a derivation must reproduce the column this often in the baseline
SEGMENT_SHARE   = 0.95    # feeds kept in a segment must hold this share of the flagged rows
TAIL            = 0.001   # values outside the baseline's [0.1%, 99.9%] quantiles count as out of range
PLACEHOLDER     = 0.5     # one value must make up this share of the out-of-range rows
MIN_FLAGGED     = 20      # fewer flagged rows than this are natural tail values, too few to localize
FEED_LIFT       = 3.0     # kept feeds must be flagged at least this many times as often as the others
UNIT_FACTORS    = [10, 12, 100, 1000, 1 / 10, 1 / 12, 1 / 100, 1 / 1000]
DATE_FORMATS    = ["%b-%Y", "%m/%Y", "%Y-%m", "%m-%Y", "%Y/%m", "%b %Y", "%B %Y", "%B-%Y", "%Y%m", "%m%Y"]
DATE_SLACK_DAYS = 62      # a re-encoded date may sit up to two months outside the baseline's months


# ── Small helpers ─────────────────────────────────────────────────────────────
def literal(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return repr(float(value)) if isinstance(value, float) else str(value)


def tables(case: Case) -> duckdb.DuckDBPyConnection:
    con = connect()
    con.register("w", case.window)
    con.register("b", case.baseline)
    return con


def mask(case: Case, condition: str) -> pd.Series:
    """Boolean Series over the window rows where an SQL condition holds."""
    con = tables(case)
    hits = con.execute(f"SELECT coalesce(({condition}), false) FROM w").fetchnumpy()
    return pd.Series(list(hits.values())[0], index=case.window.index).astype(bool)


# ── Localization: narrow flagged rows to feeds and a start time ───────────────
def localize(case: Case, flagged: pd.Series) -> list[str]:
    """SQL conditions for the feeds, and the start time, where the flagged rows concentrate."""
    w, conds = case.window, []
    counts = w.loc[flagged, "source_system"].value_counts()
    if counts.sum() < MIN_FLAGGED:
        return conds
    feeds, kept = [], 0
    for feed, n in counts.items():
        feeds.append(feed)
        kept += n
        if kept >= SEGMENT_SHARE * counts.sum():
            break
    in_feeds = w["source_system"].isin(feeds)
    rate_in, rate_out = flagged[in_feeds].mean(), (flagged[~in_feeds].mean() if (~in_feeds).any() else 0.0)
    if rate_in < FEED_LIFT * rate_out:
        in_feeds = pd.Series(True, index=w.index)          # not concentrated: keep every feed
    elif len(feeds) < w["source_system"].nunique():
        conds.append(f"source_system IN ({', '.join(literal(f) for f in sorted(feeds))})")

    # start time: the earliest point after which the feed is mostly flagged and before which it is clean
    seg  = w[in_feeds].sort_values("ingested_at")
    flag = flagged[seg.index].to_numpy()
    n, cum = len(flag), np.cumsum(flag)
    for i in np.flatnonzero(flag):
        if i < 0.1 * n:
            continue                       # too early to be a change point
        if n - i < MIN_FLAGGED:
            break                          # too few rows after it to call it a change
        before = cum[i - 1] / i
        after  = (cum[-1] - cum[i - 1]) / (n - i)
        if before <= 0.05 and after >= 0.8:
            start = seg["ingested_at"].iloc[i]
            conds += [f"ingested_at >= TIMESTAMP '{start}'", f"ingested_at <= TIMESTAMP '{case.window_end}'"]
            break
    return conds


# ── Derivations: expressions that rebuild a column from another visible column ─
def derivation(case: Case, flagged: pd.Series) -> str | None:
    """An expression that equals the column in the baseline and is available on the flagged rows."""
    col, con = case.column, tables(case)
    numeric = pd.api.types.is_numeric_dtype(case.baseline[col])
    con.register("flagged", case.window[flagged])
    for src in case.baseline.columns:
        if src in (col, "id", "ingested_at"):
            continue
        src_numeric = pd.api.types.is_numeric_dtype(case.baseline[src])
        if numeric != src_numeric:
            continue
        exprs = [src] if numeric else [src, f"substring({src}, 1, 1)", f"substring({src}, 1, 2)"]
        for expr in exprs:
            same = f"abs(({expr}) - {col}) <= 0.01" if numeric else f"({expr}) = {col}"
            try:
                matches, available = con.execute(f"""
                    SELECT (SELECT avg(CASE WHEN {same} THEN 1 ELSE 0 END) FROM b WHERE {col} IS NOT NULL),
                           (SELECT avg(CASE WHEN ({expr}) IS NOT NULL THEN 1 ELSE 0 END) FROM flagged)
                """).fetchone()
            except duckdb.Error:
                continue
            if matches is not None and available is not None and matches >= EQUAL_SHARE and available >= EQUAL_SHARE:
                return expr
    return None


# ── Rules per monitor ─────────────────────────────────────────────────────────
def null_rule(case: Case, notes: list) -> RuleCandidate:
    col   = case.column
    nulls = case.window[col].isna()
    expr  = derivation(case, nulls)
    if expr:
        notes.append(f"{expr} reproduces {col} in the baseline and is present where {col} is null")
        return RuleCandidate("RESTORE", col, f"{col} IS NULL AND ({expr}) IS NOT NULL", expr)
    conds = [f"{col} IS NULL"]
    if case.baseline[col].isna().any():   # nulls also occur naturally, so narrow to where the new ones are
        conds += localize(case, nulls)
    notes.append(f"no column rebuilds {col}; quarantining the new nulls")
    return RuleCandidate("QUARANTINE", col, " AND ".join(conds))


def numeric_rule(case: Case, notes: list) -> RuleCandidate:
    col, w, b = case.column, case.window, case.baseline
    lo, hi  = b[col].quantile([TAIL, 1 - TAIL])
    outside = w[col].notna() & ((w[col] < lo) | (w[col] > hi))
    if outside.sum() < MIN_FLAGGED:
        notes.append(f"only {int(outside.sum())} values outside the baseline range; treating them as natural tail")
        return RuleCandidate("KEEP", col)

    # 1. one placeholder value dominates the out-of-range rows
    counts = w.loc[outside, col].value_counts()
    if counts.iloc[0] >= PLACEHOLDER * outside.sum() and counts.iloc[0] >= 10:
        value = counts.index[0]
        conds = [f"{col} = {literal(value)}"]
        if (b[col] == value).any():        # the value also occurs naturally, so narrow it
            conds += localize(case, w[col] == value)
        notes.append(f"{value} makes up {counts.iloc[0]}/{outside.sum()} out-of-range values; quarantining it")
        return RuleCandidate("QUARANTINE", col, " AND ".join(conds))

    conds = localize(case, outside)
    if not conds:
        notes.append("out-of-range values are spread over every feed and the whole window; keeping")
        return RuleCandidate("KEEP", col)

    # 2. a segment that matches the baseline after a common unit factor
    segment = w.loc[mask(case, " AND ".join(conds)), col].dropna()
    ratio   = b[col].median() / segment.median() if segment.median() else float("inf")
    factor  = min(UNIT_FACTORS, key=lambda k: abs(math.log(ratio / k))) if 0 < ratio < float("inf") else None
    if factor and abs(math.log(ratio / factor)) < math.log(1.2):
        power_of_ten = abs(math.log10(factor) - round(math.log10(factor))) < 1e-9
        decision     = "STANDARDIZE" if power_of_ten else "RESTORE"
        base_min, base_max = b[col].min(), b[col].max()
        if factor > 1 and segment.max() < base_min:        # separable: a value threshold is reusable
            scope = f"{col} < {literal(float(math.sqrt(segment.max() * base_min)))}"
        elif factor < 1 and segment.min() > base_max:
            scope = f"{col} > {literal(float(math.sqrt(segment.min() * base_max)))}"
        else:
            scope = " AND ".join(conds)
        notes.append(f"the segment matches the baseline after x{factor:g}; rescaling it")
        return RuleCandidate(decision, col, scope, f"{col} * {literal(float(factor))}")

    notes.append("a segment is out of range with no unit factor behind it; quarantining it")
    return RuleCandidate("QUARANTINE", col,
                         f"({col} < {literal(float(lo))} OR {col} > {literal(float(hi))}) AND " + " AND ".join(conds))


def parse_date(value: str, fmt: str) -> datetime | None:
    try:
        return datetime.strptime(value, fmt)
    except ValueError:
        return None


def date_format(values: list[str]) -> str | None:
    """The one format every value parses with, if any."""
    return next((f for f in DATE_FORMATS if values and all(parse_date(v, f) for v in values)), None)


def category_mapping(new_values: list[str], known: set[str]) -> dict[str, str] | None:
    """Maps every new value onto exactly one known value under some normalization, else None."""
    normalizers = [lambda s: s.strip().lower(), lambda s: re.sub(r"[^a-z0-9]", "", s.lower()),
                   lambda s: re.sub(r"[^0-9]", "", s)]
    mapping = {}
    for v in new_values:
        for norm in normalizers:
            key = norm(v)
            hits = [k for k in known if key and norm(k) == key]
            if len(hits) == 1:
                mapping[v] = hits[0]
                break
        else:
            return None
    return mapping


def string_rule(case: Case, notes: list) -> RuleCandidate:
    col, w, b = case.column, case.window, case.baseline
    known      = set(b[col].dropna().astype(str))
    values     = w[col].astype("string")
    canonical  = date_format(sorted(known))
    known_dates = sorted(d for d in (parse_date(k, canonical) for k in known) if d) if canonical else []

    def is_known(v: str) -> bool:      # a later month in the canonical format is not "unseen"
        if v in known:
            return True
        d = parse_date(v, canonical) if canonical else None
        return bool(d and known_dates[0] - timedelta(DATE_SLACK_DAYS) <= d <= known_dates[-1] + timedelta(DATE_SLACK_DAYS))

    unseen = values.notna() & ~values.fillna("").map(is_known).astype(bool)
    if not unseen.any():
        notes.append("every value is already known; only the mix changed, keeping")
        return RuleCandidate("KEEP", col)
    new_values = sorted(set(values[unseen]))
    in_new     = f"{col} IN ({', '.join(literal(v) for v in new_values)})"

    if len(new_values) == 1:                                  # a placeholder that another column can rebuild
        expr = derivation(case, unseen)
        if expr:
            notes.append(f"placeholder {new_values[0]!r}; {expr} rebuilds {col}")
            return RuleCandidate("RESTORE", col, in_new, expr)

    if canonical:                                             # dates written another way
        fmt = date_format(new_values)
        if fmt:
            parsed = [parse_date(v, fmt) for v in new_values]
            lo, hi = known_dates[0] - timedelta(DATE_SLACK_DAYS), known_dates[-1] + timedelta(DATE_SLACK_DAYS)
            if all(lo <= d <= hi for d in parsed):
                pattern = "^" + re.sub(r"[0-9]", "[0-9]", re.escape(new_values[0])) + "$"
                notes.append(f"dates in {fmt} map onto the baseline months; re-encoding as {canonical}")
                return RuleCandidate("STANDARDIZE", col, f"regexp_matches({col}, {literal(pattern)})",
                                     f"strftime(strptime({col}, {literal(fmt)}), {literal(canonical)})")
            if not any(lo <= d <= hi for d in parsed):
                notes.append(f"dates {new_values} fall outside the baseline months; quarantining them")
                return RuleCandidate("QUARANTINE", col, in_new)

    mapping = category_mapping(new_values, known)
    if mapping:
        cases = " ".join(f"WHEN {literal(k)} THEN {literal(v)}" for k, v in mapping.items())
        notes.append(f"new values map one-to-one onto known categories: {mapping}")
        return RuleCandidate("STANDARDIZE", col, in_new, f"CASE {col} {cases} END")

    notes.append(f"unexplained new values {new_values[:5]}; quarantining them")
    return RuleCandidate("QUARANTINE", col, in_new)


class Rules(Arm):
    """A2: the deterministic rule library."""
    name = "A2"

    def propose(self, case: Case) -> Proposal:
        monitors, notes = {a["monitor"] for a in case.alerts}, []
        try:
            if "null_rate" in monitors:
                candidate = null_rule(case, notes)
            elif "numeric_shift" in monitors:
                candidate = numeric_rule(case, notes)
            elif monitors & {"new_categories", "string_pattern"}:
                candidate = string_rule(case, notes)
            else:
                candidate = None
        except Exception as e:                        # a rule bug must not hide behind a silent fallback
            notes.append(f"rule error: {type(e).__name__}: {e}")
            candidate = None
        if candidate is None:
            notes.append("no rule matched; falling back to A1")
            fallback = QuarantineAll().propose(case)
            return Proposal(fallback.candidate, trace=notes)
        return Proposal(candidate, trace=notes)
