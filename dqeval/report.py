"""
report.py
Turns harness records into the tables of spec §7.3.

The unit is the incident: for each arm, an incident's rate is its successes out of
its runs, and an arm's success rate is the mean of those rates over incidents.

Usage (from the project root):
    .venv/bin/python -m dqeval.report results/dev
"""

import json
import sys
from pathlib import Path

import pandas as pd


def load(run_dir: Path) -> pd.DataFrame:
    with open(Path(run_dir) / "records.jsonl", encoding="utf-8") as f:
        return pd.DataFrame([json.loads(line) for line in f])


def per_incident(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (arm, incident): success share over runs, plus the incident's labels."""
    return (df.groupby(["arm", "incident_id", "case_type", "covered", "family"], as_index=False)
              .agg(runs=("success", "size"), successes=("success", "sum"), rate=("success", "mean")))


FAILURES = ["did not execute", "harmed clean data", "not handled", "unsafe to reuse"]


def failure(run: pd.Series) -> str | None:
    """The first reason a run failed, from its score fields; None for a success."""
    if run["success"]:
        return None
    if not run["executes"]:
        return "did not execute"
    if run["clean_cells_changed"] > 0 or run["clean_rows_quarantined"] > 0.01 * max(run["clean_rows"], 1):
        return "harmed clean data"
    if not run["fixes_window"]:
        return "not handled"
    return "unsafe to reuse"


def markdown(table: pd.DataFrame) -> str:
    cols = [str(c) for c in table.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(v) for v in row) + " |" for row in table.itertuples(index=False)]
    return "\n".join(lines)


def summarize(df: pd.DataFrame) -> str:
    inc  = per_incident(df)
    arms = sorted(df["arm"].unique())

    overall = []
    for arm in arms:
        runs, incs = df[df["arm"] == arm], inc[inc["arm"] == arm]
        overall.append({
            "arm": arm,
            "success": f"{incs['rate'].mean():.2f} ({int((incs['rate'] == 1).sum())}/{len(incs)} always)",
            "executes": f"{runs['executes'].mean():.2f}",
            "fixes window": f"{runs['fixes_window'].mean():.2f}",
            "safe to reuse": f"{runs['safe_to_reuse'].mean():.2f}",
            "decision matches": f"{runs['decision_matches'].mean():.2f}" if runs["decision"].notna().any() else "n/a",
            "time-scoped successes": int((runs["success"] & runs["scope_uses_time"]).sum()),
            "LLM calls / run": f"{runs['llm_calls'].mean():.1f}",
            "tokens / run": f"{(runs['input_tokens'] + runs['output_tokens']).mean():.0f}",
            "seconds / run": f"{runs['seconds'].mean():.2f}",
        })

    by_type = inc.pivot_table(index="arm", columns="case_type", values="rate", aggfunc="mean").round(2)
    by_cover = (inc.assign(coverage=inc["covered"].map({True: "covered", False: "uncovered"}))
                   .pivot_table(index="arm", columns="coverage", values="rate", aggfunc="mean").round(2))
    reasons = (df.assign(reason=df.apply(failure, axis=1)).dropna(subset=["reason"])
                 .pivot_table(index="arm", columns="reason", values="run", aggfunc="count", fill_value=0)
                 .reindex(index=arms, columns=FAILURES, fill_value=0))
    grid = (inc.assign(cell=inc["successes"].astype(str) + "/" + inc["runs"].astype(str))
               .pivot_table(index=["incident_id", "case_type"], columns="arm", values="cell", aggfunc="first")
               .reset_index())

    return "\n\n".join([
        "## Success by arm\n\n" + markdown(pd.DataFrame(overall)),
        "## Success rate by case type\n\n" + markdown(by_type.reset_index()),
        "## Covered vs. uncovered\n\n" + markdown(by_cover.reset_index()),
        "## Why runs failed (first reason, number of runs)\n\n" + markdown(reasons.reset_index()),
        "## Per incident (successes / runs)\n\n" + markdown(grid),
    ])


def main(argv: list[str] | None = None) -> None:
    run_dir = Path((argv or sys.argv[1:])[0])
    report  = summarize(load(run_dir))
    (run_dir / "report.md").write_text(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
