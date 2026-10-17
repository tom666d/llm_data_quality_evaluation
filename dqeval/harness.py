"""
harness.py
Runs arms on an incident set, scores every proposal, and saves the raw records
(spec §6.4, §7.3, §9). Deterministic arms run once per incident; LLM arms run
LLM_RUNS times.

Held-out splits only run from a clean working tree whose HEAD carries that
milestone's freeze tag (S1: eval-freeze-m1, S2: eval-freeze-m2), so systems
cannot change after a held-out set is opened.

Usage (from the project root):
    .venv/bin/python -m dqeval.harness incident_sets/dev.json --split dev --arms A0 A1 A2 --out results/dev
"""

import argparse
import importlib.metadata
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import data, incidents, monitors, scoring
from .arms import Arm, Case, NoChange, Proposal, QuarantineAll
from .arms.rules import Rules

ARMS: dict[str, type[Arm]] = {"A0": NoChange, "A1": QuarantineAll, "A2": Rules}
LLM_RUNS    = 5
FREEZE_TAGS = {"S1": "eval-freeze-m1", "S2": "eval-freeze-m2"}
PACKAGES    = ["duckdb", "sqlglot", "pandas", "numpy", "openai"]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, cwd=data.PROJECT_ROOT).stdout.strip()


def check_freeze(split: str) -> None:
    """Refuses to touch a held-out split unless HEAD is the clean, tagged frozen state."""
    if split not in FREEZE_TAGS:
        return
    tag = FREEZE_TAGS[split]
    if git("status", "--porcelain") or tag not in git("tag", "--points-at", "HEAD").split():
        raise SystemExit(f"refusing to run {split}: HEAD must carry tag {tag} and the working tree must be clean")


def build_case(ref: pd.DataFrame, inj: incidents.Injected) -> Case:
    alerts, _ = monitors.incident_alerts(ref, inj)
    baseline  = ref[ref["window"].isin(monitors.baseline_windows(inj.incident.window))]
    return Case(column=inj.incident.column, alerts=alerts,
                window=inj.window[data.VISIBLE_COLUMNS].reset_index(drop=True),
                baseline=baseline[data.VISIBLE_COLUMNS].reset_index(drop=True))


def run(incident_path: Path, split: str, arm_names: list[str], out_dir: Path,
        runs: int = LLM_RUNS, ref: pd.DataFrame | None = None) -> list[dict]:
    check_freeze(split)
    ref  = data.load_reference() if ref is None else ref
    incs = incidents.load_incidents(incident_path)
    if any(i.split != split for i in incs):
        raise SystemExit(f"{incident_path} holds incidents outside split {split}")
    arms = [ARMS[name]() for name in arm_names]

    records = []
    for inc in incs:
        inj  = incidents.inject(ref, inc)
        case = build_case(ref, inj)
        for arm in arms:
            for run_idx in range(runs if arm.uses_llm else 1):
                started = time.time()
                try:
                    proposal = arm.propose(case)
                except Exception as e:                  # an arm crash is a failed run, not a harness crash
                    proposal = Proposal(None, error=f"{type(e).__name__}: {e}")
                seconds = round(time.time() - started, 3)
                score   = scoring.score(ref, inj, proposal.candidate, proposal.error)
                records.append({
                    "arm": arm.name, "incident_id": inc.incident_id, "run": run_idx,
                    "case_type": inc.case_type, "family": inc.family, "covered": inc.covered,
                    "column": inc.column, **score.to_dict(),
                    "candidate": proposal.candidate.to_dict() if proposal.candidate else None,
                    "proposal_error": proposal.error, "llm_calls": proposal.llm_calls,
                    "input_tokens": proposal.input_tokens, "output_tokens": proposal.output_tokens,
                    "seconds": seconds, "trace": proposal.trace,
                })

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "records.jsonl", "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
    meta = {
        "split": split, "arms": arm_names, "runs_per_llm_arm": runs,
        "incident_file": str(incident_path), "incident_sha256": data.sha256(incident_path),
        "git_commit": git("rev-parse", "HEAD"), "git_dirty": bool(git("status", "--porcelain")),
        "started": datetime.now().isoformat(timespec="seconds"),
        "packages": {p: importlib.metadata.version(p) for p in PACKAGES},
    }
    (out_dir / "run.json").write_text(json.dumps(meta, indent=2))
    return records


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run arms on an incident set and save scored records.")
    parser.add_argument("incidents", type=Path)
    parser.add_argument("--split", required=True, choices=list(data.SPLITS))
    parser.add_argument("--arms", nargs="+", required=True, choices=list(ARMS))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=LLM_RUNS, help="runs per LLM arm")
    args = parser.parse_args(argv)
    records = run(args.incidents, args.split, args.arms, args.out, args.runs)
    print(f"{len(records)} records written to {args.out}")


if __name__ == "__main__":
    main()
