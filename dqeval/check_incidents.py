"""
check_incidents.py
Authoring checker for incident files (spec §5.3). Incident authors, including
the author of the sealed held-out sets, run it until a file passes.

Per incident: the definition, the injection, the author's proofs, and that the
incident causes an alert on its column.
Per file: unique ids, a single split, and the size requirements of that split.

On success it prints the file's SHA-256, which is what gets committed for a
held-out set. --quiet prints only the summary and the hash, so the developer
can verify a sealed file without seeing what is in it.

Usage (from the project root):
    .venv/bin/python -m dqeval.check_incidents PATH --split S1 [--quiet]
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

from . import data, incidents, monitors
from .incidents import CASE_TYPES, Incident

MIN_INCIDENTS = {"dev": 0, "S1": 20, "S2": 20}
MIN_PER_TYPE  = {"dev": 2, "S1": 4,  "S2": 4}
MIN_UNCOVERED = {"dev": 0, "S1": 10, "S2": 10}


def check_incident(ref: pd.DataFrame, inc: Incident) -> list[str]:
    problems = incidents.check_definition(inc)
    if problems:
        return problems
    try:
        inj = incidents.inject(ref, inc)
    except Exception as e:
        return [f"injection failed: {type(e).__name__}: {e}"]
    problems = incidents.check_injected(inj)
    _, caused = monitors.incident_alerts(ref, inj)
    if not caused:
        problems.append("the incident causes no alert on its column")
    return problems


def check_set(incs: list[Incident], split: str) -> list[str]:
    problems = []
    dupes = [k for k, v in Counter(i.incident_id for i in incs).items() if v > 1]
    if dupes:
        problems.append(f"duplicate incident_ids: {dupes}")
    outside = sum(i.split != split for i in incs)
    if outside:
        problems.append(f"{outside} incident(s) are not in split {split}")
    if len(incs) < MIN_INCIDENTS[split]:
        problems.append(f"{len(incs)} incidents; need at least {MIN_INCIDENTS[split]}")
    per_type = Counter(i.case_type for i in incs)
    short = {t: per_type.get(t, 0) for t in CASE_TYPES if per_type.get(t, 0) < MIN_PER_TYPE[split]}
    if short:
        problems.append(f"need at least {MIN_PER_TYPE[split]} incidents per case type; have {short}")
    uncovered = sum(not i.covered for i in incs)
    if uncovered < MIN_UNCOVERED[split]:
        problems.append(f"{uncovered} uncovered incidents; need at least {MIN_UNCOVERED[split]}")
    return problems


def summarize(incs: list[Incident]) -> str:
    per_type = Counter(i.case_type for i in incs)
    covered  = sum(i.covered for i in incs)
    return (f"{len(incs)} incidents | " + ", ".join(f"{t} {per_type.get(t, 0)}" for t in CASE_TYPES)
            + f" | covered {covered}, uncovered {len(incs) - covered}"
            + f" | {len({i.window for i in incs})} distinct windows")


def main(argv: list[str] | None = None, ref: pd.DataFrame | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check an incident file against the spec.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--split", required=True, choices=list(data.SPLITS))
    parser.add_argument("--quiet", action="store_true", help="print only the summary and the hash")
    args = parser.parse_args(argv)

    incs = incidents.load_incidents(args.path)
    ref  = data.load_reference() if ref is None else ref
    per_incident = {i.incident_id: check_incident(ref, i) for i in incs}
    set_problems = check_set(incs, args.split)
    failed       = {k: v for k, v in per_incident.items() if v}

    if not args.quiet:
        for incident_id, problems in per_incident.items():
            print(f"{'FAIL' if problems else 'ok  '}  {incident_id}")
            for p in problems:
                print(f"        - {p}")
    print(summarize(incs))
    for p in set_problems:
        print(f"FILE  - {p}")

    if failed or set_problems:
        print(f"NOT PASSED: {len(failed)} incident(s) failed"
              + (" (rerun without --quiet to see why)" if args.quiet and failed else ""))
        return 1
    print(f"PASSED  sha256={data.sha256(args.path)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
