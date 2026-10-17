"""
v1
Arm A3-v1: DataGenie v1 as submitted (spec §6.3). See PROVENANCE.md.
"""

import importlib.util
import io
import json
import shutil
import tempfile
import uuid
from contextlib import redirect_stdout
from pathlib import Path

from dotenv import load_dotenv

from ... import data
from ...llm_usage import UsageTracker, tracking
from ...monitors import pattern
from .. import Arm, Case, Proposal

FROZEN = Path(__file__).resolve().parent / "frozen"

# The adapter speaks v1's detector format: its three issue types and its detail strings
V1_ISSUE_TYPE = {"null_rate": "Null Spike", "numeric_shift": "Statistical Outlier",
                 "string_pattern": "Format Inconsistency"}
MONITOR_PRIORITY = ["null_rate", "numeric_shift", "string_pattern", "new_categories"]


def to_v1_issue(case: Case) -> dict:
    """The issue v1's own detector would have produced for this alert."""
    monitors = {a["monitor"]: a for a in case.alerts}
    monitor  = next(m for m in MONITOR_PRIORITY if m in monitors)
    values   = case.window[case.column]
    n        = len(values)

    if monitor == "null_rate":
        nulls  = int(values.isna().sum())
        detail = f"Null rate: {nulls / n:.0%} ({nulls}/{n} rows)"
        sample = values.dropna().head(3).tolist()
    elif monitor == "numeric_shift":                    # v1 flagged values outside the batch's own 1.5 x IQR
        x = values.dropna()
        q1, q3 = x.quantile([0.25, 0.75])
        lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
        out    = x[(x < lo) | (x > hi)]
        detail = f"{len(out)} outlier(s) ({len(out) / n:.1%} of rows). IQR bounds: [{lo:.2f}, {hi:.2f}]"
        sample = out.head(5).tolist()
    elif monitor == "string_pattern":
        patterns = values.dropna().astype(str).map(pattern)
        new      = monitors[monitor]["detail"]
        others   = patterns[patterns != new]
        major    = others.mode().iloc[0] if not others.empty else "other"
        detail   = (f"Mixed formats detected: {len(others)} rows as {major}, "
                    f"{int((patterns == new).sum())} rows as {new}")
        sample   = values.head(5).tolist()
    else:
        share  = monitors[monitor]["window"]
        known  = set(case.baseline[case.column].dropna().astype(str))
        unseen = values.dropna().astype(str)[lambda s: ~s.isin(known)]
        detail = f"{len(unseen)} rows ({share:.1%} of rows) hold values unseen in recent data"
        sample = unseen.head(5).tolist()

    return {"column": case.column, "issue_type": V1_ISSUE_TYPE.get(monitor, monitor),
            "severity": "HIGH" if monitor == "null_rate" else "MEDIUM",
            "detail": detail, "sample_values": str(sample)}


def run_v1(issue: dict, total_rows: int, total_columns: int, tracker: UsageTracker) -> tuple[dict, str]:
    """Runs v1's own run_suggester() on one issue, in the directory layout it expects."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "src" / "llm").mkdir(parents=True)
        (root / "data").mkdir()
        shutil.copy(FROZEN / "suggester_dashboard.py", root / "src" / "llm" / "suggester_dashboard.py")
        shutil.copy(FROZEN / "historical_decisions.json", root / "data" / "historical_decisions.json")
        (root / "data" / "issues_output.json").write_text(json.dumps([issue]))
        (root / "data" / "df_shape.json").write_text(
            json.dumps({"total_rows": total_rows, "total_columns": total_columns}))

        spec   = importlib.util.spec_from_file_location(f"v1_{uuid.uuid4().hex}",
                                                        root / "src" / "llm" / "suggester_dashboard.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        log = io.StringIO()
        with tracking(tracker), redirect_stdout(log):
            results = module.run_suggester()
        return results[0], log.getvalue()


class V1(Arm):
    """A3-v1: the course system as submitted; its option 1 is the candidate."""
    name     = "A3-v1"
    uses_llm = True

    def __init__(self):
        load_dotenv(data.PROJECT_ROOT / ".env")     # v1 reads OPENAI_API_KEY from the environment

    def propose(self, case: Case) -> Proposal:
        issue, tracker = to_v1_issue(case), UsageTracker()
        try:
            result, log = run_v1(issue, len(case.window), len(case.window.columns), tracker)
        except Exception as e:
            return Proposal(None, error=f"{type(e).__name__}: {e}", llm_calls=tracker.calls,
                            input_tokens=tracker.input_tokens, output_tokens=tracker.output_tokens,
                            trace=[{"issue": issue, "exchanges": tracker.exchanges}])
        suggestions = result.get("remediation", {}).get("suggestions", [])
        option_1    = next((s for s in suggestions if s.get("option") == 1), suggestions[0] if suggestions else {})
        code        = option_1.get("pyspark_code") or None
        return Proposal(None, error=None if code else "v1 returned no option 1 code", code=code,
                        llm_calls=tracker.calls, input_tokens=tracker.input_tokens,
                        output_tokens=tracker.output_tokens,
                        trace=[{"issue": issue, "models": sorted(m for m in tracker.models if m),
                                "result": result, "exchanges": tracker.exchanges, "log": log}])
