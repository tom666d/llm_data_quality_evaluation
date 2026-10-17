import json

import pandas as pd
import pytest

from dqeval import data, harness, incidents, report, scoring
from dqeval.arms import NoChange, QuarantineAll
from dqeval.arms import rules
from dqeval.arms.rules import Rules

DEV_SET = data.PROJECT_ROOT / "incident_sets" / "dev.json"
TOY_INCIDENT = {"incident_id": "toy-1", "split": "dev", "window": 10, "column": "annual_inc",
                "case_type": "unrecoverable", "family": "course_sentinel",
                "corruption": {"scope": "id % 100 < 8", "value": "9999999"},
                "counterexamples": "annual_inc > 40000"}


# ── What systems see ──────────────────────────────────────────────────────────
def test_case_hides_ground_truth_and_metadata(stationary_ref):
    inj  = incidents.inject(stationary_ref, incidents.Incident.from_dict(TOY_INCIDENT))
    case = harness.build_case(stationary_ref, inj)
    assert list(case.window.columns) == data.VISIBLE_COLUMNS == list(case.baseline.columns)
    assert not hasattr(case, "incident_id") and not hasattr(case, "case_type")
    assert case.alerts and all(a["column"] == "annual_inc" for a in case.alerts)


# ── Controls ──────────────────────────────────────────────────────────────────
def test_controls(stationary_ref):
    inj  = incidents.inject(stationary_ref, incidents.Incident.from_dict(TOY_INCIDENT))
    case = harness.build_case(stationary_ref, inj)
    assert NoChange().propose(case).candidate.decision == "KEEP"
    quarantine_all = scoring.score(stationary_ref, inj, QuarantineAll().propose(case).candidate)
    assert quarantine_all.handled_cells == quarantine_all.injected_cells and not quarantine_all.fixes_window


# ── Rules ─────────────────────────────────────────────────────────────────────
def toy_case(flags: list[bool], feeds: list[str]) -> tuple[rules.Case, pd.Series]:
    n = len(flags)
    window = pd.DataFrame({"source_system": feeds,
                           "ingested_at": pd.date_range("2026-01-05", periods=n, freq="3s")})
    return rules.Case("x", [], window, window), pd.Series(flags)


def test_localize_ignores_a_handful_of_rows():
    case, flagged = toy_case([i % 150 == 0 for i in range(2000)], ["A", "B", "C", "D"] * 500)   # 14 rows
    assert rules.localize(case, flagged) == []


def test_localize_ignores_rows_spread_over_every_feed():
    case, flagged = toy_case([i % 101 == 0 for i in range(2000)], ["A", "B", "C", "D"] * 500)   # 20 rows, all feeds
    assert rules.localize(case, flagged) == []


def test_localize_finds_feed_and_start_time():
    feeds = ["A", "B", "C", "D"] * 500
    flags = [f == "B" and i >= 1000 for i, f in enumerate(feeds)]
    conds = rules.localize(*toy_case(flags, feeds))
    assert conds[0] == "source_system IN ('B')" and conds[1].startswith("ingested_at >=")


def test_localize_needs_rows_after_a_change_point():
    feeds = ["A"] * 2000
    conds = rules.localize(*toy_case([i == 1999 for i in range(2000)], feeds))
    assert conds == []


def test_category_mapping_and_dates():
    assert rules.category_mapping(["36M", "60M"], {" 36 months", " 60 months"}) == \
        {"36M": " 36 months", "60M": " 60 months"}
    assert rules.category_mapping(["ZZ"], {"A", "B"}) is None
    assert rules.date_format(["12/2015", "11/2015"]) == "%m/%Y"


@pytest.mark.skipif(not data.REFERENCE_PATH.exists(), reason="reference CSV not extracted")
def test_rules_solve_the_dev_set(tmp_path):
    """Regression test for A2 on the set it was developed on (not evidence for any question)."""
    records = harness.run(DEV_SET, "dev", ["A2"], tmp_path)
    failed  = [(r["incident_id"], r["trace"]) for r in records if not r["success"]]
    assert not failed, failed


# ── Harness and report ────────────────────────────────────────────────────────
def test_harness_writes_records_and_report(tmp_path, stationary_ref):
    path = tmp_path / "incidents.json"
    path.write_text(json.dumps([TOY_INCIDENT]))
    records = harness.run(path, "dev", ["A0", "A1", "A2"], tmp_path / "out", ref=stationary_ref)
    assert [r["arm"] for r in records] == ["A0", "A1", "A2"]
    assert (tmp_path / "out" / "run.json").exists()
    text = report.summarize(report.load(tmp_path / "out"))
    assert "## Success by arm" in text and "| A2 |" in text


def test_held_out_split_needs_the_freeze_tag(monkeypatch, tmp_path):
    monkeypatch.setattr(harness, "git", lambda *args: "" if args[0] == "status" else "some-other-tag")
    with pytest.raises(SystemExit, match="eval-freeze-m1"):
        harness.run(tmp_path / "S1.json", "S1", ["A0"], tmp_path)
