import importlib.util
import json
import re
from types import SimpleNamespace

import openai
import pytest

from dqeval import data, harness, incidents, scoring
from dqeval.arms import v1
from dqeval.incidents import Incident

PROVENANCE = v1.FROZEN.parent / "PROVENANCE.md"
SENTINEL = {"incident_id": "toy-1", "split": "dev", "window": 10, "column": "annual_inc",
            "case_type": "unrecoverable", "family": "course_sentinel",
            "corruption": {"scope": "id % 100 < 8", "value": "9999999"},
            "counterexamples": "annual_inc > 40000"}


def frozen_module():
    spec = importlib.util.spec_from_file_location("v1_frozen_for_tests", v1.FROZEN / "suggester_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_frozen_files_match_provenance():
    recorded = dict(re.findall(r"`frozen/([^`]+)`.*?`([0-9a-f]{64})`", PROVENANCE.read_text()))
    assert set(recorded) == {"suggester_dashboard.py", "historical_decisions.json"}
    for name, digest in recorded.items():
        assert data.sha256(v1.FROZEN / name) == digest, f"{name} was modified"


def case_for(ref, spec):
    return harness.build_case(ref, incidents.inject(ref, Incident.from_dict(spec)))


def test_adapter_speaks_v1_detector_format(stationary_ref):
    module = frozen_module()
    null_case = case_for(stationary_ref, {**SENTINEL, "column": "loan_amnt",
                                          "corruption": {"scope": "source_system = 'B'", "value": "NULL"}})
    issue = v1.to_v1_issue(null_case)
    assert issue["issue_type"] == "Null Spike" and issue["severity"] == "HIGH"
    assert module.extract_affected_rows_percent(issue["detail"]) == pytest.approx(25, abs=2)

    outlier = v1.to_v1_issue(case_for(stationary_ref, SENTINEL))
    assert outlier["issue_type"] == "Statistical Outlier"
    assert module.extract_affected_rows_percent(outlier["detail"]) is not None


class FakeOpenAI:
    """Answers v1's two agents offline; Agent 2 returns three options as v1's critic requires."""
    calls = 0

    def __init__(self, **kwargs):
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, model, response_format, messages):
        FakeOpenAI.calls += 1
        if "You are Agent 1" in messages[1]["content"]:
            body = {"root_cause": "placeholder default upstream", "business_impact": "skews income"}
        else:
            body = {"suggestions": [
                {"option": 1, "action": "Flag sentinel", "confidence": 85, "rationale": "r", "caveats": "c",
                 "pyspark_code": "from pyspark.sql import functions as F\n"
                                 "df = df.withColumn('annual_inc_quarantine', F.col('annual_inc') == 9999999)"},
                {"option": 2, "action": "Drop", "confidence": 65, "rationale": "r", "caveats": "c",
                 "pyspark_code": "df = df.filter('annual_inc < 9999999')"},
                {"option": 3, "action": "Decline changes", "confidence": 100, "rationale": "r", "caveats": "c",
                 "pyspark_code": "# No change applied to df"}]}
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(body)))],
                               usage=SimpleNamespace(prompt_tokens=100, completion_tokens=50), model="fake-mini")


def test_v1_runs_as_submitted_and_its_option_1_is_scored(stationary_ref, monkeypatch):
    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    monkeypatch.setenv("OPENAI_API_KEY", "test-not-used")
    inj      = incidents.inject(stationary_ref, Incident.from_dict(SENTINEL))
    proposal = v1.V1().propose(harness.build_case(stationary_ref, inj))
    assert proposal.error is None and "annual_inc_quarantine" in proposal.code
    assert proposal.llm_calls == 2 and proposal.input_tokens == 200 and proposal.trace[0]["models"] == ["fake-mini"]
    assert openai.OpenAI is FakeOpenAI                      # tracking restored the client class

    s = scoring.score_code(stationary_ref, inj, proposal.code)
    assert s.success and s.handled_cells == s.injected_cells


def test_code_that_drops_rows_or_edits_other_columns_is_harm(stationary_ref):
    inj = incidents.inject(stationary_ref, Incident.from_dict(SENTINEL))
    dropped = scoring.score_code(stationary_ref, inj, "df = df.filter('annual_inc < 9999999 OR loan_amnt > 30000')")
    assert dropped.handled_cells == 0 and dropped.rows_dropped > 0 and not dropped.success
    edited = scoring.score_code(stationary_ref, inj,
                                "from pyspark.sql import functions as F\n"
                                "df = df.withColumn('annual_inc_quarantine', F.col('annual_inc') == 9999999)"
                                ".withColumn('grade', F.lit('A'))")
    assert edited.handled_cells == edited.injected_cells and edited.clean_cells_changed > 0 and not edited.fixes_window


def test_code_failures_count_against_the_arm(stationary_ref):
    inj = incidents.inject(stationary_ref, Incident.from_dict(SENTINEL))
    s = scoring.score_code(stationary_ref, inj, "df = df.fillna({'loan_amnt': df['funded_amnt']})")
    assert not s.executes and "not iterable" in s.error.lower()
    assert scoring.score_code(stationary_ref, inj, None, "v1 returned no option 1 code").error == "v1 returned no option 1 code"
