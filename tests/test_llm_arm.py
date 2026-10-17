import json
from types import SimpleNamespace

import openai

from dqeval import harness, incidents
from dqeval.arms.facts import describe
from dqeval.arms.llm import LLMPipeline
from dqeval.incidents import Incident

SENTINEL = {"incident_id": "toy-1", "split": "dev", "window": 10, "column": "annual_inc",
            "case_type": "unrecoverable", "family": "course_sentinel",
            "corruption": {"scope": "source_system = 'B'", "value": "9999999"},
            "counterexamples": "annual_inc > 40000"}


def case(ref):
    return harness.build_case(ref, incidents.inject(ref, Incident.from_dict(SENTINEL)))


def test_facts_show_what_where_and_matches(stationary_ref):
    text = describe(case(stationary_ref))
    assert "Alerted column: annual_inc (numeric)" in text
    assert "'9999999.0' x" in text                          # the dominant unusual value
    assert "B 100%" in text and "A 0%" in text             # localized to feed B
    assert "Sample rows from the window (CSV):" in text


class FakeOpenAI:
    """First reply breaks the contract (a subquery); the second is valid."""
    replies = [
        {"decision": "QUARANTINE", "column": "annual_inc", "scope": "annual_inc = (SELECT max(annual_inc) FROM t)"},
        {"diagnosis": "placeholder", "decision": "QUARANTINE", "column": "annual_inc", "scope": "annual_inc = 9999999"},
    ]

    def __init__(self, **kwargs):
        self.turn = 0
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, model, response_format, messages):
        body, self.turn = FakeOpenAI.replies[min(self.turn, 1)], self.turn + 1
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(body)))],
                               usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5), model="fake")


def test_invalid_candidate_is_sent_back_and_fixed(stationary_ref, monkeypatch):
    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    proposal = LLMPipeline().propose(case(stationary_ref))
    assert proposal.candidate.scope == "annual_inc = 9999999" and proposal.error is None
    assert proposal.llm_calls == 2
    feedback = proposal.trace[0]["exchanges"][1]["request"]["messages"][-1]["content"]
    assert "invalid" in feedback and "not allowed" in feedback
