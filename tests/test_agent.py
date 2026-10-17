import json
from types import SimpleNamespace

import openai

from dqeval import harness, incidents, scoring
from dqeval.arms import agent
from dqeval.incidents import Incident

SENTINEL = {"incident_id": "toy-1", "split": "dev", "window": 10, "column": "annual_inc",
            "case_type": "unrecoverable", "family": "course_sentinel",
            "corruption": {"scope": "source_system = 'B'", "value": "9999999"},
            "counterexamples": "annual_inc > 40000"}


def setup(ref):
    inj = incidents.inject(ref, Incident.from_dict(SENTINEL))
    return inj, harness.build_case(ref, inj)


def test_run_sql_is_read_only(stationary_ref):
    _, case = setup(stationary_ref)
    out = agent.run_sql(case, "SELECT source_system, count(*) AS n FROM alert_window "
                              "WHERE annual_inc = 9999999 GROUP BY 1")
    assert out.startswith("source_system,n") and "B," in out and "(1 rows)" in out
    assert agent.run_sql(case, "DELETE FROM alert_window").startswith("error: only a single SELECT")
    assert agent.run_sql(case, "SELECT 1; SELECT 2").startswith("error")
    assert agent.run_sql(case, "SELECT * FROM read_csv('/etc/hosts')").startswith("error")


def test_dry_run_reports_window_and_baseline(stationary_ref):
    _, case = setup(stationary_ref)
    good = agent.dry_run(case, {"decision": "QUARANTINE", "scope": "annual_inc = 9999999"})
    assert "baseline: 0 values changed, 0 rows quarantined" in good
    broad = agent.dry_run(case, {"decision": "QUARANTINE", "scope": "source_system = 'B'"})
    assert "baseline: 0 values changed, 0 rows quarantined" not in broad
    assert agent.dry_run(case, {"decision": "RESTORE", "scope": "true"}).startswith("invalid candidate")


def tool_reply(name, args, call_id):
    call = SimpleNamespace(id=call_id, type="function",
                           function=SimpleNamespace(name=name, arguments=json.dumps(args)))
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=None, tool_calls=[call]))],
                           usage=SimpleNamespace(prompt_tokens=100, completion_tokens=20), model="fake")


class ScriptedOpenAI:
    """Investigates, dry-runs, submits an invalid candidate, then a valid one."""
    script = [("run_sql", {"query": "SELECT count(*) FROM alert_window WHERE annual_inc = 9999999"}),
              ("dry_run", {"decision": "QUARANTINE", "scope": "annual_inc = 9999999"}),
              ("submit", {"decision": "QUARANTINE", "scope": "(SELECT 1)", "rationale": "x"}),
              ("submit", {"decision": "QUARANTINE", "scope": "annual_inc = 9999999", "rationale": "placeholder"})]

    def __init__(self, **kwargs):
        self.turn = 0
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, model, messages, tools):
        script = type(self).script
        name, args = script[min(self.turn, len(script) - 1)]
        self.turn += 1
        return tool_reply(name, args, f"call-{self.turn}")


class EndlessOpenAI(ScriptedOpenAI):
    script = [("run_sql", {"query": "SELECT 1"})]


def test_agent_investigates_then_submits(stationary_ref, monkeypatch):
    monkeypatch.setattr(openai, "OpenAI", ScriptedOpenAI)
    inj, case = setup(stationary_ref)
    proposal  = agent.Agent().propose(case)
    assert proposal.error is None and proposal.candidate.scope == "annual_inc = 9999999"
    assert proposal.llm_calls == 4 and proposal.trace[0]["tool_calls"] == 4
    tool_results = [m["content"] for m in proposal.trace[0]["messages"] if m["role"] == "tool"]
    assert tool_results[2].startswith("invalid") and tool_results[3] == "accepted"
    assert scoring.score(stationary_ref, inj, proposal.candidate).success


def test_agent_stops_at_the_tool_budget(stationary_ref, monkeypatch):
    monkeypatch.setattr(openai, "OpenAI", EndlessOpenAI)
    _, case = setup(stationary_ref)
    proposal = agent.Agent().propose(case)
    assert proposal.candidate is None and "budget" in proposal.error
    assert proposal.trace[0]["tool_calls"] == agent.MAX_TOOL_CALLS
