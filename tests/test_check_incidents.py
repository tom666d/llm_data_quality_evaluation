import json

import pytest

from dqeval import check_incidents, incidents
from dqeval.incidents import Incident

RECOVERABLE = {
    "incident_id": "dev-ok", "split": "dev", "window": 10, "column": "loan_amnt",
    "case_type": "recoverable", "family": "course_null",
    "corruption": {"scope": "source_system = 'B'", "value": "NULL"},
    "recovery": "funded_amnt", "counterexamples": "loan_amnt > 0",
}


def inc(**overrides):
    return Incident.from_dict({**RECOVERABLE, **overrides})


def test_valid_incident_passes(stationary_ref):
    assert check_incidents.check_incident(stationary_ref, inc()) == []


def test_recovery_may_only_read_visible_columns():
    assert any("cannot see" in p for p in incidents.check_definition(inc(recovery="funded_amnt + row_idx * 0")))


def test_family_is_required():
    assert "family is required" in incidents.check_definition(inc(family=""))


def test_unrecoverable_label_on_a_recoverable_incident_is_flagged(stationary_ref):
    mislabeled = inc(case_type="unrecoverable", recovery=None)
    problems = check_incidents.check_incident(stationary_ref, mislabeled)
    assert any("visible column funded_amnt holds the reference value" in p for p in problems)


def test_incident_must_cause_an_alert(stationary_ref):
    tiny = inc(corruption={"scope": "id % 1000 = 0", "value": "NULL"})
    assert "the incident causes no alert on its column" in check_incidents.check_incident(stationary_ref, tiny)


def test_broken_sql_is_reported_not_raised(stationary_ref):
    broken = inc(corruption={"scope": "source_system = ", "value": "NULL"})
    assert check_incidents.check_incident(stationary_ref, broken)[0].startswith("injection failed")


def test_held_out_size_requirements():
    incs = [inc(incident_id=f"s1-{k}", split="S1", window=30) for k in range(3)]
    problems = check_incidents.check_set(incs, "S1")
    assert any("need at least 20" in p for p in problems)
    assert any("per case type" in p for p in problems)
    assert any("uncovered" in p for p in problems)


def test_duplicates_and_wrong_split():
    problems = check_incidents.check_set([inc(), inc()], "S1")
    assert any("duplicate" in p for p in problems)
    assert any("not in split S1" in p for p in problems)


def write(tmp_path, items):
    path = tmp_path / "incidents.json"
    path.write_text(json.dumps(items))
    return path


def test_cli_pass_prints_hash(tmp_path, stationary_ref, monkeypatch, capsys):
    monkeypatch.setitem(check_incidents.MIN_PER_TYPE, "dev", 0)
    code = check_incidents.main([str(write(tmp_path, [RECOVERABLE])), "--split", "dev"], ref=stationary_ref)
    out = capsys.readouterr().out
    assert code == 0 and "PASSED  sha256=" in out


def test_cli_quiet_failure_hides_incident_details(tmp_path, stationary_ref, capsys):
    bad = {**RECOVERABLE, "incident_id": "secret-mechanism-42", "recovery": "funded_amnt + 1"}
    code = check_incidents.main([str(write(tmp_path, [bad])), "--split", "dev", "--quiet"], ref=stationary_ref)
    out = capsys.readouterr().out
    assert code == 1 and "NOT PASSED" in out
    assert "secret-mechanism-42" not in out and "recovery" not in out
