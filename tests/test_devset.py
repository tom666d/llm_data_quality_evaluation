"""
The dev set is solvable: every dev incident has a reference candidate that the
scorer marks as a success. These answers live in tests/ on purpose; no arm may
read them.
"""

import pytest

from dqeval import data, incidents, scoring
from dqeval.contract import RuleCandidate

DEV_SET = data.PROJECT_ROOT / "incident_sets" / "dev.json"
US_DATE = "regexp_matches(issue_d, '^[0-9]{2}/[0-9]{4}$')"

ANSWERS = {
    "dev-r1": ("RESTORE", "loan_amnt IS NULL", "funded_amnt"),
    "dev-r2": ("RESTORE", "grade = 'U'", "substring(sub_grade, 1, 1)"),
    # bounded to the incident's time range: an open-ended rule would also rescale later, clean rows
    "dev-r3": ("RESTORE", "source_system = 'B' AND ingested_at >= TIMESTAMP '2026-01-05 14:10:00' "
                          "AND ingested_at < TIMESTAMP '2026-01-05 15:00:00'", "annual_inc * 12"),
    "dev-p1": ("STANDARDIZE", US_DATE, "strftime(strptime(issue_d, '%m/%Y'), '%b-%Y')"),
    "dev-p2": ("STANDARDIZE", "int_rate < 1", "int_rate * 100"),
    "dev-p3": ("STANDARDIZE", "regexp_matches(term, '^[0-9]+M$')",
               "' ' || regexp_extract(term, '[0-9]+') || ' months'"),
    "dev-u1": ("QUARANTINE", "annual_inc = 9999999", None),
    "dev-u2": ("QUARANTINE", "issue_d = '01/2020'", None),
    "dev-u3": ("QUARANTINE", "source_system = 'D' AND tot_cur_bal = 0 "
                             "AND ingested_at >= TIMESTAMP '2026-01-06 15:10:00' "
                             "AND ingested_at < TIMESTAMP '2026-01-06 16:00:00'", None),
    "dev-n1": ("KEEP", None, None),
    "dev-n2": ("KEEP", None, None),
    "dev-n3": ("KEEP", None, None),
}

pytestmark = pytest.mark.skipif(not data.REFERENCE_PATH.exists(), reason="reference CSV not extracted")


@pytest.fixture(scope="module")
def ref():
    return data.load_reference()


@pytest.fixture(scope="module")
def dev_set():
    return {inc.incident_id: inc for inc in incidents.load_incidents(DEV_SET)}


def test_every_dev_incident_has_an_answer(dev_set):
    assert set(dev_set) == set(ANSWERS)


@pytest.mark.parametrize("incident_id", sorted(ANSWERS))
def test_reference_answer_succeeds(ref, dev_set, incident_id):
    inc = dev_set[incident_id]
    decision, scope, new_value = ANSWERS[incident_id]
    s = scoring.score(ref, incidents.inject(ref, inc), RuleCandidate(decision, inc.column, scope, new_value))
    assert s.success, s.to_dict()


def test_open_ended_time_scope_fixes_the_window_but_is_unsafe(ref, dev_set):
    """dev-r3's trap: without an upper time bound, the rule also rescales later, clean rows."""
    inc = dev_set["dev-r3"]
    open_ended = RuleCandidate("RESTORE", inc.column,
                               "source_system = 'B' AND ingested_at >= TIMESTAMP '2026-01-05 14:10:00'", "annual_inc * 12")
    s = scoring.score(ref, incidents.inject(ref, inc), open_ended)
    assert s.fixes_window and not s.safe_to_reuse and s.regression_cells_changed > 0
