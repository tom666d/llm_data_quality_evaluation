import pytest

from dqeval import data, incidents, scoring
from dqeval.contract import RuleCandidate
from dqeval.incidents import Incident

WINDOW = 10
US_DATE = "regexp_matches(issue_d, '^[0-9]{2}/[0-9]{4}$')"


def injected(ref, **spec):
    base = {"incident_id": "t", "split": "dev", "window": WINDOW, "family": "test"}
    return incidents.inject(ref, Incident.from_dict({**base, **spec}))


def run(ref, inj, decision, scope=None, new_value=None):
    cand = RuleCandidate(decision=decision, column=inj.incident.column, scope=scope, new_value=new_value)
    return scoring.score(ref, inj, cand)


@pytest.fixture(scope="module")
def recoverable(stationary_ref):
    return injected(stationary_ref, column="loan_amnt", case_type="recoverable", recovery="funded_amnt",
                    corruption={"scope": "source_system = 'B'", "value": "NULL"},
                    counterexamples="loan_amnt > 40000")


@pytest.fixture(scope="module")
def sentinel(stationary_ref):
    return injected(stationary_ref, column="annual_inc", case_type="unrecoverable",
                    corruption={"scope": "source_system = 'B' AND id % 8 = 1", "value": "9999999"},
                    counterexamples="annual_inc > 40000")


@pytest.fixture(scope="module")
def us_dates(stationary_ref):
    return injected(stationary_ref, column="issue_d", case_type="representation",
                    corruption={"scope": "source_system = 'C'", "value": "strftime(strptime(issue_d, '%b-%Y'), '%m/%Y')"},
                    equivalence="strptime(out, '%m/%Y') = strptime(ref, '%b-%Y')",
                    canonical="regexp_matches(out, '^[A-Z][a-z]{2}-[0-9]{4}$')",
                    counterexamples="issue_d = 'Dec-2015'")


@pytest.fixture(scope="module")
def normal(stationary_ref):
    return injected(stationary_ref, column="annual_inc", case_type="normal",
                    population="annual_inc > 30000", counterexamples="annual_inc > 30000")


# ── Recoverable ───────────────────────────────────────────────────────────────
def test_correct_restore_succeeds(stationary_ref, recoverable):
    s = run(stationary_ref, recoverable, "RESTORE", "loan_amnt IS NULL", "funded_amnt")
    assert s.success and s.handled_cells == s.injected_cells > 0


def test_quarantining_a_recoverable_incident_fails(stationary_ref, recoverable):
    s = run(stationary_ref, recoverable, "QUARANTINE", "loan_amnt IS NULL")
    assert s.executes and not s.fixes_window and s.handled_cells == 0


def test_restore_that_also_changes_clean_cells_fails(stationary_ref, recoverable):
    s = run(stationary_ref, recoverable, "RESTORE", "loan_amnt IS NULL OR loan_amnt > 40000", "funded_amnt * 0")
    assert not s.fixes_window and s.clean_cells_changed > 0


# ── Unrecoverable ─────────────────────────────────────────────────────────────
def test_targeted_quarantine_succeeds(stationary_ref, sentinel):
    assert len(sentinel.truth) > 0          # half of feed B; ids of feed B are 1 mod 4
    s = run(stationary_ref, sentinel, "QUARANTINE", "annual_inc = 9999999")
    assert s.success and s.clean_rows_quarantined == 0


def test_quarantining_the_whole_feed_fails(stationary_ref, sentinel):
    s = run(stationary_ref, sentinel, "QUARANTINE", "source_system = 'B'")
    assert s.handled_cells == s.injected_cells and not s.fixes_window
    assert s.clean_rows_quarantined > scoring.MAX_QUARANTINE_SHARE * s.clean_rows


def test_inventing_values_is_not_handling(stationary_ref, sentinel):
    s = run(stationary_ref, sentinel, "RESTORE", "annual_inc = 9999999", "50000")
    assert s.executes and s.handled_cells == 0 and not s.success


def test_too_broad_scope_is_unsafe_to_reuse(stationary_ref, sentinel):
    s = run(stationary_ref, sentinel, "QUARANTINE", "annual_inc > 35000")
    assert not s.safe_to_reuse and s.counterexample_rows_quarantined > 0


def test_time_scoped_rule_hits_later_windows(stationary_ref, sentinel):
    start = sentinel.window["ingested_at"].min()
    s = run(stationary_ref, sentinel, "QUARANTINE", f"ingested_at >= TIMESTAMP '{start}' AND source_system = 'B'")
    assert not s.safe_to_reuse and s.regression_rows_quarantined > 0


# ── Representation ────────────────────────────────────────────────────────────
def test_standardizing_succeeds(stationary_ref, us_dates):
    s = run(stationary_ref, us_dates, "STANDARDIZE", US_DATE, "strftime(strptime(issue_d, '%m/%Y'), '%b-%Y')")
    assert s.success and s.handled_cells == s.injected_cells


def test_keeping_the_other_encoding_fails(stationary_ref, us_dates):
    s = run(stationary_ref, us_dates, "KEEP")
    assert s.executes and s.handled_cells == 0 and not s.success


def test_wrong_month_fails(stationary_ref, us_dates):
    s = run(stationary_ref, us_dates, "STANDARDIZE", US_DATE, "'Jan-2015'")
    assert s.handled_cells == 0 and not s.success


# ── Normal ────────────────────────────────────────────────────────────────────
def test_keep_on_normal_succeeds(stationary_ref, normal):
    assert run(stationary_ref, normal, "KEEP").success


def test_quarantine_on_normal_fails(stationary_ref, normal):
    s = run(stationary_ref, normal, "QUARANTINE", "annual_inc > 30000")
    assert not s.fixes_window


# ── Execution failures and the regression slice ───────────────────────────────
def test_contract_violation_does_not_execute(stationary_ref, sentinel):
    s = run(stationary_ref, sentinel, "QUARANTINE", "(SELECT 1)")
    assert not s.executes and "not allowed" in s.error


def test_uncastable_value_does_not_execute(stationary_ref, recoverable):
    s = run(stationary_ref, recoverable, "RESTORE", "loan_amnt IS NULL", "'n/a'")
    assert not s.executes and s.error


def test_missing_candidate(stationary_ref, sentinel):
    s = scoring.score(stationary_ref, sentinel, None, error="model returned invalid JSON")
    assert not s.executes and s.error == "model returned invalid JSON"


def test_regression_slice_prefers_later_windows(stationary_ref, sentinel):
    rows = scoring.regression_slice(stationary_ref, sentinel)
    assert len(rows) == scoring.REGRESSION_ROWS
    assert rows["window"].min() > WINDOW and not set(rows["id"]) & set(sentinel.window["id"])


def test_regression_slice_tops_up_from_earlier_windows(stationary_ref):
    last = data.split_windows("dev")[-1]
    inj = injected(stationary_ref, window=last, column="annual_inc", case_type="unrecoverable",
                   corruption={"scope": "true", "value": "9999999"}, counterexamples="true")
    rows = scoring.regression_slice(stationary_ref, inj)
    assert len(rows) == scoring.REGRESSION_ROWS and (rows["window"] < last).all()


def test_incident_without_injected_cells_is_rejected(stationary_ref):
    empty = injected(stationary_ref, column="annual_inc", case_type="unrecoverable",
                     corruption={"scope": "source_system = 'B' AND id % 2 = 0", "value": "9999999"},
                     counterexamples="true")
    with pytest.raises(ValueError, match="injected no cells"):
        run(stationary_ref, empty, "KEEP")
