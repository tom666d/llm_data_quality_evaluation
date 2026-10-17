import numpy as np
import pandas as pd
import pytest

from dqeval import data, incidents
from dqeval.incidents import Incident

DEV_WINDOW = 3   # any window inside the dev split


@pytest.fixture(scope="module")
def ref():
    """Toy reference with the real stream layout: 100k rows, a few real column names."""
    n   = 100_000
    rng = np.random.default_rng(0)
    amt = rng.integers(10, 400, n) * 100.0
    df  = pd.DataFrame({
        "id":          np.arange(1, n + 1),
        "loan_amnt":   amt,
        "funded_amnt": amt,
        "annual_inc":  rng.integers(20, 200, n) * 1000.0,
        "issue_d":     rng.choice(["Oct-2015", "Nov-2015", "Dec-2015"], n),
    })
    return data.enrich(df)


def make(**overrides):
    base = dict(incident_id="t", split="dev", window=DEV_WINDOW, column="loan_amnt",
                case_type="unrecoverable", counterexamples="loan_amnt > 0")
    base.update(overrides)
    return Incident.from_dict(base)


def problems(ref, inc):
    found = incidents.check_definition(inc)
    return found or incidents.check_injected(incidents.inject(ref, inc))


def test_recoverable_with_valid_proof(ref):
    inc = make(case_type="recoverable", corruption={"scope": "source_system = 'B'", "value": "NULL"},
               recovery="funded_amnt")
    inj = incidents.inject(ref, inc)
    assert problems(ref, inc) == []
    assert len(inj.truth) == (inj.clean["source_system"] == "B").sum()
    assert inj.truth["injected"].isna().all()


def test_recoverable_with_wrong_proof_is_flagged(ref):
    inc = make(case_type="recoverable", corruption={"scope": "source_system = 'B'", "value": "NULL"},
               recovery="funded_amnt + 1")
    assert any("recovery rebuilds" in p for p in problems(ref, inc))


def test_representation_checks(ref):
    to_us = "strftime(strptime(issue_d, '%b-%Y'), '%m/%Y')"
    good = make(column="issue_d", case_type="representation",
                corruption={"scope": "source_system = 'C'", "value": to_us},
                equivalence="strptime(out, '%m/%Y') = strptime(ref, '%b-%Y')",
                canonical="regexp_matches(out, '^[A-Z][a-z]{2}-[0-9]{4}$')",
                counterexamples="issue_d = 'Dec-2015'")
    assert problems(ref, good) == []

    changes_meaning = make(column="issue_d", case_type="representation",
                           corruption={"scope": "source_system = 'C'", "value": "'01/2020'"},
                           equivalence=good.equivalence, canonical=good.canonical,
                           counterexamples="issue_d = 'Dec-2015'")
    assert any("do not keep the reference meaning" in p for p in problems(ref, changes_meaning))


def test_ground_truth_skips_unchanged_cells(ref):
    inc = make(column="issue_d", corruption={"scope": "true", "value": "'Dec-2015'"},
               counterexamples="issue_d = 'Dec-2015'")
    inj = incidents.inject(ref, inc)
    assert len(inj.truth) == (inj.clean["issue_d"] != "Dec-2015").sum()
    assert (inj.window["issue_d"] == "Dec-2015").all()


def test_corruption_must_change_something(ref):
    inc = make(corruption={"scope": "true", "value": "loan_amnt"})
    assert "corruption changed no cells" in problems(ref, inc)


def test_normal_incident_with_population(ref):
    inc = make(case_type="normal", column="annual_inc", population="annual_inc >= 150000",
               counterexamples="annual_inc >= 150000")
    inj = incidents.inject(ref, inc)
    assert problems(ref, inc) == []
    assert (inj.window["annual_inc"] >= 150_000).all() and inj.truth.empty
    natural = ref[ref["window"] == DEV_WINDOW]
    assert inj.window["ingested_at"].tolist() == natural["ingested_at"].tolist()
    assert not set(inj.counterexamples["id"]) & set(inj.window["id"])


def test_definition_errors(ref):
    assert "a normal incident must not have a corruption" in incidents.check_definition(
        make(case_type="normal", corruption={"scope": "true", "value": "NULL"}))
    assert any("not inside split" in p for p in incidents.check_definition(make(window=30)))
    assert "a recoverable incident needs a recovery expression" in incidents.check_definition(
        make(case_type="recoverable", corruption={"scope": "true", "value": "NULL"}))


def test_too_few_counterexamples(ref):
    inc = make(corruption={"scope": "true", "value": "NULL"}, counterexamples="loan_amnt < 0")
    assert any("counterexamples" in p for p in problems(ref, inc))


def test_external_access_is_disabled():
    with pytest.raises(Exception):
        incidents.connect().execute("SELECT * FROM read_csv('/etc/hosts')")
