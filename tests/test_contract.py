import numpy as np
import pandas as pd
import pytest

from dqeval import contract
from dqeval.contract import RuleCandidate


def cand(decision, scope=None, new_value=None, column="loan_amnt"):
    return RuleCandidate(decision=decision, column=column, scope=scope, new_value=new_value)


@pytest.fixture
def table():
    return pd.DataFrame({
        "id":          [1, 2, 3, 4],
        "loan_amnt":   [1000.0, np.nan, 3000.0, np.nan],
        "funded_amnt": [1000.0, 2000.0, 3000.0, 4000.0],
        "issue_d":     ["Dec-2015", "12/2015", "Nov-2015", "11/2015"],
        "source_system": ["A", "B", "A", "B"],
    })


@pytest.mark.parametrize("expression", [
    "coalesce(loan_amnt, funded_amnt)",
    "loan_amnt IS NULL AND funded_amnt IS NOT NULL",
    "source_system = 'B' AND ingested_at >= TIMESTAMP '2026-01-05 12:30:00'",
    "strftime(strptime(issue_d, '%m/%Y'), '%b-%Y')",
    "regexp_matches(issue_d, '^[0-9]{2}/[0-9]{4}$')",
    "CASE WHEN term = ' 36 months' THEN 36 ELSE 60 END",
    "annual_inc BETWEEN 1000 AND 20000 OR issue_d IN ('Oct-2015', 'Nov-2015')",
    "round(annual_inc * 12, 0)",
    "loan_amnt IS DISTINCT FROM funded_amnt",
])
def test_allowed_expressions(expression):
    assert contract.expression_problems(expression) == []


@pytest.mark.parametrize("expression, reason", [
    ("(SELECT max(annual_inc) FROM t)", "not allowed"),        # subquery reads other rows
    ("avg(annual_inc) OVER ()", "not allowed"),                 # window function
    ("max(annual_inc)", "not allowed"),                         # aggregate
    ("read_csv('/etc/hosts')", "not allowed"),                  # file access
    ("getenv('HOME')", "getenv is not allowed"),                # unknown function
    ("1; DROP TABLE t", "single expression"),
    ("row_idx < 100", "row_idx is not visible"),                # evaluation metadata
    ("t.loan_amnt > 0", "must not be qualified"),
    ("loan_amnt >", "does not parse"),
])
def test_rejected_expressions(expression, reason):
    assert any(reason in p for p in contract.expression_problems(expression))


def test_field_rules():
    assert contract.validate(cand("RESTORE", "loan_amnt IS NULL", "funded_amnt"), "loan_amnt") == []
    assert contract.validate(cand("QUARANTINE", "loan_amnt IS NULL"), "loan_amnt") == []
    assert contract.validate(cand("KEEP"), "loan_amnt") == []
    assert "RESTORE needs a new_value" in contract.validate(cand("RESTORE", "true"), "loan_amnt")
    assert "QUARANTINE must not set new_value" in contract.validate(cand("QUARANTINE", "true", "1"), "loan_amnt")
    assert "KEEP must not set a scope" in contract.validate(cand("KEEP", "true"), "loan_amnt")
    assert any("alerted column" in p for p in contract.validate(cand("KEEP"), "annual_inc"))
    assert contract.validate(cand("FIX"), "loan_amnt")[0].startswith("decision must be")


def test_parse_accepts_json_text():
    c = contract.parse('{"decision": "restore", "column": "loan_amnt", "scope": "true", "new_value": "1"}')
    assert c.decision == "RESTORE" and c.to_dict()["new_value"] == "1"
    with pytest.raises(ValueError):
        contract.parse("not json")


def test_restore_only_touches_scope_and_keeps_type(table):
    out = contract.apply(table, cand("RESTORE", "loan_amnt IS NULL", "funded_amnt"))
    assert out["loan_amnt"].tolist() == [1000.0, 2000.0, 3000.0, 4000.0]
    assert out["loan_amnt"].dtype == table["loan_amnt"].dtype
    assert not out["_quarantine"].any()
    pd.testing.assert_frame_equal(out[["id", "funded_amnt", "issue_d"]], table[["id", "funded_amnt", "issue_d"]])


def test_standardize(table):
    c = cand("STANDARDIZE", "regexp_matches(issue_d, '^[0-9]{2}/[0-9]{4}$')",
             "strftime(strptime(issue_d, '%m/%Y'), '%b-%Y')", column="issue_d")
    assert contract.apply(table, c)["issue_d"].tolist() == ["Dec-2015", "Dec-2015", "Nov-2015", "Nov-2015"]


def test_quarantine_flags_without_changing_values(table):
    out = contract.apply(table, cand("QUARANTINE", "source_system = 'B'"))
    assert out["_quarantine"].tolist() == [False, True, False, True]
    pd.testing.assert_series_equal(out["loan_amnt"], table["loan_amnt"])


def test_null_scope_does_not_quarantine(table):
    out = contract.apply(table, cand("QUARANTINE", "loan_amnt > 2000"))   # NULL for missing loan_amnt
    assert out["_quarantine"].tolist() == [False, False, True, False]


def test_keep_changes_nothing(table):
    out = contract.apply(table, cand("KEEP"))
    pd.testing.assert_frame_equal(out.drop(columns="_quarantine"), table)


def test_uncastable_value_raises(table):
    with pytest.raises(Exception):
        contract.apply(table, cand("RESTORE", "true", "'not a number'"))
