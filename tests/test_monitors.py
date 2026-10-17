import numpy as np
import pandas as pd
import pytest

from dqeval import data, incidents, monitors
from dqeval.incidents import Incident

WINDOW = 10


@pytest.fixture(scope="module")
def ref():
    """Stationary toy reference over every evaluation column, so clean windows raise no alerts."""
    n   = 100_000
    rng = np.random.default_rng(1)
    df  = pd.DataFrame({"id": np.arange(1, n + 1)})
    for col in data.COLUMNS:
        df[col] = rng.lognormal(10, 0.3, n).round(2)
    for col in ["term", "grade", "sub_grade", "emp_length", "home_ownership", "verification_status",
                "loan_status", "purpose", "addr_state"]:
        df[col] = rng.choice(["Alpha", "Beta", "Gamma"], n)
    df["issue_d"] = rng.choice(["Oct-2015", "Nov-2015", "Dec-2015"], n)
    return data.enrich(df[["id", *data.COLUMNS]])


def monitors_for(ref, scope, value, column):
    inc = Incident.from_dict({"incident_id": "t", "split": "dev", "window": WINDOW, "column": column,
                              "case_type": "unrecoverable", "corruption": {"scope": scope, "value": value},
                              "counterexamples": "true"})
    _, caused = monitors.incident_alerts(ref, incidents.inject(ref, inc))
    return {a["monitor"]: a for a in caused}


def test_pattern_signature():
    assert monitors.pattern("Dec-2015") == "Aaa-9999"
    assert monitors.pattern("01/2020") == "99/9999"
    assert monitors.pattern("10+ years") == "99+ aaaaa"


def test_fold_change():
    assert monitors.fold_change(30, 10) == monitors.fold_change(10, 30) == 3
    assert monitors.fold_change(0, 5) == float("inf")
    assert monitors.fold_change(4, 4) == 1


def test_baseline_stays_inside_the_split():
    first_dev, first_s1 = data.split_windows("dev")[0], data.split_windows("S1")[0]
    assert monitors.baseline_windows(first_dev + 7) == list(range(first_dev + 2, first_dev + 7))
    assert monitors.baseline_windows(first_s1) == list(range(first_s1 + 1, first_s1 + 6))


def test_clean_window_raises_no_alerts(ref):
    baseline = ref[ref["window"].isin(monitors.baseline_windows(WINDOW))]
    assert monitors.run_monitors(ref[ref["window"] == WINDOW], baseline) == []


def test_null_spike(ref):
    caused = monitors_for(ref, "id % 10 < 4", "NULL", "loan_amnt")
    assert caused["null_rate"]["window"] == pytest.approx(0.4, abs=0.02)


def test_upward_sentinel_moves_p95(ref):
    caused = monitors_for(ref, "id % 100 < 8", "9999999", "annual_inc")
    assert caused["numeric_shift"]["detail"] == "p95"


def test_downward_shift_on_one_source_moves_p5(ref):
    caused = monitors_for(ref, "source_system = 'B'", "annual_inc / 12", "annual_inc")
    assert caused["numeric_shift"]["detail"] == "p5"


def test_overwritten_dates(ref):
    caused = monitors_for(ref, "id % 10 < 3", "'01/2020'", "issue_d")
    assert caused["string_pattern"]["detail"] == "99/9999"
    assert caused["new_categories"]["window"] == pytest.approx(0.3, abs=0.03)


def test_tiny_corruption_causes_nothing(ref):
    assert monitors_for(ref, "id % 1000 = 0", "NULL", "loan_amnt") == {}


def test_alerts_do_not_reveal_scope(ref):
    caused = monitors_for(ref, "source_system = 'B'", "NULL", "loan_amnt")
    assert set(caused["null_rate"]) == {"column", "monitor", "baseline", "window", "detail",
                                        "window_start", "window_end"}
