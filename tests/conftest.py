import numpy as np
import pandas as pd
import pytest

from dqeval import data

CATEGORICAL = ["term", "grade", "sub_grade", "emp_length", "home_ownership",
               "verification_status", "loan_status", "purpose", "addr_state"]


@pytest.fixture(scope="session")
def stationary_ref():
    """
    Toy reference over every evaluation column with the real stream layout.
    Values are stationary, so clean windows raise no alerts, and funded_amnt
    equals loan_amnt, so loan_amnt can be recovered from it.
    """
    n   = 100_000
    rng = np.random.default_rng(1)
    df  = pd.DataFrame({"id": np.arange(1, n + 1)})
    for col in data.COLUMNS:
        df[col] = rng.lognormal(10, 0.3, n).round(2)
    df["funded_amnt"] = df["loan_amnt"]
    for col in CATEGORICAL:
        df[col] = rng.choice(["Alpha", "Beta", "Gamma"], n)
    df["issue_d"] = rng.choice(["Oct-2015", "Nov-2015", "Dec-2015"], n)
    return data.enrich(df[["id", *data.COLUMNS]])
