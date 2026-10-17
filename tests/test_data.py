import pandas as pd
import pytest

from dqeval import data


def toy(n=10_000):
    return pd.DataFrame({"id": range(1000, 1000 + n), "loan_amnt": 1.0})


def test_enrich_is_deterministic():
    pd.testing.assert_frame_equal(data.enrich(toy()), data.enrich(toy()))


def test_enrich_layout():
    df = data.enrich(toy())
    assert set(df["source_system"]) == set(data.SOURCES)
    assert df["ingested_at"].is_monotonic_increasing
    assert df.groupby("window").size().eq(data.WINDOW_ROWS).all()
    # one window spans WINDOW_ROWS * SECONDS_PER_ROW seconds
    first = df[df["window"] == 0]["ingested_at"]
    assert (first.max() - first.min()).total_seconds() == (data.WINDOW_ROWS - 1) * data.SECONDS_PER_ROW


def test_splits_cover_rows_without_overlap():
    df = data.enrich(toy(100_000))
    assert df["split"].notna().all()
    assert df["split"].value_counts().to_dict() == {"dev": 50_000, "S1": 25_000, "S2": 25_000}
    windows = [w for s in data.SPLITS for w in data.split_windows(s)]
    assert len(windows) == len(set(windows)) == 50


def test_hash_mismatch_is_rejected(tmp_path):
    bad = tmp_path / "ref.csv"
    bad.write_text("id,loan_amnt\n1,2\n")
    with pytest.raises(ValueError, match="SHA-256"):
        data.load_reference(bad)


@pytest.mark.skipif(not data.REFERENCE_PATH.exists(), reason="reference CSV not extracted")
def test_reference_is_clean_and_complete():
    df = data.load_reference()
    assert len(df) == 100_000 and df["id"].is_unique
    assert list(df.columns[: len(data.COLUMNS) + 1]) == ["id", *data.COLUMNS]
    # none of the course project's injected anomalies are present
    assert df["loan_amnt"].notna().all()
    assert not (df["annual_inc"] == 9_999_999).any()
    assert not (df["issue_d"] == "01/2020").any()
