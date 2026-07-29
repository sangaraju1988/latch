import pandas as pd

from src import extract


def test_extract_diamonds_row_count_and_columns(tmp_path):
    out = tmp_path / "diamonds.csv"
    df = extract.extract(str(out))
    assert len(df) == 53940
    assert list(df.columns) == [
        "row_id", "carat", "cut", "color", "clarity",
        "depth", "table_pct", "price", "x", "y", "z",
    ]
    assert out.exists()


def test_extract_diamonds_sha256_is_deterministic(tmp_path):
    out1 = tmp_path / "a.csv"
    out2 = tmp_path / "b.csv"
    extract.extract(str(out1))
    extract.extract(str(out2))
    assert extract.sha256_of_file(str(out1)) == extract.sha256_of_file(str(out2))


def test_extract_diamonds_values_are_real_public_data(tmp_path):
    out = tmp_path / "diamonds.csv"
    df = extract.extract(str(out))
    # Well-known facts about the public `diamonds` dataset -- if these ever
    # stop holding, either the dependency changed the data or something in
    # this pipeline corrupted it.
    assert df["price"].min() == 326
    assert df["price"].max() == 18823
    assert set(df["cut"].unique()) == {"Fair", "Good", "Very Good", "Premium", "Ideal"}


def test_extract_housing_row_count(tmp_path):
    out = tmp_path / "housing.csv"
    df = extract.extract_housing(str(out))
    assert len(df) == 546
    assert "price" in df.columns


def test_describe_matches_written_file(tmp_path):
    out = tmp_path / "diamonds.csv"
    extract.extract(str(out))
    meta = extract.describe(str(out))
    reread = pd.read_csv(out)
    assert meta["row_count"] == len(reread)
    assert meta["sha256"] == extract.sha256_of_file(str(out))
