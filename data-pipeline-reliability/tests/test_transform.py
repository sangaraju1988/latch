from datetime import date

import pandas as pd

from src import transform


def _fake_df(n: int) -> pd.DataFrame:
    return pd.DataFrame({"row_id": range(n), "value": range(n)})


def test_make_batches_covers_all_rows_exactly_once():
    df = _fake_df(105)
    batches = transform.make_batches(df, batch_size=10, start_date=date(2026, 1, 1))
    assert sum(len(b) for b in batches) == 105
    all_row_ids = [r["row_id"] for b in batches for r in b.rows]
    assert sorted(all_row_ids) == list(range(105))


def test_make_batches_batch_count_and_sizes():
    df = _fake_df(105)
    batches = transform.make_batches(df, batch_size=10, start_date=date(2026, 1, 1))
    assert len(batches) == 11  # 10 full + 1 with 5 rows
    assert [len(b) for b in batches[:-1]] == [10] * 10
    assert len(batches[-1]) == 5


def test_make_batches_ids_are_sequential_dates_with_prefix():
    df = _fake_df(25)
    batches = transform.make_batches(df, batch_size=10, start_date=date(2026, 1, 1), prefix="housing")
    assert [b.batch_id for b in batches] == [
        "housing-2026-01-01",
        "housing-2026-01-02",
        "housing-2026-01-03",
    ]


def test_default_batch_size_yields_40_batches_for_full_diamonds_dataset():
    df = _fake_df(53940)
    batches = transform.make_batches(df, batch_size=transform.DEFAULT_BATCH_SIZE)
    assert len(batches) == 40
    assert sum(len(b) for b in batches) == 53940


def test_summarize_batches():
    df = _fake_df(30)
    batches = transform.make_batches(df, batch_size=10)
    summary = transform.summarize_batches(batches)
    assert summary["num_batches"] == 3
    assert summary["total_rows"] == 30
    assert summary["min_batch_size"] == summary["max_batch_size"] == 10
