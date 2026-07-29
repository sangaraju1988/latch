"""Integration test of the real chaos-injected agent retry loop
(agent_runner.py + load_tools.py) against a small, fast slice of real
batches. Uses genuine `threading.Thread` + wall-clock timeouts (same as
the full benchmark), so exact duplicate *counts* are not asserted here --
real OS thread scheduling near a tight timeout boundary is not perfectly
reproducible run to run (see benchmarks/aggregate_seeds.py's module
docstring for the full explanation). What IS asserted, and holds every
single time regardless of scheduling noise, is the qualitative guarantee:

  - the protected pipeline never writes more than the expected row count
    and never produces a duplicated batch, no matter what the chaos
    injector does
  - the naive pipeline never writes FEWER than the expected row count
    (nothing is silently lost -- every abandoned background thread still
    eventually completes its write)
"""

from pathlib import Path

from latch import InMemoryStore, Tracer

from src import transform, warehouse
from src.agent_runner import run_naive_load, run_protected_load
from src.load_tools import make_protected_load, make_raw_load


def _make_batches(n_rows: int, batch_size: int):
    import pandas as pd

    df = pd.DataFrame(
        {
            "row_id": range(n_rows),
            "carat": [0.3] * n_rows,
            "cut": ["Ideal"] * n_rows,
            "color": ["E"] * n_rows,
            "clarity": ["SI2"] * n_rows,
            "depth": [61.0] * n_rows,
            "table_pct": [55.0] * n_rows,
            "price": [500] * n_rows,
            "x": [4.0] * n_rows,
            "y": [4.0] * n_rows,
            "z": [2.5] * n_rows,
        }
    )
    return transform.make_batches(df, batch_size=batch_size)


def test_naive_never_loses_rows(tmp_path):
    db_path = str(tmp_path / "naive.db")
    warehouse.init_warehouse(db_path)
    batches = _make_batches(50, batch_size=10)  # 5 batches
    raw = make_raw_load(db_path, seed=42)

    for batch in batches:
        run_naive_load(raw, batch.batch_id, batch.rows)

    import time

    time.sleep(0.5)  # let any abandoned background threads finish

    expected = sum(len(b) for b in batches)
    assert warehouse.total_row_count(db_path) >= expected


def test_protected_never_duplicates_and_matches_expected_exactly(tmp_path):
    db_path = str(tmp_path / "protected.db")
    warehouse.init_warehouse(db_path)
    batches = _make_batches(50, batch_size=10)  # 5 batches
    store = InMemoryStore()
    tracer = Tracer()
    protected = make_protected_load(db_path, seed=42, store=store, tracer=tracer)

    for batch in batches:
        run_protected_load(protected, batch.batch_id, batch.rows)

    import time

    time.sleep(0.5)

    expected = sum(len(b) for b in batches)
    assert warehouse.total_row_count(db_path) == expected
    dup = {b: n for b, n in warehouse.loads_per_batch(db_path).items() if n > 1}
    assert dup == {}
