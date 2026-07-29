"""Fast, deterministic tests of the core idempotency contract against the
real warehouse -- no chaos-injected latency, no thread timing, so these
never flake. (The chaos-timing behavior itself -- the ambiguous-timeout
retry scenario -- is exercised by tests/test_agent_runner_integration.py
and benchmarks/pipeline_chaos_benchmark.py, where some timing variance is
expected and documented.)
"""

from latch import InMemoryStore, idempotent

from src import warehouse

ROWS = [
    {"row_id": 1, "carat": 0.23, "cut": "Ideal", "color": "E", "clarity": "SI2",
     "depth": 61.5, "table_pct": 55.0, "price": 326, "x": 3.95, "y": 3.98, "z": 2.43},
]


def _db(tmp_path):
    path = str(tmp_path / "warehouse.db")
    warehouse.init_warehouse(path)
    return path


def test_same_idempotency_key_inserts_exactly_once(tmp_path):
    db_path = _db(tmp_path)
    store = InMemoryStore()

    @idempotent(store=store)
    def load_sales_batch(batch_id, rows):
        return warehouse.insert_batch(db_path, batch_id, rows)

    r1 = load_sales_batch(batch_id="batch-1", rows=ROWS, idempotency_key="run-1-batch-1")
    r2 = load_sales_batch(batch_id="batch-1", rows=ROWS, idempotency_key="run-1-batch-1")

    assert r1 == r2  # second call is a cache hit, not a fresh insert
    assert warehouse.total_row_count(db_path) == 1
    assert warehouse.loads_per_batch(db_path)["batch-1"] == 1


def test_different_idempotency_keys_both_execute(tmp_path):
    db_path = _db(tmp_path)
    store = InMemoryStore()

    @idempotent(store=store)
    def load_sales_batch(batch_id, rows):
        return warehouse.insert_batch(db_path, batch_id, rows)

    load_sales_batch(batch_id="batch-1", rows=ROWS, idempotency_key="run-1-batch-1")
    load_sales_batch(batch_id="batch-1", rows=ROWS, idempotency_key="run-2-batch-1")

    # Two distinct logical operations (different keys) => two real inserts,
    # even though it's nominally "the same batch_id" -- idempotency keys
    # are the caller's responsibility, exactly as CLAUDE.md's non-negotiables
    # describe.
    assert warehouse.total_row_count(db_path) == 2
    assert warehouse.loads_per_batch(db_path)["batch-1"] == 2


def test_no_idempotency_wrapper_means_every_call_inserts(tmp_path):
    """The control case: this is the naive bug, demonstrated directly and
    deterministically -- no @idempotent at all means calling the raw
    warehouse write twice for "the same" batch really does write twice."""
    db_path = _db(tmp_path)

    def load_sales_batch_naive(batch_id, rows):
        return warehouse.insert_batch(db_path, batch_id, rows)

    load_sales_batch_naive("batch-1", ROWS)
    load_sales_batch_naive("batch-1", ROWS)

    assert warehouse.total_row_count(db_path) == 2
    assert warehouse.loads_per_batch(db_path)["batch-1"] == 2
