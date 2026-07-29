from src import warehouse

ROWS = [
    {"row_id": 1, "carat": 0.23, "cut": "Ideal", "color": "E", "clarity": "SI2",
     "depth": 61.5, "table_pct": 55.0, "price": 326, "x": 3.95, "y": 3.98, "z": 2.43},
    {"row_id": 2, "carat": 0.21, "cut": "Premium", "color": "E", "clarity": "SI1",
     "depth": 59.8, "table_pct": 61.0, "price": 326, "x": 3.89, "y": 3.84, "z": 2.31},
]


def _db(tmp_path):
    return str(tmp_path / "warehouse.db")


def test_init_and_insert_batch(tmp_path):
    db_path = _db(tmp_path)
    warehouse.init_warehouse(db_path)
    result = warehouse.insert_batch(db_path, "batch-1", ROWS)
    assert result["rows_loaded"] == 2
    assert result["batch_id"] == "batch-1"
    assert warehouse.total_row_count(db_path) == 2


def test_insert_batch_has_no_dedup_of_its_own(tmp_path):
    """warehouse.insert_batch is the raw write -- it must insert every time
    it's called, with no memory of prior calls. This is deliberate: the
    naive/protected distinction lives entirely in load_tools.py +
    agent_runner.py, not here. If this test ever starts failing because
    insert_batch silently dedupes, the whole benchmark's premise breaks.
    """
    db_path = _db(tmp_path)
    warehouse.init_warehouse(db_path)
    warehouse.insert_batch(db_path, "batch-1", ROWS)
    warehouse.insert_batch(db_path, "batch-1", ROWS)
    assert warehouse.total_row_count(db_path) == 4
    lpb = warehouse.loads_per_batch(db_path)
    assert lpb["batch-1"] == 2  # two distinct physical inserts, same batch_id


def test_each_insert_gets_a_distinct_load_id(tmp_path):
    db_path = _db(tmp_path)
    warehouse.init_warehouse(db_path)
    r1 = warehouse.insert_batch(db_path, "batch-1", ROWS)
    r2 = warehouse.insert_batch(db_path, "batch-1", ROWS)
    assert r1["load_id"] != r2["load_id"]


def test_distinct_batch_count(tmp_path):
    db_path = _db(tmp_path)
    warehouse.init_warehouse(db_path)
    warehouse.insert_batch(db_path, "batch-1", ROWS)
    warehouse.insert_batch(db_path, "batch-2", ROWS)
    warehouse.insert_batch(db_path, "batch-1", ROWS)  # duplicate of batch-1
    assert warehouse.distinct_batch_count(db_path) == 2
    assert warehouse.total_row_count(db_path) == 6


def test_rows_per_batch(tmp_path):
    db_path = _db(tmp_path)
    warehouse.init_warehouse(db_path)
    warehouse.insert_batch(db_path, "batch-1", ROWS)
    rpb = warehouse.rows_per_batch(db_path)
    assert rpb["batch-1"] == 2
