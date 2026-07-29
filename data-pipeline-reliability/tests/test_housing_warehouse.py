from src import housing_warehouse

ROWS = [
    {"row_id": 1, "price": 42000.0, "lotsize": 5850.0, "bedrooms": 3.0, "bathrms": 1.0,
     "stories": 2.0, "driveway": "yes", "recroom": "no", "fullbase": "yes", "gashw": "no",
     "airco": "no", "garagepl": 1.0, "prefarea": "no"},
]


def _db(tmp_path):
    return str(tmp_path / "housing.db")


def test_insert_and_delete_batch_roundtrip(tmp_path):
    db_path = _db(tmp_path)
    housing_warehouse.init_warehouse(db_path)
    housing_warehouse.insert_housing_batch(db_path, "batch-1", ROWS)
    assert housing_warehouse.rows_for_batch(db_path, "batch-1") == 1
    deleted = housing_warehouse.delete_housing_batch(db_path, "batch-1")
    assert deleted == 1
    assert housing_warehouse.rows_for_batch(db_path, "batch-1") == 0


def test_mark_processed_and_unprocessed(tmp_path):
    db_path = _db(tmp_path)
    housing_warehouse.init_warehouse(db_path)
    assert housing_warehouse.is_processed(db_path, "batch-1") is False
    housing_warehouse.mark_processed(db_path, "batch-1")
    assert housing_warehouse.is_processed(db_path, "batch-1") is True
    housing_warehouse.mark_unprocessed(db_path, "batch-1")
    assert housing_warehouse.is_processed(db_path, "batch-1") is False


def test_total_row_count(tmp_path):
    db_path = _db(tmp_path)
    housing_warehouse.init_warehouse(db_path)
    housing_warehouse.insert_housing_batch(db_path, "batch-1", ROWS)
    housing_warehouse.insert_housing_batch(db_path, "batch-2", ROWS)
    assert housing_warehouse.total_row_count(db_path) == 2
