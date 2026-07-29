from src import housing_warehouse, saga_demo


def _batch_rows(n: int):
    return [
        {"row_id": i, "price": 40000.0 + i, "lotsize": 5000.0, "bedrooms": 3.0,
         "bathrms": 1.0, "stories": 1.0, "driveway": "yes", "recroom": "no",
         "fullbase": "no", "gashw": "no", "airco": "no", "garagepl": 0.0,
         "prefarea": "no"}
        for i in range(n)
    ]


def test_successful_batch_commits_both_steps(tmp_path):
    db_path = str(tmp_path / "housing.db")
    housing_warehouse.init_warehouse(db_path)
    saga_demo.POISON_BATCH_ID = "does-not-exist"

    result = saga_demo.run_batch_saga(db_path, "batch-1", _batch_rows(5))

    assert result["outcome"] == "success"
    assert housing_warehouse.rows_for_batch(db_path, "batch-1") == 5
    assert housing_warehouse.is_processed(db_path, "batch-1") is True


def test_poisoned_batch_rolls_back_both_steps(tmp_path):
    db_path = str(tmp_path / "housing.db")
    housing_warehouse.init_warehouse(db_path)
    saga_demo.POISON_BATCH_ID = "batch-poison"

    result = saga_demo.run_batch_saga(db_path, "batch-poison", _batch_rows(5))

    assert result["outcome"] == "rolled_back"
    assert result["failed_step"] == "trigger_refresh"
    assert set(result["compensated_steps"]) == {"load_fact_table", "mark_processed"}
    # The whole point: the compensations must have really run against the DB.
    assert housing_warehouse.rows_for_batch(db_path, "batch-poison") == 0
    assert housing_warehouse.is_processed(db_path, "batch-poison") is False


def test_poisoned_batch_does_not_affect_other_batches(tmp_path):
    db_path = str(tmp_path / "housing.db")
    housing_warehouse.init_warehouse(db_path)
    saga_demo.POISON_BATCH_ID = "batch-poison"

    saga_demo.run_batch_saga(db_path, "batch-ok", _batch_rows(3))
    saga_demo.run_batch_saga(db_path, "batch-poison", _batch_rows(5))

    assert housing_warehouse.rows_for_batch(db_path, "batch-ok") == 3
    assert housing_warehouse.is_processed(db_path, "batch-ok") is True
    assert housing_warehouse.rows_for_batch(db_path, "batch-poison") == 0
    assert housing_warehouse.total_row_count(db_path) == 3
