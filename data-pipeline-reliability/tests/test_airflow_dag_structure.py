"""Static-structure test for the Airflow DAG file. Does not require a
running Airflow metadata DB (the full, actually-executed `airflow dags
test` run used to validate this DAG for real is captured separately in
results/airflow_dag_test_run.log -- see README "Reproducing the results").

This test only confirms the DAG module imports cleanly and declares the
four expected tasks wired in the expected order -- a regression guard
against someone breaking the DAG file without re-running Airflow by hand.
"""

import importlib.util
import sys
from pathlib import Path

DAG_PATH = Path(__file__).resolve().parents[1] / "airflow_dags" / "diamond_sales_load_dag.py"


def _import_dag_module():
    spec = importlib.util.spec_from_file_location("diamond_sales_load_dag", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["diamond_sales_load_dag"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_dag_module_imports_without_error():
    pytest = __import__("pytest")
    airflow_sdk = pytest.importorskip("airflow.sdk")
    del airflow_sdk
    module = _import_dag_module()
    assert hasattr(module, "diamond_sales_load_pipeline")


def test_dag_has_expected_tasks_in_order():
    pytest = __import__("pytest")
    pytest.importorskip("airflow.sdk")
    module = _import_dag_module()

    # airflow.sdk's @dag decorator, when called, returns a real DAG object
    # registered on the module via the function call at the bottom of the
    # file. Re-invoke the factory to get a fresh DAG object for inspection
    # (TaskFlow DAG factories are safe to call multiple times).
    dag_obj = module.diamond_sales_load_pipeline()
    task_ids = set(dag_obj.task_dict.keys())
    assert task_ids == {"extract_task", "transform_task", "load_task", "verify_task"}

    # extract -> transform -> load -> verify
    assert dag_obj.task_dict["transform_task"].upstream_task_ids == {"extract_task"}
    assert dag_obj.task_dict["load_task"].upstream_task_ids == {"transform_task"}
    assert dag_obj.task_dict["verify_task"].upstream_task_ids == {"transform_task", "load_task"}
