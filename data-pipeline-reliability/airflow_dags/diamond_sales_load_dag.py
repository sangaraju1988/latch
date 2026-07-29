"""Airflow 3.x TaskFlow DAG: extract -> transform -> load -> verify.

Orchestrates the exact same, already-tested pipeline modules used by
`benchmarks/pipeline_chaos_benchmark.py` (`src.extract`, `src.transform`,
`src.load_tools`, `src.agent_runner`, `src.warehouse`) -- this DAG is a
thin orchestration layer over that code, not a second implementation of it,
so there is only one place the actual pipeline logic lives.

The `load_task` uses the `@with_timeout` + `@idempotent`-protected load
path (see `src.load_tools.make_protected_load`) -- this is the
production-shaped DAG a real deployment would run; the naive/unprotected
comparison lives in the benchmark, not here.

Deliberately keeps XCom payloads small: `transform_task` does not push
53,940 rows of row data through XCom (a well-known Airflow anti-pattern).
It pushes back only the batching parameters; `load_task` re-derives the
identical batches by re-reading the same CSV with the same deterministic
`make_batches()` call.

Run with a real (locally installed) Airflow 3.x:
    export AIRFLOW_HOME=/tmp/airflow_home
    airflow db migrate
    airflow dags test diamond_sales_load_pipeline 2026-01-01
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pendulum  # noqa: E402
from airflow.sdk import dag, task  # noqa: E402

DATA_DIR = _REPO_ROOT / "data"
CSV_PATH = str(DATA_DIR / "diamonds_raw.csv")
DB_PATH = str(DATA_DIR / "warehouse_airflow_protected.db")


@dag(
    dag_id="diamond_sales_load_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["latch", "reliability", "data-pipeline-reliability"],
)
def diamond_sales_load_pipeline() -> None:
    @task
    def extract_task() -> str:
        from src import extract

        if not Path(CSV_PATH).exists():
            extract.extract(CSV_PATH)
        return CSV_PATH

    @task
    def transform_task(csv_path: str) -> dict:
        from src import transform

        _df, batches = transform.load_and_batch(csv_path)
        return {
            "csv_path": csv_path,
            "batch_size": transform.DEFAULT_BATCH_SIZE,
            "num_batches": len(batches),
            "total_rows": sum(len(b) for b in batches),
        }

    @task
    def load_task(transform_meta: dict) -> dict:
        from latch import InMemoryStore, Tracer

        from src import transform, warehouse
        from src.agent_runner import run_protected_load
        from src.load_tools import make_protected_load

        _df, batches = transform.load_and_batch(
            transform_meta["csv_path"], batch_size=transform_meta["batch_size"]
        )

        if Path(DB_PATH).exists():
            Path(DB_PATH).unlink()
        warehouse.init_warehouse(DB_PATH)

        store = InMemoryStore()
        tracer = Tracer()
        events: list = []
        tracer.subscribe(events.append)
        protected = make_protected_load(DB_PATH, seed=1, store=store, tracer=tracer)

        successes = 0
        for batch in batches:
            result = run_protected_load(protected, batch.batch_id, batch.rows)
            if result is not None:
                successes += 1

        cache_hits = sum(1 for e in events if e.primitive == "idempotent" and e.event == "cache_hit")
        return {
            "db_path": DB_PATH,
            "batches_attempted": len(batches),
            "batches_successful": successes,
            "idempotency_cache_hits": cache_hits,
        }

    @task
    def verify_task(transform_meta: dict, load_result: dict) -> dict:
        from src import warehouse

        db_path = load_result["db_path"]
        total_rows = warehouse.total_row_count(db_path)
        expected_rows = transform_meta["total_rows"]
        loads_per_batch = warehouse.loads_per_batch(db_path)
        duplicated = {b: n for b, n in loads_per_batch.items() if n > 1}

        assert total_rows == expected_rows, (
            f"Row count mismatch: warehouse has {total_rows}, expected {expected_rows}"
        )
        assert len(duplicated) == 0, f"Found physically duplicated batches: {duplicated}"

        return {
            "total_rows": total_rows,
            "expected_rows": expected_rows,
            "distinct_batches": warehouse.distinct_batch_count(db_path),
            "duplicated_batches": len(duplicated),
            "verified": True,
        }

    extracted_csv = extract_task()
    tmeta = transform_task(extracted_csv)
    lresult = load_task(tmeta)
    verify_task(tmeta, lresult)


diamond_sales_load_pipeline()
