#!/usr/bin/env python3
"""Saga / compensation demo against a second real public dataset (`Housing`
-- Windsor house sales prices, 546 rows, independent of `diamonds`).

Each batch runs a 3-step Saga: load the batch into `fact_housing_sales`,
mark a `control_batch_status` control table as processed, then call a
"trigger downstream refresh" step. One specific batch (`POISON_BATCH_ID`)
is deliberately made to fail at the third step, every run, deterministically
(not randomly) -- this demo is about proving rollback actually deletes real
rows from a real table, not about re-testing chaos-injected latency (that's
what benchmarks/pipeline_chaos_benchmark.py is for).

After running every batch, this script queries the two real SQLite tables
directly to confirm:
  - every non-poison batch has its rows present and is marked processed
  - the poison batch has ZERO rows in fact_housing_sales (the compensating
    DELETE actually ran) and is NOT marked processed (its compensation also
    ran) -- i.e. the Saga left the warehouse as if that batch never
    started, not half-applied.

Run directly:
    python -m src.saga_demo
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from latch import Saga, SagaExecutionError

from src import housing_warehouse, transform

POISON_BATCH_ID = None  # set at runtime to the 5th batch's id (see main())


def trigger_downstream_refresh(batch_id: str) -> Dict[str, Any]:
    if batch_id == POISON_BATCH_ID:
        raise RuntimeError(f"simulated downstream refresh outage for {batch_id}")
    return {"batch_id": batch_id, "status": "refreshed"}


def run_batch_saga(db_path: str, batch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    saga = Saga(name=f"housing-load-{batch_id}")
    saga.add_step(
        lambda: housing_warehouse.insert_housing_batch(db_path, batch_id, rows),
        name="load_fact_table",
        compensation=lambda result: housing_warehouse.delete_housing_batch(db_path, result["batch_id"]),
    )
    saga.add_step(
        lambda: (housing_warehouse.mark_processed(db_path, batch_id), {"batch_id": batch_id})[1],
        name="mark_processed",
        compensation=lambda _: housing_warehouse.mark_unprocessed(db_path, batch_id),
    )
    saga.add_step(
        lambda: trigger_downstream_refresh(batch_id),
        name="trigger_refresh",
    )
    try:
        saga.run()
        return {"batch_id": batch_id, "outcome": "success"}
    except SagaExecutionError as exc:
        return {
            "batch_id": batch_id,
            "outcome": "rolled_back",
            "failed_step": exc.step_name,
            "compensated_steps": exc.compensated_steps,
            "compensation_errors": [str(e) for e in exc.compensation_errors],
            "original_exception": str(exc.original_exception),
        }


def main() -> int:
    global POISON_BATCH_ID

    data_dir = Path("data")
    housing_csv = data_dir / "housing_raw.csv"
    if not housing_csv.exists():
        from src import extract

        extract.extract_housing(str(housing_csv))

    _df, batches = transform.load_and_batch(str(housing_csv), batch_size=50, prefix="housing")
    POISON_BATCH_ID = batches[4].batch_id  # 5th batch, arbitrary but fixed

    db_path = str(data_dir / "warehouse_housing_saga.db")
    if Path(db_path).exists():
        Path(db_path).unlink()
    housing_warehouse.init_warehouse(db_path)

    per_batch_results = []
    for batch in batches:
        outcome = run_batch_saga(db_path, batch.batch_id, batch.rows)
        outcome["expected_rows"] = len(batch)
        per_batch_results.append(outcome)

    # Independent verification against the real tables (not the saga's own
    # return values) -- same "don't trust the doer" principle as
    # checker/verify_results.py, applied inline here too.
    checks = []
    total_expected_surviving_rows = 0
    for batch, outcome in zip(batches, per_batch_results):
        actual_rows = housing_warehouse.rows_for_batch(db_path, batch.batch_id)
        actual_processed = housing_warehouse.is_processed(db_path, batch.batch_id)
        if batch.batch_id == POISON_BATCH_ID:
            ok = actual_rows == 0 and not actual_processed and outcome["outcome"] == "rolled_back"
        else:
            ok = actual_rows == len(batch) and actual_processed and outcome["outcome"] == "success"
            total_expected_surviving_rows += len(batch)
        checks.append(
            {
                "batch_id": batch.batch_id,
                "is_poison_batch": batch.batch_id == POISON_BATCH_ID,
                "outcome": outcome["outcome"],
                "rows_in_fact_table": actual_rows,
                "marked_processed": actual_processed,
                "check_passed": ok,
            }
        )

    actual_total_rows = housing_warehouse.total_row_count(db_path)
    all_checks_passed = all(c["check_passed"] for c in checks) and (
        actual_total_rows == total_expected_surviving_rows
    )

    summary = {
        "source_dataset": "Housing (Windsor house sales, via pydataset)",
        "db_path": db_path,
        "poison_batch_id": POISON_BATCH_ID,
        "num_batches": len(batches),
        "per_batch_saga_outcomes": per_batch_results,
        "post_run_db_checks": checks,
        "actual_total_rows_in_fact_table": actual_total_rows,
        "expected_total_rows_excluding_poison_batch": total_expected_surviving_rows,
        "all_checks_passed": all_checks_passed,
    }

    Path("results").mkdir(exist_ok=True)
    with open("results/saga_demo_results.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    if not all_checks_passed:
        print("\nSAGA DEMO FAILED VERIFICATION", file=sys.stderr)
        return 1
    print(f"\nAll {len(checks)} batch-level checks passed. Poison batch {POISON_BATCH_ID} "
          f"was fully rolled back (0 rows, unprocessed); every other batch committed in full.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
