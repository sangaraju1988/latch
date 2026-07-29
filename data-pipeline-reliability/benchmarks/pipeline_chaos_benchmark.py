#!/usr/bin/env python3
"""Chaos-injection benchmark: naive vs. latch-protected agentic data
pipeline, driven by a real public dataset and writing to real SQLite
warehouse files.

This is the "doer" script. It does NOT grade its own output for the
article -- `checker/verify_results.py` is a separate, independently written
program that re-derives every number below directly from the raw SQLite
files and the raw source CSV, and fails loudly if anything here was wrong.

Usage:
    python -m benchmarks.pipeline_chaos_benchmark --seed 1
    python -m benchmarks.pipeline_chaos_benchmark --seed 1 --json results/benchmark_results.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import extract, transform, warehouse  # noqa: E402
from src.agent_runner import run_naive_load, run_protected_load  # noqa: E402
from src.load_tools import (  # noqa: E402
    CLIENT_TIMEOUT_SECONDS,
    RAW_LATENCY_JITTER_SECONDS,
    RETRIES_PER_BATCH,
    RETRY_DELAY_SECONDS,
    make_protected_load,
    make_raw_load,
)
from latch import InMemoryStore, Tracer  # noqa: E402


@dataclass
class RunResult:
    label: str
    db_path: str
    batches_attempted: int
    batches_reported_successful: int
    batches_reported_failed: int
    total_rows_in_warehouse: int
    expected_rows: int
    distinct_batches_in_warehouse: int
    batches_with_duplicate_physical_inserts: int
    total_duplicate_physical_inserts: int
    wall_clock_seconds: float
    idempotency_cache_hits: int = 0
    loads_per_batch: Dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        return d


def run_naive(db_path: str, batches: List[transform.Batch], seed: int) -> RunResult:
    if Path(db_path).exists():
        Path(db_path).unlink()
    warehouse.init_warehouse(db_path)
    raw = make_raw_load(db_path, seed=seed)

    start = time.monotonic()
    successes = 0
    for batch in batches:
        result = run_naive_load(raw, batch.batch_id, batch.rows)
        if result is not None:
            successes += 1
    # Let any still-in-flight abandoned background threads from the final
    # retries finish before we measure the warehouse's actual contents.
    time.sleep(RAW_LATENCY_JITTER_SECONDS + RETRY_DELAY_SECONDS + 0.2)
    elapsed = time.monotonic() - start

    lpb = warehouse.loads_per_batch(db_path)
    dup_batches = {b: n for b, n in lpb.items() if n > 1}
    expected_rows = sum(len(b) for b in batches)
    return RunResult(
        label="naive (no latch)",
        db_path=db_path,
        batches_attempted=len(batches),
        batches_reported_successful=successes,
        batches_reported_failed=len(batches) - successes,
        total_rows_in_warehouse=warehouse.total_row_count(db_path),
        expected_rows=expected_rows,
        distinct_batches_in_warehouse=warehouse.distinct_batch_count(db_path),
        batches_with_duplicate_physical_inserts=len(dup_batches),
        total_duplicate_physical_inserts=sum(n - 1 for n in lpb.values()),
        wall_clock_seconds=elapsed,
        loads_per_batch=lpb,
    )


def run_protected(db_path: str, batches: List[transform.Batch], seed: int) -> RunResult:
    if Path(db_path).exists():
        Path(db_path).unlink()
    warehouse.init_warehouse(db_path)
    store = InMemoryStore()
    tracer = Tracer()
    events: List[Any] = []
    tracer.subscribe(events.append)
    protected = make_protected_load(db_path, seed=seed, store=store, tracer=tracer)

    start = time.monotonic()
    successes = 0
    for batch in batches:
        result = run_protected_load(protected, batch.batch_id, batch.rows)
        if result is not None:
            successes += 1
    time.sleep(RAW_LATENCY_JITTER_SECONDS + RETRY_DELAY_SECONDS + 0.2)
    elapsed = time.monotonic() - start

    lpb = warehouse.loads_per_batch(db_path)
    dup_batches = {b: n for b, n in lpb.items() if n > 1}
    expected_rows = sum(len(b) for b in batches)
    cache_hits = sum(1 for e in events if e.primitive == "idempotent" and e.event == "cache_hit")
    return RunResult(
        label="protected (with_timeout + idempotent)",
        db_path=db_path,
        batches_attempted=len(batches),
        batches_reported_successful=successes,
        batches_reported_failed=len(batches) - successes,
        total_rows_in_warehouse=warehouse.total_row_count(db_path),
        expected_rows=expected_rows,
        distinct_batches_in_warehouse=warehouse.distinct_batch_count(db_path),
        batches_with_duplicate_physical_inserts=len(dup_batches),
        total_duplicate_physical_inserts=sum(n - 1 for n in lpb.values()),
        wall_clock_seconds=elapsed,
        idempotency_cache_hits=cache_hits,
        loads_per_batch=lpb,
    )


def print_comparison(naive: RunResult, protected: RunResult) -> None:
    rows = [
        ("batches attempted", naive.batches_attempted, protected.batches_attempted),
        ("batches reported successful", naive.batches_reported_successful, protected.batches_reported_successful),
        ("batches reported failed", naive.batches_reported_failed, protected.batches_reported_failed),
        ("expected rows (source of truth)", naive.expected_rows, protected.expected_rows),
        ("actual rows in warehouse (SQL COUNT)", naive.total_rows_in_warehouse, protected.total_rows_in_warehouse),
        ("distinct batch_ids in warehouse", naive.distinct_batches_in_warehouse, protected.distinct_batches_in_warehouse),
        ("batches physically inserted >1 time", naive.batches_with_duplicate_physical_inserts, protected.batches_with_duplicate_physical_inserts),
        ("total duplicate physical inserts", naive.total_duplicate_physical_inserts, protected.total_duplicate_physical_inserts),
        ("idempotency cache hits", "n/a", protected.idempotency_cache_hits),
    ]
    label_w = max(len(r[0]) for r in rows) + 2
    col_w = 12
    print()
    print(f"{'metric':<{label_w}}{'naive':>{col_w}}{'protected':>{col_w}}")
    print("-" * (label_w + col_w * 2))
    for label, n, p in rows:
        print(f"{label:<{label_w}}{str(n):>{col_w}}{str(p):>{col_w}}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--results-dir", type=str, default="results")
    parser.add_argument("--batch-size", type=int, default=transform.DEFAULT_BATCH_SIZE)
    parser.add_argument("--json", type=str, default=None)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    raw_csv = data_dir / "diamonds_raw.csv"
    if not raw_csv.exists():
        print(f"Extracting public `diamonds` dataset to {raw_csv} ...")
        extract.extract(str(raw_csv))
    source_sha256 = extract.sha256_of_file(str(raw_csv))
    source_row_count = sum(1 for _ in open(raw_csv)) - 1  # minus header

    df, batches = transform.load_and_batch(str(raw_csv), batch_size=args.batch_size)
    print(f"Source: {raw_csv} ({source_row_count} rows, sha256={source_sha256})")
    print(f"Batched into {len(batches)} batches (seed={args.seed})")

    naive_db = str(data_dir / f"warehouse_naive_seed{args.seed}.db")
    protected_db = str(data_dir / f"warehouse_protected_seed{args.seed}.db")

    print("\n--- Running NAIVE pipeline (no latch) ---")
    naive = run_naive(naive_db, batches, seed=args.seed)

    print("--- Running PROTECTED pipeline (@with_timeout + @idempotent) ---")
    protected = run_protected(protected_db, batches, seed=args.seed)

    print_comparison(naive, protected)

    payload = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seed": args.seed,
        "source_dataset": "diamonds (ggplot2, via pydataset PyPI package)",
        "source_csv_path": str(raw_csv),
        "source_csv_sha256": source_sha256,
        "source_row_count": source_row_count,
        "num_batches": len(batches),
        "batch_size": args.batch_size,
        "constants": {
            "raw_latency_jitter_seconds": RAW_LATENCY_JITTER_SECONDS,
            "client_timeout_seconds": CLIENT_TIMEOUT_SECONDS,
            "retry_delay_seconds": RETRY_DELAY_SECONDS,
            "retries_per_batch": RETRIES_PER_BATCH,
        },
        "naive": naive.as_dict(),
        "protected": protected.as_dict(),
    }

    json_path = args.json or str(results_dir / "benchmark_results.json")
    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote results to {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
