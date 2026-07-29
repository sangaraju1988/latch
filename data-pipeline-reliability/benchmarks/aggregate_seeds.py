#!/usr/bin/env python3
"""Aggregate several single-seed `pipeline_chaos_benchmark.py` runs into one
summary, so the article's headline numbers are backed by a distribution
across seeds rather than one possibly-lucky run.

The real-thread-scheduling nature of this benchmark (a genuine
`threading.Thread` racing a genuine wall-clock deadline, not a mocked
clock) means the *exact* count of naive duplicate inserts varies slightly
run to run -- real OS thread scheduling near a tight 50ms timeout boundary
is not perfectly reproducible even with a seeded latency generator. This
script exists to show that the *qualitative* finding is not run-to-run
noise: across every seed tested, the protected pipeline writes exactly the
expected row count with zero duplicates, and the naive pipeline
over-writes the warehouse substantially every time.

Usage:
    python -m benchmarks.aggregate_seeds results/benchmark_seed*.json --json results/multi_seed_summary.json
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from typing import Any, Dict, List


def load_runs(paths: List[str]) -> List[Dict[str, Any]]:
    runs = []
    for p in sorted(paths):
        with open(p) as f:
            runs.append(json.load(f))
    return runs


def summarize(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    naive_dup_batches = [r["naive"]["batches_with_duplicate_physical_inserts"] for r in runs]
    naive_dup_inserts = [r["naive"]["total_duplicate_physical_inserts"] for r in runs]
    naive_rows = [r["naive"]["total_rows_in_warehouse"] for r in runs]
    naive_expected = [r["naive"]["expected_rows"] for r in runs]
    protected_dup_batches = [r["protected"]["batches_with_duplicate_physical_inserts"] for r in runs]
    protected_rows = [r["protected"]["total_rows_in_warehouse"] for r in runs]
    protected_expected = [r["protected"]["expected_rows"] for r in runs]

    return {
        "num_runs": len(runs),
        "seeds": [r["seed"] for r in runs],
        "naive": {
            "batches_with_duplicate_physical_inserts": {
                "min": min(naive_dup_batches),
                "max": max(naive_dup_batches),
                "mean": sum(naive_dup_batches) / len(naive_dup_batches),
                "all_runs": naive_dup_batches,
            },
            "total_duplicate_physical_inserts": {
                "min": min(naive_dup_inserts),
                "max": max(naive_dup_inserts),
                "mean": sum(naive_dup_inserts) / len(naive_dup_inserts),
                "all_runs": naive_dup_inserts,
            },
            "rows_vs_expected_all_runs": list(zip(naive_rows, naive_expected)),
            "every_run_over_wrote_the_warehouse": all(a > b for a, b in zip(naive_rows, naive_expected)),
            "every_run_had_at_least_one_duplicated_batch": all(n > 0 for n in naive_dup_batches),
        },
        "protected": {
            "batches_with_duplicate_physical_inserts": {
                "min": min(protected_dup_batches),
                "max": max(protected_dup_batches),
                "all_runs": protected_dup_batches,
            },
            "rows_vs_expected_all_runs": list(zip(protected_rows, protected_expected)),
            "every_run_matched_expected_rows_exactly": all(a == b for a, b in zip(protected_rows, protected_expected)),
            "every_run_had_zero_duplicated_batches": all(n == 0 for n in protected_dup_batches),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", help="glob(s) of per-seed benchmark JSON files")
    parser.add_argument("--json", type=str, default="results/multi_seed_summary.json")
    args = parser.parse_args()

    paths: List[str] = []
    for pattern in args.inputs:
        paths.extend(glob.glob(pattern))
    if not paths:
        print("No input files matched.", file=sys.stderr)
        return 1

    runs = load_runs(paths)
    summary = summarize(runs)
    print(json.dumps(summary, indent=2))
    with open(args.json, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nWrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
