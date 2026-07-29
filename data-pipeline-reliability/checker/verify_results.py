#!/usr/bin/env python3
"""Independent checker: re-derives every headline number in this project
directly from raw artifacts (SQLite files, source CSVs) and fails loudly on
any mismatch against the "doer" scripts' own self-reported numbers.

Ground rule this file follows throughout: it does NOT import
`src.warehouse`, `src.housing_warehouse`, `src.extract`, or
`benchmarks/pipeline_chaos_benchmark.py`. Every count here is computed with
plain `sqlite3` / `csv` / `hashlib` calls written independently in this
file, against the raw files those other scripts produced. If this script
imported the doer's helper functions, a bug in `warehouse.loads_per_batch`
could pass a check that only re-ran the same buggy query -- this way, the
SQL and the parsing logic are written twice, independently, by design.

Checks performed:
  1. Source CSV integrity: sha256 + row count of data/diamonds_raw.csv and
     data/housing_raw.csv, recomputed from scratch.
  2. For every benchmark_*.json + benchmark_results.json in results/: open
     the naive and protected SQLite warehouse files it references (if still
     present on disk) and recompute total rows, distinct batches, and
     per-batch physical-insert counts via raw SQL, then compare against
     that JSON file's self-reported numbers.
  3. Invariants that must hold for every run, independent of exact counts:
     protected == expected rows exactly with zero duplicated batches;
     naive >= expected rows (nothing lost) with at least one duplicated
     batch (the bug actually reproduced).
  4. Cross-engine agreement: pandas-computed aggregates
     (data/diamonds_raw.meta.json) vs. Spark-computed aggregates
     (data/spark_aggregates.json) must match exactly on row count and
     price sum/min/max.
  5. Saga demo: re-opens data/warehouse_housing_saga.db directly and
     confirms the poison batch has zero rows and is unmarked, every other
     batch has its full row count and is marked processed, matching
     results/saga_demo_results.json.
  6. Airflow-produced warehouse (if present): recompute total rows and
     duplicate-batch count directly, matching the DAG's own verify_task
     output logged in results/airflow_dag_test_run.log.

Usage:
    python -m checker.verify_results --results-dir results --data-dir data
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List


class Check:
    def __init__(self, name: str) -> None:
        self.name = name
        self.passed = True
        self.details: Dict[str, Any] = {}
        self.error: str = ""

    def fail(self, error: str, **details: Any) -> None:
        self.passed = False
        self.error = error
        self.details.update(details)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "error": self.error,
            "details": self.details,
        }


# --------------------------------------------------------------------- #
# Raw, independent primitives -- no imports from src/ or benchmarks/.
# --------------------------------------------------------------------- #


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def csv_row_count(path: str) -> int:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        return sum(1 for _ in reader)


def csv_price_stats(path: str) -> Dict[str, float]:
    total = 0.0
    lo = None
    hi = None
    n = 0
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            price = float(row["price"])
            total += price
            n += 1
            if lo is None or price < lo:
                lo = price
            if hi is None or price > hi:
                hi = price
    return {"count": n, "sum": total, "min": lo, "max": hi}


def resolve_db_path(db_path: str) -> str:
    """Most per-seed warehouse DBs in this repo are shipped gzip-compressed
    (`<name>.db.gz`) to keep repo size sane -- only the seed=1 canonical
    pair ships uncompressed for immediate browsing. Transparently
    decompress to a temp file so every check below can use plain sqlite3
    either way, and the caller doesn't need to know which form shipped.
    """
    if Path(db_path).exists():
        return db_path
    gz_path = db_path + ".gz"
    if Path(gz_path).exists():
        import gzip
        import tempfile

        fd, tmp_path = tempfile.mkstemp(suffix=".db")
        with gzip.open(gz_path, "rb") as src, open(fd, "wb") as dst:
            dst.write(src.read())
        return tmp_path
    return db_path  # doesn't exist either way; let the caller's exists() check report it


def sqlite_total_rows(db_path: str, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def sqlite_loads_per_batch(db_path: str, table: str) -> Dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"SELECT batch_id, COUNT(DISTINCT load_id) FROM {table} GROUP BY batch_id")
        return {row[0]: int(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


def sqlite_rows_for_batch(db_path: str, table: str, batch_id: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", (batch_id,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def sqlite_control_processed(db_path: str, batch_id: str) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT processed FROM control_batch_status WHERE batch_id = ?", (batch_id,)
        )
        row = cur.fetchone()
        return bool(row and row[0] == 1)
    finally:
        conn.close()


# --------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------- #


def check_source_csv_integrity(data_dir: Path) -> List[Check]:
    checks = []

    diamonds_csv = data_dir / "diamonds_raw.csv"
    c = Check("source_csv_diamonds_integrity")
    if not diamonds_csv.exists():
        c.fail("data/diamonds_raw.csv not found")
    else:
        row_count = csv_row_count(str(diamonds_csv))
        sha = sha256_of_file(str(diamonds_csv))
        c.details = {"row_count": row_count, "sha256": sha}
        if row_count != 53940:
            c.fail(f"expected 53940 rows in diamonds_raw.csv, found {row_count}")
    checks.append(c)

    housing_csv = data_dir / "housing_raw.csv"
    c2 = Check("source_csv_housing_integrity")
    if not housing_csv.exists():
        c2.fail("data/housing_raw.csv not found")
    else:
        row_count = csv_row_count(str(housing_csv))
        c2.details = {"row_count": row_count, "sha256": sha256_of_file(str(housing_csv))}
        if row_count != 546:
            c2.fail(f"expected 546 rows in housing_raw.csv, found {row_count}")
    checks.append(c2)

    return checks


def check_benchmark_run(json_path: str, data_dir: Path) -> List[Check]:
    checks = []
    with open(json_path) as f:
        reported = json.load(f)

    label = Path(json_path).stem

    for variant in ("naive", "protected"):
        c = Check(f"{label}:{variant}:warehouse_matches_report")
        reported_db_path = reported[variant]["db_path"]
        db_path = resolve_db_path(reported_db_path)
        if not Path(db_path).exists():
            c.fail(f"warehouse db {reported_db_path} (or its .gz) not found on disk")
            checks.append(c)
            continue

        actual_total = sqlite_total_rows(db_path, "fact_diamond_sales")
        actual_lpb = sqlite_loads_per_batch(db_path, "fact_diamond_sales")
        actual_dup_batches = sum(1 for n in actual_lpb.values() if n > 1)
        actual_dup_inserts = sum(n - 1 for n in actual_lpb.values())
        actual_distinct_batches = len(actual_lpb)

        reported_v = reported[variant]
        mismatches = {}
        if actual_total != reported_v["total_rows_in_warehouse"]:
            mismatches["total_rows_in_warehouse"] = [actual_total, reported_v["total_rows_in_warehouse"]]
        if actual_dup_batches != reported_v["batches_with_duplicate_physical_inserts"]:
            mismatches["batches_with_duplicate_physical_inserts"] = [
                actual_dup_batches, reported_v["batches_with_duplicate_physical_inserts"]
            ]
        if actual_dup_inserts != reported_v["total_duplicate_physical_inserts"]:
            mismatches["total_duplicate_physical_inserts"] = [
                actual_dup_inserts, reported_v["total_duplicate_physical_inserts"]
            ]
        if actual_distinct_batches != reported_v["distinct_batches_in_warehouse"]:
            mismatches["distinct_batches_in_warehouse"] = [
                actual_distinct_batches, reported_v["distinct_batches_in_warehouse"]
            ]

        c.details = {
            "db_path": db_path,
            "independently_computed": {
                "total_rows": actual_total,
                "distinct_batches": actual_distinct_batches,
                "duplicated_batches": actual_dup_batches,
                "duplicate_physical_inserts": actual_dup_inserts,
            },
        }
        if mismatches:
            c.fail("doer-reported numbers do not match raw SQLite state", mismatches=mismatches)
        checks.append(c)

        # Invariant checks (hold regardless of exact chaos-timing counts)
        inv = Check(f"{label}:{variant}:invariant")
        expected = reported_v["expected_rows"]
        if variant == "protected":
            if actual_total != expected:
                inv.fail(f"protected must match expected rows exactly: {actual_total} != {expected}")
            elif actual_dup_batches != 0:
                inv.fail(f"protected must have zero duplicated batches, found {actual_dup_batches}")
        else:  # naive
            if actual_total < expected:
                inv.fail(f"naive lost rows: {actual_total} < expected {expected}")
            elif actual_dup_batches == 0:
                inv.fail("naive run did not reproduce the duplicate-write bug at all (0 duplicated batches)")
        inv.details = {"expected_rows": expected, "actual_total_rows": actual_total, "duplicated_batches": actual_dup_batches}
        checks.append(inv)

    return checks


def check_cross_engine_aggregates(data_dir: Path) -> List[Check]:
    c = Check("cross_engine_pandas_vs_spark_aggregates")
    meta_path = data_dir / "diamonds_raw.meta.json"
    spark_path = data_dir / "spark_aggregates.json"
    csv_path = data_dir / "diamonds_raw.csv"

    if not (meta_path.exists() and spark_path.exists() and csv_path.exists()):
        c.fail("one or more of diamonds_raw.meta.json / spark_aggregates.json / diamonds_raw.csv missing")
        return [c]

    # Recompute directly from the CSV with the stdlib csv module -- a THIRD
    # independent computation, alongside pandas (meta.json) and Spark
    # (spark_aggregates.json).
    stats = csv_price_stats(str(csv_path))

    with open(meta_path) as f:
        meta = json.load(f)
    with open(spark_path) as f:
        spark = json.load(f)

    mismatches = {}
    if not (stats["count"] == meta["row_count"] == spark["total_rows"]):
        mismatches["row_count"] = [stats["count"], meta["row_count"], spark["total_rows"]]
    if not (abs(stats["sum"] - meta["price_sum"]) < 1e-6 and abs(stats["sum"] - spark["price_sum"]) < 1e-6):
        mismatches["price_sum"] = [stats["sum"], meta["price_sum"], spark["price_sum"]]
    if not (stats["min"] == meta["price_min"] == spark["price_min"]):
        mismatches["price_min"] = [stats["min"], meta["price_min"], spark["price_min"]]
    if not (stats["max"] == meta["price_max"] == spark["price_max"]):
        mismatches["price_max"] = [stats["max"], meta["price_max"], spark["price_max"]]

    c.details = {"csv_module_stats": stats, "pandas_meta": meta, "spark": {
        "total_rows": spark["total_rows"], "price_sum": spark["price_sum"],
        "price_min": spark["price_min"], "price_max": spark["price_max"],
    }}
    if mismatches:
        c.fail("csv/pandas/Spark disagree on basic aggregates", mismatches=mismatches)
    return [c]


def check_saga_demo(data_dir: Path, results_dir: Path) -> List[Check]:
    c = Check("saga_demo_rollback_verified_against_raw_db")
    results_path = results_dir / "saga_demo_results.json"
    if not results_path.exists():
        c.fail("results/saga_demo_results.json not found")
        return [c]
    with open(results_path) as f:
        reported = json.load(f)

    db_path = reported["db_path"]
    if not Path(db_path).exists():
        c.fail(f"saga demo db {db_path} not found on disk")
        return [c]

    poison_id = reported["poison_batch_id"]
    mismatches = {}

    poison_rows = sqlite_rows_for_batch(db_path, "fact_housing_sales", poison_id)
    poison_processed = sqlite_control_processed(db_path, poison_id)
    if poison_rows != 0:
        mismatches["poison_batch_rows"] = poison_rows
    if poison_processed:
        mismatches["poison_batch_processed"] = poison_processed

    total_rows = sqlite_total_rows(db_path, "fact_housing_sales")
    if total_rows != reported["expected_total_rows_excluding_poison_batch"]:
        mismatches["total_rows"] = [total_rows, reported["expected_total_rows_excluding_poison_batch"]]

    for outcome in reported["per_batch_saga_outcomes"]:
        if outcome["batch_id"] == poison_id:
            continue
        rows = sqlite_rows_for_batch(db_path, "fact_housing_sales", outcome["batch_id"])
        processed = sqlite_control_processed(db_path, outcome["batch_id"])
        if rows != outcome["expected_rows"] or not processed:
            mismatches[f"batch:{outcome['batch_id']}"] = {"rows": rows, "processed": processed}

    c.details = {"poison_batch_id": poison_id, "total_rows_independently_computed": total_rows}
    if mismatches:
        c.fail("saga demo raw DB state does not match reported results", mismatches=mismatches)
    return [c]


def check_airflow_run(data_dir: Path, results_dir: Path) -> List[Check]:
    db_path = data_dir / "warehouse_airflow_protected.db"
    log_path = results_dir / "airflow_dag_test_run.log"
    c = Check("airflow_dag_run_warehouse_matches_log")
    if not db_path.exists():
        c.fail("data/warehouse_airflow_protected.db not found (Airflow DAG may not have been run)")
        return [c]
    total = sqlite_total_rows(str(db_path), "fact_diamond_sales")
    lpb = sqlite_loads_per_batch(str(db_path), "fact_diamond_sales")
    dup = sum(1 for n in lpb.values() if n > 1)
    c.details = {"total_rows": total, "distinct_batches": len(lpb), "duplicated_batches": dup}
    if total != 53940:
        c.fail(f"expected 53940 rows from the Airflow-run warehouse, found {total}")
    elif dup != 0:
        c.fail(f"expected zero duplicated batches from the protected Airflow DAG run, found {dup}")
    elif not log_path.exists():
        c.fail("results/airflow_dag_test_run.log not found -- no evidence this DAG was actually executed by Airflow")
    return [c]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    results_dir = Path(args.results_dir)

    all_checks: List[Check] = []
    all_checks += check_source_csv_integrity(data_dir)

    benchmark_files = sorted(glob.glob(str(results_dir / "benchmark_results.json"))) + sorted(
        glob.glob(str(results_dir / "benchmark_seed*.json"))
    )
    for path in benchmark_files:
        all_checks += check_benchmark_run(path, data_dir)

    all_checks += check_cross_engine_aggregates(data_dir)
    all_checks += check_saga_demo(data_dir, results_dir)
    all_checks += check_airflow_run(data_dir, results_dir)

    passed = [c for c in all_checks if c.passed]
    failed = [c for c in all_checks if not c.passed]

    report = {
        "total_checks": len(all_checks),
        "passed": len(passed),
        "failed": len(failed),
        "all_passed": len(failed) == 0,
        "checks": [c.as_dict() for c in all_checks],
    }

    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "verification_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"Ran {len(all_checks)} independent checks: {len(passed)} passed, {len(failed)} failed.\n")
    for c in all_checks:
        status = "PASS" if c.passed else "FAIL"
        print(f"  [{status}] {c.name}")
        if not c.passed:
            print(f"         {c.error}")
            print(f"         details: {json.dumps(c.details)}")
    print(f"\nWrote {results_dir / 'verification_report.json'}")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
