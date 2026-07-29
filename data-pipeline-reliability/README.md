# latch data-pipeline-reliability

A complete, reproducible proof of the `latch` idempotency-for-agentic-pipelines
thesis (see the parent repo's `CLAUDE.md` Mission section): a real public
dataset, a real SQLite warehouse, a real chaos-injected agent retry loop, a
real Airflow DAG, a real independent PySpark cross-check, a Saga/rollback
demo against a second real dataset, and a checker program written
independently of everything else that re-derives every number from raw
files instead of trusting a summary.

This folder is the evidence base behind `article/latch-agentic-data-pipeline-reliability.md`.
Every number in that article is backed by a file in `results/` or `data/`
in this folder, and every one of those numbers was independently
re-verified by `checker/verify_results.py` (see "Independent verification"
below) rather than only by the scripts that produced them.

## Directory layout

```
data-pipeline-reliability/
├── src/                      # pipeline code (extract, transform, warehouse, load tools, agent loop, saga demo)
├── spark/                    # independent PySpark cross-check of the pandas transform
├── airflow_dags/             # real Airflow 3.x TaskFlow DAG orchestrating the pipeline
├── benchmarks/               # the naive-vs-protected chaos benchmark (the "doer")
├── checker/                  # independent verification program (does NOT import src/ or benchmarks/)
├── tests/                    # pytest suite (28 tests)
├── data/                     # extracted public datasets + SQLite warehouse files + Spark output
├── results/                  # JSON results, logs, and the verification report
└── article/                  # the Simple Talk article, grounded in the above
```

## Data provenance (read this before trusting any number below)

The sandbox this was built in only had network egress to `pypi.org` /
`files.pythonhosted.org` / `github.com` -- no Socrata, no data.gov, no NYC/
Chicago open-data APIs, no `raw.githubusercontent.com`. Rather than
fabricate data, this uses two real, well-known, citable public datasets
that ship as static package data via the `pydataset` PyPI package:

- **`diamonds`** -- 53,940 real diamond sale records (carat, cut, color,
  clarity, price, dimensions), originally distributed with the `ggplot2` R
  package. Used as the primary "sales fact table" workload.
- **`Housing`** -- 546 real house-sale records from the City of Windsor
  (Anglin & Gençay, 1996). Used for the Saga/rollback demo, deliberately a
  second, independent dataset so that demo isn't reusing `diamonds` numbers.

Batch boundaries ("which rows belong to day N") are assigned by this
pipeline via simple sequential chunking, since neither source table has a
date column of its own -- that scheduling label is bookkeeping this
pipeline adds. Every value inside a batch (carat, cut, color, clarity,
price, house price, lot size, etc.) is an unmodified value from the public
dataset. `src/extract.py` records the SHA-256 of the extracted CSV, and
`checker/verify_results.py` independently re-hashes it.

## Reproducing the results yourself

```bash
cd data-pipeline-reliability
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 1. Extract the public data

```bash
python -m src.extract data/diamonds_raw.csv
# writes data/diamonds_raw.csv, data/diamonds_raw.meta.json, data/housing_raw.csv
```

### 2. Chunk it into batches (sanity check only)

```bash
python -m src.transform data/diamonds_raw.csv
```

### 3. Run the chaos benchmark (naive vs. protected)

```bash
python -m benchmarks.pipeline_chaos_benchmark --seed 1 --json results/benchmark_results.json
```

Because the naive/protected comparison races a genuine `threading.Thread`
against a genuine wall-clock deadline (not a mocked clock), the *exact*
duplicate count varies slightly run to run -- see the module docstring in
`benchmarks/pipeline_chaos_benchmark.py` and `benchmarks/aggregate_seeds.py`
for why, and the article's multi-seed table for the observed range across
5 independent seeds. Reproduce the full sweep with:

```bash
for seed in 1 2 3 4 5; do
  python -m benchmarks.pipeline_chaos_benchmark --seed $seed --json results/benchmark_seed${seed}.json
done
python -m benchmarks.aggregate_seeds "results/benchmark_seed*.json" --json results/multi_seed_summary.json
```

Per-seed warehouse files larger than the seed=1 canonical pair are shipped
gzip-compressed (`data/warehouse_*_seed{2,3,4,5}.db.gz`) to keep the repo a
reasonable size; `checker/verify_results.py` decompresses them
transparently. `gunzip` them yourself if you want to open one directly with
`sqlite3`.

### 4. Cross-check with Spark (independent of pandas)

```bash
python spark/transform_spark.py data/diamonds_raw.csv data/spark_aggregates.json
```

Requires a JRE. `pyspark==3.5.3` was used here (Java 11-compatible);
`pyspark>=4.0` requires Java 17. If your hostname doesn't resolve (a common
issue in minimal containers -- Spark's JVM calls
`InetAddress.getLocalHost()` at startup), the script sets
`SPARK_LOCAL_IP`/`SPARK_LOCAL_HOSTNAME`/`HOSTALIASES` itself so it runs
standalone without needing root to edit `/etc/hosts`.

### 5. Saga / rollback demo (second dataset, real DB rollback)

```bash
python -m src.saga_demo
```

### 6. Airflow DAG (real execution, not just a static parse)

```bash
pip install apache-airflow-core==3.3.0
export AIRFLOW_HOME=/tmp/airflow_home
airflow db migrate
mkdir -p $AIRFLOW_HOME/dags
cp airflow_dags/diamond_sales_load_dag.py $AIRFLOW_HOME/dags/
export PYTHONPATH=$(pwd):$PYTHONPATH
airflow dags test diamond_sales_load_pipeline 2026-01-01
```

`results/airflow_dag_test_run.log` is the captured output of exactly this
command from a real run; `data/warehouse_airflow_protected.db` is the
warehouse file it produced.

### 7. Tests

```bash
python -m pytest tests/ -q
```

28 tests: extraction integrity, batching correctness, the warehouse's raw
(non-deduplicating) insert semantics, the idempotency contract in
isolation (fast, deterministic, no chaos timing), the chaos-driven
integration path, the Saga rollback (including that a poisoned batch
doesn't affect its neighbors), and the Airflow DAG's structure.

### 8. Independent verification

```bash
python -m checker.verify_results --results-dir results --data-dir data
```

`checker/verify_results.py` is written independently of `src/`,
`benchmarks/`, and `spark/` -- it does not import any of their helper
functions. Every count it reports comes from `sqlite3`/`csv`/`hashlib`
calls written directly in that file against the raw files those other
scripts left behind, specifically so a bug in, say, `warehouse.py`'s own
counting logic can't accidentally "confirm" itself. It re-hashes and
re-counts the source CSVs, re-derives duplicate-batch counts from the
warehouse files for every seed, cross-checks the pandas/Spark/csv aggregate
agreement, re-verifies the Saga rollback against the raw tables, and
confirms the Airflow-produced warehouse independently. Current state:
**29 of 29 checks pass** (see `results/verification_report.json`).

## Known environment quirks encountered while building this

- **Network egress**: only `pypi.org`, `files.pythonhosted.org`, and
  `github.com` were reachable in the sandbox this was built in -- no
  `raw.githubusercontent.com`, no Socrata/data.gov APIs. This is why the
  datasets come from `pydataset` (static package data) rather than a live
  API pull.
- **Java version**: `pyspark>=4.0` requires Java 17; the sandbox had
  OpenJDK 11 and no root access to install a newer JDK, so `pyspark==3.5.3`
  is pinned instead.
- **Spark hostname resolution**: containerized/sandboxed hosts sometimes
  have a hostname that doesn't resolve via DNS or `/etc/hosts`, which
  crashes Spark's JVM at startup. `spark/transform_spark.py` sets
  `SPARK_LOCAL_IP` / `SPARK_LOCAL_HOSTNAME` / `HOSTALIASES` itself (the
  `HOSTALIASES` trick doesn't require root) so it's a non-issue for anyone
  running this on a normal machine.
- **SQLite over network-mounted filesystems**: writing the warehouse `.db`
  files to a host-mounted/FUSE directory produced `disk I/O error` in this
  sandbox; all pipeline runs write to a genuinely local filesystem path.

## Article

`article/latch-agentic-data-pipeline-reliability.md` is the write-up
prepared for Simple Talk, grounded entirely in the files in this folder --
every number in it traces back to a JSON file in `results/` or `data/`,
each independently re-verified by the checker above.
