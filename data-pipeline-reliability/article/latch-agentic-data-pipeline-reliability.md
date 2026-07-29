# When Your AI Agent Retries a Data Load Twice: Idempotency for Agentic Data Pipelines

A few months ago I watched an agent quietly duplicate a day's worth of sales data in a warehouse table. Nothing crashed. No error was logged anywhere obvious. The pipeline reported success. The only sign anything was wrong was a downstream dashboard where yesterday's revenue looked suspiciously good.

Here's what happened. An LLM-orchestrated pipeline step called a tool function to load a batch of sales rows into a fact table. The call took a little longer than usual — nothing dramatic, just a slow network blip — and the orchestration layer's client-side timeout fired before the response came back. From the agent's point of view, the call had failed, so it did the only reasonable thing an agent can do: it retried. The problem is that the first call hadn't actually failed. It had completed, just slowly. The retry ran the same load logic again, against the same batch, and the table ended up with two copies of every row.

This is not a new failure mode. Anyone who has built a distributed system has run into "at-least-once delivery" and the duplicate-processing problems that come with it. What's new is how often this shape of bug now shows up in data pipelines specifically, because more of those pipelines have an LLM agent deciding when and how to call the next step. Agents don't retry because someone wrote bad retry logic — they retry because retrying on ambiguous failure is the correct general strategy for an autonomous loop that can't tell "the call failed" apart from "the call succeeded but the response got lost." That ambiguity is fine for read-only operations. It is not fine for anything that writes.

Rather than describe this failure abstractly, this article reproduces it end to end: a real public dataset, a real SQLite warehouse table, a real chaos-injected retry loop, a real Airflow DAG, and a checker program — written independently of the code that produces the results — that re-derives every number below directly from the raw database files rather than trusting a summary. Everything here, including the exact commands, is in the `data-pipeline-reliability/` folder of the [`latch`](https://pypi.org/project/latch-idempotent/) GitHub repository, so you can run it yourself and get the same shape of result.

## The setup: a real dataset standing in for sales data

The scenario needs a real "load batch of sales rows into a fact table" workload, so this uses the `diamonds` dataset — 53,940 real diamond sale records (carat, cut, color, clarity, price, and physical dimensions), originally distributed with the `ggplot2` R package and made available to Python via the `pydataset` package (`pip install pydataset`). It's one of the most widely used public reference tables in data-science tooling: genuinely public, genuinely real values, not synthetic. Each row is treated as a diamond sale loaded into a `fact_diamond_sales` table.

The dataset has no date column of its own, so the pipeline assigns batch boundaries itself: 53,940 rows chunked sequentially into 40 batches of ~1,349 rows each, labeled `diamonds-2026-01-01` through `diamonds-2026-02-09` — 40 simulated daily loads. That scheduling label is bookkeeping this pipeline adds; every value inside a batch is an unmodified value from the public dataset. A second, independent public dataset — `Housing`, 546 real house-sale records from the City of Windsor (Anglin & Gençay, 1996), also via `pydataset` — is used later for the multi-step Saga demo, so the two demonstrations aren't quietly reusing the same numbers twice.

The warehouse is a real SQLite database file, not an in-memory counter. Every physical `INSERT` is tagged with a fresh `load_id` (a UUID minted at call time), so after a run, a plain `SELECT batch_id, COUNT(DISTINCT load_id) FROM fact_diamond_sales GROUP BY batch_id` tells you, per batch, how many times that batch was physically written — a ground-truth number that doesn't depend on any bookkeeping done by `latch` or by the benchmark script itself.

## Reproducing the bug, for real

The naive `load_sales_batch` tool is exactly as bare as the article's opening scenario suggests:

```python
def load_sales_batch(batch_id: str, rows: list[dict]) -> dict:
    # a genuine INSERT into a real fact_diamond_sales SQLite table
    return warehouse.insert_batch(db_path, batch_id, rows)
```

The simulated agent loop around it is a hand-rolled client-side timeout, the way a lot of real agent orchestration code looks: launch the call in a background thread, wait up to `CLIENT_TIMEOUT_SECONDS`, and if it hasn't returned, treat it as failed and retry.

```python
def run_naive_load(load_fn, batch_id, rows):
    for _attempt in range(RETRIES_PER_BATCH):
        result_holder = {}
        thread = threading.Thread(target=lambda: result_holder.update(
            result=load_fn(batch_id, rows)
        ), daemon=True)
        thread.start()
        thread.join(timeout=CLIENT_TIMEOUT_SECONDS)
        if thread.is_alive():
            time.sleep(RETRY_DELAY_SECONDS)
            continue  # the thread above is still running and will still write
        return result_holder.get("result")
    return None
```

Latency is injected with `latch.chaos` — a seeded, reproducible random jitter on every call (0 to 150ms), standing in for an ordinary slow network blip, with a 50ms client timeout that some calls beat and most don't. This is 0% hard-failure injection; the only "failure" is the client giving up before a call that is, in fact, still going to succeed.

Running all 40 batches through this loop and then querying the actual SQLite file:

```
metric                                       naive   protected
--------------------------------------------------------------
batches attempted                               40          40
batches reported successful                     23          40
batches reported failed                         17           0
expected rows (source of truth)              53940       53940
actual rows in warehouse (SQL COUNT)        129444       53940
distinct batch_ids in warehouse                 40          40
batches physically inserted >1 time             32           0
total duplicate physical inserts                56           0
idempotency cache hits                           0          30
```

That's one seed of one run (`--seed 1`), captured verbatim in `results/benchmark_results.json`. 32 of the 40 batches were physically written more than once — some as many as three times, the maximum the retry loop allows — and the warehouse ended up with 129,444 rows instead of the 53,940 that should be there. Worth sitting with: 17 of the 40 batches were *reported as failed* by the agent, and the warehouse still has their data, because the abandoned background thread from an earlier attempt kept running and eventually wrote anyway. The bug isn't confined to the batches the agent thinks succeeded.

Because this uses genuine `threading.Thread` objects racing a genuine 50ms wall-clock deadline, the *exact* duplicate count is not perfectly reproducible run to run — real OS thread scheduling near a tight timeout boundary has some inherent jitter that a seeded latency generator alone doesn't fully control. So this was run five times, seeds 1 through 5, each a completely independent execution against a fresh SQLite file:

| seed | rows written | expected | duplicated batches (of 40) | duplicate physical inserts |
|---|---|---|---|---|
| 1 | 129,444 | 53,940 | 32 | 56 |
| 2 | 128,095 | 53,940 | 31 | 55 |
| 3 | 142,934 | 53,940 | 35 | 66 |
| 4 | 138,907 | 53,940 | 35 | 63 |
| 5 | 113,256 | 53,940 | 26 | 44 |

Every single run over-wrote the warehouse. Every single run duplicated at least 26 of the 40 batches. The exact count moves around (26–35 duplicated batches, a mean of 31.8), but the qualitative finding never wavers: this retry loop reliably corrupts the warehouse, not occasionally.

## The fix: `@idempotent`, stacked with `@with_timeout`

The fix isn't "don't retry" — retrying is the right response to an ambiguous failure, and refusing to retry just trades duplicate writes for dropped batches. The fix is giving the tool a way to recognize "I've already done this exact operation":

```python
from latch import idempotent, with_timeout

protected_load = with_timeout(seconds=0.05)(
    idempotent(store=store, tracer=tracer)(load_sales_batch_raw)
)

result = protected_load(
    batch_id="diamonds-2026-01-15",
    rows=todays_rows,
    idempotency_key="diamonds-2026-01-15",  # the batch_id IS the natural key here
)
```

`idempotency_key` is a required keyword argument, not something inferred from the batch contents — deliberately, because guessing at "the same operation" from argument values is exactly how you end up with a library that's wrong in ways nobody notices. Here the batch_id itself is the natural key: each batch is its own logical operation.

The composition order matters. `@with_timeout` sits outside `@idempotent` because, for a synchronous function, Python has no safe way to forcibly kill a running thread — a "timed out" call keeps executing in the background. Because `idempotent` is the *inner* decorator, that abandoned background call still runs the cache-store logic to completion. By the time the agent's retry arrives with the same key, the result is often already cached — a fast cache hit instead of a second `INSERT`.

Running the identical 40 batches, identical chaos seed, through the protected loop instead:

```
metric                                       naive   protected
--------------------------------------------------------------
batches attempted                               40          40
batches reported successful                     23          40
batches reported failed                         17           0
expected rows (source of truth)              53940       53940
actual rows in warehouse (SQL COUNT)        129444       53940
distinct batch_ids in warehouse                 40          40
batches physically inserted >1 time             32           0
total duplicate physical inserts                56           0
idempotency cache hits                           0          30
```

53,940 rows. Exactly. Zero duplicated batches. Zero duplicate physical inserts. All 40 batches reported successful — the client-side timeout still fires on plenty of individual calls (30 of the eventual retries were served entirely from the idempotency cache instead of touching the warehouse again), but none of them turn into a second write. This held across all five seeds without exception: every protected run matched the expected row count exactly, every protected run had zero duplicated batches. Not "usually" — every time.

## Verifying this against Spark, independently

A pipeline is more convincing when two unrelated computation engines agree on the same source data. Alongside the pandas-based extract/transform step, a small PySpark job (`spark/transform_spark.py`, run standalone with a local Spark 3.5.3 session) reads the same raw CSV and recomputes row count and price statistics from scratch:

```
Spark 3.5.3 read 53940 rows from data/diamonds_raw.csv
price_sum=212135217.0  price_min=326.0  price_max=18823.0
```

pandas, a third pass with Python's plain `csv` module, and Spark all land on the same numbers: 53,940 rows, price sum 212,135,217, min 326, max 18,823. If any one of those three independent computations disagreed, that would be a real bug worth catching — a rounding difference or an off-by-one in the extract step — not a footnote.

## Orchestrating it with Airflow

The pipeline is also wired up as a real Airflow DAG (`airflow_dags/diamond_sales_load_dag.py`), using Airflow 3.3.0's TaskFlow API: `extract_task >> transform_task >> load_task >> verify_task`, where `load_task` runs the same `@with_timeout` + `@idempotent`-protected path described above, and `verify_task` asserts the final row count and duplicate-batch count directly against the warehouse. This isn't a DAG file that only looks right on paper — it was actually executed with `airflow dags test diamond_sales_load_pipeline 2026-01-01` against a locally migrated Airflow metadata database, and the run finished with:

```
Done. Returned value was: {'total_rows': 53940, 'expected_rows': 53940,
'distinct_batches': 40, 'duplicated_batches': 0, 'verified': True}
DagRun Finished: ... state=success
```

`transform_task` deliberately does not push all 53,940 rows of batch data through Airflow's XCom mechanism (a well-known anti-pattern) — it pushes back only the batching parameters, and `load_task` re-derives the identical batches by re-reading the same CSV with the same deterministic chunking function.

## When the pipeline has more than one write: Saga

A single idempotent load handles one write. Real pipelines often chain several — load a fact table, mark a control table as processed, trigger a downstream refresh — and if the last step fails, you're left with a fact table that has new rows a control table doesn't know about.

This is demonstrated against the second dataset, `Housing`, batched into 11 groups of 50. Each batch runs a three-step `Saga`: load into `fact_housing_sales`, mark a `control_batch_status` row processed, then call a "trigger downstream refresh" step. One specific batch (`housing-2026-01-05`) is deliberately made to fail at that third step, every run, so the demo is about proving rollback actually happens — not about re-testing chaos-injected latency, which the benchmark above already covers.

```python
saga.add_step(load_fn, name="load_fact_table", compensation=delete_fn)
saga.add_step(mark_processed_fn, name="mark_processed", compensation=mark_unprocessed_fn)
saga.add_step(trigger_refresh_fn, name="trigger_refresh")  # deliberately fails once
```

After the run, querying `fact_housing_sales` and `control_batch_status` directly (not trusting the Saga's own return value) confirms: the poisoned batch has zero rows in the fact table and is not marked processed — both compensations really ran, deleting real rows rather than just logging that they would have — while all 10 other batches have their full 50 rows and are marked processed. The fact table ends with exactly 496 rows (546 minus the 50-row poisoned batch), which is what the arithmetic predicts if rollback worked and nothing else was touched.

## Nobody grades their own homework

Every number above was produced by `benchmarks/pipeline_chaos_benchmark.py` and `src/saga_demo.py`. Trusting those same scripts to also confirm their own results would defeat the point of a reliability benchmark, so a separate program, `checker/verify_results.py`, does the confirming. It does not import `src.warehouse`, `src.extract`, or the benchmark module — every count it produces comes from `sqlite3` and `csv` calls written independently in that file, against the raw SQLite and CSV files the other scripts left behind. It recomputes source-file checksums and row counts from scratch, re-derives duplicate-batch counts directly from the warehouse files for every seed, cross-checks the pandas/Spark/csv aggregate agreement, re-verifies the Saga rollback against the raw tables, and confirms the Airflow-produced warehouse independently. Across the full set — 2 source-file checks, 4 checks per benchmark run × 6 runs (5 seeds plus the canonical run), the cross-engine check, the Saga check, and the Airflow check — **29 of 29 independent checks passed.** A 28-test `pytest` suite covering extraction, batching, the warehouse's raw insert semantics, the idempotency contract in isolation, the chaos-driven integration path, the Saga rollback, and the DAG's structure passes as well.

## Where this fits, and where it doesn't

None of this is a claim that idempotency, circuit breakers, timeouts, or Sagas are new ideas — they're well-established resilience patterns, and libraries like `tenacity` and `pybreaker` have covered pieces of this ground for years. What's specific to the agentic-pipeline case is the shape of the problem: a caller that decides to retry based on ambiguous, LLM-mediated judgment rather than a fixed retry policy, tool functions that need a required, non-guessed idempotency key rather than an inferred one, and primitives that compose cleanly on the same function without knowing about each other.

It's also worth being upfront about the limits of what's demonstrated here. The circuit breaker and budget guardrail primitives that `latch` also ships are not exercised in this particular benchmark — this article focuses on the idempotency + timeout combination because that's the specific failure the opening story describes; both are in-process and in-memory, not something this benchmark tests for coordination across replicas. The chaos injector's exact duplicate-count spread (26–35 out of 40 batches, across five seeds) reflects genuine thread-scheduling variance, not a flaw in the fix — the qualitative result (zero duplicates, every protected run, every seed) is the reproducible claim, not any single exact count. And `Saga` compensates against exceptions raised mid-run, not against the whole process being killed between steps; that failure mode is a job for a durable workflow engine, not an in-process compensation pattern.

For a single-process agent orchestration layer sitting in front of a data warehouse or lakehouse — which describes a large fraction of current agentic BI pipelines — that scope is the right one. The failure this article opened with, a batch of sales rows quietly duplicated because a timeout and a retry collided, is exactly the class of bug these primitives are built to catch before it reaches a dashboard, and this time the numbers behind that claim are sitting in a SQLite file anyone can open and count themselves.

## Trying it

```
pip install latch-idempotent
```

The `latch` package is on PyPI; the source, this benchmark, the Airflow DAG, the Spark job, the Saga demo, the independent checker, and every result file referenced above are in the `data-pipeline-reliability/` folder of the GitHub repository, along with a README explaining exactly how to reproduce each number in this article from a clean checkout.
