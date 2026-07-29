"""The `load_sales_batch` tool, in its naive and latch-protected forms.

Both forms wrap the exact same real, physical warehouse write
(`warehouse.insert_batch` -- a genuine `INSERT` into a SQLite fact table),
under the exact same injected chaos latency (`latch.chaos`, same seed). The
*only* difference between the two is whether `@idempotent` (and
`@with_timeout`) sit between the agent-facing call and that physical write.
Every result each variant produces is grounded in a real SQLite file that
can be inspected independently after the fact.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from latch import InMemoryStore, Tracer, idempotent, with_timeout
from latch.chaos import chaos

from src import warehouse

# Injected latency jitter, uniform on [0, RAW_LATENCY_JITTER_SECONDS). This
# stands in for "the warehouse connection is occasionally slow" -- a real
# network blip, not a hard failure. 0% hard-failure rate: the bug this
# benchmark measures is purely the ambiguous-timeout-and-retry pattern, the
# same scenario documented in examples/naive_agent_example.py and
# benchmarks/chaos_benchmark.py in the parent `latch` project.
RAW_LATENCY_JITTER_SECONDS = 0.15
CLIENT_TIMEOUT_SECONDS = 0.05
RETRY_DELAY_SECONDS = 0.25
RETRIES_PER_BATCH = 3


def make_raw_load(db_path: str, seed: int) -> Callable[[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """The real, unprotected warehouse write, with injected latency only.
    No idempotency, no timeout -- calling this twice for the same batch_id
    inserts the batch twice, for real, into `db_path`.
    """

    @chaos(
        failure_rate=0.0,
        latency_seconds=0.0,
        latency_jitter_seconds=RAW_LATENCY_JITTER_SECONDS,
        seed=seed,
    )
    def load_sales_batch_raw(batch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return warehouse.insert_batch(db_path, batch_id, rows)

    return load_sales_batch_raw


def make_protected_load(
    db_path: str,
    seed: int,
    store: Optional[InMemoryStore] = None,
    tracer: Optional[Tracer] = None,
) -> Callable[..., Dict[str, Any]]:
    """Same physical write, same injected latency (same seed => identical
    latency draws as make_raw_load), wrapped with `@with_timeout` (outer)
    around `@idempotent` (inner) -- the same composition documented in
    `examples/resilient_agent_example.py` and used by
    `benchmarks/chaos_benchmark.py` in the parent project. Because
    `idempotent` sits inside `with_timeout`, an abandoned background call
    (see `with_timeout`'s documented non-forcible-kill tradeoff) still
    completes its cache-store logic, so a same-key retry becomes a cache hit
    instead of a second physical INSERT.
    """
    active_store = store if store is not None else InMemoryStore()

    @chaos(
        failure_rate=0.0,
        latency_seconds=0.0,
        latency_jitter_seconds=RAW_LATENCY_JITTER_SECONDS,
        seed=seed,
    )
    def load_sales_batch_raw(batch_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return warehouse.insert_batch(db_path, batch_id, rows)

    protected = with_timeout(seconds=CLIENT_TIMEOUT_SECONDS, tracer=tracer)(
        idempotent(store=active_store, tracer=tracer)(load_sales_batch_raw)
    )
    return protected
