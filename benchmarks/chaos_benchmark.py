"""Chaos-injection benchmark: naive vs. latch-protected agent loop.

This script simulates the exact bug `latch` exists to prevent, using real
`latch` primitives (not a mock) on one side and no protection at all on the
other, under identical injected chaos.

The scenario
------------
An agent calls a "charge card for this order" tool. The payment API is
occasionally slow -- sometimes slower than the agent's own client-side
timeout. When that happens, the agent's orchestration loop gives up on the
call and retries. `latch`'s own documented tradeoff for `@with_timeout` is
that it cannot forcibly kill a running sync call (Python has no safe
mechanism to do that) -- the original call keeps executing in the
background even after the caller has moved on. That's precisely the
"ambiguous timeout" failure mode described in CLAUDE.md's Mission section:
the agent doesn't know whether the underlying action actually completed.

- The "naive" agent has no idempotency layer. Every retry after an
  apparent timeout re-runs the real charge. Money moves more than once,
  silently, even on orders the agent ultimately reports as *failed*.
- The "protected" agent wraps the same charge function with
  `@with_timeout` (outer) around `@idempotent` (inner) -- the same
  composition order documented in `examples/resilient_tool_example.py`.
  Because `idempotent` sits *inside* `with_timeout`, the abandoned
  background call still runs `idempotent`'s cache-store logic to
  completion. By the time the agent retries with the same
  `idempotency_key`, the result is already cached and the retry is a fast
  cache hit -- no second charge, no spurious failure.

Both runs use `latch.chaos` with the *same seed* to inject the identical
latency profile, so the only variable being measured is the presence or
absence of latch's protection.

This script is intentionally not part of the installed package (see
`[tool.hatch.build.targets.wheel]` in pyproject.toml) -- it's a
reproducibility artifact, meant to double as evaluation data for a future
paper, the same way `examples/*.py` are demonstrations rather than library
code.

Run it directly:

    python benchmarks/chaos_benchmark.py
    python benchmarks/chaos_benchmark.py --seed 7 --operations 50 --json out.json
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from latch import InMemoryStore, LatchTimeoutError, Tracer, idempotent, with_timeout
from latch.chaos import chaos

# --- Tunable constants -------------------------------------------------
#
# RAW_LATENCY_JITTER_SECONDS is drawn uniformly from [0, RAW_LATENCY_JITTER_SECONDS).
# CLIENT_TIMEOUT_SECONDS sits inside that range so *some* calls finish in
# time (clean success) and *most* don't (triggering the ambiguous-timeout
# retry path) -- a realistic mix rather than a scenario that always or
# never reproduces the bug.
#
# RETRY_DELAY_SECONDS is deliberately set comfortably longer than the
# maximum possible raw latency. That's not cheating the benchmark -- it
# reflects realistic agent backoff (agents don't hammer a retry
# immediately) and it makes the demonstration deterministic: by the time a
# retry fires, any earlier background call is guaranteed to have finished,
# so the comparison isolates "does idempotency catch the completed
# background call" rather than "did we get lucky with thread scheduling."
RAW_LATENCY_JITTER_SECONDS = 0.15
CLIENT_TIMEOUT_SECONDS = 0.05
RETRY_DELAY_SECONDS = 0.25
RETRIES_PER_OPERATION = 3


@dataclass
class BenchmarkResult:
    label: str
    orders_attempted: int
    orders_reported_successful: int
    orders_reported_failed: int
    total_real_charges: int
    orders_double_charged: int
    idempotency_cache_hits: int = 0
    events: List[Any] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "orders_attempted": self.orders_attempted,
            "orders_reported_successful": self.orders_reported_successful,
            "orders_reported_failed": self.orders_reported_failed,
            "total_real_charges": self.total_real_charges,
            "orders_double_charged": self.orders_double_charged,
            "idempotency_cache_hits": self.idempotency_cache_hits,
        }


def _make_raw_charge(ledger: Dict[str, int], seed: int) -> Callable[..., Dict[str, Any]]:
    """The simulated payment API. Chaos injects latency only (0% hard
    failure rate) -- the "failure" this benchmark cares about is purely a
    client-side timeout, not a server-side error. Each call that actually
    runs increments `ledger[order_id]`, our observable proxy for "a real
    charge was issued."
    """

    @chaos(
        failure_rate=0.0,
        latency_seconds=0.0,
        latency_jitter_seconds=RAW_LATENCY_JITTER_SECONDS,
        seed=seed,
    )
    def charge_card_raw(order_id: str, amount: float) -> Dict[str, Any]:
        ledger[order_id] = ledger.get(order_id, 0) + 1
        return {"order_id": order_id, "amount": amount, "status": "charged"}

    return charge_card_raw


def _naive_agent_charge(
    charge_fn: Callable[..., Any], order_id: str, amount: float
) -> Optional[Dict[str, Any]]:
    """No latch protection. Enforces its own ad-hoc client-side timeout
    (the way many hand-rolled agent loops do) via a background thread that
    is never killed on timeout -- matching real thread semantics, not just
    latch's. On a timeout, it retries by calling the raw function again.
    """
    for _attempt in range(RETRIES_PER_OPERATION):
        result_holder: Dict[str, Any] = {}

        def target() -> None:
            result_holder["result"] = charge_fn(order_id, amount)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=CLIENT_TIMEOUT_SECONDS)
        if thread.is_alive():
            # Client gives up and will retry -- but the thread above keeps
            # running in the background and will still charge the card.
            time.sleep(RETRY_DELAY_SECONDS)
            continue
        return result_holder.get("result")
    return None  # exhausted retries; agent reports this order as failed


def _protected_agent_charge(
    protected_charge_fn: Callable[..., Any], order_id: str, amount: float
) -> Optional[Dict[str, Any]]:
    """Same scenario, protected by `@with_timeout` (outer) wrapping
    `@idempotent` (inner) -- see module docstring for why that order
    matters."""
    key = f"charge-{order_id}"
    for _attempt in range(RETRIES_PER_OPERATION):
        try:
            return protected_charge_fn(order_id=order_id, amount=amount, idempotency_key=key)  # type: ignore[no-any-return]
        except LatchTimeoutError:
            time.sleep(RETRY_DELAY_SECONDS)
            continue
    return None


def run_naive_benchmark(*, seed: int, num_operations: int) -> BenchmarkResult:
    ledger: Dict[str, int] = {}
    raw = _make_raw_charge(ledger, seed=seed)

    successes = 0
    for i in range(num_operations):
        result = _naive_agent_charge(raw, order_id=f"order-{i}", amount=42.0)
        if result is not None:
            successes += 1

    # Give any still-in-flight background threads from the final retries a
    # moment to finish so the ledger reflects their (silent) side effects
    # before we measure it.
    time.sleep(RAW_LATENCY_JITTER_SECONDS + 0.05)

    total_real_charges = sum(ledger.values())
    double_charged = sum(1 for count in ledger.values() if count > 1)
    return BenchmarkResult(
        label="naive (no latch)",
        orders_attempted=num_operations,
        orders_reported_successful=successes,
        orders_reported_failed=num_operations - successes,
        total_real_charges=total_real_charges,
        orders_double_charged=double_charged,
    )


def run_protected_benchmark(*, seed: int, num_operations: int) -> BenchmarkResult:
    ledger: Dict[str, int] = {}
    raw = _make_raw_charge(ledger, seed=seed)  # same seed -> identical latency draws
    store = InMemoryStore()
    tracer = Tracer()
    events: List[Any] = []
    tracer.subscribe(events.append)

    protected_charge = with_timeout(seconds=CLIENT_TIMEOUT_SECONDS, tracer=tracer)(
        idempotent(store=store, tracer=tracer)(raw)
    )

    successes = 0
    for i in range(num_operations):
        result = _protected_agent_charge(protected_charge, order_id=f"order-{i}", amount=42.0)
        if result is not None:
            successes += 1

    time.sleep(RAW_LATENCY_JITTER_SECONDS + 0.05)

    total_real_charges = sum(ledger.values())
    double_charged = sum(1 for count in ledger.values() if count > 1)
    cache_hits = sum(1 for e in events if e.primitive == "idempotent" and e.event == "cache_hit")
    return BenchmarkResult(
        label="protected (with_timeout + idempotent)",
        orders_attempted=num_operations,
        orders_reported_successful=successes,
        orders_reported_failed=num_operations - successes,
        total_real_charges=total_real_charges,
        orders_double_charged=double_charged,
        idempotency_cache_hits=cache_hits,
        events=events,
    )


def print_comparison(naive: BenchmarkResult, protected: BenchmarkResult) -> None:
    rows = [
        ("orders attempted", naive.orders_attempted, protected.orders_attempted),
        (
            "orders reported successful",
            naive.orders_reported_successful,
            protected.orders_reported_successful,
        ),
        ("orders reported failed", naive.orders_reported_failed, protected.orders_reported_failed),
        ("total real charges issued", naive.total_real_charges, protected.total_real_charges),
        ("orders double-charged", naive.orders_double_charged, protected.orders_double_charged),
        ("idempotency cache hits", "n/a", protected.idempotency_cache_hits),
    ]
    label_w = max(len(r[0]) for r in rows) + 2
    col_w = 12
    print()
    print(f"{'metric':<{label_w}}{'naive':>{col_w}}{'protected':>{col_w}}")
    print("-" * (label_w + col_w * 2))
    for label, naive_val, protected_val in rows:
        print(f"{label:<{label_w}}{str(naive_val):>{col_w}}{str(protected_val):>{col_w}}")
    print()
    if naive.orders_double_charged > 0:
        print(
            f"Naive agent silently double-charged {naive.orders_double_charged} order(s) "
            f"while reporting {naive.orders_reported_failed} order(s) as failed outright."
        )
    if protected.orders_double_charged == 0:
        print("Protected agent issued zero duplicate charges despite the same injected latency.")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--seed", type=int, default=1, help="RNG seed shared by both runs (default: 1)"
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=30,
        help="Number of simulated orders per run (default: 30)",
    )
    parser.add_argument(
        "--json", type=str, default=None, help="Optional path to write results as JSON"
    )
    args = parser.parse_args()

    print(f"Running chaos benchmark (seed={args.seed}, operations={args.operations})...")
    naive = run_naive_benchmark(seed=args.seed, num_operations=args.operations)
    protected = run_protected_benchmark(seed=args.seed, num_operations=args.operations)
    print_comparison(naive, protected)

    if args.json:
        payload = {
            "seed": args.seed,
            "operations": args.operations,
            "constants": {
                "raw_latency_jitter_seconds": RAW_LATENCY_JITTER_SECONDS,
                "client_timeout_seconds": CLIENT_TIMEOUT_SECONDS,
                "retry_delay_seconds": RETRY_DELAY_SECONDS,
                "retries_per_operation": RETRIES_PER_OPERATION,
            },
            "naive": naive.as_dict(),
            "protected": protected.as_dict(),
        }
        with open(args.json, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Wrote results to {args.json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
