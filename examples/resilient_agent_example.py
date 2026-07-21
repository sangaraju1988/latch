"""The "after" example: the same scenario as
examples/naive_agent_example.py, this time protected by every primitive
latch ships -- idempotency, circuit breaking, timeouts, a budget
guardrail, and saga/compensation -- with a Tracer wired through all five
so you can see exactly what happened.

Run directly: python examples/resilient_agent_example.py

See examples/naive_agent_example.py for the unprotected version of this
exact scenario -- same latency, same retry loop shape, same order.
"""

import logging
import time
from typing import Any, Dict

from latch import (
    BudgetGuardrail,
    CircuitBreaker,
    InMemoryStore,
    LatchTimeoutError,
    LoggingTracer,
    Saga,
    budget_guardrail,
    circuit_breaker,
    idempotent,
    with_timeout,
)

# LoggingTracer logs to logging.getLogger("latch"); configure it like any
# other logger in your application. Here we just print INFO+ to stdout so
# the trace events are visible when running this example directly.
logging.basicConfig(level=logging.INFO, format="  [trace] %(name)s %(message)s")

CLIENT_TIMEOUT_SECONDS = 0.2
CALL_LATENCY_SECONDS = 0.4  # same slow call as the naive example

ledger: Dict[str, int] = {}
hotel_reservations: Dict[str, int] = {}

# One tracer, shared across every primitive below, so a single subscriber
# sees the full picture across idempotency, the circuit breaker, timeouts,
# the budget guardrail, and the saga -- see latch.tracing / README
# "Observability" section.
tracer = LoggingTracer()

payments_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=15.0, tracer=tracer)
payments_budget = BudgetGuardrail(max_calls=100, window_seconds=3600, tracer=tracer)
payments_store = InMemoryStore()
hotel_store = InMemoryStore()


@budget_guardrail(guardrail=payments_budget)
@circuit_breaker(breaker=payments_breaker)
@with_timeout(seconds=CLIENT_TIMEOUT_SECONDS, tracer=tracer)
@idempotent(store=payments_store, tracer=tracer)
def charge_card(order_id: str, amount: float) -> Dict[str, Any]:
    """Same slow simulated payment call as the naive example -- it still
    takes longer than the client timeout below, so `@with_timeout` still
    raises on the first attempt. The difference is entirely in what
    happens next: the retry below carries the same idempotency_key, so
    `@idempotent` (which keeps running in the abandoned background thread
    -- see `@with_timeout`'s documented tradeoff in the README) has
    already cached the result by the time the retry checks."""
    time.sleep(CALL_LATENCY_SECONDS)
    ledger[order_id] = ledger.get(order_id, 0) + 1
    return {"order_id": order_id, "amount": amount, "status": "charged"}


@idempotent(store=hotel_store, tracer=tracer)
def reserve_hotel(order_id: str) -> Dict[str, Any]:
    hotel_reservations[order_id] = hotel_reservations.get(order_id, 0) + 1
    return {"order_id": order_id, "status": "reserved"}


def refund_card(charge_result: Dict[str, Any]) -> None:
    print(f"  compensating: refunding order {charge_result['order_id']}")


def agent_charge_with_retry(order_id: str, amount: float, max_retries: int = 3) -> Dict[str, Any]:
    """Same client-side retry loop shape as the naive example's -- the fix
    lives entirely inside `charge_card` now, not in this loop."""
    key = f"charge-{order_id}"
    for attempt in range(1, max_retries + 1):
        try:
            result = charge_card(order_id=order_id, amount=amount, idempotency_key=key)
            print(f"  attempt {attempt}: succeeded")
            return result
        except LatchTimeoutError:
            print(f"  attempt {attempt}: no response within {CLIENT_TIMEOUT_SECONDS}s -- retrying")
            continue
    raise RuntimeError(f"exhausted retries charging order {order_id}")


if __name__ == "__main__":
    print(
        "Resilient agent charges order A1 (idempotent + timeout + circuit breaker + budget guardrail):"
    )
    result = agent_charge_with_retry(order_id="A1", amount=42.0)
    print(f"\nAgent's view: {result}")
    print(f"Actual charges issued for order A1: {ledger['A1']}")
    assert ledger["A1"] == 1, "expected exactly one real charge"
    print("No double charge -- the retry was a cache hit, not a second call to the payment API.")

    print(f"\nCircuit state: {payments_breaker.state.value}")
    print(
        f"Calls used this budget window: {payments_budget.call_count}/{payments_budget.max_calls}"
    )

    print("\nNow the same charge, inside a two-step Saga (charge card, then reserve hotel):")
    saga = Saga(name="book-trip", tracer=tracer)
    saga.add_step(
        lambda: agent_charge_with_retry(order_id="A2", amount=99.0),
        name="charge_card",
        compensation=refund_card,
    )
    saga.add_step(
        lambda: reserve_hotel(order_id="A2", idempotency_key="reserve-A2"),
        name="reserve_hotel",
    )
    saga_results = saga.run()
    print(f"Saga completed. Results: {saga_results}")
    print(f"Actual charges issued for order A2: {ledger['A2']}")
    assert ledger["A2"] == 1, "expected exactly one real charge"

    print(
        "\nEvery cache hit, timeout, circuit/budget check, and saga step above was reported "
        "through the shared LoggingTracer (see the 'latch.*' log lines interleaved above)."
    )
