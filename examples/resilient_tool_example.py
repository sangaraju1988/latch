"""v0.2 example: composing idempotency, circuit breaking, timeouts, and a
budget guardrail around a single agent tool call.

This is the pattern latch's v0.2 is built around: none of these
primitives depend on each other, but a production agent tool call
typically wants all four layered together --

    budget guardrail   -- refuse to even attempt the call once the
                           per-window cost/count cap is hit
    circuit breaker     -- refuse to attempt the call if the dependency
                           has been failing repeatedly (fail fast instead
                           of piling on load)
    timeout              -- bound how long a single attempt is allowed to
                           block the agent loop
    idempotency         -- if the call *is* attempted and a retry with
                           the same key comes in later, don't repeat the
                           side effect

Decorators apply bottom-up, so the order below means: on each call,
latch checks the budget first, then the circuit, then runs the call
under a timeout, and only executes the underlying function body if there
was no cached idempotent result already.
"""

import random

from latch import (
    BudgetGuardrail,
    CircuitBreaker,
    InMemoryStore,
    budget_guardrail,
    circuit_breaker,
    idempotent,
    with_timeout,
)

# Shared state, one instance per protected dependency ("the payments API"),
# not per call site -- so failures/spend across every call to this tool
# count against the same breaker/budget.
payments_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=15.0)
payments_budget = BudgetGuardrail(max_calls=100, window_seconds=3600)
payments_idempotency_store = InMemoryStore()


@budget_guardrail(guardrail=payments_budget)
@circuit_breaker(breaker=payments_breaker)
@with_timeout(seconds=5)
@idempotent(store=payments_idempotency_store, ttl_seconds=86400)
def charge_card(order_id: str, amount: float) -> dict:
    """Simulated payments API call. In real code this would be an HTTP
    call to a payment processor."""
    if random.random() < 0.0:  # deterministic for the example; wire up real failures here
        raise RuntimeError("payment processor unavailable")
    return {"order_id": order_id, "amount": amount, "status": "charged"}


if __name__ == "__main__":
    # The agent framework supplies a unique key per logical operation --
    # e.g. "{run_id}-{step_id}" -- so a retry after a timeout dedupes
    # instead of double-charging.
    result = charge_card(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
    print("First call:", result)

    retry = charge_card(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
    print("Retry (same key, no double charge):", retry)
    assert result == retry

    print(f"Circuit state: {payments_breaker.state.value}")
    print(f"Calls used this budget window: {payments_budget.call_count}/{payments_budget.max_calls}")
