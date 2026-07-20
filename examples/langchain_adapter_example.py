"""v0.3 example: `latch.adapters.langchain.resilient_tool` wired into a
real `langchain_core.tools.StructuredTool`.

Requires `langchain-core` (`pip install latch-idempotent[dev]`, or just
`pip install langchain-core` -- it is NOT a dependency of `latch` itself;
`latch.adapters.langchain` never imports it). This example constructs and
invokes a real `StructuredTool` -- no LLM or API key needed, since
invoking a LangChain tool directly is just a function call.
"""

from latch import BudgetGuardrail, CircuitBreaker, InMemoryStore
from latch.adapters.langchain import resilient_tool

try:
    from langchain_core.tools import StructuredTool
except ImportError as exc:  # pragma: no cover -- example-only guidance
    raise SystemExit(
        "This example requires langchain-core. Install it with:\n"
        "    pip install langchain-core\n"
        "(langchain-core is an optional dev dependency of latch, not a "
        "required one -- see latch.adapters.langchain module docs.)"
    ) from exc


payments_store = InMemoryStore()
payments_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=15.0)
payments_budget = BudgetGuardrail(max_calls=100, window_seconds=3600)


def charge_card(order_id: str, amount: float) -> dict:
    """Charge a customer's card for an order. Returns the charge record."""
    print(f"  charging card for {order_id}: ${amount}")
    return {"order_id": order_id, "amount": amount, "status": "charged"}


resilient_charge_card = resilient_tool(
    charge_card,
    idempotency_store=payments_store,
    idempotency_ttl_seconds=86400,
    breaker=payments_breaker,
    timeout_seconds=5.0,
    guardrail=payments_budget,
)

charge_card_tool = StructuredTool.from_function(
    func=resilient_charge_card,
    name="charge_card",
    description="Charge a customer's card for an order.",
)


if __name__ == "__main__":
    # A LangChain agent would call this the same way; invoking it
    # directly here to keep the example runnable without an LLM.
    result = charge_card_tool.invoke(
        {"order_id": "A1", "amount": 42.0, "idempotency_key": "run-7-step-3"}
    )
    print("First call:", result)

    retry = charge_card_tool.invoke(
        {"order_id": "A1", "amount": 42.0, "idempotency_key": "run-7-step-3"}
    )
    print("Retry (same key, no double charge):", retry)
    assert result == retry

    print(f"Circuit state: {payments_breaker.state.value}")
    print(f"Calls used this budget window: {payments_budget.call_count}")
