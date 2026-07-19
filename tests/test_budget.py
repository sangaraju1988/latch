import asyncio
import time

import pytest

from latch import BudgetExceededError, BudgetGuardrail, budget_guardrail


def test_max_calls_allows_up_to_limit():
    guardrail = BudgetGuardrail(max_calls=2)

    @budget_guardrail(guardrail=guardrail)
    def call():
        return "ok"

    assert call() == "ok"
    assert call() == "ok"
    with pytest.raises(BudgetExceededError):
        call()


def test_max_cost_allows_up_to_limit():
    guardrail = BudgetGuardrail(max_cost=1.0)

    @budget_guardrail(guardrail=guardrail, cost_fn=lambda amount: amount)
    def spend(amount):
        return amount

    assert spend(0.4) == 0.4
    assert spend(0.5) == 0.5
    # 0.4 + 0.5 + 0.5 = 1.4 > 1.0
    with pytest.raises(BudgetExceededError):
        spend(0.5)


def test_rejected_call_does_not_execute_function():
    guardrail = BudgetGuardrail(max_calls=1)
    calls = []

    @budget_guardrail(guardrail=guardrail)
    def call():
        calls.append(1)
        return "ok"

    call()
    with pytest.raises(BudgetExceededError):
        call()

    assert len(calls) == 1


def test_window_resets_after_window_seconds():
    guardrail = BudgetGuardrail(max_calls=1, window_seconds=0.05)

    @budget_guardrail(guardrail=guardrail)
    def call():
        return "ok"

    assert call() == "ok"
    with pytest.raises(BudgetExceededError):
        call()

    time.sleep(0.06)
    assert call() == "ok"  # new window


def test_call_count_and_total_cost_properties():
    guardrail = BudgetGuardrail(max_calls=5, max_cost=100.0)

    @budget_guardrail(guardrail=guardrail, cost_fn=lambda x: 2.5)
    def call(x):
        return x

    call(1)
    call(2)

    assert guardrail.call_count == 2
    assert guardrail.total_cost == 5.0


def test_reset_clears_state():
    guardrail = BudgetGuardrail(max_calls=1)

    @budget_guardrail(guardrail=guardrail)
    def call():
        return "ok"

    call()
    with pytest.raises(BudgetExceededError):
        call()

    guardrail.reset()
    assert call() == "ok"


def test_requires_at_least_one_limit():
    with pytest.raises(ValueError):
        BudgetGuardrail()


def test_invalid_construction_raises():
    with pytest.raises(ValueError):
        BudgetGuardrail(max_calls=0)
    with pytest.raises(ValueError):
        BudgetGuardrail(max_cost=-1)
    with pytest.raises(ValueError):
        BudgetGuardrail(max_calls=1, window_seconds=0)


@pytest.mark.asyncio
async def test_async_budget_guardrail():
    guardrail = BudgetGuardrail(max_calls=1)

    @budget_guardrail(guardrail=guardrail)
    async def call():
        await asyncio.sleep(0)
        return "ok"

    assert await call() == "ok"
    with pytest.raises(BudgetExceededError):
        await call()


def test_no_cost_fn_defaults_to_zero_cost():
    guardrail = BudgetGuardrail(max_cost=0.0)

    @budget_guardrail(guardrail=guardrail)
    def call():
        return "ok"

    # Every call costs 0 by default, so an unlimited number of calls fit
    # under a max_cost of 0.
    for _ in range(5):
        assert call() == "ok"
