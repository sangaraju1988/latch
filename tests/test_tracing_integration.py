import asyncio
import time

import pytest

from latch import (
    BudgetExceededError,
    BudgetGuardrail,
    CircuitBreaker,
    CircuitOpenError,
    LatchTimeoutError,
    Saga,
    SagaExecutionError,
    Tracer,
    budget_guardrail,
    circuit_breaker,
    idempotent,
    with_timeout,
)


def _events(tracer_events, primitive=None):
    if primitive is None:
        return [(e.primitive, e.event) for e in tracer_events]
    return [e.event for e in tracer_events if e.primitive == primitive]


class TestIdempotentTracing:
    def test_emits_cache_miss_then_stored_on_first_call(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)

        @idempotent(tracer=tracer)
        def create_order(order_id: str) -> dict:
            return {"order_id": order_id}

        create_order(order_id="A1", idempotency_key="k1")

        assert _events(events, "idempotent") == ["cache_miss", "stored"]

    def test_emits_cache_hit_on_retry(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)

        @idempotent(tracer=tracer)
        def create_order(order_id: str) -> dict:
            return {"order_id": order_id}

        create_order(order_id="A1", idempotency_key="k1")
        events.clear()
        create_order(order_id="A1", idempotency_key="k1")

        assert _events(events, "idempotent") == ["cache_hit"]


class TestCircuitBreakerTracing:
    def test_emits_call_succeeded(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=10, tracer=tracer)

        @circuit_breaker(breaker=breaker)
        def call():
            return "ok"

        call()

        assert _events(events, "circuit_breaker") == ["call_succeeded"]

    def test_emits_call_failed_and_state_changed_on_trip(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10, tracer=tracer)

        @circuit_breaker(breaker=breaker)
        def flaky():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            flaky()

        cb_events = [(e.event, e.metadata) for e in events if e.primitive == "circuit_breaker"]
        assert cb_events[0][0] == "call_failed"
        assert cb_events[1] == ("state_changed", {"from_state": "closed", "to_state": "open"})

    def test_emits_call_rejected_when_open(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10, tracer=tracer)

        @circuit_breaker(breaker=breaker)
        def flaky():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            flaky()
        events.clear()

        with pytest.raises(CircuitOpenError):
            flaky()

        assert _events(events, "circuit_breaker") == ["call_rejected"]

    def test_emits_state_changed_on_half_open_recovery(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05, tracer=tracer)
        should_fail = {"value": True}

        @circuit_breaker(breaker=breaker)
        def sometimes():
            if should_fail["value"]:
                raise RuntimeError("boom")
            return "ok"

        with pytest.raises(RuntimeError):
            sometimes()
        time.sleep(0.06)
        events.clear()
        should_fail["value"] = False

        sometimes()

        state_changes = [
            e.metadata
            for e in events
            if e.primitive == "circuit_breaker" and e.event == "state_changed"
        ]
        assert {"from_state": "open", "to_state": "half_open"} in state_changes
        assert {"from_state": "half_open", "to_state": "closed"} in state_changes


class TestTimeoutTracing:
    def test_emits_completed_within_deadline(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)

        @with_timeout(seconds=5, tracer=tracer)
        def fast():
            return "ok"

        fast()

        assert _events(events, "timeout") == ["completed"]

    def test_emits_timed_out(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)

        @with_timeout(seconds=0.05, tracer=tracer)
        def slow():
            time.sleep(1)

        with pytest.raises(LatchTimeoutError):
            slow()

        assert _events(events, "timeout") == ["timed_out"]

    @pytest.mark.asyncio
    async def test_async_emits_timed_out(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)

        @with_timeout(seconds=0.05, tracer=tracer)
        async def slow():
            await asyncio.sleep(1)

        with pytest.raises(LatchTimeoutError):
            await slow()

        assert _events(events, "timeout") == ["timed_out"]


class TestBudgetGuardrailTracing:
    def test_emits_call_recorded(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        guardrail = BudgetGuardrail(max_calls=10, window_seconds=60, tracer=tracer)

        @budget_guardrail(guardrail=guardrail)
        def call():
            return "ok"

        call()

        assert _events(events, "budget_guardrail") == ["call_recorded"]
        assert events[0].metadata["call_count"] == 1

    def test_emits_budget_exceeded(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        guardrail = BudgetGuardrail(max_calls=1, window_seconds=60, tracer=tracer)

        @budget_guardrail(guardrail=guardrail)
        def call():
            return "ok"

        call()
        events.clear()

        with pytest.raises(BudgetExceededError):
            call()

        assert _events(events, "budget_guardrail") == ["budget_exceeded"]


class TestSagaTracing:
    def test_emits_step_lifecycle_on_success(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        saga = Saga(tracer=tracer)
        saga.add_step(lambda: "a", name="a")
        saga.add_step(lambda: "b", name="b")

        saga.run()

        assert _events(events, "saga") == [
            "step_started",
            "step_succeeded",
            "step_started",
            "step_succeeded",
            "saga_succeeded",
        ]

    def test_emits_failure_and_compensation_lifecycle(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        saga = Saga(tracer=tracer)

        saga.add_step(lambda: "charge-1", name="charge", compensation=lambda r: None)

        def boom():
            raise RuntimeError("hotel down")

        saga.add_step(boom, name="hotel")

        with pytest.raises(SagaExecutionError):
            saga.run()

        assert _events(events, "saga") == [
            "step_started",  # charge
            "step_succeeded",  # charge
            "step_started",  # hotel
            "step_failed",  # hotel
            "compensation_started",  # charge
            "compensation_succeeded",  # charge
            "saga_failed",
        ]

    @pytest.mark.asyncio
    async def test_async_emits_step_lifecycle(self):
        tracer = Tracer()
        events = []
        tracer.subscribe(events.append)
        saga = Saga(tracer=tracer)

        async def a():
            return "a"

        saga.add_step(a, name="a")

        await saga.run_async()

        assert _events(events, "saga") == ["step_started", "step_succeeded", "saga_succeeded"]
