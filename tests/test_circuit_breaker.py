import asyncio
import threading
import time

import pytest

from latch import CircuitBreaker, CircuitOpenError, CircuitState, circuit_breaker


def test_closed_circuit_passes_calls_through():
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10)

    @circuit_breaker(breaker=breaker)
    def call():
        return "ok"

    assert call() == "ok"
    assert breaker.state is CircuitState.CLOSED


def test_opens_after_failure_threshold():
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10)

    @circuit_breaker(breaker=breaker)
    def flaky():
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            flaky()

    assert breaker.state is CircuitState.OPEN


def test_open_circuit_rejects_without_calling_function():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10)
    calls = []

    @circuit_breaker(breaker=breaker)
    def flaky():
        calls.append(1)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        flaky()
    assert breaker.state is CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        flaky()

    # Second call was rejected before ever invoking the function again.
    assert len(calls) == 1


def test_half_open_recovers_on_success():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

    should_fail = {"value": True}

    @circuit_breaker(breaker=breaker)
    def sometimes():
        if should_fail["value"]:
            raise RuntimeError("boom")
        return "recovered"

    with pytest.raises(RuntimeError):
        sometimes()
    assert breaker.state is CircuitState.OPEN

    time.sleep(0.06)  # past recovery_timeout -> HALF_OPEN
    assert breaker.state is CircuitState.HALF_OPEN

    should_fail["value"] = False
    result = sometimes()
    assert result == "recovered"
    assert breaker.state is CircuitState.CLOSED


def test_half_open_reopens_on_trial_failure():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

    @circuit_breaker(breaker=breaker)
    def always_fails():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        always_fails()
    assert breaker.state is CircuitState.OPEN

    time.sleep(0.06)
    assert breaker.state is CircuitState.HALF_OPEN

    with pytest.raises(RuntimeError):
        always_fails()
    assert breaker.state is CircuitState.OPEN


def test_success_resets_failure_count():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=10)
    outcomes = iter([RuntimeError, None, RuntimeError, RuntimeError])

    @circuit_breaker(breaker=breaker)
    def maybe_fail():
        outcome = next(outcomes)
        if outcome is not None:
            raise outcome("boom")
        return "ok"

    with pytest.raises(RuntimeError):
        maybe_fail()  # failure 1
    assert maybe_fail() == "ok"  # success resets count to 0
    with pytest.raises(RuntimeError):
        maybe_fail()  # failure 1 again (not 2)
    assert breaker.state is CircuitState.CLOSED
    with pytest.raises(RuntimeError):
        maybe_fail()  # failure 2 -> opens
    assert breaker.state is CircuitState.OPEN


def test_unexpected_exception_type_not_counted():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10, expected_exception=ValueError)

    @circuit_breaker(breaker=breaker)
    def raises_type_error():
        raise TypeError("not tracked by this breaker")

    with pytest.raises(TypeError):
        raises_type_error()

    # TypeError isn't `expected_exception`, so it propagates but does not
    # trip the circuit.
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_async_circuit_breaker():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10)

    @circuit_breaker(breaker=breaker)
    async def flaky():
        await asyncio.sleep(0)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await flaky()
    assert breaker.state is CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await flaky()


def test_invalid_construction_raises():
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker(recovery_timeout=-1)


def test_reset_forces_closed():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=100)

    @circuit_breaker(breaker=breaker)
    def flaky():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        flaky()
    assert breaker.state is CircuitState.OPEN

    breaker.reset()
    assert breaker.state is CircuitState.CLOSED


def test_half_open_allows_only_one_concurrent_trial_call():
    # Regression test: half-open must let exactly one trial call through
    # at a time. Without this, every caller queued up while the circuit
    # was OPEN arrives the instant recovery_timeout elapses and all of
    # them get let through simultaneously, hammering a dependency that
    # has barely started recovering -- exactly what the circuit breaker
    # exists to prevent.
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

    @circuit_breaker(breaker=breaker)
    def flaky():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        flaky()
    time.sleep(0.06)  # -> HALF_OPEN
    assert breaker.state is CircuitState.HALF_OPEN

    concurrent = {"current": 0, "max": 0}
    lock = threading.Lock()

    @circuit_breaker(breaker=breaker)
    def slow_trial():
        with lock:
            concurrent["current"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["current"])
        time.sleep(0.05)
        with lock:
            concurrent["current"] -= 1
        return "ok"

    results = []

    def worker():
        try:
            results.append(slow_trial())
        except CircuitOpenError:
            results.append("rejected")

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert concurrent["max"] == 1
    assert results.count("ok") == 1
    assert results.count("rejected") == 7
    assert breaker.state is CircuitState.CLOSED  # the one trial succeeded


def test_half_open_trial_slot_released_on_unexpected_exception():
    # Regression test: an exception type outside expected_exception during
    # a half-open trial must still release the trial slot. Otherwise the
    # breaker gets permanently wedged rejecting every future call, since
    # neither _on_success nor _on_failure runs for an unexpected exception
    # type.
    breaker = CircuitBreaker(
        failure_threshold=1, recovery_timeout=0.05, expected_exception=ValueError
    )

    @circuit_breaker(breaker=breaker)
    def raises_value_error():
        raise ValueError("tracked")

    with pytest.raises(ValueError):
        raises_value_error()
    time.sleep(0.06)
    assert breaker.state is CircuitState.HALF_OPEN

    @circuit_breaker(breaker=breaker)
    def raises_type_error():
        raise TypeError("not tracked by this breaker")

    with pytest.raises(TypeError):
        raises_type_error()

    # Breaker must not be stuck: still half-open, and a subsequent trial
    # must be allowed through rather than rejected.
    assert breaker.state is CircuitState.HALF_OPEN

    @circuit_breaker(breaker=breaker)
    def succeeds():
        return "ok"

    assert succeeds() == "ok"
    assert breaker.state is CircuitState.CLOSED


def test_default_breaker_created_per_decorator_when_not_shared():
    @circuit_breaker(failure_threshold=1, recovery_timeout=10)
    def a():
        raise RuntimeError("boom")

    @circuit_breaker(failure_threshold=1, recovery_timeout=10)
    def b():
        return "ok"

    with pytest.raises(RuntimeError):
        a()

    # `b` has its own independent breaker, unaffected by `a` opening.
    assert b() == "ok"
