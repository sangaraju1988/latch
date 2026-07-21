import asyncio
import time

import pytest

from latch.chaos import Chaos, ChaosInjectedError, chaos


def test_zero_failure_rate_never_raises():
    @chaos(failure_rate=0.0, seed=1)
    def call():
        return "ok"

    for _ in range(50):
        assert call() == "ok"


def test_full_failure_rate_always_raises():
    @chaos(failure_rate=1.0, seed=1)
    def call():
        return "ok"

    for _ in range(10):
        with pytest.raises(ChaosInjectedError):
            call()


def test_wrapped_function_not_invoked_on_injected_failure():
    calls = []

    @chaos(failure_rate=1.0, seed=1)
    def call():
        calls.append(1)
        return "ok"

    with pytest.raises(ChaosInjectedError):
        call()

    assert calls == []


def test_seeded_rng_is_deterministic():
    @chaos(failure_rate=0.5, seed=42)
    def call_a():
        return "ok"

    @chaos(failure_rate=0.5, seed=42)
    def call_b():
        return "ok"

    results_a = []
    for _ in range(20):
        try:
            results_a.append(call_a())
        except ChaosInjectedError:
            results_a.append("failed")

    results_b = []
    for _ in range(20):
        try:
            results_b.append(call_b())
        except ChaosInjectedError:
            results_b.append("failed")

    assert results_a == results_b


def test_custom_exception_type():
    class MyDependencyError(Exception):
        pass

    @chaos(failure_rate=1.0, exception_type=MyDependencyError, seed=1)
    def call():
        return "ok"

    with pytest.raises(MyDependencyError):
        call()


def test_latency_injection_sync():
    @chaos(failure_rate=0.0, latency_seconds=0.05, seed=1)
    def call():
        return "ok"

    start = time.monotonic()
    call()
    elapsed = time.monotonic() - start

    assert elapsed >= 0.05


@pytest.mark.asyncio
async def test_latency_injection_async():
    @chaos(failure_rate=0.0, latency_seconds=0.05, seed=1)
    async def call():
        return "ok"

    start = time.monotonic()
    await call()
    elapsed = time.monotonic() - start

    assert elapsed >= 0.05


@pytest.mark.asyncio
async def test_async_failure():
    @chaos(failure_rate=1.0, seed=1)
    async def call():
        await asyncio.sleep(0)
        return "ok"

    with pytest.raises(ChaosInjectedError):
        await call()


def test_shared_injector_across_functions():
    injector = Chaos(failure_rate=1.0, seed=1)

    @chaos(injector=injector)
    def a():
        return "a"

    @chaos(injector=injector)
    def b():
        return "b"

    with pytest.raises(ChaosInjectedError):
        a()
    with pytest.raises(ChaosInjectedError):
        b()


def test_invalid_failure_rate_raises():
    with pytest.raises(ValueError):
        Chaos(failure_rate=1.5)
    with pytest.raises(ValueError):
        Chaos(failure_rate=-0.1)


def test_invalid_latency_raises():
    with pytest.raises(ValueError):
        Chaos(latency_seconds=-1)
    with pytest.raises(ValueError):
        Chaos(latency_jitter_seconds=-1)


def test_default_construction_never_fails_or_delays():
    @chaos()
    def call():
        return "ok"

    start = time.monotonic()
    assert call() == "ok"
    assert time.monotonic() - start < 0.05
