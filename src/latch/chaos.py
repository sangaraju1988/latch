"""Chaos-injection testing utility for LLM agent tool calls.

Every other module in `latch` *protects* a call. `chaos` does the
opposite on purpose: it injects configurable failures and latency into a
function so you can verify your protection actually works, instead of
trusting it never gets exercised. Wrap a tool function in `@chaos(...)`
in a test or a local benchmark to simulate the payment API that's down
10% of the time, or the network call that occasionally takes 8 seconds --
then assert that whatever `latch` primitives you've stacked on top behave
the way you expect under that stress.

This is deliberately not a general-purpose fuzzing framework. It injects
exactly two things -- a probability of raising, and an amount of added
latency -- because that covers the two failure shapes every other
primitive in this library exists to handle (retries needing idempotency,
slow calls needing timeouts). Don't reach for this to simulate anything
more elaborate; write a purpose-built test double instead.

`benchmarks/chaos_benchmark.py` in the repository (not part of the
installed package) uses this module to compare a naive vs. a
`latch`-protected simulated agent loop under the same injected chaos --
that's the intended end-to-end use case this module is scoped for.
"""

import asyncio
import functools
import inspect
import random
import time
from typing import Any, Callable, Optional, Type, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class ChaosInjectedError(Exception):
    """Raised by `@chaos`-wrapped calls when the injected failure roll
    hits. Not a `LatchError` subclass -- this represents a simulated
    failure of *your* dependency, not a decision made by a latch
    primitive, the same way a real `requests.ConnectionError` wouldn't
    be a latch exception either.
    """


class Chaos:
    """Injects a configurable probability of failure and/or added latency
    into calls. Shareable across call sites the same way `CircuitBreaker`
    and `BudgetGuardrail` are, if you want several functions to draw from
    the same simulated failure profile and RNG stream.
    """

    def __init__(
        self,
        *,
        failure_rate: float = 0.0,
        latency_seconds: float = 0.0,
        latency_jitter_seconds: float = 0.0,
        exception_type: Type[BaseException] = ChaosInjectedError,
        seed: Optional[int] = None,
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be between 0.0 and 1.0")
        if latency_seconds < 0:
            raise ValueError("latency_seconds must be >= 0")
        if latency_jitter_seconds < 0:
            raise ValueError("latency_jitter_seconds must be >= 0")

        self.failure_rate = failure_rate
        self.latency_seconds = latency_seconds
        self.latency_jitter_seconds = latency_jitter_seconds
        self.exception_type = exception_type
        self._rng = random.Random(seed)

    def _roll_latency(self) -> float:
        if self.latency_seconds == 0 and self.latency_jitter_seconds == 0:
            return 0.0
        jitter = self._rng.uniform(0, self.latency_jitter_seconds)
        return self.latency_seconds + jitter

    def _roll_failure(self) -> bool:
        return self._rng.random() < self.failure_rate

    def before_call(self) -> float:
        """Roll for latency and failure. Returns the latency (seconds) the
        caller should sleep/await *before* deciding whether to raise --
        callers apply the delay themselves so sync and async call sites
        can sleep the right way (`time.sleep` vs `asyncio.sleep`)."""
        return self._roll_latency()

    def maybe_raise(self) -> None:
        if self._roll_failure():
            raise self.exception_type(f"Chaos-injected failure (failure_rate={self.failure_rate})")


def chaos(
    *,
    injector: Optional[Chaos] = None,
    failure_rate: float = 0.0,
    latency_seconds: float = 0.0,
    latency_jitter_seconds: float = 0.0,
    exception_type: Type[BaseException] = ChaosInjectedError,
    seed: Optional[int] = None,
) -> Callable[[F], F]:
    """Decorator form of `Chaos`.

    Either pass a pre-built `injector` (to share a failure profile and
    RNG stream across functions, or to inspect it from calling code), or
    let one be created from `failure_rate` / `latency_seconds` /
    `latency_jitter_seconds` / `exception_type` / `seed`.

    On each call: sleeps for the rolled latency (added to
    `latency_jitter_seconds` uniformly, if set), then rolls for failure
    and raises `exception_type` (default `ChaosInjectedError`) at
    `failure_rate` probability *before* invoking the wrapped function --
    a chaos-injected failure never actually runs the real call, the same
    way a real downstream outage would never have run it either.

    Works transparently on both sync and async functions.
    """
    active_injector = (
        injector
        if injector is not None
        else Chaos(
            failure_rate=failure_rate,
            latency_seconds=latency_seconds,
            latency_jitter_seconds=latency_jitter_seconds,
            exception_type=exception_type,
            seed=seed,
        )
    )

    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                delay = active_injector.before_call()
                if delay > 0:
                    await asyncio.sleep(delay)
                active_injector.maybe_raise()
                return await func(*args, **kwargs)

            async_wrapper.chaos = active_injector  # type: ignore[attr-defined]
            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = active_injector.before_call()
            if delay > 0:
                time.sleep(delay)
            active_injector.maybe_raise()
            return func(*args, **kwargs)

        sync_wrapper.chaos = active_injector  # type: ignore[attr-defined]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = ["Chaos", "chaos", "ChaosInjectedError"]
