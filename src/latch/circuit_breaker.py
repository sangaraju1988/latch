"""Circuit breaker for LLM agent tool calls.

An agent that keeps retrying a tool call against a downstream dependency
that is already down just amplifies the outage: every retry adds load,
latency, and (for paid APIs) cost, with no chance of success. A circuit
breaker stops that pattern by "opening" after a run of failures and
failing calls fast (raising `CircuitOpenError`) until the downstream has
had a chance to recover, instead of hammering it.

States
-------
CLOSED      Normal operation. Calls pass through. Failures are counted in
            a rolling window; once `failure_threshold` failures happen
            within that window, the circuit trips to OPEN.
OPEN        Calls are rejected immediately (raise `CircuitOpenError`)
            without invoking the wrapped function, for `recovery_timeout`
            seconds. This is the "stop hammering it" state.
HALF_OPEN   After `recovery_timeout` elapses, the next call is let
            through as a trial. Success -> CLOSED (reset). Failure ->
            back to OPEN and the recovery timer restarts.

The breaker is intentionally decoupled from `@idempotent`: they compose
(idempotency dedupes retries of the *same* logical operation; the circuit
breaker protects against retry storms across *many* operations hitting a
failing dependency) but neither requires the other.
"""

import functools
import inspect
import threading
import time
from enum import Enum
from typing import Any, Callable, Optional, Type, TypeVar

from latch.exceptions import CircuitOpenError
from latch.tracing import Tracer

F = TypeVar("F", bound=Callable[..., Any])


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Tracks failure/success state for a single protected call site.

    Thread-safe. One instance guards one logical dependency (e.g. "the
    payments API") and can be shared across multiple decorated functions
    if they should trip together, or given one-per-function for
    independent circuits.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        expected_exception: Type[BaseException] = Exception,
        tracer: Optional[Tracer] = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.tracer = tracer

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: Optional[float] = None
        self._half_open_trial_in_flight = False
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._resolve_state()

    def _resolve_state(self) -> CircuitState:
        """Must be called while holding `self._lock`."""
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._emit_state_changed(CircuitState.OPEN, CircuitState.HALF_OPEN)
        return self._state

    def _emit_state_changed(self, from_state: CircuitState, to_state: CircuitState) -> None:
        if self.tracer is not None:
            self.tracer.emit(
                "circuit_breaker",
                "state_changed",
                from_state=from_state.value,
                to_state=to_state.value,
            )

    def _before_call(self) -> bool:
        """Returns True if this call acquired the (single) half-open trial
        slot -- the caller must release it via `_on_success`/`_on_failure`
        or, for an exception type outside `expected_exception`,
        `_release_half_open_trial()` directly, so a bug in the wrapped
        function can never wedge the breaker in a permanently-rejecting
        state."""
        with self._lock:
            state = self._resolve_state()
            if state is CircuitState.OPEN:
                remaining = self.recovery_timeout - (time.monotonic() - (self._opened_at or 0.0))
                rejected = True
                acquired_trial = False
            elif state is CircuitState.HALF_OPEN:
                if self._half_open_trial_in_flight:
                    # Half-open allows exactly one trial call through at a
                    # time -- without this, every caller queued up while
                    # the circuit was open arrives the instant
                    # recovery_timeout elapses and all of them get let
                    # through simultaneously, hammering a dependency that
                    # has barely started recovering (the exact thing this
                    # primitive exists to prevent).
                    remaining = 0.0
                    rejected = True
                    acquired_trial = False
                else:
                    self._half_open_trial_in_flight = True
                    rejected = False
                    acquired_trial = True
            else:
                rejected = False
                acquired_trial = False
        if rejected:
            if self.tracer is not None:
                self.tracer.emit("circuit_breaker", "call_rejected")
            raise CircuitOpenError(
                f"Circuit is open; rejecting call without executing it. "
                f"Retry in ~{max(remaining, 0.0):.1f}s."
            )
        return acquired_trial

    def _release_half_open_trial(self) -> None:
        with self._lock:
            self._half_open_trial_in_flight = False

    def _on_success(self) -> None:
        with self._lock:
            was_open = self._state is not CircuitState.CLOSED
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None
            self._half_open_trial_in_flight = False
        if self.tracer is not None:
            self.tracer.emit("circuit_breaker", "call_succeeded")
            if was_open:
                self._emit_state_changed(CircuitState.HALF_OPEN, CircuitState.CLOSED)

    def _on_failure(self, exc: BaseException) -> None:
        with self._lock:
            state = self._resolve_state()
            if state is CircuitState.HALF_OPEN:
                # Trial call failed: back to OPEN, restart recovery timer.
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                self._half_open_trial_in_flight = False
                newly_opened = True
                from_state = CircuitState.HALF_OPEN
            else:
                self._failure_count += 1
                newly_opened = self._failure_count >= self.failure_threshold
                from_state = CircuitState.CLOSED
                if newly_opened:
                    self._state = CircuitState.OPEN
                    self._opened_at = time.monotonic()
        if self.tracer is not None:
            self.tracer.emit("circuit_breaker", "call_failed", exception=repr(exc))
            if newly_opened:
                self._emit_state_changed(from_state, CircuitState.OPEN)

    def reset(self) -> None:
        """Force the circuit back to CLOSED. Primarily useful for tests."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None
            self._half_open_trial_in_flight = False

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        acquired_trial = self._before_call()
        try:
            result = func(*args, **kwargs)
        except self.expected_exception as exc:
            self._on_failure(exc)
            raise
        except BaseException:
            if acquired_trial:
                self._release_half_open_trial()
            raise
        else:
            self._on_success()
            return result

    async def call_async(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        acquired_trial = self._before_call()
        try:
            result = await func(*args, **kwargs)
        except self.expected_exception as exc:
            self._on_failure(exc)
            raise
        except BaseException:
            if acquired_trial:
                self._release_half_open_trial()
            raise
        else:
            self._on_success()
            return result


def circuit_breaker(
    *,
    breaker: Optional[CircuitBreaker] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    expected_exception: Type[BaseException] = Exception,
    tracer: Optional[Tracer] = None,
) -> Callable[[F], F]:
    """Decorator form of `CircuitBreaker`.

    Either pass a pre-built `breaker` (to share state across functions or
    to inspect/reset it from calling code), or let one be created from
    `failure_threshold` / `recovery_timeout` / `expected_exception` /
    `tracer`. As with those other construction args, `tracer` is only
    used when building a *new* breaker -- if you pass a pre-built
    `breaker`, set its tracer when you construct it.

    `tracer` emits `state_changed(from_state, to_state)`,
    `call_rejected`, `call_succeeded`, and `call_failed(exception)`
    events (see `latch.tracing`).
    """
    active_breaker = breaker if breaker is not None else CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        expected_exception=expected_exception,
        tracer=tracer,
    )

    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await active_breaker.call_async(func, *args, **kwargs)

            async_wrapper.circuit_breaker = active_breaker  # type: ignore[attr-defined]
            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return active_breaker.call(func, *args, **kwargs)

        sync_wrapper.circuit_breaker = active_breaker  # type: ignore[attr-defined]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = ["CircuitBreaker", "CircuitState", "circuit_breaker"]
