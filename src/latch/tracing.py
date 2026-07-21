"""Observability hooks for LLM agent tool calls.

Every other module in `latch` makes a decision that's worth knowing about
in production: a cache hit dedupes a retry, a circuit trips open, a call
gets timed out, a budget rejects a call, a saga rolls back. Up through
v0.3 those decisions were invisible from the outside — you'd only notice
a circuit had opened when calls started raising `CircuitOpenError`, with
no record of *when* it tripped or *why*. `tracing` adds a small, optional,
zero-dependency event stream so callers can wire that into logging,
metrics, or a dashboard without `latch` taking a dependency on any
specific observability stack.

Design
------
- `TraceEvent` is a plain dataclass: `primitive`, `event`, `timestamp`,
  and a free-form `metadata` dict. No schema beyond that — different
  primitives emit different metadata keys, documented per-primitive
  below, but the envelope is uniform so one subscriber can handle events
  from every primitive.
- `Tracer` is a thread-safe pub/sub object, shared across primitives the
  same way a `CircuitBreaker` or `BudgetGuardrail` is: build one, pass it
  to every `tracer=` parameter you want events from, subscribe as many
  callbacks as you want. A subscriber that raises is not allowed to break
  the call it's tracing — errors are caught and swallowed at the point of
  emission (this is the one deliberate exception to latch's "never
  swallow errors" rule elsewhere: a broken logging callback must not take
  down the payment call it's merely observing).
- `LoggingTracer` is a batteries-included `Tracer` subclass that logs
  every event to the standard library `logging` module (logger name
  `"latch"`) instead of requiring callers to write their own subscriber
  for the common case of "just log this somewhere".
- Every `tracer=` parameter across the library defaults to `None` (no
  tracing), so this is fully opt-in and costs nothing when unused.

Event catalog
-------------
idempotent:        cache_hit(key), cache_miss(key), stored(key)
circuit_breaker:    state_changed(from_state, to_state), call_rejected,
                    call_succeeded, call_failed(exception)
with_timeout:        completed(seconds), timed_out(seconds)
budget_guardrail:    call_recorded(call_count, total_cost, cost),
                    budget_exceeded(reason)
saga:               step_started(step), step_succeeded(step),
                    step_failed(step, exception), compensation_started(step),
                    compensation_succeeded(step),
                    compensation_failed(step, exception),
                    saga_succeeded, saga_failed
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List

TraceCallback = Callable[["TraceEvent"], None]


@dataclass(frozen=True)
class TraceEvent:
    """One observed transition inside a latch primitive.

    `primitive` is the module that emitted it (`"idempotent"`,
    `"circuit_breaker"`, `"timeout"`, `"budget_guardrail"`, `"saga"`).
    `event` is a short, stable, snake_case name (see the module docstring
    for the full catalog per primitive). `metadata` carries whatever
    extra detail that specific event provides -- always a plain dict of
    JSON-serializable values, so events can be logged or shipped as-is.
    """

    primitive: str
    event: str
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Tracer:
    """Thread-safe event bus. Share one instance across every primitive
    you want events from; subscribe as many callbacks as you like.

    A subscriber callback that raises does not propagate -- it would
    otherwise turn "I attached a logger" into "I broke production
    payments", which defeats the point of an observability hook being
    optional. Exceptions from subscribers are silently discarded; if you
    need to know your subscriber is broken, that's a bug in the
    subscriber to catch in its own tests, not something `emit()` should
    surface to the caller of the traced primitive.
    """

    def __init__(self) -> None:
        self._subscribers: List[TraceCallback] = []
        self._lock = threading.Lock()

    def subscribe(self, callback: TraceCallback) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: TraceCallback) -> None:
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def emit(self, primitive: str, event: str, **metadata: Any) -> None:
        trace_event = TraceEvent(primitive=primitive, event=event, metadata=metadata)
        with self._lock:
            subscribers = list(self._subscribers)
        for callback in subscribers:
            try:
                callback(trace_event)
            except Exception:  # noqa: BLE001 -- deliberately broad, see class docstring
                continue


class LoggingTracer(Tracer):
    """A `Tracer` that logs every event to `logging.getLogger("latch")`
    at INFO level, formatted as `<primitive>.<event> {metadata}`.

    Use this when you just want events to show up in your application's
    existing logs without writing a subscriber:

        tracer = LoggingTracer()

        @circuit_breaker(tracer=tracer, failure_threshold=5)
        def call_flaky_api(): ...

    Configure the `"latch"` logger (level, handlers, formatting) the same
    way you configure any other logger in your application.
    """

    def __init__(self, logger: Any = None) -> None:
        super().__init__()
        self._logger = logger if logger is not None else logging.getLogger("latch")
        self.subscribe(self._log_event)

    def _log_event(self, trace_event: TraceEvent) -> None:
        self._logger.info(
            "%s.%s %s", trace_event.primitive, trace_event.event, trace_event.metadata
        )


__all__ = ["TraceEvent", "Tracer", "LoggingTracer"]
