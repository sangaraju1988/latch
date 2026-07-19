class LatchError(Exception):
    """Base exception for all latch errors."""


class IdempotencyKeyMissingError(LatchError):
    """Raised when a decorated function is called without a required
    `idempotency_key` keyword argument.

    latch never generates idempotency keys automatically — the caller
    (typically the agent framework or orchestration layer) must supply
    a key that uniquely identifies the logical operation being performed.
    """


class CircuitOpenError(LatchError):
    """Raised when `@circuit_breaker` rejects a call because the circuit
    is open.

    The wrapped function was NOT invoked. This is the "fail fast instead
    of hammering a known-broken dependency" signal — callers (typically
    the agent framework) should treat it as a retryable-later error, not
    a permanent failure of this specific logical operation.
    """


class LatchTimeoutError(LatchError):
    """Raised when `@with_timeout` aborts a call that exceeded its
    deadline.

    For sync functions, the underlying call may still be running in a
    background thread when this is raised (Python cannot forcibly
    cancel a running thread) — see `latch.timeout` module docs.
    """


class BudgetExceededError(LatchError):
    """Raised when `@budget_guardrail` rejects a call because it would
    exceed the configured call-count or cost budget for the current
    window.

    The wrapped function was NOT invoked, so no cost was incurred by the
    call that raised this.
    """
