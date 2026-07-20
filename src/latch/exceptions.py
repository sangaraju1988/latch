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


class SagaExecutionError(LatchError):
    """Raised when a step in a `Saga` fails.

    All steps that had already completed successfully are compensated
    (in reverse order) before this is raised. Carries enough detail to
    tell the caller exactly what happened without them needing to parse
    the message string:

    - `step_name`: the step whose action raised.
    - `original_exception`: the exception the failing step raised (also
      set as `__cause__`, so `raise ... from original_exception` semantics
      apply and tracebacks chain normally).
    - `compensated_steps`: names of steps whose compensation ran
      successfully, in the order they were compensated (reverse of
      execution order).
    - `compensation_errors`: `(step_name, exception)` pairs for steps
      whose compensation itself raised. Compensation is best-effort — one
      failing compensation does not stop the rest from being attempted —
      but these errors are never silently swallowed; it is the caller's
      responsibility to inspect this list and decide what manual cleanup
      is needed.
    """

    def __init__(
        self,
        message: str,
        *,
        step_name: str,
        original_exception: BaseException,
        compensated_steps: "list[str]",
        compensation_errors: "list[tuple[str, BaseException]]",
    ) -> None:
        super().__init__(message)
        self.step_name = step_name
        self.original_exception = original_exception
        self.compensated_steps = compensated_steps
        self.compensation_errors = compensation_errors
