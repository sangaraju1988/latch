"""Budget guardrails for LLM agent tool calls.

An autonomous agent loop can call a paid tool (an API with per-call cost,
a rate-limited third-party service, a metered internal endpoint) far more
times than any human would ever approve, especially under a retry loop or
a planning bug that causes it to re-enter the same step. `BudgetGuardrail`
caps either the *count* of calls or their *cumulative cost* within a
rolling or fixed window and raises `BudgetExceededError` once the cap is
hit, so the agent framework gets a clear, catchable signal instead of a
runaway bill.

This is intentionally simple (a fixed window, not a token-bucket rate
limiter) — the goal is a hard ceiling an agent cannot blow through, not
smooth traffic shaping.
"""

import functools
import inspect
import threading
import time
from typing import Any, Callable, Optional, TypeVar

from latch.exceptions import BudgetExceededError

F = TypeVar("F", bound=Callable[..., Any])

# Signature: cost_fn(*args, **kwargs) -> float, evaluated with the same
# arguments the wrapped function was called with (before the budget
# decorator strips anything), so it can price the call.
CostFn = Callable[..., float]


class BudgetGuardrail:
    """Tracks cumulative call count and/or cost within a fixed time window.

    Thread-safe. Shared across calls the same way a `CircuitBreaker` is:
    one instance per thing you want to cap (e.g. "spend on the pricing
    API this hour"), shared across every call site that should count
    against the same budget.
    """

    def __init__(
        self,
        *,
        max_calls: Optional[int] = None,
        max_cost: Optional[float] = None,
        window_seconds: Optional[float] = None,
    ) -> None:
        if max_calls is None and max_cost is None:
            raise ValueError("at least one of max_calls or max_cost must be set")
        if max_calls is not None and max_calls < 1:
            raise ValueError("max_calls must be >= 1")
        if max_cost is not None and max_cost < 0:
            raise ValueError("max_cost must be >= 0")
        if window_seconds is not None and window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")

        self.max_calls = max_calls
        self.max_cost = max_cost
        self.window_seconds = window_seconds

        self._lock = threading.Lock()
        self._window_start = time.monotonic()
        self._call_count = 0
        self._total_cost = 0.0

    def _maybe_roll_window(self) -> None:
        """Must be called while holding `self._lock`."""
        if self.window_seconds is None:
            return
        now = time.monotonic()
        if now - self._window_start >= self.window_seconds:
            self._window_start = now
            self._call_count = 0
            self._total_cost = 0.0

    def check_and_record(self, cost: float = 0.0) -> None:
        """Raise `BudgetExceededError` if recording this call would exceed
        the budget; otherwise record it.

        Checked *before* the wrapped function runs, so a call that would
        blow the budget never executes at all.
        """
        with self._lock:
            self._maybe_roll_window()

            if self.max_calls is not None and self._call_count + 1 > self.max_calls:
                raise BudgetExceededError(
                    f"Call budget exceeded: {self._call_count}/{self.max_calls} calls "
                    f"already used in this window."
                )
            projected_cost = self._total_cost + cost
            if self.max_cost is not None and projected_cost > self.max_cost:
                raise BudgetExceededError(
                    f"Cost budget exceeded: {self._total_cost:.4f} + {cost:.4f} would "
                    f"exceed max_cost={self.max_cost:.4f} in this window."
                )

            self._call_count += 1
            self._total_cost = projected_cost

    @property
    def call_count(self) -> int:
        with self._lock:
            self._maybe_roll_window()
            return self._call_count

    @property
    def total_cost(self) -> float:
        with self._lock:
            self._maybe_roll_window()
            return self._total_cost

    def reset(self) -> None:
        """Force the window to reset immediately. Primarily useful for tests."""
        with self._lock:
            self._window_start = time.monotonic()
            self._call_count = 0
            self._total_cost = 0.0


def budget_guardrail(
    *,
    guardrail: Optional[BudgetGuardrail] = None,
    max_calls: Optional[int] = None,
    max_cost: Optional[float] = None,
    window_seconds: Optional[float] = None,
    cost_fn: Optional[CostFn] = None,
) -> Callable[[F], F]:
    """Decorator form of `BudgetGuardrail`.

    Either pass a pre-built `guardrail` (to share budget across functions
    or inspect/reset it from calling code), or let one be created from
    `max_calls` / `max_cost` / `window_seconds`.

    `cost_fn`, if provided, is called with the same arguments as the
    wrapped function and must return the cost (in whatever unit you're
    budgeting) of this specific call — useful when different calls to the
    same tool have different prices (e.g. cost scales with token count or
    payload size).
    """
    active_guardrail = guardrail if guardrail is not None else BudgetGuardrail(
        max_calls=max_calls, max_cost=max_cost, window_seconds=window_seconds
    )

    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                cost = cost_fn(*args, **kwargs) if cost_fn is not None else 0.0
                active_guardrail.check_and_record(cost)
                return await func(*args, **kwargs)

            async_wrapper.budget_guardrail = active_guardrail  # type: ignore[attr-defined]
            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            cost = cost_fn(*args, **kwargs) if cost_fn is not None else 0.0
            active_guardrail.check_and_record(cost)
            return func(*args, **kwargs)

        sync_wrapper.budget_guardrail = active_guardrail  # type: ignore[attr-defined]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = ["BudgetGuardrail", "budget_guardrail"]
