"""Timeout / cancellation propagation for LLM agent tool calls.

Agent orchestration loops often impose a deadline on a step ("this tool
call gets 10 seconds") but the tool function itself has no idea — a slow
network call, a hung subprocess, or a stuck DB query can block well past
what the caller was willing to wait, tying up the agent loop.

`@with_timeout` enforces the deadline at the call site:

- `async def` functions are cancelled via `asyncio.wait_for`, which is
  true cooperative cancellation — the coroutine gets a
  `asyncio.CancelledError` injected at its next await point.
- `def` (sync) functions cannot be safely cancelled mid-execution in
  Python (no cooperative cancellation point), so the call runs in a
  daemon thread and the *caller* is unblocked at the deadline by raising
  `LatchTimeoutError`. The underlying thread is not killed — it keeps
  running in the background until it finishes or the process exits. This
  is the same tradeoff every sync-timeout wrapper in the ecosystem makes
  (e.g. `func_timeout`, `stopit`); it is documented here so it isn't a
  surprise.
"""

import asyncio
import functools
import inspect
import threading
import time
from typing import Any, Callable, Optional, TypeVar

from latch.exceptions import LatchTimeoutError
from latch.tracing import Tracer

F = TypeVar("F", bound=Callable[..., Any])


def with_timeout(*, seconds: float, tracer: Optional[Tracer] = None) -> Callable[[F], F]:
    """Enforce a wall-clock deadline on a tool call.

    Raises `LatchTimeoutError` if the wrapped function does not complete
    within `seconds`.

    Args:
        seconds: Deadline in seconds. Must be > 0.
        tracer: Optional `Tracer` (see `latch.tracing`). Emits
            `completed(seconds)` or `timed_out(seconds)`.
    """
    if seconds <= 0:
        raise ValueError("seconds must be > 0")

    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.monotonic()
                try:
                    result = await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
                except asyncio.TimeoutError as exc:
                    if tracer is not None:
                        tracer.emit("timeout", "timed_out", seconds=seconds)
                    raise LatchTimeoutError(
                        f"{func.__name__} did not complete within {seconds}s"
                    ) from exc
                if tracer is not None:
                    tracer.emit("timeout", "completed", seconds=time.monotonic() - start)
                return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            result_box: "list[Any]" = []
            error_box: "list[BaseException]" = []

            def target() -> None:
                try:
                    result_box.append(func(*args, **kwargs))
                except BaseException as exc:  # noqa: BLE001 - re-raised in caller thread
                    error_box.append(exc)

            start = time.monotonic()
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            thread.join(timeout=seconds)

            if thread.is_alive():
                if tracer is not None:
                    tracer.emit("timeout", "timed_out", seconds=seconds)
                raise LatchTimeoutError(
                    f"{func.__name__} did not complete within {seconds}s "
                    f"(the underlying call is still running in the background "
                    f"and cannot be forcibly killed; make sure it is safe to "
                    f"retry if you call it again)"
                )
            if tracer is not None:
                tracer.emit("timeout", "completed", seconds=time.monotonic() - start)
            if error_box:
                raise error_box[0]
            return result_box[0]

        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = ["with_timeout"]
