import functools
import inspect
from typing import Any, Callable, Dict, Optional, TypeVar

from latch.exceptions import IdempotencyKeyMissingError
from latch.stores.base import IdempotencyStore
from latch.stores.memory import InMemoryStore
from latch.tracing import Tracer

F = TypeVar("F", bound=Callable[..., Any])

_DEFAULT_STORE: IdempotencyStore = InMemoryStore()
_DEFAULT_TTL_SECONDS = 24 * 60 * 60  # 24 hours


def idempotent(
    *,
    store: Optional[IdempotencyStore] = None,
    key_arg: str = "idempotency_key",
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    on_duplicate: Optional[Callable[[str, Any], None]] = None,
    tracer: Optional[Tracer] = None,
) -> Callable[[F], F]:
    """Make a tool function idempotent.

    The decorated function must be called with a keyword argument matching
    `key_arg` (default: "idempotency_key"). If a call with the same key has
    already completed successfully within `ttl_seconds`, the cached result
    is returned instead of re-executing the function.

    Works transparently on both sync (`def`) and async (`async def`)
    functions. The `key_arg` is consumed by the decorator and is not
    passed through to the wrapped function.

    Args:
        store: Idempotency storage backend. Defaults to a shared
            process-wide in-memory store if not provided.
        key_arg: Name of the keyword argument callers use to supply the
            idempotency key.
        ttl_seconds: How long a cached result remains valid.
        on_duplicate: Optional callback invoked as `on_duplicate(key, cached_result)`
            when a duplicate call is detected. Useful for logging/metrics.
        tracer: Optional `Tracer` (see `latch.tracing`). Emits
            `cache_hit(key)`, `cache_miss(key)`, and `stored(key)` events.

    Raises:
        IdempotencyKeyMissingError: if the caller does not supply `key_arg`.
    """
    active_store = store if store is not None else _DEFAULT_STORE

    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                key = _extract_key(kwargs, key_arg)
                cached = active_store.get(key)
                if cached is not None:
                    if on_duplicate is not None:
                        on_duplicate(key, cached)
                    if tracer is not None:
                        tracer.emit("idempotent", "cache_hit", key=key)
                    return cached
                if tracer is not None:
                    tracer.emit("idempotent", "cache_miss", key=key)
                result = await func(*args, **kwargs)
                active_store.set(key, result, ttl_seconds)
                if tracer is not None:
                    tracer.emit("idempotent", "stored", key=key)
                return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _extract_key(kwargs, key_arg)
            cached = active_store.get(key)
            if cached is not None:
                if on_duplicate is not None:
                    on_duplicate(key, cached)
                if tracer is not None:
                    tracer.emit("idempotent", "cache_hit", key=key)
                return cached
            if tracer is not None:
                tracer.emit("idempotent", "cache_miss", key=key)
            result = func(*args, **kwargs)
            active_store.set(key, result, ttl_seconds)
            if tracer is not None:
                tracer.emit("idempotent", "stored", key=key)
            return result

        return sync_wrapper  # type: ignore[return-value]

    return decorator


def _extract_key(kwargs: Dict[str, Any], key_arg: str) -> str:
    if key_arg not in kwargs or kwargs[key_arg] is None:
        raise IdempotencyKeyMissingError(
            f"Missing required idempotency key argument '{key_arg}'. "
            f"Callers (typically the agent framework) must supply a unique "
            f"key per logical operation so retries can be deduplicated."
        )
    # Pop so the wrapped function's own signature stays clean.
    return str(kwargs.pop(key_arg))
