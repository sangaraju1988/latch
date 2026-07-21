import asyncio
import functools
import inspect
import threading
import weakref
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

from latch.exceptions import IdempotencyKeyMissingError
from latch.stores.base import IdempotencyStore
from latch.stores.memory import InMemoryStore
from latch.tracing import Tracer

F = TypeVar("F", bound=Callable[..., Any])

_DEFAULT_STORE: IdempotencyStore = InMemoryStore()
_DEFAULT_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# Per-storage-key locks so concurrent calls that share a key (not just
# sequential retries) don't both observe a cache miss and both execute the
# wrapped function -- see _get_sync_key_lock / _get_async_key_lock below.
# WeakValueDictionary so a key's lock is reclaimed once nothing is using
# it, rather than growing forever for every distinct key ever seen by this
# process.
_sync_key_locks: "weakref.WeakValueDictionary[str, threading.RLock]" = weakref.WeakValueDictionary()
_sync_key_locks_guard = threading.Lock()

_async_key_locks: "weakref.WeakValueDictionary[str, asyncio.Lock]" = weakref.WeakValueDictionary()
_async_key_locks_guard = threading.Lock()


def _get_sync_key_lock(storage_key: str) -> threading.RLock:
    with _sync_key_locks_guard:
        lock = _sync_key_locks.get(storage_key)
        if lock is None:
            lock = threading.RLock()
            _sync_key_locks[storage_key] = lock
        return lock


def _get_async_key_lock(storage_key: str) -> asyncio.Lock:
    # Only ever called from inside a running coroutine (async_wrapper), so
    # asyncio.Lock() always binds to the caller's running loop -- no
    # ambiguity about which loop owns it.
    with _async_key_locks_guard:
        lock = _async_key_locks.get(storage_key)
        if lock is None:
            lock = asyncio.Lock()
            _async_key_locks[storage_key] = lock
        return lock


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
                storage_key = _storage_key(func, key)
                # Hold the per-key lock across the whole check-then-act
                # sequence so two concurrent callers with the same key
                # can't both observe a cache miss and both run the side
                # effect -- the second waits and gets the first's cached
                # result instead of racing it. See _get_async_key_lock.
                async with _get_async_key_lock(storage_key):
                    found, cached = _lookup(active_store, storage_key)
                    if found:
                        if on_duplicate is not None:
                            on_duplicate(key, cached)
                        if tracer is not None:
                            tracer.emit("idempotent", "cache_hit", key=key)
                        return cached
                    if tracer is not None:
                        tracer.emit("idempotent", "cache_miss", key=key)
                    result = await func(*args, **kwargs)
                    active_store.set(storage_key, result, ttl_seconds)
                    if tracer is not None:
                        tracer.emit("idempotent", "stored", key=key)
                    return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _extract_key(kwargs, key_arg)
            storage_key = _storage_key(func, key)
            # See the async_wrapper comment above -- same rationale, a
            # threading.RLock so a caller whose wrapped function
            # re-enters the same key on the same thread doesn't deadlock.
            with _get_sync_key_lock(storage_key):
                found, cached = _lookup(active_store, storage_key)
                if found:
                    if on_duplicate is not None:
                        on_duplicate(key, cached)
                    if tracer is not None:
                        tracer.emit("idempotent", "cache_hit", key=key)
                    return cached
                if tracer is not None:
                    tracer.emit("idempotent", "cache_miss", key=key)
                result = func(*args, **kwargs)
                active_store.set(storage_key, result, ttl_seconds)
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


def _storage_key(func: Callable[..., Any], key: str) -> str:
    """Namespace the caller-supplied key by the wrapped function's identity
    before it ever reaches the store.

    Without this, two different `@idempotent`-decorated functions that
    happen to share a store (most commonly: both left `store=` unset and
    so both landed on the same process-wide `_DEFAULT_STORE`) would collide
    whenever a caller reused the same `idempotency_key` string for two
    different logical operations -- e.g. `"run-99-step-1"` used once for
    `create_order` and once for `send_email` in the same run. The second
    call would silently get back the first call's cached result instead of
    executing at all. `idempotency_key` uniqueness is the caller's
    responsibility *within one protected function*, but nothing about the
    public API asks callers to also account for every other
    `@idempotent`-decorated function in the process when choosing a key --
    so latch closes that gap itself rather than documenting it as a
    footgun. This only changes what's used as the *storage* key; the raw,
    caller-supplied `key` is still what's surfaced to `on_duplicate` and
    `tracer` events.
    """
    return f"{func.__module__}.{getattr(func, '__qualname__', func.__name__)}::{key}"


def _lookup(store: IdempotencyStore, storage_key: str) -> Tuple[bool, Any]:
    """Look up `storage_key`, distinguishing "no cached result" from "the
    cached result happens to be `None`" (or any other falsy value).

    `IdempotencyStore.get()` alone can't make that distinction -- it
    returns `None` for both "not present" and "present, and the value is
    `None`". A function that legitimately returns `None` (a fire-and-forget
    side effect with no meaningful return value) would otherwise never be
    deduped: every retry would look like a cache miss and re-execute the
    side effect, silently defeating idempotency for exactly the kind of
    call-with-no-return-value that's common for tool functions. Checking
    `exists()` first resolves the ambiguity without changing the
    `IdempotencyStore` ABC's `get()` contract, so custom store
    implementations don't need to change.
    """
    if store.exists(storage_key):
        return True, store.get(storage_key)
    return False, None
