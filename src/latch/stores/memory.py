import threading
import time
from typing import Any, Dict, Optional, Tuple

from latch.stores.base import IdempotencyStore


class InMemoryStore(IdempotencyStore):
    """Thread-safe in-memory idempotency store.

    Not persistent across process restarts and not shared across
    processes/nodes — suitable for development, tests, and single-process
    deployments. Use a distributed store (e.g. Redis, planned for v0.2)
    for multi-process or multi-node production deployments.
    """

    def __init__(self) -> None:
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        entry = self._valid_entry(key)
        return entry[0] if entry is not None else None

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        with self._lock:
            self._data[key] = (value, time.time() + ttl_seconds)

    def exists(self, key: str) -> bool:
        return self._valid_entry(key) is not None

    def _valid_entry(self, key: str) -> Optional[Tuple[Any, float]]:
        """Return `(value, expires_at)` if `key` is present and not
        expired, or `None` otherwise -- shared by `get()` and `exists()`
        so they can never disagree about whether a key is present. `get()`
        alone can't distinguish "not present" from "present, value is
        `None`" (both look like `None` to a caller); `exists()` must not
        make that same mistake by being implemented in terms of `get()`
        returning non-`None` -- that would silently break idempotency for
        any wrapped function whose return value is `None` (see
        `latch.core._lookup`, which relies on `exists()` being accurate
        independent of the stored value)."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            _, expires_at = entry
            if time.time() > expires_at:
                del self._data[key]
                return None
            return entry

    def clear(self) -> None:
        """Remove all entries. Primarily useful for tests."""
        with self._lock:
            self._data.clear()
