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
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if time.time() > expires_at:
                del self._data[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        with self._lock:
            self._data[key] = (value, time.time() + ttl_seconds)

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def clear(self) -> None:
        """Remove all entries. Primarily useful for tests."""
        with self._lock:
            self._data.clear()
