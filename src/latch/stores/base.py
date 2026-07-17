from abc import ABC, abstractmethod
from typing import Any, Optional


class IdempotencyStore(ABC):
    """Interface for pluggable idempotency result storage.

    Implementations must be safe to call from concurrent contexts
    (threads and/or async tasks, depending on where the store is used).
    """

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Return the cached result for `key`, or None if not present
        or expired."""
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        """Store `value` under `key` with the given time-to-live."""
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if a non-expired entry exists for `key`."""
        raise NotImplementedError
