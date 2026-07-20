"""Redis-backed idempotency store.

Optional: requires `pip install latch-idempotent[redis]`. The core
package has zero required dependencies, so `redis` is imported lazily
inside `RedisStore.__init__`, not at module load time — importing
`latch.stores.redis` itself is always safe; only *instantiating*
`RedisStore` without the extra installed raises a clear error.

Use this instead of `InMemoryStore` whenever more than one process (or
more than one machine) needs to share idempotency state — e.g. an agent
framework running behind multiple workers, where a retry of the same
logical operation could land on a different worker than the original
call.

Values are pickled before storage so arbitrary Python objects (dicts,
dataclasses, etc.) round-trip the same way `InMemoryStore` handles them
in-process. Only unpickle data from a Redis instance you trust — this
follows the same caveat as Python's `pickle` module generally.
"""

import pickle
from typing import TYPE_CHECKING, Any, Optional

from latch.stores.base import IdempotencyStore

if TYPE_CHECKING:
    import redis as redis_module


class RedisStore(IdempotencyStore):
    """Idempotency store backed by Redis.

    Requires the `redis` package (`pip install latch-idempotent[redis]`).
    TTL is delegated to Redis's native key expiry (`SET ... EX`), so
    expired entries are cleaned up by Redis itself rather than on read.
    """

    def __init__(
        self,
        *,
        client: Optional["redis_module.Redis"] = None,
        url: Optional[str] = None,
        key_prefix: str = "latch:idempotency:",
        **redis_kwargs: Any,
    ) -> None:
        """
        Args:
            client: A pre-configured `redis.Redis` instance to reuse
                (e.g. one already connected to a shared pool). Takes
                precedence over `url`.
            url: A `redis://` connection URL, used to build a client if
                `client` is not provided. Defaults to `redis://localhost:6379/0`.
            key_prefix: Prefix applied to every key this store writes, so
                it can share a Redis instance with other applications
                without colliding.
            **redis_kwargs: Passed through to `redis.Redis.from_url` when
                building a client from `url`.
        """
        try:
            import redis as redis_module_import
        except ImportError as exc:
            raise ImportError(
                "RedisStore requires the 'redis' package. Install it with: "
                "pip install latch-idempotent[redis]"
            ) from exc

        if client is not None:
            self._client = client
        else:
            self._client = redis_module_import.Redis.from_url(
                url or "redis://localhost:6379/0", **redis_kwargs
            )

        self._key_prefix = key_prefix

    def _full_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    def get(self, key: str) -> Optional[Any]:
        raw = self._client.get(self._full_key(key))
        if raw is None:
            return None
        if isinstance(raw, str):
            # Only reachable if the client was configured with
            # decode_responses=True, which corrupts pickled bytes; latch
            # writes bytes and expects to read bytes back.
            raw = raw.encode("latin-1")
        if not isinstance(raw, (bytes, bytearray)):
            # redis-py's type stubs describe GET's return type as a broad
            # union (it's shared with the async client), and how far mypy
            # narrows it through the reassignment above has been observed
            # to differ across mypy/redis-py stub versions (this surfaced
            # as a real CI failure on Python 3.9 that didn't reproduce on
            # 3.10-3.12 with the same source). Narrow explicitly instead
            # of relying on that inference, which is also a legitimate
            # runtime safety check: an unexpected type here means either
            # a misconfigured client or a Redis response shape we don't
            # support, and that should fail loudly, not get passed to
            # pickle.loads and produce a confusing error two frames away.
            raise TypeError(
                f"Unexpected type from Redis GET for key {key!r}: {type(raw)!r}. "
                f"Expected bytes/bytearray/str; check the client isn't configured "
                f"in a way (e.g. a custom response callback) that changes GET's "
                f"return shape."
            )
        return pickle.loads(raw)

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        payload = pickle.dumps(value)
        if ttl_seconds > 0:
            self._client.set(self._full_key(key), payload, ex=ttl_seconds)
        else:
            # A non-positive TTL means "already expired" — don't write a
            # key that never expires; write nothing at all, matching
            # InMemoryStore's behavior where a 0s-TTL entry is immediately
            # stale on next read.
            return

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(self._full_key(key)))


__all__ = ["RedisStore"]
