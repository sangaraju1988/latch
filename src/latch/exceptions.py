class LatchError(Exception):
    """Base exception for all latch errors."""


class IdempotencyKeyMissingError(LatchError):
    """Raised when a decorated function is called without a required
    `idempotency_key` keyword argument.

    latch never generates idempotency keys automatically — the caller
    (typically the agent framework or orchestration layer) must supply
    a key that uniquely identifies the logical operation being performed.
    """
