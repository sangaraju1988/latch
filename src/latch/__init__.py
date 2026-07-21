from latch.budget import BudgetGuardrail, budget_guardrail
from latch.circuit_breaker import CircuitBreaker, CircuitState, circuit_breaker
from latch.core import idempotent
from latch.exceptions import (
    BudgetExceededError,
    CircuitOpenError,
    IdempotencyKeyMissingError,
    LatchError,
    LatchTimeoutError,
    SagaExecutionError,
)
from latch.saga import Saga, SagaStep
from latch.stores.base import IdempotencyStore
from latch.stores.memory import InMemoryStore
from latch.timeout import with_timeout
from latch.tracing import LoggingTracer, TraceEvent, Tracer

__all__ = [
    "idempotent",
    "IdempotencyStore",
    "InMemoryStore",
    "LatchError",
    "IdempotencyKeyMissingError",
    "CircuitBreaker",
    "CircuitState",
    "circuit_breaker",
    "CircuitOpenError",
    "with_timeout",
    "LatchTimeoutError",
    "BudgetGuardrail",
    "budget_guardrail",
    "BudgetExceededError",
    "Saga",
    "SagaStep",
    "SagaExecutionError",
    "Tracer",
    "TraceEvent",
    "LoggingTracer",
]

__version__ = "0.4.1"

# RedisStore is intentionally NOT imported here: it lazily imports the
# optional `redis` package inside RedisStore.__init__ so that `import latch`
# never requires `redis` to be installed. Import it directly when needed:
#   from latch.stores.redis import RedisStore
