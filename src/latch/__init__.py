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
    "BudgetExceededError",
    "BudgetGuardrail",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "IdempotencyKeyMissingError",
    "IdempotencyStore",
    "InMemoryStore",
    "LatchError",
    "LatchTimeoutError",
    "LoggingTracer",
    "Saga",
    "SagaExecutionError",
    "SagaStep",
    "TraceEvent",
    "Tracer",
    "budget_guardrail",
    "circuit_breaker",
    "idempotent",
    "with_timeout",
]

__version__ = "0.4.1"

# RedisStore is intentionally NOT imported here: it lazily imports the
# optional `redis` package inside RedisStore.__init__ so that `import latch`
# never requires `redis` to be installed. Import it directly when needed:
#   from latch.stores.redis import RedisStore
