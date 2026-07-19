# latch

[![PyPI version](https://img.shields.io/pypi/v/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![Python versions](https://img.shields.io/pypi/pyversions/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Reliability middleware for LLM agent tool calls: idempotency, circuit breaking, timeouts, and budget guardrails. Prevents duplicate side effects (double-charged orders, duplicate emails, duplicate records), retry storms against failing dependencies, hung tool calls, and runaway spend when an autonomous agent retries or loops on a tool call.

Zero required dependencies in the core. Works with sync and async functions. Drop it into any agent framework — OpenAI tool calling, LangChain, or your own orchestration loop.

## The problem

An agent calls a tool. The call times out. The agent doesn't know if the underlying action completed before the timeout — it just knows it didn't get a response. Retrying is the only reasonable move, but if the tool isn't idempotent, retrying can execute the side effect twice. And an agent loop that retries blindly can also pile load onto an already-failing dependency, block indefinitely on a hung call, or run up unbounded cost — idempotency alone doesn't solve those.

`latch` addresses all four with small, independent, composable primitives:

| Primitive | Decorator | Failure mode it prevents |
|---|---|---|
| Idempotency | `@idempotent` | Duplicate side effects on retry |
| Circuit breaker | `@circuit_breaker` | Retry storms against a failing dependency |
| Timeout | `@with_timeout` | A single tool call hanging the agent loop |
| Budget guardrail | `@budget_guardrail` | Unbounded call count / cost from a runaway loop |

Each works standalone. They also stack on the same function — see [`examples/resilient_tool_example.py`](examples/resilient_tool_example.py).

## Install

```bash
pip install latch-idempotent
```

## Quickstart

```python
from latch import idempotent

@idempotent()
def create_order(order_id: str, amount: float) -> dict:
    # ... call your payment/order API ...
    return {"order_id": order_id, "status": "created"}

# The agent framework supplies a unique key per logical operation.
result = create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")

# A retry with the same key returns the cached result instead of
# re-executing create_order — no duplicate order.
result_again = create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
assert result == result_again
```

Works the same way for `async def` tool functions.

See [`examples/`](examples/) for a plain-function example, an OpenAI-tool-call-shaped example, and a v0.2 example composing all four primitives.

## Circuit breaker

Stop retrying a tool call against a dependency that's already down. After `failure_threshold` consecutive failures the circuit opens and further calls are rejected immediately (`CircuitOpenError`) without invoking the wrapped function, until `recovery_timeout` elapses. Then one trial call is let through (half-open); success closes the circuit, failure reopens it.

```python
from latch import circuit_breaker, CircuitOpenError

@circuit_breaker(failure_threshold=5, recovery_timeout=30.0)
def call_flaky_api():
    ...

try:
    call_flaky_api()
except CircuitOpenError:
    # Failing fast — the dependency is known-down, don't pile on more load.
    ...
```

Share one `CircuitBreaker()` instance across multiple functions that hit the same dependency so they trip together; pass it via `circuit_breaker(breaker=my_breaker)`.

## Timeout

Bound how long a single tool call is allowed to block the agent loop.

```python
from latch import with_timeout, LatchTimeoutError

@with_timeout(seconds=10)
def slow_call():
    ...
```

`async def` functions are cancelled cooperatively via `asyncio.wait_for`. Sync functions run in a background thread that is *not* forcibly killed on timeout (Python has no safe way to do that) — the caller is unblocked and gets `LatchTimeoutError`, but make sure whatever the call was doing is safe to have running in the background if you retry it.

## Budget guardrail

Cap call count and/or cumulative cost within a time window, so a runaway agent loop can't blow through a spend limit.

```python
from latch import budget_guardrail, BudgetExceededError

@budget_guardrail(max_calls=100, window_seconds=3600)
def call_paid_api():
    ...

# Or cap by cost, priced per call:
@budget_guardrail(max_cost=50.0, window_seconds=3600, cost_fn=lambda tokens: tokens * 0.0001)
def call_llm(tokens: int):
    ...
```

A call that would exceed the budget raises `BudgetExceededError` and is never executed.

## How it works

- `idempotency_key` is a required keyword argument — `latch` never guesses or auto-generates one. The caller (your agent framework or orchestration code) decides what constitutes "the same logical operation."
- On first call with a given key, the function executes and the result is cached.
- On any subsequent call with the same key, within the TTL window (default 24h), the cached result is returned and the function is not re-executed.
- If the wrapped function raises, nothing is cached — the exception propagates normally so retries can still happen.

## Storage backends

Default is an in-memory store (single-process, not persistent across restarts). For multi-process or production deployments, use a distributed store:

```python
from latch import idempotent, InMemoryStore

store = InMemoryStore()

@idempotent(store=store, ttl_seconds=3600)
def send_email(to: str) -> dict:
    ...
```

Redis backend (`pip install latch-idempotent[redis]`) shares idempotency state across processes/machines:

```python
from latch import idempotent
from latch.stores.redis import RedisStore

store = RedisStore(url="redis://localhost:6379/0")

@idempotent(store=store, ttl_seconds=3600)
def send_email(to: str) -> dict:
    ...
```

## Roadmap

- [x] **v0.1** — Idempotency core, in-memory store, sync + async support (shipped)
- [x] **v0.2** — Circuit breaker, timeout/cancellation propagation, budget guardrails, Redis store (shipped)
- [ ] **v0.3** — Saga/compensation pattern, OpenAI + LangChain adapter modules
- [ ] **v0.4** — Chaos-injection benchmark harness, example agents, tracing hooks
- [ ] **v1.0** — Docs site, public launch, companion paper

Full design notes and phase-by-phase plan are in [`CLAUDE.md`](CLAUDE.md).

## Contributing

Issues and PRs welcome. Before opening a PR: run `pytest`, `ruff check src tests`, and `mypy src/latch` — all three should pass clean. See `CLAUDE.md` for architecture principles and non-negotiables (no silent error swallowing, no auto-generated idempotency keys, zero required core dependencies).

## Prior art

This library exists alongside academic work on agent reliability — see [SagaLLM](https://arxiv.org/abs/2503.11951), [Robust Agent Compensation](https://arxiv.org/pdf/2605.03409), and [ReliabilityBench](https://arxiv.org/pdf/2601.06112). Circuit breakers, timeouts, and budget caps are each well-established patterns individually (see e.g. `tenacity`, `pybreaker`, general resilience-engineering literature); `latch`'s contribution is composing them specifically for the LLM-agent-tool-call shape of the problem — a required, non-guessed `idempotency_key`, decorators that transparently support both sync and async tool functions, and a pluggable store abstraction shared by every primitive — as a small, adoptable, production-ready package, not a claim of inventing any one pattern in isolation.

## License

MIT
