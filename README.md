# latch

[![PyPI version](https://img.shields.io/pypi/v/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![Python versions](https://img.shields.io/pypi/pyversions/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Reliability middleware for LLM agent tool calls: idempotency, circuit breaking, timeouts, budget guardrails, and saga/compensation for multi-step workflows. Prevents duplicate side effects (double-charged orders, duplicate emails, duplicate records), retry storms against failing dependencies, hung tool calls, runaway spend, and half-finished multi-step operations when an autonomous agent retries, loops, or fails partway through a sequence of tool calls.

Zero required dependencies in the core. Works with sync and async functions. Drop it into any agent framework — OpenAI tool calling, LangChain, or your own orchestration loop; see [Adapters](#adapters) for framework-specific helpers.

## The problem

An agent calls a tool. The call times out. The agent doesn't know if the underlying action completed before the timeout — it just knows it didn't get a response. Retrying is the only reasonable move, but if the tool isn't idempotent, retrying can execute the side effect twice. And an agent loop that retries blindly can also pile load onto an already-failing dependency, block indefinitely on a hung call, or run up unbounded cost. Beyond a single call, a multi-step task (charge card, then book flight, then book hotel) can fail partway through and leave real side effects with nothing to undo them. Idempotency alone doesn't solve any of the last four.

`latch` addresses all five with small, independent, composable primitives:

| Primitive | Decorator / class | Failure mode it prevents |
|---|---|---|
| Idempotency | `@idempotent` | Duplicate side effects on retry |
| Circuit breaker | `@circuit_breaker` | Retry storms against a failing dependency |
| Timeout | `@with_timeout` | A single tool call hanging the agent loop |
| Budget guardrail | `@budget_guardrail` | Unbounded call count / cost from a runaway loop |
| Saga / compensation | `Saga` | Half-finished multi-step operations when a later step fails |

Each works standalone. The four single-call primitives also stack on the same function — see [`examples/resilient_tool_example.py`](examples/resilient_tool_example.py).

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

See [`examples/`](examples/) for a plain-function example, an OpenAI-tool-call-shaped example, a v0.2 example composing all four single-call primitives, a v0.3 saga example, and v0.3 OpenAI/LangChain adapter examples.

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

## Saga / compensation

Undo a multi-step operation when a later step fails. Register steps in order, each with an optional compensating action; if a step raises, every already-completed step is compensated in reverse order before the failure is re-raised.

```python
from latch import Saga, SagaExecutionError

saga = Saga(name="book-trip")
saga.add_step(
    lambda: charge_card(order_id, amount),
    name="charge_card",
    compensation=lambda charge: refund_card(charge["charge_id"]),
)
saga.add_step(
    lambda: reserve_flight(order_id),
    name="reserve_flight",
    compensation=lambda res: cancel_flight(res["flight_reservation_id"]),
)
saga.add_step(lambda: reserve_hotel(order_id), name="reserve_hotel")

try:
    results = saga.run()
except SagaExecutionError as exc:
    # charge_card and reserve_flight were already compensated (in reverse
    # order) before this was raised. exc.step_name tells you where it
    # failed; exc.compensated_steps and exc.compensation_errors tell you
    # exactly what rollback did and didn't succeed at.
    ...
```

A step's `action`/`compensation` are zero-argument callables — bind arguments with a lambda or `functools.partial` before registering. `compensation`, if given, receives the value the action returned (e.g. a charge ID to refund). Rollback is best-effort: if one compensation fails, the rest still run, but the failure is captured on `SagaExecutionError.compensation_errors` rather than silently swallowed. `await saga.run_async()` supports `async def` actions/compensations (and plain sync ones mixed in); `saga.run()` raises a clear `TypeError` up front if it finds an async step instead of silently mis-invoking it. See [`examples/saga_example.py`](examples/saga_example.py) for a full success/failure walkthrough.

## Adapters

`latch.adapters` has real, importable (not just copy-paste-doc-snippet) helpers for wiring latch into specific agent frameworks — without making the framework's SDK a hard dependency of `latch`'s core. Import the one you need directly.

**OpenAI tool calling** (`latch.adapters.openai.dispatch_tool_call`): dispatches one `tool_calls[i]` entry from a chat completion to the matching Python function, deriving `idempotency_key` from the call's `id`, and returns an OpenAI-shaped tool response message.

```python
from latch.adapters.openai import dispatch_tool_call

message = dispatch_tool_call(
    tool_call,  # response.choices[0].message.tool_calls[0]
    tools={"send_email": send_email},  # send_email is @idempotent-wrapped
    run_id="run-42",
)
# {"role": "tool", "tool_call_id": ..., "content": "..."}
```

**LangChain** (`latch.adapters.langchain.resilient_tool`): wraps a plain function with whichever latch primitives you configure, ready to hand to `StructuredTool.from_function`.

```python
from latch.adapters.langchain import resilient_tool
from langchain_core.tools import StructuredTool

tool = StructuredTool.from_function(
    func=resilient_tool(charge_card, idempotency_store=store, breaker=breaker),
    name="charge_card",
)
```

See [`examples/openai_adapter_example.py`](examples/openai_adapter_example.py) and [`examples/langchain_adapter_example.py`](examples/langchain_adapter_example.py) for full, runnable versions (the LangChain example builds and invokes a real `StructuredTool` — no LLM or API key needed).

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

## Production considerations

`latch` is Alpha software (see the `Development Status` classifier on PyPI) — the primitives are well-tested for what they do, but "well-tested" and "understands every production topology" aren't the same claim. Know these before you rely on it:

- **`Saga` has no persistence.** Compensation runs in-process, synchronously, as part of the same `run()`/`run_async()` call that failed. If the process crashes or is killed mid-saga, nothing resumes or compensates automatically — `Saga` protects against a step raising an exception, not against the process dying. If you need crash-durable, resumable multi-step workflows, look at a workflow engine (Temporal, AWS Step Functions) instead; `Saga` is for the common case where a step's own exception is the failure mode you're guarding against.
- **`CircuitBreaker` and `BudgetGuardrail` state is per-process, in-memory.** Run several replicas of your service and each has its own circuit and budget — they don't coordinate unless you build a shared backend yourself. Only idempotency has a distributed option (`RedisStore`); the other three primitives don't ship one. If you're horizontally scaled and need a shared circuit/budget, you'll need to add that layer.
- **Sync `@with_timeout` doesn't kill the underlying call.** Python has no safe way to forcibly cancel a running thread, so a "timed out" sync call may still be executing in the background after `LatchTimeoutError` is raised. Only retry/react to the timeout in ways that are safe even if the original call eventually completes (this is exactly the scenario `@idempotent` is for). `async def` functions don't have this problem — they're cancelled cooperatively via `asyncio.wait_for`.
- **No built-in observability.** `@idempotent` has an `on_duplicate` callback; the other primitives don't yet have equivalent hooks for logging/metrics on state transitions (circuit open/close, budget rejection, saga compensation). Wire your own logging inside the functions you wrap, or around the `CircuitBreaker`/`BudgetGuardrail` instances directly, until tracing hooks land in v0.4.
- **Compatibility is verified statically for 3.9–3.12** (via `mypy --strict --python-version 3.9` and minimum-version analysis), and by the full test suite on whichever interpreter CI actually runs — check the badge/workflow status for the version you care about rather than assuming.

None of this means "don't use it" — it means use it for what it's scoped for: single-process agent tool-call reliability, not a distributed orchestration platform.

## Roadmap

- [x] **v0.1** — Idempotency core, in-memory store, sync + async support (shipped)
- [x] **v0.2** — Circuit breaker, timeout/cancellation propagation, budget guardrails, Redis store (shipped)
- [x] **v0.3** — Saga/compensation pattern, OpenAI + LangChain adapter modules (shipped)
- [ ] **v0.4** — Chaos-injection benchmark harness, example agents, tracing hooks
- [ ] **v1.0** — Docs site, public launch, companion paper

Full design notes and phase-by-phase plan are in [`CLAUDE.md`](CLAUDE.md).

## Contributing

Issues and PRs welcome. Before opening a PR: run `pytest`, `ruff check src tests`, and `mypy src/latch` — all three should pass clean. See `CLAUDE.md` for architecture principles and non-negotiables (no silent error swallowing, no auto-generated idempotency keys, zero required core dependencies).

## Prior art

This library exists alongside academic work on agent reliability — see [SagaLLM](https://arxiv.org/abs/2503.11951), [Robust Agent Compensation](https://arxiv.org/pdf/2605.03409), and [ReliabilityBench](https://arxiv.org/pdf/2601.06112). Circuit breakers, timeouts, and budget caps are each well-established patterns individually (see e.g. `tenacity`, `pybreaker`, general resilience-engineering literature); `latch`'s contribution is composing them specifically for the LLM-agent-tool-call shape of the problem — a required, non-guessed `idempotency_key`, decorators that transparently support both sync and async tool functions, and a pluggable store abstraction shared by every primitive — as a small, adoptable package (see [Production considerations](#production-considerations) for its current maturity), not a claim of inventing any one pattern in isolation.

## License

MIT
