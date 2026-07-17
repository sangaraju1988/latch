# latch

[![PyPI version](https://img.shields.io/pypi/v/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![Python versions](https://img.shields.io/pypi/pyversions/latch-idempotent.svg)](https://pypi.org/project/latch-idempotent/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Idempotency middleware for LLM agent tool calls. Prevents duplicate side effects (double-charged orders, duplicate emails, duplicate records) when an agent retries a tool call after a timeout or transient failure.

Zero required dependencies. Works with sync and async functions. Drop it into any agent framework — OpenAI tool calling, LangChain, or your own orchestration loop.

## The problem

An agent calls a tool. The call times out. The agent doesn't know if the underlying action completed before the timeout — it just knows it didn't get a response. Retrying is the only reasonable move, but if the tool isn't idempotent, retrying can execute the side effect twice.

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

See [`examples/`](examples/) for a plain-function example and an OpenAI-tool-call-shaped example.

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

Redis backend is planned for v0.2 (`pip install latch-idempotent[redis]`).

## Roadmap

- [x] **v0.1** — Idempotency core, in-memory store, sync + async support (shipped)
- [ ] **v0.2** — Circuit breaker, timeout/cancellation propagation, budget guardrails, Redis store
- [ ] **v0.3** — Saga/compensation pattern, OpenAI + LangChain adapter modules
- [ ] **v0.4** — Chaos-injection benchmark harness, example agents, tracing hooks
- [ ] **v1.0** — Docs site, public launch, companion paper

Full design notes and phase-by-phase plan are in [`CLAUDE.md`](CLAUDE.md).

## Contributing

Issues and PRs welcome. Before opening a PR: run `pytest`, `ruff check src tests`, and `mypy src/latch` — all three should pass clean. See `CLAUDE.md` for architecture principles and non-negotiables (no silent error swallowing, no auto-generated idempotency keys, zero required core dependencies).

## Prior art

This library exists alongside academic work on agent reliability — see [SagaLLM](https://arxiv.org/abs/2503.11951), [Robust Agent Compensation](https://arxiv.org/pdf/2605.03409), and [ReliabilityBench](https://arxiv.org/pdf/2601.06112). `latch`'s contribution is a small, adoptable, production-ready implementation of the idempotency pattern specifically — not a claim of inventing it.

## License

MIT
