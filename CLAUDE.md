# latch — Idempotency Middleware for LLM Agent Tool Calls

This file is the persistent context for Claude Code working in this repo. Read it fully before making changes. Keep it updated as phases complete.

## Mission

`latch` is a small, dependency-free Python library that makes LLM agent tool calls safe to retry. When an agent's tool call times out or errors, the agent doesn't know whether the underlying action actually completed — retrying blindly can double-charge a card, send a duplicate email, or create a duplicate order. `latch` prevents that: wrap a tool function with `@idempotent`, pass a unique `idempotency_key` per logical operation, and repeated calls with the same key return the cached result instead of re-executing.

## Why this exists (prior art — be honest about it)

This is not an unclaimed idea. Before building, we searched and found:
- **SagaLLM** (arXiv 2503.11951) — academic paper applying the Saga pattern to multi-agent LLM planning with compensation.
- **Robust Agent Compensation (RAC)** (ACM CAIS '26) — academic paper on teaching agents to compensate for failures.
- **ReliabilityBench** (arXiv 2601.06112) — a benchmark for agent reliability under stress, not a library.
- Multiple engineering blog posts (Zylos Research, MightyBot, Chanl, TianPan.co) describe idempotency-for-agents as a pattern, but none of them shipped a polished, adoptable, framework-agnostic OSS package for it.

**The gap we're actually filling:** nobody has shipped the practical, pip-installable version of this pattern. The contribution of v0.1 is execution and adoptability, not novelty of the underlying idea. Don't oversell originality in the README or paper — cite the above as prior art and position `latch` as "the missing implementation," not "the first to think of this."

## Scope

### v0.1 (current phase — idempotency only)
- `@idempotent` decorator for sync and async functions
- Pluggable `IdempotencyStore` interface
- `InMemoryStore` (default, thread-safe, TTL-based expiry)
- Explicit, required `idempotency_key` kwarg (caller/agent framework supplies it — `latch` does not guess or auto-hash args, because silent auto-keying can hide bugs)
- Tests for sync, async, cache-hit, cache-miss, missing-key error, and TTL expiry
- One usage example for a plain function and one for an OpenAI-style tool call

### Explicit non-goals for v0.1 (do not build yet)
- Circuit breaker (v0.2)
- Timeout/cancellation propagation (v0.2)
- Budget/cost guardrails (v0.2)
- Saga/compensation (v0.3)
- Framework adapters beyond a documented example (v0.3 — real adapter package)
- Redis store (stub the interface only; implement in v0.2 as an optional extra)
- Distributed tracing/observability (v0.4)

Resist scope creep. Ship v0.1 narrow and solid before touching v0.2.

## Roadmap (from the 90-day plan)

- [x] v0.1 — Idempotency core + in-memory store + tests + docs
- [ ] v0.2 — Circuit breaker, timeout/cancellation, budget guardrails, Redis store
- [ ] v0.3 — Saga/compensation pattern, real OpenAI + LangChain adapter modules
- [ ] v0.4 — Chaos-injection benchmark harness, example agents (before/after), tracing hooks
- [ ] v1.0 — Docs site, public launch, paper submitted to arXiv

Update the checkboxes above as phases land.

## API design (already decided — implement to this contract)

```python
from latch import idempotent, InMemoryStore

store = InMemoryStore()

@idempotent(store=store, ttl_seconds=86400)
def create_order(order_id: str, amount: float) -> dict:
    # idempotency_key is consumed by the decorator, not passed to this function
    ...
    return {"order_id": order_id, "status": "created"}

# Agent framework supplies a unique key per logical operation:
create_order(order_id="A1", amount=42.0, idempotency_key="agent-run-7-step-3")
```

Key behaviors:
- `idempotency_key` is a required keyword argument at call time. If missing, raise `IdempotencyKeyMissingError` — never silently generate one.
- On a cache hit within the TTL window, return the cached result without calling the wrapped function.
- On a cache miss, execute the function, store the result keyed by `idempotency_key`, and return it.
- Must work transparently on both `def` and `async def` functions.
- No required third-party dependencies for the core. Redis support is an optional extra (`pip install latch-idempotent[redis]`).

## Architecture principles

- Core has zero required dependencies — anyone can `pip install` it without dragging in redis/httpx/etc.
- `IdempotencyStore` is an abstract interface (`get`, `set`, `exists`) so storage backends are swappable.
- Every public function is type-hinted; run `mypy` clean.
- Every behavior has a test before being considered done — no untested code paths in `core.py`.
- Keep files small and single-purpose (`core.py`, `stores/memory.py`, `exceptions.py` — don't merge concerns).

## Conventions

- Format/lint with `ruff`; format with `black` (or ruff format).
- Conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
- Every new feature: implementation + tests + README update in the same commit/PR.
- Run `pytest` before considering any task complete.

## Definition of done for v0.1

1. `pytest` passes with tests covering: sync dedup, async dedup, different-key re-execution, missing-key error, TTL expiry.
2. `mypy src/latch` is clean.
3. README has a working quickstart example that a stranger could copy-paste and run.
4. Package builds (`python -m build`) and is ready to publish to PyPI (actual publish is a manual step — don't automate credentials).
5. `examples/openai_example.py` shows the decorator wrapping an OpenAI-style tool function.

## Immediate next tasks (work through in order)

1. Verify the scaffolded code in `src/latch/` builds and `pytest` passes as-is.
2. Add TTL-expiry test (not yet covered by the scaffold — see `tests/test_core.py` TODO).
3. Write `examples/openai_example.py`.
4. Set up `ruff` + `mypy` config and run them clean.
5. Set up GitHub Actions CI (test + lint on push).
6. Once v0.1 is solid: write the launch README section, tag `v0.1.0`, publish to PyPI, post on GitHub.
7. Only then move to v0.2 (circuit breaker) — do not start v0.2 work before v0.1 is tagged and published.

## Non-negotiables

- Don't silently swallow errors — if the wrapped function raises, `latch` should propagate the exception and NOT cache a failed result.
- Don't auto-generate idempotency keys from function args by default — that's a footgun (subtly different args should sometimes get different keys, sometimes not, and only the caller knows which).
- Keep the public API tiny. Every new parameter on `idempotent()` should justify its complexity cost.
