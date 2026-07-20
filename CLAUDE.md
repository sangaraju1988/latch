# latch — Reliability Middleware for LLM Agent Tool Calls

This file is the persistent context for Claude Code working in this repo. Read it fully before making changes. Keep it updated as phases complete.

## Mission

`latch` is a small Python library (zero required dependencies in the core) that makes LLM agent tool calls safe to retry and hard to run away with. When an agent's tool call times out or errors, the agent doesn't know whether the underlying action actually completed — retrying blindly can double-charge a card, send a duplicate email, or create a duplicate order. Beyond that, an autonomous agent loop can also hammer an already-failing dependency, block indefinitely on a hung call, blow through a cost budget, or fail partway through a multi-step operation and leave real side effects with nothing to undo them. `latch` addresses all five with independent, composable primitives: `@idempotent` (dedupe retries), `@circuit_breaker` (fail fast against a known-down dependency), `@with_timeout` (bound call duration), `@budget_guardrail` (cap call count/cost per window), and `Saga` (compensate a multi-step sequence when a later step fails). `latch.adapters` layers framework-specific glue (OpenAI tool calling, LangChain) on top without making those SDKs hard dependencies of the core.

## Why this exists (prior art — be honest about it)

This is not an unclaimed idea. Before building, we searched and found:
- **SagaLLM** (arXiv 2503.11951) — academic paper applying the Saga pattern to multi-agent LLM planning with compensation.
- **Robust Agent Compensation (RAC)** (ACM CAIS '26) — academic paper on teaching agents to compensate for failures.
- **ReliabilityBench** (arXiv 2601.06112) — a benchmark for agent reliability under stress, not a library.
- Multiple engineering blog posts (Zylos Research, MightyBot, Chanl, TianPan.co) describe idempotency-for-agents as a pattern, but none of them shipped a polished, adoptable, framework-agnostic OSS package for it.

**The gap we're actually filling:** nobody has shipped the practical, pip-installable version of this pattern. The contribution of v0.1 is execution and adoptability, not novelty of the underlying idea. Don't oversell originality in the README or paper — cite the above as prior art and position `latch` as "the missing implementation," not "the first to think of this."

## Packaging decision (v0.2)

**Decision: v0.2 ships inside the same package (`latch-idempotent`), not a new one.**

Rationale:
- v0.2's four primitives (idempotency, circuit breaker, timeout, budget guardrail) are independent but share one thesis — "reliability middleware for LLM agent tool calls" — and one architecture (decorator + pluggable backend). Splitting into separate packages would fragment that thesis across repos for no technical reason.
- For the EB1A original-contribution narrative, one package with a continuous version history, a growing star/download/citation trail, and a single canonical name to cite is stronger evidence of sustained original contribution than several disconnected v0.x packages that each look like an abandoned experiment next to the others.
- The v0.1 CLAUDE.md roadmap already committed to this: v0.2 was scoped as "circuit breaker, timeout/cancellation, budget guardrails, Redis store" as part of *this* project's 90-day plan, not as a new project.
- Precedent: comparable resilience libraries (`tenacity`, `pybreaker`, `resilience4j` in the JVM world) ship all their primitives from one package with one identity.
- The one genuinely optional piece (Redis) is handled via `pip install latch-idempotent[redis]` — an extra, not a separate package — so "core stays dependency-free" is preserved without a package split.

A new package would only make sense for a future phase needing a fundamentally different distribution model (e.g. a framework adapter that pulls in LangChain/OpenAI SDKs as hard dependencies) — which is why v0.3's adapters are scoped as documented examples, not a dependency of the core package, and a real adapter package is explicitly deferred.

## Packaging decision (v0.3 adapters)

**Decision: `latch.adapters.openai` and `latch.adapters.langchain` ship as real, tested modules inside `latch-idempotent` — not inline doc snippets, and not a separate `latch-langchain`/`latch-openai` package.**

This looks like it contradicts the v0.2 packaging note above ("v0.3's adapters are scoped as documented examples... a real adapter package is explicitly deferred"), so the reconciliation is worth spelling out:

- The "documented example, not a dependency of the core package" framing was about avoiding a **hard dependency** on `openai`/`langchain`, not about avoiding real, importable, tested code. Both adapter modules achieve zero hard dependency by operating on duck-typed shapes (`tool_call.id` / `.function.name` / `.function.arguments` for OpenAI; a plain callable for LangChain) instead of importing the SDK types — the same trick `RedisStore` uses for `redis`, just without even needing a lazy `import` since no SDK object needs to be constructed by `latch` itself.
- Because neither module imports its target framework, there's no technical reason to fragment them into separate packages the way there would be if, say, `latch.adapters.langchain` needed to subclass `langchain_core.tools.BaseTool` (which would require `langchain-core` at import time). That fundamentally-different-distribution-model trigger from the v0.2 decision above hasn't been hit — so the same "one package, one identity, one EB1A-citable trail" reasoning from the v0.2 packaging decision applies again here.
- A **real standalone adapter package** (e.g. one that provides its own `BaseTool` subclasses, or auto-registers with an agent framework's plugin system) is still explicitly deferred — that's the "fundamentally different distribution model" case, and nothing in v0.3 needed it.
- `langchain-core` was added to the `dev` extra (not `dependencies`, not even a new `langchain` extra) purely so the optional integration test/example can exercise a real `StructuredTool` in CI; end users installing `latch-idempotent` never pull it in.

## Scope

### v0.3 (shipped 2026-07-20 — saga/compensation + framework adapters)
- `Saga` / `SagaStep` (`latch.saga`) — ordered multi-step execution with automatic reverse-order compensation on failure; sync (`run()`) and async (`run_async()`, mixed sync/async steps) support; imperative (`add_step`) and decorator (`@saga.step`) registration
- `SagaExecutionError` — carries `step_name`, `original_exception` (also set as `__cause__`), `compensated_steps`, `compensation_errors` (compensation failures are collected, never swallowed, and don't stop the rest of rollback from being attempted)
- `latch.adapters.openai.dispatch_tool_call` — OpenAI tool-call dispatch + idempotency-key derivation + response formatting, duck-typed against `tool_call.id`/`.function.name`/`.function.arguments`, zero hard dependency on `openai`
- `latch.adapters.langchain.resilient_tool` — composes the four single-call primitives onto a plain function for `StructuredTool.from_function`; patches `__signature__`/`__annotations__` so LangChain's pydantic-based schema inference doesn't silently drop `idempotency_key` (a real integration bug caught by testing against actual `langchain_core`, not assumed away) — zero hard dependency on `langchain`/`langchain_core`
- `examples/saga_example.py`, `examples/openai_adapter_example.py`, `examples/langchain_adapter_example.py`
- 27 new tests (15 saga, 11 adapters — including a real `langchain_core.tools.StructuredTool` integration test, not a mock — plus 1 regression test for the `RedisStore` fix below) on top of the 45 from v0.1+v0.2. 72 total.
- Fixed a pre-existing v0.2 bug in `RedisStore.get()` that only surfaced when CI actually ran on the full GitHub Actions matrix (`mypy --strict` failed on Python 3.9 only — see CHANGELOG for detail). This is why "confirm CI passes on the actual matrix" is a hard gate below, not a formality: it caught something local single-version testing didn't.
- `ruff` clean, `mypy --strict` clean, package builds and installs cleanly

See `CHANGELOG.md` for full detail.

### v0.2 (shipped 2026-07-19 — resilience primitives)
- `@circuit_breaker` / `CircuitBreaker` (`latch.circuit_breaker`) — closed/open/half-open state machine, sync + async, shareable across call sites via a passed-in `CircuitBreaker` instance
- `@with_timeout` (`latch.timeout`) — wall-clock deadline enforcement; `asyncio.wait_for` for async, daemon-thread bound for sync (documented non-forcible-kill tradeoff — Python cannot safely kill a running thread)
- `@budget_guardrail` / `BudgetGuardrail` (`latch.budget`) — call-count and/or cost cap per fixed window, optional per-call `cost_fn` for variable pricing
- `RedisStore` (`latch.stores.redis`) — optional-dependency `IdempotencyStore` backend, lazy-imports `redis` so core stays dependency-free
- New exceptions: `CircuitOpenError`, `LatchTimeoutError`, `BudgetExceededError`
- `examples/resilient_tool_example.py` — composes all four primitives on one function
- 45 tests total (7 v0.1 + 38 new): state transitions, half-open recovery, TTL/window expiry, construction-time validation, missing-optional-dependency error path (via `fakeredis`, no live Redis needed)
- `ruff` clean, `mypy --strict` clean, package builds and installs cleanly

See `CHANGELOG.md` for full detail.

### v0.1 (idempotency only — shipped)
- `@idempotent` decorator for sync and async functions
- Pluggable `IdempotencyStore` interface
- `InMemoryStore` (default, thread-safe, TTL-based expiry)
- Explicit, required `idempotency_key` kwarg (caller/agent framework supplies it — `latch` does not guess or auto-hash args, because silent auto-keying can hide bugs)
- Tests for sync, async, cache-hit, cache-miss, missing-key error, and TTL expiry
- One usage example for a plain function and one for an OpenAI-style tool call

### Explicit non-goals for v0.3 (do not build yet)
- A standalone `latch-langchain` / `latch-openai` adapter package with hard SDK dependencies (see "Packaging decision (v0.3 adapters)" above) — deferred until an adapter genuinely needs one
- Adapters for other frameworks (CrewAI, AutoGen, Semantic Kernel, etc.) — add only on real demand, not speculatively
- Parallel/fan-out saga steps, nested sagas, or saga persistence/replay across process restarts — v0.3's `Saga` is in-process and sequential only; that covers the common case and keeps the abstraction small
- Distributed tracing/observability (still v0.4)
- Chaos-injection benchmark harness, example agents (still v0.4)

### Explicit non-goals for v0.2 (met — kept for history)
- Saga/compensation (v0.3 — done)
- Framework adapters beyond a documented example (v0.3 — done, see reconciliation in "Packaging decision (v0.3 adapters)")
- Distributed tracing/observability (v0.4)
- Combining the four v0.2 primitives into a single "one decorator to rule them all" convenience wrapper — keep them independently composable (see `examples/resilient_tool_example.py` for the recommended stacking pattern) rather than adding a fifth abstraction on top

Resist scope creep. v0.4 (chaos-injection benchmark harness, example agents, tracing hooks) starts only after v0.3 is tagged and published, same discipline that gated v0.3 behind v0.2 and v0.2 behind v0.1.

## Roadmap (from the 90-day plan)

- [x] v0.1 — Idempotency core + in-memory store + tests + docs
- [x] v0.2 — Circuit breaker, timeout/cancellation, budget guardrails, Redis store
- [x] v0.3 — Saga/compensation pattern, real OpenAI + LangChain adapter modules
- [ ] v0.4 — Chaos-injection benchmark harness, example agents (before/after), tracing hooks
- [ ] v1.0 — Docs site, public launch, paper submitted to arXiv

Update the checkboxes above as phases land.

## API design (idempotency contract — already decided, implemented in v0.1; the v0.2 primitives follow the same decorator-factory shape; `Saga` (v0.3) intentionally does not — see Architecture principles below)

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

- Core has zero required dependencies — anyone can `pip install` it without dragging in redis/httpx/etc. `RedisStore` enforces this by lazily importing `redis` inside `__init__`, not at module load time.
- `IdempotencyStore` is an abstract interface (`get`, `set`, `exists`) so storage backends are swappable.
- Every public function is type-hinted; run `mypy --strict` clean.
- Every behavior has a test before being considered done — no untested code paths in any module under `src/latch/`.
- Keep files small and single-purpose (`core.py`, `circuit_breaker.py`, `timeout.py`, `budget.py`, `saga.py`, `stores/memory.py`, `stores/redis.py`, `exceptions.py`, `adapters/openai.py`, `adapters/langchain.py` — don't merge concerns).
- v0.2 primitives (`circuit_breaker`, `with_timeout`, `budget_guardrail`) each follow the same shape as `idempotent`: a decorator factory that optionally accepts a pre-built, shareable stateful object (`CircuitBreaker`, `BudgetGuardrail`) so callers can share state across multiple decorated functions, or let the decorator build a private one. Keep new *single-call* primitives consistent with this shape rather than inventing a new configuration style per module.
- `Saga` (v0.3) intentionally does NOT follow the decorator-factory shape — it orchestrates a *sequence* of calls, not one call, so it's a builder object (`add_step`/`step` to register, `run`/`run_async` to execute) instead. Don't force multi-step orchestration into the single-call decorator shape; don't force single-call primitives into the builder shape either. Pick the shape that matches what's being protected.
- Adapter modules (`latch.adapters.*`) never import their target framework's SDK at module scope — they operate on duck-typed shapes (a callable, an object with the attributes the SDK's real objects have) so `import latch` and `import latch.adapters.<x>` both stay dependency-free. This is a harder constraint than `RedisStore`'s lazy-import-in-`__init__` pattern (adapters don't even need a lazy import, since they never construct an SDK object themselves) — keep it that way rather than reaching for `TYPE_CHECKING`-gated real imports the first time it'd be convenient.

## Conventions

- Format/lint with `ruff`; format with `black` (or ruff format).
- Conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).
- Every new feature: implementation + tests + README update in the same commit/PR.
- Run `pytest` before considering any task complete.

## Definition of done for v0.1 (met)

1. `pytest` passes with tests covering: sync dedup, async dedup, different-key re-execution, missing-key error, TTL expiry.
2. `mypy src/latch` is clean.
3. README has a working quickstart example that a stranger could copy-paste and run.
4. Package builds (`python -m build`) and is ready to publish to PyPI (actual publish is a manual step — don't automate credentials).
5. `examples/openai_example.py` shows the decorator wrapping an OpenAI-style tool function.

## Definition of done for v0.2 (met)

1. `pytest` passes with all v0.1 tests plus new coverage for circuit breaker (state transitions, half-open recovery/reopen, unexpected-exception-type passthrough), timeout (sync + async deadline exceeded/not-exceeded, exception passthrough), budget guardrail (call-count cap, cost cap, window reset, `cost_fn`), and Redis store (roundtrip, TTL via native Redis expiry, key-prefix isolation, missing-optional-dependency error). 45 tests total.
2. `mypy --strict src/latch` is clean (verify with a pinned stable mypy — see note below).
3. README documents all four primitives with copy-pasteable examples, plus the "how they compose" table.
4. Package builds (`python -m build`) and installs cleanly; `import latch` requires no optional dependencies; `RedisStore()` gives a clear `ImportError` pointing at the `[redis]` extra if `redis` isn't installed.
5. `examples/resilient_tool_example.py` shows all four primitives stacked on one function.
6. `CHANGELOG.md` documents the release.

**mypy environment note:** the latest mypy (2.3.0 as of this writing) hit an unrelated internal error in this dev environment; pinning `mypy==1.13.0` runs clean. If CI or a contributor's mypy throws an `INTERNAL ERROR` unrelated to this codebase, try a pinned stable version before assuming the code is at fault.

## Definition of done for v0.3 (met)

1. `pytest` passes with all v0.1+v0.2 tests plus new coverage for `Saga` (all-succeed, mid-failure reverse-order rollback, no-compensation steps, compensation failure captured-not-swallowed with rollback continuing, empty saga, decorator registration, sync-rejects-async with a clear `TypeError`, async execution with mixed sync/async steps, error message/chaining) and both adapters (OpenAI dispatch happy path/unknown-tool/exception-propagation/`run_id` prefixing/opt-out flag, LangChain wrapping with zero/one/all layers, and — not mocked — a real `langchain_core.tools.StructuredTool` built and invoked, including the full-stack `idempotency_key` schema-inference regression test) and a regression test for the `RedisStore.get()` fix (see CHANGELOG). 72 tests total.
2. `mypy --strict src/latch` clean (same pinned-mypy caveat as v0.2).
3. README documents `Saga` and both adapters with copy-pasteable examples; roadmap checklist updated.
4. Package builds and installs cleanly; `import latch` and `import latch.adapters.openai`/`import latch.adapters.langchain` all require no optional dependencies.
5. `examples/saga_example.py` (success + rollback walkthrough), `examples/openai_adapter_example.py`, `examples/langchain_adapter_example.py` (builds and invokes a real `StructuredTool`) all run standalone and were actually executed, not just written.
6. `CHANGELOG.md` documents the release.

## Immediate next tasks (v0.4 — work through in order)

v0.3 is code-complete, tested, and documented as of 2026-07-20. Companion article (`latch-article.docx`) was caught up through v0.3 on 2026-07-20 — no longer outstanding. Before starting v0.4:

1. Push to `main` and confirm CI is green on the actual GitHub Actions matrix (3.9–3.12). Done once already: the first push (commit `ab101bf`) failed on Python 3.9 only — `mypy --strict` caught a real pre-existing bug in `RedisStore.get()` that static local checks (`vermin`, `mypy --python-version 3.9` run by hand) had missed, because the failure came from mypy/redis-py stub *version resolution* differing per interpreter, not from source-level 3.9 incompatibility. Fixed in commit `9639748`. Confirms the CI-gate discipline in this file is load-bearing, not a formality — push the fix and re-check the matrix before tagging.
2. Tag `v0.3.0` and push the tag.
3. Publish `0.3.0` to PyPI (manual step — do not automate credentials). Verify with a fresh-venv install of the published (not locally-built) artifact.
4. Only then start v0.4 (chaos-injection benchmark harness, example agents, tracing hooks) — do not start v0.4 work before v0.3 is tagged and published.

## Non-negotiables

- Don't silently swallow errors — if the wrapped function raises, `latch` should propagate the exception and NOT cache a failed result.
- Don't auto-generate idempotency keys from function args by default — that's a footgun (subtly different args should sometimes get different keys, sometimes not, and only the caller knows which).
- Keep the public API tiny. Every new parameter on `idempotent()` should justify its complexity cost.
