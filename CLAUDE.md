# latch — Reliability Middleware for LLM Agent Tool Calls

This file is the persistent context for Claude Code working in this repo. Read it fully before making changes. Keep it updated as phases complete.

## Mission

`latch` is a small Python library (zero required dependencies in the core) that makes LLM agent tool calls safe to retry and hard to run away with. When an agent's tool call times out or errors, the agent doesn't know whether the underlying action actually completed — retrying blindly can double-charge a card, send a duplicate email, or create a duplicate order. Beyond that, an autonomous agent loop can also hammer an already-failing dependency, block indefinitely on a hung call, blow through a cost budget, or fail partway through a multi-step operation and leave real side effects with nothing to undo them. `latch` addresses all five with independent, composable primitives: `@idempotent` (dedupe retries), `@circuit_breaker` (fail fast against a known-down dependency), `@with_timeout` (bound call duration), `@budget_guardrail` (cap call count/cost per window), and `Saga` (compensate a multi-step sequence when a later step fails). `latch.adapters` layers framework-specific glue (OpenAI tool calling, LangChain) on top without making those SDKs hard dependencies of the core. `latch.tracing` gives every primitive above an optional, opt-in event stream (cache hits, circuit trips, timeouts, budget rejections, saga rollbacks) for observability, and `latch.chaos` is a companion testing utility that injects configurable failure/latency so the protection those primitives provide can actually be exercised and verified rather than assumed.

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
- One package with a continuous version history is easier for the community to discover, adopt, and trust than several disconnected v0.x packages that each look like an abandoned experiment next to the others — a single `pip install latch-idempotent` should cover the whole reliability story.
- The v0.1 CLAUDE.md roadmap already committed to this: v0.2 was scoped as "circuit breaker, timeout/cancellation, budget guardrails, Redis store" as part of *this* project's 90-day plan, not as a new project.
- Precedent: comparable resilience libraries (`tenacity`, `pybreaker`, `resilience4j` in the JVM world) ship all their primitives from one package with one identity.
- The one genuinely optional piece (Redis) is handled via `pip install latch-idempotent[redis]` — an extra, not a separate package — so "core stays dependency-free" is preserved without a package split.

A new package would only make sense for a future phase needing a fundamentally different distribution model (e.g. a framework adapter that pulls in LangChain/OpenAI SDKs as hard dependencies) — which is why v0.3's adapters are scoped as documented examples, not a dependency of the core package, and a real adapter package is explicitly deferred.

## Packaging decision (v0.3 adapters)

**Decision: `latch.adapters.openai` and `latch.adapters.langchain` ship as real, tested modules inside `latch-idempotent` — not inline doc snippets, and not a separate `latch-langchain`/`latch-openai` package.**

This looks like it contradicts the v0.2 packaging note above ("v0.3's adapters are scoped as documented examples... a real adapter package is explicitly deferred"), so the reconciliation is worth spelling out:

- The "documented example, not a dependency of the core package" framing was about avoiding a **hard dependency** on `openai`/`langchain`, not about avoiding real, importable, tested code. Both adapter modules achieve zero hard dependency by operating on duck-typed shapes (`tool_call.id` / `.function.name` / `.function.arguments` for OpenAI; a plain callable for LangChain) instead of importing the SDK types — the same trick `RedisStore` uses for `redis`, just without even needing a lazy `import` since no SDK object needs to be constructed by `latch` itself.
- Because neither module imports its target framework, there's no technical reason to fragment them into separate packages the way there would be if, say, `latch.adapters.langchain` needed to subclass `langchain_core.tools.BaseTool` (which would require `langchain-core` at import time). That fundamentally-different-distribution-model trigger from the v0.2 decision above hasn't been hit — so the same "one package, one identity" reasoning from the v0.2 packaging decision applies again here.
- A **real standalone adapter package** (e.g. one that provides its own `BaseTool` subclasses, or auto-registers with an agent framework's plugin system) is still explicitly deferred — that's the "fundamentally different distribution model" case, and nothing in v0.3 needed it.
- `langchain-core` was added to the `dev` extra (not `dependencies`, not even a new `langchain` extra) purely so the optional integration test/example can exercise a real `StructuredTool` in CI; end users installing `latch-idempotent` never pull it in.

## Scope

### v0.4.1 (shipped 2026-07-20 — pre-public-launch reliability audit, patch release)
Before starting v1.0, ran a full end-to-end readiness audit at the user's explicit request ("make sure this utility is ready for public use and any dev can just import and use it 100%"): fresh-install smoke tests against the real published 0.4.0 PyPI artifact, every README/CLAUDE.md code snippet executed verbatim (not just eyeballed), every `examples/`/`benchmarks/` script actually run, a systematic edge-case pass per primitive, and multi-threaded/asyncio concurrency stress tests — a class of testing the existing 111-test suite didn't cover (it exercises correctness of logic, not behavior under real concurrent load). Found and fixed four real bugs, all silent (no exception, no crash — just wrong behavior or no protection):
- `@idempotent` never deduped functions returning `None` (`store.get() is not None` can't distinguish "no entry" from "entry is `None`"; `InMemoryStore.exists()` had the same bug one layer down, since it was implemented as `self.get(key) is not None`).
- Two different `@idempotent`-decorated functions sharing a store (most commonly both landing on the same process-wide default store) could silently return each other's cached results if a caller ever reused an `idempotency_key` across two unrelated operations. Fixed by namespacing the storage key with the wrapped function's identity — invisible to callers, the raw key is still what tracer/`on_duplicate` see.
- `@idempotent` gave zero protection under true concurrency (only sequential retries) — N threads/tasks racing the same key could all observe a cache miss and all execute the side effect. Fixed with a per-storage-key lock (`threading.RLock` sync / `asyncio.Lock` async) held in a `weakref.WeakValueDictionary` so it doesn't grow unboundedly.
- `CircuitBreaker` half-open state let unlimited concurrent calls through as "trial" calls simultaneously, contradicting its own docstring ("the next call is let through as a trial") and defeating the point of half-open (limiting exposure while a dependency is barely recovering). Fixed with a single trial slot, carefully released even when the trial raises an exception type outside `expected_exception` (otherwise that path would leave the breaker permanently wedged, since neither `_on_success` nor `_on_failure` runs for an unexpected exception type).
- `src/latch/py.typed` was missing from the package, so any downstream `mypy`/`pyright` user saw `import-untyped` and lost all real type information (everything resolved to `Any`) despite `latch`'s own source being `mypy --strict` clean. Added; hatchling picks it up automatically from `src/latch/`.

None of these fixes change any documented public API signature — every one corrects behavior that already contradicted its own docstring or README description. 17 new regression tests (multi-threaded and `asyncio.gather`-based, not just single-threaded) added across `test_core.py`, the new `test_stores_memory.py`, `test_redis_store.py`, and `test_circuit_breaker.py`. 128 tests total (up from 111). `ruff` and `mypy --strict src/latch` (both default 3.10 config and explicit `--python-version 3.9`) clean; package builds, and the built wheel was confirmed (not assumed) to contain `py.typed` via a fresh non-editable install plus an actual `mypy --strict` run against downstream code showing real inferred types instead of `Any`.

See `CHANGELOG.md` `[0.4.1]` for full detail.

### v0.4 (shipped 2026-07-20 — observability + chaos testing)
- `latch.tracing`: `TraceEvent` (frozen dataclass), `Tracer` (thread-safe pub/sub event bus), `LoggingTracer` (`Tracer` subclass that logs to `logging.getLogger("latch")` at INFO with zero extra wiring). `tracer: Optional[Tracer] = None` added to `idempotent()`, `circuit_breaker()`/`CircuitBreaker.__init__`, `with_timeout()`, `budget_guardrail()`/`BudgetGuardrail.__init__`, and `Saga.__init__` — opt-in, zero overhead when unused.
- `latch.chaos`: `Chaos` class + `@chaos` decorator, injecting a configurable `failure_rate` and/or `latency_seconds`/`latency_jitter_seconds` into sync or async functions, with a seedable RNG (`seed=`) for reproducible runs. Ships as real, tested library code, not a doc-only snippet.
- `benchmarks/chaos_benchmark.py` (not part of the installed package, like `examples/`) — runs an identical seeded-chaos agent retry loop against a naive vs. a `latch`-protected (`@with_timeout` outer, `@idempotent` inner) simulated payment call, and prints/writes a comparison table (orders attempted/succeeded/failed, real charges issued, orders double-charged, idempotency cache hits).
- `examples/naive_agent_example.py` / `examples/resilient_agent_example.py` — a runnable before/after pair. Naive has zero latch imports and reproduces the double-charge bug live and deterministically (slow call exceeds a hand-rolled client timeout, agent retries, abandoned background thread still charges the card). Resilient protects the identical scenario with all five primitives plus a shared `LoggingTracer`, including a two-step `Saga` (charge card, then reserve hotel) with compensation.
- 39 new tests (13 `test_tracing.py`, 14 `test_tracing_integration.py`, 12 `test_chaos.py`) on top of the 72 from v0.1–v0.3. 111 total.
- `ruff` clean, `mypy --strict src/latch` clean, package builds and installs cleanly. `mypy --strict` was also run against `benchmarks/` and `examples/` as source alongside `src/latch` (not as an installed-package import, since neither directory ships a `py.typed` marker or is part of the wheel) — clean, aside from pre-existing `[type-arg]`/`[call-arg]` noise already present in every other `examples/*.py` file from earlier releases (mypy can't see that `@idempotent` adds an `idempotency_key` kwarg to the wrapped function's visible signature; not a regression, not addressed here — see Definition of done below).

See `CHANGELOG.md` for full detail.

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

### Explicit non-goals for v0.4 (do not build yet)
- Exporting trace events to a specific observability backend (OpenTelemetry, Datadog, Prometheus, etc.) — `Tracer`/`LoggingTracer` are the generic hook; write your own subscriber to forward events to whatever backend you use, rather than `latch` taking a dependency on one
- A general-purpose fuzzing/property-based-testing framework — `latch.chaos` injects exactly two failure shapes (probability of raising, added latency) on purpose; see the module docstring in `latch/chaos.py`
- Distributed/multi-process `CircuitBreaker` or `BudgetGuardrail` state — still unchanged from the README's Production Considerations; only idempotency has a distributed backend (`RedisStore`)
- A docs site, public launch materials, or the companion paper — that's v1.0, gated behind v0.4 being tagged and published the same way v0.4 was gated behind v0.3

### Explicit non-goals for v0.3 (met — kept for history)
- A standalone `latch-langchain` / `latch-openai` adapter package with hard SDK dependencies (see "Packaging decision (v0.3 adapters)" above) — deferred until an adapter genuinely needs one
- Adapters for other frameworks (CrewAI, AutoGen, Semantic Kernel, etc.) — add only on real demand, not speculatively
- Parallel/fan-out saga steps, nested sagas, or saga persistence/replay across process restarts — v0.3's `Saga` is in-process and sequential only; that covers the common case and keeps the abstraction small
- Distributed tracing/observability (v0.4 — done)
- Chaos-injection benchmark harness, example agents (v0.4 — done)

### Explicit non-goals for v0.2 (met — kept for history)
- Saga/compensation (v0.3 — done)
- Framework adapters beyond a documented example (v0.3 — done, see reconciliation in "Packaging decision (v0.3 adapters)")
- Distributed tracing/observability (v0.4 — done)
- Combining the four v0.2 primitives into a single "one decorator to rule them all" convenience wrapper — keep them independently composable (see `examples/resilient_tool_example.py` for the recommended stacking pattern) rather than adding a fifth abstraction on top

Resist scope creep. v1.0 (docs site, public launch, paper submitted to arXiv) starts only after v0.4 is tagged and published, same discipline that gated v0.4 behind v0.3, v0.3 behind v0.2, and v0.2 behind v0.1.

## Roadmap (from the 90-day plan)

- [x] v0.1 — Idempotency core + in-memory store + tests + docs
- [x] v0.2 — Circuit breaker, timeout/cancellation, budget guardrails, Redis store
- [x] v0.3 — Saga/compensation pattern, real OpenAI + LangChain adapter modules
- [x] v0.4 — Tracing/observability hooks, chaos-injection testing utility + benchmark harness, before/after example agents
- [x] v0.4.1 — Pre-public-launch reliability audit; 4 real bugs found and fixed (see Scope above)
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
- `tracer: Optional[Tracer] = None` (v0.4) follows the same "optional, shareable, stateful object" shape already established for `breaker=`/`guardrail=`/`store=` — every primitive accepts it, defaults to `None`, and does nothing (`if self.tracer is not None: ...`) when not given, so tracing is free when unused and one `Tracer`/`LoggingTracer` instance can be shared across every primitive protecting a given dependency. Emit events with enough metadata to reconstruct what happened (`repr(exc)` for exceptions, not just an event name) but never let a subscriber's exception propagate out of `emit()` — that's the one deliberate, documented exception to the "never swallow errors" non-negotiable below, scoped narrowly to `Tracer.emit`'s subscriber-callback loop and nowhere else. When a primitive needs to emit from inside a lock (`CircuitBreaker`, `BudgetGuardrail`), compute what to emit while holding the lock but call `tracer.emit()` after releasing it, so an arbitrary subscriber callback never runs inside the primitive's critical section.
- `latch.chaos` is a testing utility, not a protection primitive — don't give it a `tracer=` parameter or otherwise fold it into the five-primitives-plus-adapters architecture above; it deliberately sits to the side, the way a test double would.

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

## Definition of done for v0.4 (met)

1. `pytest` passes with all v0.1–v0.3 tests plus new coverage for `latch.tracing` (subscribe/unsubscribe/emit, subscriber-exception isolation, `LoggingTracer` default and custom-logger behavior, metadata roundtrip across every primitive's event shape), the tracing integration into all five primitives (every documented event actually gets emitted at the right point — including circuit-breaker half-open recovery and a full saga failure-and-compensation event-sequence assertion), and `latch.chaos` (failure-rate boundaries including 0.0/1.0, seeded determinism, latency injection sync + async, custom exception types, shared injector across functions, construction-time validation). 111 tests total.
2. `mypy --strict src/latch` clean (same pinned-mypy caveat as v0.2/v0.3). Also verified clean against `benchmarks/` and `examples/` when checked as source alongside `src/latch` in one invocation; pre-existing `[type-arg]`/`[call-arg]` findings in `examples/*.py` when checked standalone against the *installed* package are unrelated to this release (no `py.typed` marker ships yet — a pre-existing gap, not new) and out of scope for v0.4.
3. README documents `latch.tracing` (Observability section) and `latch.chaos` (Chaos testing section) with copy-pasteable examples; Production Considerations' "no built-in observability" bullet corrected to reflect what shipped; roadmap checklist updated.
4. Package builds and installs cleanly; `import latch` requires no optional dependencies; `tracer=` defaults to `None` on every primitive so existing callers are unaffected.
5. `benchmarks/chaos_benchmark.py`, `examples/naive_agent_example.py`, and `examples/resilient_agent_example.py` all run standalone and were actually executed (not just written) — the benchmark was run at two different seeds to confirm the naive-vs-protected gap isn't a seed-specific fluke.
6. `CHANGELOG.md` documents the release.

## Definition of done for v0.4.1 (met)

1. `pytest` passes with all v0.1–v0.4 tests plus new regression coverage for all four bugs listed under Scope above, including multi-threaded (`threading.Thread`) and `asyncio.gather`-based concurrency tests, not just single-threaded logic assertions — a testing gap the v0.1–v0.4 suite had throughout. 128 tests total.
2. `mypy --strict src/latch` clean (default 3.10 config and explicit `--python-version 3.9`, same pinned-mypy caveat as prior releases). `py.typed` presence verified concretely, not assumed: built the wheel, installed it (not editable) into a fresh venv, confirmed `latch/py.typed` is physically present in site-packages, and ran `mypy --strict` against a downstream script importing `latch` to confirm it reports real inferred types instead of collapsing to `Any`.
3. `ruff check`/`ruff format --check` clean.
4. Package builds (`python -m build`) and both the wheel and sdist pass `twine check`; wheel contents inspected directly (not assumed) to confirm `py.typed` is included and nothing unwanted (tests/examples/benchmarks/`.git`) leaked in.
5. No documented public API signature changed — confirmed by re-running every README/CLAUDE.md code snippet verbatim against the fixed source and every `examples/`/`benchmarks/` script, all of which still produce the same documented output.
6. `CHANGELOG.md` documents the release with one entry per bug, explaining what was silently wrong and why the fix doesn't change the documented contract.

## Immediate next tasks (v1.0 — work through in order)

v0.4.0 was already tagged, published to PyPI, and verified live in a fresh venv. A subsequent end-to-end readiness audit (at the user's explicit request, ahead of public launch) found four real bugs in that already-published release — see the `v0.4.1` Scope entry above — and fixed them locally as of 2026-07-20 (128 tests passing, `mypy --strict src/latch` clean on both 3.10 and 3.9 targets, `ruff` clean, wheel/sdist verified). `v0.4.1` is **not yet pushed, tagged, or published** — same manual-step discipline as every prior release. Before starting v1.0:

1. Push to `main` and confirm CI is green on the actual GitHub Actions matrix (3.9–3.12) — not a formality; this exact gate caught a real pre-existing bug before v0.3 shipped (see `CHANGELOG.md` `[0.3.0]` Fixed section), so don't skip re-checking it here even though local checks were clean this time too.
2. Tag `v0.4.1` and push the tag.
3. Publish `0.4.1` to PyPI (manual step — do not automate credentials). Verify with a fresh-venv install of the published (not locally-built) artifact, the same way every prior release was verified — this time also confirm `py.typed` is present in the installed package and that `mypy --strict` against downstream code shows real types, since that's the specific thing this release fixed.
4. Update `latch-article.docx` to cover v0.4 (tracing, chaos testing, the benchmark harness's naive-vs-protected numbers are good candidate evaluation data for the companion paper discussed for after v1.0) if not already done; a brief v0.4.1 mention is optional (it's a reliability/correctness patch, not new user-facing capability) but should stay honest if omitted — don't claim v0.4.0's numbers without noting a race-condition fix landed after they were captured.
5. Only then start v1.0 (docs site, public launch, paper submitted to arXiv) — do not start v1.0 work before v0.4.1 is tagged and published, same discipline that has gated every phase so far.

## Non-negotiables

- Don't silently swallow errors — if the wrapped function raises, `latch` should propagate the exception and NOT cache a failed result.
- Don't auto-generate idempotency keys from function args by default — that's a footgun (subtly different args should sometimes get different keys, sometimes not, and only the caller knows which).
- Keep the public API tiny. Every new parameter on `idempotent()` should justify its complexity cost.
