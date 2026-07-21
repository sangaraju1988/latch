# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] — 2026-07-20

### Added

- `latch.tracing`: `TraceEvent` (frozen dataclass: `primitive`, `event`, `timestamp`, `metadata`), `Tracer` (thread-safe pub/sub event bus — `subscribe()`/`unsubscribe()`/`emit()`), and `LoggingTracer` (a `Tracer` subclass that logs every event to `logging.getLogger("latch")` at INFO with no extra wiring). `tracer=` is now an optional parameter on `idempotent()`, `circuit_breaker()`/`CircuitBreaker`, `with_timeout()`, `budget_guardrail()`/`BudgetGuardrail`, and `Saga` — defaults to `None` everywhere (fully opt-in, zero overhead when unused). Full event catalog documented in `latch/tracing.py`'s module docstring and the README's Observability section. A subscriber that raises is caught and discarded at the point of emission — the one deliberate, documented exception to latch's "never swallow errors" rule, since a broken logging callback must not be able to break the call it's merely observing.
- `latch.chaos`: `Chaos` class and `@chaos` decorator inject a configurable probability of failure and/or added latency (with optional jitter) into any sync or async function, with a seedable RNG for reproducible runs. Ships as real, tested library code (not a doc snippet) so it's usable directly in your own tests, not just internally. Deliberately narrow by design — two failure shapes only (raise, or add latency) — see the module docstring for why it isn't a general-purpose fuzzer.
- `benchmarks/chaos_benchmark.py` (not part of the installed package, like `examples/`): runs the identical simulated agent retry loop twice under the same seeded `latch.chaos` latency profile — once against a naive/unprotected tool function, once against the same function protected by `@with_timeout` (outer) wrapping `@idempotent` (inner). Prints a comparison table (orders attempted/succeeded/failed, total real charges issued, orders double-charged, idempotency cache hits) and can write the same data as JSON via `--json`. Demonstrates, with real latch code rather than a mocked scenario, why composition order matters: because `idempotent` sits inside `with_timeout`, the background thread `with_timeout` can't kill still finishes and caches its result, so the agent's retry becomes a cache hit instead of a second charge.
- `examples/naive_agent_example.py` / `examples/resilient_agent_example.py`: a runnable before/after pair. The naive version has zero latch imports and reproduces the exact double-charge bug described in CLAUDE.md's Mission section, live and deterministically. The resilient version protects the same scenario with all five primitives (idempotency, circuit breaker, timeout, budget guardrail, saga) plus a shared `LoggingTracer`, including a two-step `Saga` (charge card, then reserve hotel) with compensation wired up.
- 39 new tests: `test_tracing.py` (13 — subscribe/unsubscribe/emit, subscriber-exception isolation, `LoggingTracer` behavior, metadata roundtrip across all five primitive event shapes), `test_tracing_integration.py` (14 — every primitive's actual emitted event sequence, including a full saga failure-and-compensation lifecycle assertion and circuit-breaker half-open recovery), `test_chaos.py` (12 — failure-rate boundaries, seeded determinism, latency injection sync/async, custom exception types, shared injector, construction-time validation). Full suite: 111 tests (up from 72).

### Changed

- Package description updated to mention tracing and chaos testing alongside the existing five primitives.
- README: new "Observability (tracing)" and "Chaos testing" sections; Production Considerations' "No built-in observability" bullet replaced with an accurate description of what `tracer=` does and doesn't do (in-process event stream, not a tracing-backend export); roadmap checklist updated to mark v0.4 shipped.
- `CircuitBreaker._on_failure` signature changed from `_on_failure(self)` to `_on_failure(self, exc: BaseException)` so the `call_failed` trace event can include `repr(exc)` — internal only, not part of the public decorator API.

### Fixed

- Nothing — no regressions found. `pytest`, `mypy --strict src/latch`, and `ruff check`/`ruff format` were all clean on the first full run after this release's changes (unlike v0.3, where CI caught a real pre-existing bug).

## [0.3.0] — 2026-07-20

### Added

- `Saga` / `SagaStep` (`latch.saga`): ordered multi-step execution with automatic compensation. If a step's action raises, every already-completed step is compensated in reverse order (best-effort — one failing compensation doesn't stop the rest) before `SagaExecutionError` is raised, chained from the original exception. Sync (`saga.run()`) and async (`await saga.run_async()`, mixed sync/async steps supported) execution; steps can be registered imperatively (`saga.add_step(...)`) or via `@saga.step(...)` decorator.
- `SagaExecutionError` (new exception, subclasses `LatchError`): carries `step_name`, `original_exception`, `compensated_steps`, and `compensation_errors` so callers can inspect exactly what rolled back and what didn't, without parsing a message string.
- `latch.adapters.openai.dispatch_tool_call`: dispatches one OpenAI `tool_calls[i]` entry to a registered tool function, deriving `idempotency_key` from `tool_call.id` (optionally prefixed by `run_id`), and returns an OpenAI-shaped tool response message. Duck-typed — never imports `openai`.
- `latch.adapters.langchain.resilient_tool`: wraps a plain function with whichever latch primitives are configured (idempotency, circuit breaker, timeout, budget guardrail), ready to hand to `StructuredTool.from_function` or any other LangChain constructor that accepts a callable. Also patches the wrapped function's visible signature/annotations so frameworks that build a call schema via `inspect.signature`/`typing.get_type_hints` (LangChain's pydantic-based schema inference included) see the `idempotency_key` parameter instead of silently dropping it — a real bug caught by the integration test against actual `langchain_core.tools.StructuredTool`, not just a documented example. Never imports `langchain`/`langchain_core`.
- `examples/saga_example.py`, `examples/openai_adapter_example.py`, `examples/langchain_adapter_example.py`.
- 27 new tests across `test_saga.py` (rollback ordering, best-effort compensation with captured-not-swallowed errors, sync/async, decorator registration, empty saga, error chaining), `test_adapters.py` (OpenAI dispatch, LangChain wrapping, and a real `langchain_core.StructuredTool` integration run — no mocking of the target SDK), and one regression test in `test_redis_store.py` (see Fixed, below). Full suite: 72 tests.
- `dev` extra now includes `langchain-core`, used only by the optional LangChain integration tests/examples (skipped automatically if not installed).

### Changed

- Package description updated to mention saga/compensation alongside the existing four primitives.
- `latch.adapters` is a new subpackage; deliberately not imported from `latch/__init__.py` (import what you need directly, e.g. `from latch.adapters.openai import dispatch_tool_call`) to keep the top-level namespace to primitives you'd use in every integration, matching the existing convention for `latch.stores.redis`.

### Fixed

- `RedisStore.get()` (`latch.stores.redis`, pre-existing since v0.2): `mypy --strict` failed on the real Python 3.9 CI matrix — `redis-py`'s type stubs describe `GET`'s return as a broad union shared with the async client, and how far mypy narrows that union through the existing `isinstance(raw, str)` reassignment turned out to differ between mypy/redis-py stub versions resolved on 3.9 vs 3.10–3.12, even with the same source and the same `pyproject.toml` `python_version = "3.10"` mypy config. Fixed with an explicit `isinstance(raw, (bytes, bytearray))` check before `pickle.loads` — also a legitimate runtime safety improvement (an unexpected response shape now fails loudly with a clear `TypeError` instead of a confusing error from inside `pickle`). This bug shipped in 0.2.0 and was only caught now because 0.2.0 was never actually run against the full GitHub Actions matrix before being tagged and published — see the "Immediate next tasks" note in `CLAUDE.md`.

## [0.2.0] — 2026-07-19

### Added

- `@circuit_breaker` decorator and `CircuitBreaker` class (`latch.circuit_breaker`): closed/open/half-open state machine that fails calls fast (`CircuitOpenError`) after a run of failures, instead of retrying against a known-down dependency. Sync and async support. Breaker instances are shareable across multiple decorated functions.
- `@with_timeout` decorator (`latch.timeout`): enforces a wall-clock deadline on a tool call, raising `LatchTimeoutError` if exceeded. `async def` functions are cancelled cooperatively via `asyncio.wait_for`; sync functions are bounded via a daemon thread (documented tradeoff: the underlying call is not forcibly killed).
- `@budget_guardrail` decorator and `BudgetGuardrail` class (`latch.budget`): caps call count and/or cumulative cost within a fixed time window, raising `BudgetExceededError` before the wrapped function is ever invoked once the cap would be exceeded. Supports a `cost_fn` for per-call variable pricing.
- `RedisStore` (`latch.stores.redis`): `IdempotencyStore` implementation backed by Redis, for sharing idempotency state across processes/machines. Optional dependency — `pip install latch-idempotent[redis]`; importing `latch.stores.redis` never requires `redis` to be installed, only instantiating `RedisStore` does.
- New exceptions: `CircuitOpenError`, `LatchTimeoutError`, `BudgetExceededError` (all subclass `LatchError`).
- `examples/resilient_tool_example.py`: composes all four primitives (budget guardrail, circuit breaker, timeout, idempotency) around a single simulated payments tool call.
- 38 new tests across `test_circuit_breaker.py`, `test_timeout.py`, `test_budget.py`, `test_redis_store.py` (uses `fakeredis`, no live Redis required), covering state transitions, half-open recovery, concurrency-relevant reset behavior, TTL/window expiry, and construction-time validation errors. Full suite: 45 tests.

### Changed

- Package description updated to reflect the broader "reliability middleware" scope (idempotency was the v0.1-only framing).
- `pyproject.toml` homepage/issue URLs corrected to the actual repository.
- `dev` extra now includes `fakeredis` for testing `RedisStore` without a live Redis instance.

### Fixed

- `mypy --strict` now passes clean on `src/latch` (the v0.1 baseline had never actually been verified clean against a stable mypy release — the `RedisStore.get` bytes/str typing added in this release was caught and fixed by that check).

## [0.1.0] — 2026-07-16

Initial release: `@idempotent` decorator, `InMemoryStore`, sync + async support, required (never auto-generated) `idempotency_key`. Published to PyPI as `latch-idempotent`.
