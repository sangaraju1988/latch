# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
