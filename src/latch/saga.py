"""Saga / compensation pattern for multi-step LLM agent tool calls.

The other three v0.2 primitives protect a *single* tool call. Many agent
tasks aren't a single call, though -- "book the trip" is charge card, then
reserve flight, then reserve hotel. If reserving the hotel fails after the
card was already charged and the flight already booked, the agent has left
the world in a half-finished state with real side effects that nothing
will automatically undo. Timeouts, circuit breakers, and idempotency don't
help here: the individual calls all succeeded, the *sequence* didn't.

`Saga` addresses this with the Saga pattern (see SagaLLM, arXiv 2503.11951,
for the academic treatment applied to multi-agent planning; this module is
a small, practical implementation of the same idea, not a claim of
inventing it -- see README "Prior art"): register a sequence of steps,
each with an optional compensating action. If a step fails, every step
that already completed is compensated, in reverse order, before the
failure is raised to the caller.

Design notes
------------
- A step's `action` and `compensation` are zero-argument callables. Bind
  whatever arguments the underlying operation needs with `functools.
  partial` or a lambda before registering the step -- this keeps `Saga`'s
  own API surface tiny (see CLAUDE.md non-negotiable: every parameter
  must justify its complexity cost) instead of inventing a parallel
  argument-passing convention.
- `compensation`, if given, is called with the single positional value
  the action returned -- typically enough to know what to undo (e.g. a
  charge ID to refund, a reservation ID to cancel).
- Compensation is best-effort: if one compensation raises, `Saga` still
  attempts the rest (leaving more of the world undone is worse than
  leaving less of it undone) -- but that error is never swallowed. It is
  collected on `SagaExecutionError.compensation_errors` for the caller to
  inspect and, if needed, act on manually.
- `run()` is sync-only and rejects coroutine-function steps up front with
  a clear `TypeError` rather than silently returning an un-awaited
  coroutine object. Use `run_async()` for sagas containing `async def`
  actions/compensations; it also accepts plain sync callables mixed in.
"""

import inspect
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from latch.exceptions import SagaExecutionError


@dataclass
class SagaStep:
    name: str
    action: Callable[[], Any]
    compensation: Optional[Callable[[Any], Any]] = None


class _CompletedStep:
    """A step that finished executing, paired with the result its action
    produced -- the result is what gets handed to `compensation`."""

    __slots__ = ("step", "result")

    def __init__(self, step: SagaStep, result: Any) -> None:
        self.step = step
        self.result = result


class Saga:
    """An ordered sequence of steps, each with an optional compensating
    action, executed with automatic rollback on failure.

    Not thread-safe for concurrent `add_step` calls during `run` -- build
    the saga fully, then execute it. Nothing prevents calling `run()`
    again on the same instance (e.g. to retry the whole saga from the top
    after fixing the underlying issue) -- each call is an independent
    execution.
    """

    def __init__(self, name: str = "saga") -> None:
        self.name = name
        self._steps: List[SagaStep] = []

    def add_step(
        self,
        action: Callable[[], Any],
        *,
        compensation: Optional[Callable[[Any], Any]] = None,
        name: Optional[str] = None,
    ) -> "Saga":
        """Register a step. Returns `self` so calls can be chained."""
        step_name = name or str(getattr(action, "__name__", f"step_{len(self._steps)}"))
        self._steps.append(SagaStep(name=step_name, action=action, compensation=compensation))
        return self

    def step(
        self,
        *,
        compensation: Optional[Callable[[Any], Any]] = None,
        name: Optional[str] = None,
    ) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        """Decorator form of `add_step`.

        Registers the decorated zero-argument function as the next step
        and returns it unchanged, so it stays independently callable:

            saga = Saga()

            @saga.step(compensation=lambda charge: refund(charge["id"]))
            def charge_card():
                return charge_customer(order_id, amount)

        Steps run in the order they're registered, whether via `add_step`
        or `step`; mixing the two styles on one `Saga` is fine.
        """

        def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
            self.add_step(func, compensation=compensation, name=name)
            return func

        return decorator

    @property
    def steps(self) -> Tuple[SagaStep, ...]:
        return tuple(self._steps)

    def run(self) -> List[Any]:
        """Execute all steps in order (sync).

        Raises `TypeError` immediately, before running anything, if any
        registered action or compensation is a coroutine function -- use
        `run_async()` for those instead of silently mis-invoking them.

        On failure, compensates every already-completed step in reverse
        order (best-effort) and raises `SagaExecutionError` chained from
        the original exception.
        """
        self._reject_async_steps()

        results: List[Any] = []
        completed: List[_CompletedStep] = []

        for step in self._steps:
            try:
                result = step.action()
            except Exception as exc:
                compensated, errors = self._compensate_sync(completed)
                raise SagaExecutionError(
                    f"Saga '{self.name}' failed at step '{step.name}': {exc}",
                    step_name=step.name,
                    original_exception=exc,
                    compensated_steps=compensated,
                    compensation_errors=errors,
                ) from exc
            results.append(result)
            completed.append(_CompletedStep(step, result))

        return results

    async def run_async(self) -> List[Any]:
        """Execute all steps in order (async).

        Each step's action/compensation may be a regular function or a
        coroutine function; coroutine functions are awaited, regular
        functions are called directly. Rollback semantics match `run()`.
        """
        results: List[Any] = []
        completed: List[_CompletedStep] = []

        for step in self._steps:
            try:
                result = await self._call_action(step.action)
            except Exception as exc:
                compensated, errors = await self._compensate_async(completed)
                raise SagaExecutionError(
                    f"Saga '{self.name}' failed at step '{step.name}': {exc}",
                    step_name=step.name,
                    original_exception=exc,
                    compensated_steps=compensated,
                    compensation_errors=errors,
                ) from exc
            results.append(result)
            completed.append(_CompletedStep(step, result))

        return results

    def _reject_async_steps(self) -> None:
        for step in self._steps:
            if inspect.iscoroutinefunction(step.action) or inspect.iscoroutinefunction(
                step.compensation
            ):
                raise TypeError(
                    f"Saga '{self.name}' step '{step.name}' has an async action or "
                    f"compensation; use `await saga.run_async()` instead of `saga.run()`."
                )

    def _compensate_sync(
        self, completed: List[_CompletedStep]
    ) -> Tuple[List[str], List[Tuple[str, BaseException]]]:
        compensated: List[str] = []
        errors: List[Tuple[str, BaseException]] = []
        for entry in reversed(completed):
            if entry.step.compensation is None:
                continue
            try:
                entry.step.compensation(entry.result)
            except Exception as comp_exc:  # noqa: BLE001 -- deliberately broad, best-effort rollback
                errors.append((entry.step.name, comp_exc))
            else:
                compensated.append(entry.step.name)
        return compensated, errors

    async def _compensate_async(
        self, completed: List[_CompletedStep]
    ) -> Tuple[List[str], List[Tuple[str, BaseException]]]:
        compensated: List[str] = []
        errors: List[Tuple[str, BaseException]] = []
        for entry in reversed(completed):
            compensation = entry.step.compensation
            if compensation is None:
                continue
            try:
                if inspect.iscoroutinefunction(compensation):
                    await compensation(entry.result)
                else:
                    maybe_awaitable = compensation(entry.result)
                    if inspect.isawaitable(maybe_awaitable):
                        await maybe_awaitable
            except Exception as comp_exc:  # noqa: BLE001 -- deliberately broad, best-effort rollback
                errors.append((entry.step.name, comp_exc))
            else:
                compensated.append(entry.step.name)
        return compensated, errors

    @staticmethod
    async def _call_action(action: Callable[[], Any]) -> Any:
        if inspect.iscoroutinefunction(action):
            return await action()
        result = action()
        if inspect.isawaitable(result):
            return await result
        return result


__all__ = ["Saga", "SagaStep"]
