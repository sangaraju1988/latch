"""LangChain tool adapter.

`resilient_tool` applies latch's primitives to a plain function so the
result can be handed straight to `langchain_core.tools.StructuredTool.
from_function` (or `@tool`, or any other LangChain constructor that takes
a plain callable) with resilience already built in.

This module never imports `langchain` or `langchain_core` -- it operates
purely on plain callables. Only code that actually constructs a
`StructuredTool` needs LangChain installed; see
`examples/langchain_adapter_example.py` for a full, runnable integration
against real `langchain_core` tool objects. See `latch.adapters.__init__`
and CLAUDE.md "Packaging decision (v0.3 adapters)" for why this lives
here as a module rather than a separate `latch-langchain` package.
"""

import inspect
from typing import Any, Callable, Optional

from latch.budget import BudgetGuardrail, budget_guardrail
from latch.circuit_breaker import CircuitBreaker, circuit_breaker
from latch.core import idempotent
from latch.stores.base import IdempotencyStore
from latch.timeout import with_timeout


def resilient_tool(
    func: Callable[..., Any],
    *,
    idempotency_store: Optional[IdempotencyStore] = None,
    idempotency_ttl_seconds: int = 86400,
    breaker: Optional[CircuitBreaker] = None,
    timeout_seconds: Optional[float] = None,
    guardrail: Optional[BudgetGuardrail] = None,
) -> Callable[..., Any]:
    """Wrap `func` with whichever latch primitives are configured, in the
    same stacking order as `examples/resilient_tool_example.py`:
    budget guardrail -> circuit breaker -> timeout -> idempotency.

    Every layer is optional -- omit an argument and that layer is skipped
    entirely. Pass a shared `breaker` / `guardrail` / `idempotency_store`
    across multiple `resilient_tool` calls that protect the same
    underlying dependency so they trip and budget together, exactly as
    you would with the decorators directly.

    Returns a plain wrapped callable with the same call signature as
    `func` (plus `idempotency_key` if `idempotency_store` is set) --
    suitable for `StructuredTool.from_function(resilient_tool(...))`.
    """
    wrapped = func
    if idempotency_store is not None:
        wrapped = idempotent(store=idempotency_store, ttl_seconds=idempotency_ttl_seconds)(wrapped)
        # `@idempotent` uses functools.wraps, which sets __wrapped__ and
        # therefore makes inspect.signature() auto-unwrap back to the
        # *original* function -- silently hiding the idempotency_key
        # parameter it actually requires. Frameworks that build a tool
        # schema from inspect.signature (LangChain's
        # StructuredTool.from_function among them) would then drop
        # idempotency_key before ever calling the tool. Patch the
        # visible signature so schema inference sees the real one.
        wrapped = _with_idempotency_key_in_signature(wrapped)
    if timeout_seconds is not None:
        wrapped = with_timeout(seconds=timeout_seconds)(wrapped)
    if breaker is not None:
        wrapped = circuit_breaker(breaker=breaker)(wrapped)
    if guardrail is not None:
        wrapped = budget_guardrail(guardrail=guardrail)(wrapped)
    return wrapped


def _with_idempotency_key_in_signature(func: Callable[..., Any]) -> Callable[..., Any]:
    sig = inspect.signature(func)
    if "idempotency_key" in sig.parameters:
        return func
    idempotency_key_param = inspect.Parameter(
        "idempotency_key",
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=str,
    )
    func.__signature__ = sig.replace(  # type: ignore[attr-defined]
        parameters=[*sig.parameters.values(), idempotency_key_param]
    )
    # Schema builders that use typing.get_type_hints() (as LangChain's
    # pydantic-based schema inference does) read __annotations__
    # directly rather than __signature__, so both need updating.
    func.__annotations__ = {**func.__annotations__, "idempotency_key": str}
    return func


__all__ = ["resilient_tool"]
