"""OpenAI tool-calling adapter.

An OpenAI chat completion with tool calling returns a list of
`tool_calls`, each carrying a call `id`, a function `name`, and a
JSON-encoded `arguments` string. Dispatching one of those to the matching
Python function and deriving a stable `idempotency_key` for it is
boilerplate every integration re-implements; `dispatch_tool_call` does it
once, correctly.

This module never imports `openai`. `tool_call` is accepted duck-typed --
anything with `.id`, `.function.name`, and `.function.arguments`
attributes works, whether that's a real `openai.types.chat.
ChatCompletionMessageToolCall`, a `SimpleNamespace` in a test, or another
SDK's compatible shape. That keeps `latch`'s core zero-required-
dependency promise intact for this adapter too -- see
`latch.adapters.__init__` and CLAUDE.md "Packaging decision (v0.3
adapters)".
"""

import json
from typing import Any, Callable, Dict, Optional

ToolFunction = Callable[..., Any]


def dispatch_tool_call(
    tool_call: Any,
    *,
    tools: Dict[str, ToolFunction],
    run_id: Optional[str] = None,
    pass_idempotency_key: bool = True,
) -> Dict[str, Any]:
    """Execute one OpenAI tool call and return an OpenAI-shaped tool
    response message.

    Looks up `tool_call.function.name` in `tools`, JSON-decodes
    `tool_call.function.arguments`, and invokes the matching function.

    If `pass_idempotency_key` is True (the default), an `idempotency_key`
    keyword argument is derived from `tool_call.id` (prefixed with
    `run_id` if given, so the same tool_call id across two different agent
    runs can't collide) and passed to the tool function -- this is what
    lets a function wrapped in `@idempotent` dedupe a retried tool call.
    Set it to False for tools that aren't `@idempotent`-wrapped and don't
    accept that keyword.

    Returns:
        `{"role": "tool", "tool_call_id": tool_call.id, "content": <json>}`,
        ready to append to the conversation's message list.

    Raises:
        `KeyError` if no tool is registered under `tool_call.function.name`.
        Whatever the tool function itself raises, unmodified -- this
        adapter does not swallow errors, matching every other latch
        primitive.
    """
    name = tool_call.function.name
    if name not in tools:
        raise KeyError(f"No tool registered for '{name}'. Known tools: {sorted(tools)}")

    arguments = json.loads(tool_call.function.arguments)

    if pass_idempotency_key:
        idempotency_key = f"{run_id}:{tool_call.id}" if run_id else tool_call.id
        result = tools[name](**arguments, idempotency_key=idempotency_key)
    else:
        result = tools[name](**arguments)

    return {
        "role": "tool",
        "tool_call_id": tool_call.id,
        "content": json.dumps(result),
    }


__all__ = ["dispatch_tool_call"]
