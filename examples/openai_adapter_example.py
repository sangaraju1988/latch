"""v0.3 example: `latch.adapters.openai.dispatch_tool_call` wired into an
OpenAI-shaped tool-calling loop.

This replaces the manual `idempotency_key = f"{run_id}:{step_id}"` /
if-elif dispatch shown in `openai_example.py` (v0.1) with the adapter
that ships in v0.3. It doesn't require a live OpenAI API call to
demonstrate: `tool_call` below is a stand-in with the same shape a real
`openai.types.chat.ChatCompletionMessageToolCall` has (`.id`,
`.function.name`, `.function.arguments`) -- `dispatch_tool_call` never
imports `openai`, so a real SDK object works identically.
"""

import json
from types import SimpleNamespace

from latch import idempotent
from latch.adapters.openai import dispatch_tool_call


@idempotent()
def send_email(to: str, subject: str, body: str) -> dict:
    print(f"  sending email to {to}: {subject}")
    return {"to": to, "subject": subject, "status": "sent"}


TOOLS = {"send_email": send_email}


def fake_openai_tool_call(call_id: str, name: str, arguments: dict) -> SimpleNamespace:
    """Stand-in for the tool_calls[i] entry OpenAI's API returns. Real
    code gets this from `response.choices[0].message.tool_calls[0]`."""
    return SimpleNamespace(
        id=call_id,
        function=SimpleNamespace(name=name, arguments=json.dumps(arguments)),
    )


if __name__ == "__main__":
    tool_call = fake_openai_tool_call(
        "call_abc123",
        "send_email",
        {"to": "user@example.com", "subject": "Welcome", "body": "Hi there!"},
    )

    # First attempt: the model asked for this tool call, we execute it.
    message = dispatch_tool_call(tool_call, tools=TOOLS, run_id="run-42")
    print("First attempt:", message)

    # Agent framework retries after a timeout -- same tool_call.id, same
    # run_id, so the derived idempotency_key matches and @idempotent
    # returns the cached result instead of sending a second email.
    retry = dispatch_tool_call(tool_call, tools=TOOLS, run_id="run-42")
    print("Retry (same tool_call.id, no duplicate email):", retry)

    assert message == retry
    print("No duplicate email sent on retry.")
