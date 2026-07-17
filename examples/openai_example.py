"""Example: wrapping an OpenAI tool-calling function with latch.

This shows the shape of the integration; it doesn't require an actual
OpenAI API call to demonstrate the idempotency behavior. In a real agent
loop, `idempotency_key` would typically be derived from something like
`f"{run_id}:{step_id}"` and passed alongside the tool call arguments the
model returns.

TODO (v0.3): a real `latch.adapters.openai` module that auto-wraps a list
of tool functions and injects idempotency_key generation into the agent
loop, so callers don't have to thread it through manually.
"""

from latch import idempotent


@idempotent()
def send_email(to: str, subject: str, body: str) -> dict:
    print(f"Sending email to {to}: {subject}")
    return {"to": to, "subject": subject, "status": "sent"}


def handle_tool_call(tool_name: str, arguments: dict, run_id: str, step_id: str) -> dict:
    """Simulates how an agent loop would dispatch a model-requested tool call."""
    idempotency_key = f"{run_id}:{step_id}"

    if tool_name == "send_email":
        return send_email(**arguments, idempotency_key=idempotency_key)

    raise ValueError(f"Unknown tool: {tool_name}")


if __name__ == "__main__":
    args = {"to": "user@example.com", "subject": "Welcome", "body": "Hi there!"}

    # First attempt.
    result = handle_tool_call("send_email", args, run_id="run-42", step_id="step-1")
    print("First attempt:", result)

    # Agent retries the same step after a timeout — same run_id/step_id,
    # so the same idempotency_key is derived, and no duplicate email is sent.
    retry = handle_tool_call("send_email", args, run_id="run-42", step_id="step-1")
    print("Retry:", retry)

    assert result == retry
    print("No duplicate email sent on retry.")
