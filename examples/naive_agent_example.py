"""The "before" example: the exact bug latch exists to fix, live and
deterministic -- no latch import anywhere in this file.

A simulated payment API is slow enough that the agent's own client-side
timeout gives up on it before it responds. The agent, seeing what looks
like a failure, retries the same logical charge. Nothing here remembers
that the first attempt is still running in the background -- so the card
gets charged twice, and the agent never finds out.

Run directly: python examples/naive_agent_example.py

See examples/resilient_agent_example.py for the same scenario protected
by latch, where the retry becomes a cache hit instead of a second charge.
"""

import threading
import time
from typing import Any, Dict, Optional

CLIENT_TIMEOUT_SECONDS = 0.2
CALL_LATENCY_SECONDS = 0.4  # deliberately slower than the timeout above

ledger: Dict[str, int] = {}


def charge_card(order_id: str, amount: float) -> Dict[str, Any]:
    """Simulated payment processor call -- a stand-in for a real HTTP call
    to a payment API. No retries-safety of any kind."""
    time.sleep(CALL_LATENCY_SECONDS)
    ledger[order_id] = ledger.get(order_id, 0) + 1
    return {"order_id": order_id, "amount": amount, "status": "charged"}


def agent_charge_with_naive_retry(
    order_id: str, amount: float, max_retries: int = 2
) -> Optional[Dict[str, Any]]:
    """A hand-rolled agent orchestration loop: call the tool, and if it
    doesn't respond within the agent's own timeout, retry. This is
    ordinary, reasonable-looking retry logic -- the missing piece is that
    retrying can't tell whether the first attempt already succeeded."""
    for attempt in range(1, max_retries + 1):
        result_holder: Dict[str, Any] = {}

        def target() -> None:
            result_holder["result"] = charge_card(order_id, amount)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=CLIENT_TIMEOUT_SECONDS)
        if thread.is_alive():
            print(f"  attempt {attempt}: no response within {CLIENT_TIMEOUT_SECONDS}s -- retrying")
            continue
        print(f"  attempt {attempt}: got a response")
        return result_holder["result"]
    return None


if __name__ == "__main__":
    print("Naive agent charges order A1 (no latch protection):")
    result = agent_charge_with_naive_retry(order_id="A1", amount=42.0)

    # Give the abandoned background thread from the first attempt time to
    # finish, so the ledger reflects its (silent) side effect too -- this
    # sleep exists only so the demo can measure the bug; the real bug
    # doesn't need anyone to wait around for it.
    time.sleep(CALL_LATENCY_SECONDS)

    print(f"\nAgent's own view of what happened: {result}")
    print(f"Actual charges issued for order A1: {ledger['A1']}")
    if ledger["A1"] > 1:
        print(
            f"\nBUG: order A1 was charged {ledger['A1']} times. The agent has no way to "
            "know this happened -- its retry looked like an entirely independent call, "
            "and the first attempt's result was thrown away when it 'timed out'."
        )
