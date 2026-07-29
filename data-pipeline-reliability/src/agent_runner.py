"""The simulated agent orchestration loop: calls the load tool for each
batch, and on an apparent timeout, retries -- exactly the ambiguous-failure
behavior described in CLAUDE.md's Mission section and demonstrated in
`examples/naive_agent_example.py` of the parent `latch` project.

Two variants, identical shape, identical retry policy:

- `run_naive_load`: hand-rolled client-side timeout via a background thread
  that is never killed on timeout (Python cannot forcibly kill a thread).
  On a timeout, the agent retries by calling the raw load function again.
  No idempotency layer exists anywhere in this path.
- `run_protected_load`: the same retry loop, but the load function passed
  in is wrapped with `@with_timeout` + `@idempotent` (see
  `load_tools.make_protected_load`). The loop catches `LatchTimeoutError`
  instead of polling a thread, but the *retry policy itself* (3 attempts,
  same backoff) is unchanged -- the fix lives entirely in the tool, not in
  the loop, matching the article's thesis.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from latch import LatchTimeoutError

from src.load_tools import CLIENT_TIMEOUT_SECONDS, RETRIES_PER_BATCH, RETRY_DELAY_SECONDS


def run_naive_load(
    load_fn: Callable[[str, List[Dict[str, Any]]], Dict[str, Any]],
    batch_id: str,
    rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    for _attempt in range(RETRIES_PER_BATCH):
        result_holder: Dict[str, Any] = {}

        def target() -> None:
            result_holder["result"] = load_fn(batch_id, rows)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=CLIENT_TIMEOUT_SECONDS)
        if thread.is_alive():
            # Client gives up and will retry -- the thread above keeps
            # running in the background and will still write to the
            # warehouse when it eventually finishes.
            time.sleep(RETRY_DELAY_SECONDS)
            continue
        return result_holder.get("result")
    return None  # exhausted retries; agent reports this batch as failed


def run_protected_load(
    protected_load_fn: Callable[..., Dict[str, Any]],
    batch_id: str,
    rows: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    for _attempt in range(RETRIES_PER_BATCH):
        try:
            return protected_load_fn(batch_id, rows, idempotency_key=batch_id)  # type: ignore[no-any-return]
        except LatchTimeoutError:
            time.sleep(RETRY_DELAY_SECONDS)
            continue
    return None
