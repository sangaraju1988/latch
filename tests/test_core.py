import asyncio
import threading
import time

import pytest

from latch import IdempotencyKeyMissingError, InMemoryStore, idempotent


def test_sync_function_executes_once_for_same_key():
    store = InMemoryStore()
    calls = []

    @idempotent(store=store)
    def create_order(order_id, amount):
        calls.append(order_id)
        return {"order_id": order_id, "amount": amount, "status": "created"}

    result1 = create_order(order_id="A1", amount=42.0, idempotency_key="key-1")
    result2 = create_order(order_id="A1", amount=42.0, idempotency_key="key-1")

    assert result1 == result2
    assert len(calls) == 1  # only executed once


def test_sync_function_executes_again_for_different_key():
    store = InMemoryStore()
    calls = []

    @idempotent(store=store)
    def create_order(order_id):
        calls.append(order_id)
        return {"order_id": order_id}

    create_order(order_id="A1", idempotency_key="key-1")
    create_order(order_id="A1", idempotency_key="key-2")

    assert len(calls) == 2


def test_missing_key_raises():
    @idempotent()
    def do_thing():
        return "done"

    with pytest.raises(IdempotencyKeyMissingError):
        do_thing()


def test_exception_is_not_cached():
    store = InMemoryStore()
    attempts = []

    @idempotent(store=store)
    def flaky(idempotency_key=None):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient failure")
        return "ok"

    with pytest.raises(RuntimeError):
        flaky(idempotency_key="k1")

    # Retry with the same key should re-execute, not return a cached failure.
    result = flaky(idempotency_key="k1")
    assert result == "ok"
    assert len(attempts) == 2


def test_ttl_expiry():
    store = InMemoryStore()
    calls = []

    @idempotent(store=store, ttl_seconds=0)
    def do_thing(idempotency_key=None):
        calls.append(1)
        return "done"

    do_thing(idempotency_key="k1")
    time.sleep(0.01)  # ensure we're past the 0-second TTL
    do_thing(idempotency_key="k1")

    assert len(calls) == 2  # expired, so it re-executed


def test_on_duplicate_callback_invoked():
    store = InMemoryStore()
    seen = []

    @idempotent(store=store, on_duplicate=lambda key, result: seen.append((key, result)))
    def do_thing(idempotency_key=None):
        return "done"

    do_thing(idempotency_key="k1")
    do_thing(idempotency_key="k1")

    assert seen == [("k1", "done")]


def test_none_returning_function_still_dedupes():
    # Regression test: a wrapped function whose legitimate return value is
    # None (a fire-and-forget side effect with nothing meaningful to
    # return) must still be deduped on retry. The store's get() returns
    # None both for "not present" and "present, value is None" -- the
    # decorator must not conflate the two.
    store = InMemoryStore()
    calls = []

    @idempotent(store=store)
    def delete_record(record_id, idempotency_key=None):
        calls.append(record_id)
        return None

    r1 = delete_record(record_id="R1", idempotency_key="k1")
    r2 = delete_record(record_id="R1", idempotency_key="k1")

    assert r1 is None
    assert r2 is None
    assert len(calls) == 1  # only executed once, not once per call


def test_different_functions_sharing_a_store_do_not_collide_on_key():
    # Regression test: two different @idempotent-decorated functions that
    # happen to share a store (most commonly both leaving store= unset and
    # landing on the shared process-wide default store) must not return
    # each other's cached results just because a caller reused the same
    # idempotency_key string for two unrelated logical operations.
    store = InMemoryStore()

    @idempotent(store=store)
    def create_order(order_id, idempotency_key=None):
        return {"kind": "order", "order_id": order_id}

    @idempotent(store=store)
    def send_email(to, idempotency_key=None):
        return {"kind": "email", "to": to}

    order_result = create_order(order_id="A1", idempotency_key="shared-key")
    email_result = send_email(to="a@example.com", idempotency_key="shared-key")

    assert order_result == {"kind": "order", "order_id": "A1"}
    assert email_result == {"kind": "email", "to": "a@example.com"}


def test_default_store_is_shared_but_still_namespaced_by_function():
    # Two decorators that both omit store= land on the same process-wide
    # default store (documented behavior) -- confirm that sharing doesn't
    # reintroduce the cross-function collision the previous test guards
    # against.
    @idempotent()
    def func_a(idempotency_key=None):
        return "from-a"

    @idempotent()
    def func_b(idempotency_key=None):
        return "from-b"

    assert func_a(idempotency_key="collide") == "from-a"
    assert func_b(idempotency_key="collide") == "from-b"


def test_concurrent_calls_with_same_key_execute_only_once():
    # Regression test: idempotency must hold under true concurrency, not
    # just sequential retries. Without a per-key lock around the
    # check-then-act sequence, N threads racing on the same key could all
    # observe a cache miss and all execute the wrapped function.
    store = InMemoryStore()
    execution_count = {"n": 0}
    lock = threading.Lock()

    @idempotent(store=store)
    def slow_charge(order_id, idempotency_key=None):
        with lock:
            execution_count["n"] += 1
        time.sleep(0.05)
        return {"order_id": order_id, "status": "charged"}

    results = []
    results_lock = threading.Lock()

    def worker():
        result = slow_charge(order_id="A1", idempotency_key="same-key")
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert execution_count["n"] == 1
    assert all(r == results[0] for r in results)


def test_concurrent_calls_with_different_keys_are_not_falsely_serialized():
    # The per-key lock introduced for the race fix above must scope to
    # one key -- it must not accidentally serialize unrelated calls.
    store = InMemoryStore()

    @idempotent(store=store)
    def slow_op(x, idempotency_key=None):
        time.sleep(0.2)
        return x

    start = time.monotonic()
    threads = [
        threading.Thread(target=slow_op, kwargs={"x": i, "idempotency_key": f"key-{i}"})
        for i in range(10)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    assert elapsed < 1.0  # ~0.2s if parallel; ~2.0s if falsely serialized


@pytest.mark.asyncio
async def test_concurrent_async_calls_with_same_key_execute_only_once():
    store = InMemoryStore()
    execution_count = {"n": 0}

    @idempotent(store=store)
    async def slow_charge(order_id, idempotency_key=None):
        execution_count["n"] += 1
        await asyncio.sleep(0.05)
        return {"order_id": order_id, "status": "charged"}

    results = await asyncio.gather(
        *[slow_charge(order_id="A1", idempotency_key="same-key") for _ in range(20)]
    )

    assert execution_count["n"] == 1
    assert all(r == results[0] for r in results)


@pytest.mark.asyncio
async def test_async_function_dedupes():
    store = InMemoryStore()
    calls = []

    @idempotent(store=store)
    async def send_email(to, idempotency_key=None):
        calls.append(to)
        await asyncio.sleep(0)
        return {"sent_to": to}

    r1 = await send_email(to="a@example.com", idempotency_key="k1")
    r2 = await send_email(to="a@example.com", idempotency_key="k1")

    assert r1 == r2
    assert len(calls) == 1
