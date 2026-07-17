import asyncio
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
