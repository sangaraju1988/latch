import time

import pytest

fakeredis = pytest.importorskip("fakeredis")

from latch import idempotent  # noqa: E402
from latch.stores.redis import RedisStore  # noqa: E402


@pytest.fixture
def redis_store():
    fake_client = fakeredis.FakeStrictRedis()
    return RedisStore(client=fake_client)


def test_set_and_get_roundtrip(redis_store):
    redis_store.set("k1", {"order_id": "A1", "status": "created"}, ttl_seconds=60)
    assert redis_store.get("k1") == {"order_id": "A1", "status": "created"}


def test_get_missing_key_returns_none(redis_store):
    assert redis_store.get("does-not-exist") is None


def test_exists(redis_store):
    assert redis_store.exists("k1") is False
    redis_store.set("k1", "value", ttl_seconds=60)
    assert redis_store.exists("k1") is True


def test_ttl_expiry_via_redis_native_expiry():
    fake_client = fakeredis.FakeStrictRedis()
    store = RedisStore(client=fake_client)
    store.set("k1", "value", ttl_seconds=1)
    assert store.get("k1") == "value"
    fake_client.pexpire("latch:idempotency:k1", 1)  # force near-immediate expiry
    time.sleep(0.05)
    assert store.get("k1") is None


def test_zero_ttl_writes_nothing(redis_store):
    redis_store.set("k1", "value", ttl_seconds=0)
    assert redis_store.get("k1") is None


def test_key_prefix_isolates_stores():
    fake_client = fakeredis.FakeStrictRedis()
    store_a = RedisStore(client=fake_client, key_prefix="app-a:")
    store_b = RedisStore(client=fake_client, key_prefix="app-b:")

    store_a.set("k1", "from-a", ttl_seconds=60)
    assert store_b.get("k1") is None
    assert store_a.get("k1") == "from-a"


def test_idempotent_decorator_works_with_redis_store():
    fake_client = fakeredis.FakeStrictRedis()
    store = RedisStore(client=fake_client)
    calls = []

    @idempotent(store=store)
    def create_order(order_id):
        calls.append(order_id)
        return {"order_id": order_id}

    r1 = create_order(order_id="A1", idempotency_key="k1")
    r2 = create_order(order_id="A1", idempotency_key="k1")

    assert r1 == r2 == {"order_id": "A1"}
    assert len(calls) == 1


def test_idempotent_decorator_dedupes_none_returning_function_via_redis():
    # Regression test (same class of bug as InMemoryStore): a None return
    # value must still be cached and deduped, not treated as a cache miss
    # forever. RedisStore.exists() uses Redis's native EXISTS, which is
    # unaffected by what value is stored, but this exercises the full
    # decorator + store integration end to end.
    fake_client = fakeredis.FakeStrictRedis()
    store = RedisStore(client=fake_client)
    calls = []

    @idempotent(store=store)
    def delete_record(record_id):
        calls.append(record_id)
        return None

    r1 = delete_record(record_id="R1", idempotency_key="k1")
    r2 = delete_record(record_id="R1", idempotency_key="k1")

    assert r1 is None
    assert r2 is None
    assert len(calls) == 1


def test_idempotent_decorator_different_functions_sharing_redis_store_do_not_collide():
    fake_client = fakeredis.FakeStrictRedis()
    store = RedisStore(client=fake_client)

    @idempotent(store=store)
    def create_order(order_id):
        return {"kind": "order", "order_id": order_id}

    @idempotent(store=store)
    def send_email(to):
        return {"kind": "email", "to": to}

    order_result = create_order(order_id="A1", idempotency_key="shared-key")
    email_result = send_email(to="a@example.com", idempotency_key="shared-key")

    assert order_result == {"kind": "order", "order_id": "A1"}
    assert email_result == {"kind": "email", "to": "a@example.com"}


def test_get_raises_clear_error_on_unexpected_client_response_type(redis_store, monkeypatch):
    # Simulates a client whose GET returns something other than
    # bytes/bytearray/str/None (e.g. a misconfigured response callback) --
    # should fail loudly rather than hand a surprising type to pickle.loads.
    monkeypatch.setattr(redis_store._client, "get", lambda _key: 12345)

    with pytest.raises(TypeError, match="Unexpected type from Redis GET"):
        redis_store.get("k1")


def test_missing_redis_package_raises_clear_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "redis":
            raise ImportError("no module named redis")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError, match="pip install latch-idempotent\\[redis\\]"):
        RedisStore()
