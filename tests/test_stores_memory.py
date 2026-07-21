import time

from latch import InMemoryStore


def test_set_and_get_roundtrip():
    store = InMemoryStore()
    store.set("k1", {"order_id": "A1"}, ttl_seconds=60)
    assert store.get("k1") == {"order_id": "A1"}


def test_get_missing_key_returns_none():
    store = InMemoryStore()
    assert store.get("does-not-exist") is None


def test_exists_true_for_present_key():
    store = InMemoryStore()
    assert store.exists("k1") is False
    store.set("k1", "value", ttl_seconds=60)
    assert store.exists("k1") is True


def test_exists_true_even_when_stored_value_is_none():
    # Regression test: exists() must not be implemented in terms of
    # get() returning a non-None value -- a stored value of None is a
    # legitimate cached result (e.g. from a fire-and-forget function)
    # and must still be reported as present.
    store = InMemoryStore()
    store.set("k1", None, ttl_seconds=60)
    assert store.exists("k1") is True
    assert store.get("k1") is None  # value itself is still None


def test_exists_false_after_ttl_expiry():
    store = InMemoryStore()
    store.set("k1", "value", ttl_seconds=0)
    time.sleep(0.01)
    assert store.exists("k1") is False


def test_get_and_exists_agree_after_expiry():
    store = InMemoryStore()
    store.set("k1", None, ttl_seconds=0)
    time.sleep(0.01)
    # Both must report "not present" once expired, even for a None value.
    assert store.exists("k1") is False
    assert store.get("k1") is None


def test_clear_removes_all_entries():
    store = InMemoryStore()
    store.set("k1", "a", ttl_seconds=60)
    store.set("k2", "b", ttl_seconds=60)
    store.clear()
    assert store.get("k1") is None
    assert store.get("k2") is None
    assert store.exists("k1") is False
