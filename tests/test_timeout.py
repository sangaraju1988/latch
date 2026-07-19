import asyncio
import time

import pytest

from latch import LatchTimeoutError, with_timeout


def test_sync_function_completes_within_deadline():
    @with_timeout(seconds=1)
    def fast():
        return "done"

    assert fast() == "done"


def test_sync_function_exceeds_deadline_raises():
    @with_timeout(seconds=0.05)
    def slow():
        time.sleep(0.5)
        return "done"

    with pytest.raises(LatchTimeoutError):
        slow()


def test_sync_function_propagates_exception_within_deadline():
    @with_timeout(seconds=1)
    def raises():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        raises()


def test_sync_function_returns_args_correctly():
    @with_timeout(seconds=1)
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


@pytest.mark.asyncio
async def test_async_function_completes_within_deadline():
    @with_timeout(seconds=1)
    async def fast():
        await asyncio.sleep(0)
        return "done"

    assert await fast() == "done"


@pytest.mark.asyncio
async def test_async_function_exceeds_deadline_raises():
    @with_timeout(seconds=0.05)
    async def slow():
        await asyncio.sleep(0.5)
        return "done"

    with pytest.raises(LatchTimeoutError):
        await slow()


@pytest.mark.asyncio
async def test_async_function_propagates_exception_within_deadline():
    @with_timeout(seconds=1)
    async def raises():
        await asyncio.sleep(0)
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await raises()


def test_invalid_seconds_raises():
    with pytest.raises(ValueError):
        with_timeout(seconds=0)
    with pytest.raises(ValueError):
        with_timeout(seconds=-1)


def test_sync_timeout_does_not_swallow_return_value_of_slow_but_ok_call():
    @with_timeout(seconds=1)
    def slightly_slow():
        time.sleep(0.01)
        return 42

    assert slightly_slow() == 42
