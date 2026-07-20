import asyncio

import pytest

from latch import Saga, SagaExecutionError


def test_all_steps_succeed_no_compensation_called():
    compensated = []
    saga = Saga(name="happy-path")

    saga.add_step(lambda: "a-result", compensation=lambda r: compensated.append(("a", r)))
    saga.add_step(lambda: "b-result", compensation=lambda r: compensated.append(("b", r)))

    results = saga.run()

    assert results == ["a-result", "b-result"]
    assert compensated == []


def test_mid_failure_compensates_completed_steps_in_reverse_order():
    order = []
    saga = Saga()

    saga.add_step(
        lambda: {"id": "charge-1"},
        name="charge",
        compensation=lambda r: order.append(("compensate", "charge", r["id"])),
    )
    saga.add_step(
        lambda: {"id": "ship-1"},
        name="ship",
        compensation=lambda r: order.append(("compensate", "ship", r["id"])),
    )

    def boom():
        raise RuntimeError("hotel API down")

    saga.add_step(
        boom, name="hotel", compensation=lambda r: order.append(("compensate", "hotel", r))
    )

    with pytest.raises(SagaExecutionError) as exc_info:
        saga.run()

    err = exc_info.value
    assert err.step_name == "hotel"
    assert isinstance(err.original_exception, RuntimeError)
    assert err.__cause__ is err.original_exception
    # Reverse order: ship compensated before charge; hotel never ran so
    # it's not in the completed set at all.
    assert order == [("compensate", "ship", "ship-1"), ("compensate", "charge", "charge-1")]
    assert err.compensated_steps == ["ship", "charge"]
    assert err.compensation_errors == []


def test_first_step_failure_needs_no_compensation():
    saga = Saga()

    def boom():
        raise ValueError("nope")

    saga.add_step(boom, name="only-step")

    with pytest.raises(SagaExecutionError) as exc_info:
        saga.run()

    assert exc_info.value.compensated_steps == []
    assert exc_info.value.compensation_errors == []


def test_step_without_compensation_is_skipped_on_rollback():
    saga = Saga()
    saga.add_step(lambda: "a", name="a")  # no compensation
    saga.add_step(lambda: (_ for _ in ()).throw(RuntimeError("boom")), name="b")

    with pytest.raises(SagaExecutionError) as exc_info:
        saga.run()

    assert exc_info.value.compensated_steps == []


def test_compensation_failure_is_captured_not_swallowed_and_rollback_continues():
    order = []
    saga = Saga()

    saga.add_step(lambda: "a-result", name="a", compensation=lambda r: order.append("compensate-a"))

    def bad_compensation(_result):
        raise RuntimeError("refund API also down")

    saga.add_step(lambda: "b-result", name="b", compensation=bad_compensation)

    def boom():
        raise RuntimeError("step c failed")

    saga.add_step(boom, name="c")

    with pytest.raises(SagaExecutionError) as exc_info:
        saga.run()

    err = exc_info.value
    # b's compensation failed, but a's still ran (best-effort rollback).
    assert order == ["compensate-a"]
    assert err.compensated_steps == ["a"]
    assert len(err.compensation_errors) == 1
    failed_name, failed_exc = err.compensation_errors[0]
    assert failed_name == "b"
    assert isinstance(failed_exc, RuntimeError)
    assert "refund API" in str(failed_exc)


def test_empty_saga_runs_trivially():
    assert Saga().run() == []


def test_decorator_registration_returns_original_function():
    saga = Saga()
    calls = []

    @saga.step(compensation=lambda r: calls.append(("undo", r)))
    def do_thing():
        calls.append("do")
        return "done"

    # Decorated function is returned unwrapped and independently callable.
    assert do_thing() == "done"
    calls.clear()

    results = saga.run()
    assert results == ["done"]
    assert calls == ["do"]


def test_step_name_defaults_to_function_name():
    saga = Saga()

    def charge_card():
        return "ok"

    saga.add_step(charge_card)
    assert saga.steps[0].name == "charge_card"


def test_step_name_defaults_to_index_for_lambdas_without_name():
    saga = Saga()
    saga.add_step(lambda: "ok")
    assert saga.steps[0].name in ("<lambda>", "step_0")


def test_add_step_is_chainable():
    saga = Saga()
    result = saga.add_step(lambda: 1).add_step(lambda: 2)
    assert result is saga
    assert len(saga.steps) == 2


def test_sync_run_rejects_coroutine_action_with_clear_typeerror():
    saga = Saga()

    async def async_action():
        return "nope"

    saga.add_step(async_action, name="bad")

    with pytest.raises(TypeError, match="run_async"):
        saga.run()


@pytest.mark.asyncio
async def test_async_saga_all_steps_succeed():
    saga = Saga()

    async def a():
        await asyncio.sleep(0)
        return "a"

    def b():
        return "b"

    saga.add_step(a, name="a")
    saga.add_step(b, name="b")

    results = await saga.run_async()
    assert results == ["a", "b"]


@pytest.mark.asyncio
async def test_async_saga_rolls_back_with_mixed_sync_async_compensations():
    order = []

    async def charge():
        return "charge-1"

    async def undo_charge(charge_id):
        await asyncio.sleep(0)
        order.append(("undo", "charge", charge_id))

    def ship():
        return "ship-1"

    def undo_ship(ship_id):
        order.append(("undo", "ship", ship_id))

    async def boom():
        raise RuntimeError("hotel down")

    saga = Saga()
    saga.add_step(charge, name="charge", compensation=undo_charge)
    saga.add_step(ship, name="ship", compensation=undo_ship)
    saga.add_step(boom, name="hotel")

    with pytest.raises(SagaExecutionError) as exc_info:
        await saga.run_async()

    assert order == [("undo", "ship", "ship-1"), ("undo", "charge", "charge-1")]
    assert exc_info.value.compensated_steps == ["ship", "charge"]


@pytest.mark.asyncio
async def test_async_compensation_failure_captured_not_swallowed():
    async def bad_compensation(_result):
        raise RuntimeError("async refund failed")

    async def boom():
        raise RuntimeError("step failed")

    saga = Saga()
    saga.add_step(lambda: "ok", name="a", compensation=bad_compensation)
    saga.add_step(boom, name="b")

    with pytest.raises(SagaExecutionError) as exc_info:
        await saga.run_async()

    assert exc_info.value.compensated_steps == []
    assert len(exc_info.value.compensation_errors) == 1
    assert exc_info.value.compensation_errors[0][0] == "a"


def test_error_message_includes_saga_and_step_names():
    saga = Saga(name="checkout")

    def boom():
        raise ValueError("card declined")

    saga.add_step(boom, name="charge")

    with pytest.raises(SagaExecutionError, match="checkout.*charge.*card declined"):
        saga.run()
