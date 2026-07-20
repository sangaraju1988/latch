"""v0.3 example: a multi-step "book a trip" saga with compensations.

Booking a trip is charge the card, then reserve a flight, then reserve a
hotel. Each individual call might be wrapped in `@idempotent` /
`@circuit_breaker` / `@with_timeout` / `@budget_guardrail` (see
`resilient_tool_example.py`), but none of those help if the *sequence*
fails partway through -- if the hotel reservation fails after the card
was already charged and the flight already booked, something needs to
undo the flight and refund the card. That's what `Saga` is for.

This example runs the saga twice: once where every step succeeds, and
once where the hotel step fails, to show the automatic reverse-order
rollback.
"""

from latch import Saga, SagaExecutionError


def charge_card(order_id: str, amount: float) -> dict:
    print(f"  charging card for {order_id}: ${amount}")
    return {"charge_id": f"chg_{order_id}", "amount": amount}


def refund_card(charge: dict) -> None:
    print(f"  refunding charge {charge['charge_id']} (${charge['amount']})")


def reserve_flight(order_id: str) -> dict:
    print(f"  reserving flight for {order_id}")
    return {"flight_reservation_id": f"flt_{order_id}"}


def cancel_flight(reservation: dict) -> None:
    print(f"  cancelling flight {reservation['flight_reservation_id']}")


def reserve_hotel(order_id: str, *, should_fail: bool) -> dict:
    print(f"  reserving hotel for {order_id}")
    if should_fail:
        raise RuntimeError("hotel API returned no availability")
    return {"hotel_reservation_id": f"htl_{order_id}"}


def build_trip_saga(order_id: str, amount: float, *, hotel_should_fail: bool) -> Saga:
    saga = Saga(name=f"book-trip-{order_id}")
    saga.add_step(
        lambda: charge_card(order_id, amount),
        name="charge_card",
        compensation=refund_card,
    )
    saga.add_step(
        lambda: reserve_flight(order_id),
        name="reserve_flight",
        compensation=cancel_flight,
    )
    saga.add_step(
        lambda: reserve_hotel(order_id, should_fail=hotel_should_fail),
        name="reserve_hotel",
        # No compensation needed -- if this step itself fails, there's
        # nothing to undo for it specifically.
    )
    return saga


if __name__ == "__main__":
    print("Happy path: all three steps succeed.")
    ok_saga = build_trip_saga("T1", 850.0, hotel_should_fail=False)
    results = ok_saga.run()
    print("Results:", results)

    print("\nFailure path: hotel reservation fails, card + flight are rolled back.")
    failing_saga = build_trip_saga("T2", 850.0, hotel_should_fail=True)
    try:
        failing_saga.run()
    except SagaExecutionError as exc:
        print(f"Saga failed at step '{exc.step_name}': {exc.original_exception}")
        print(f"Compensated (in order): {exc.compensated_steps}")
        print(f"Compensation errors: {exc.compensation_errors}")
