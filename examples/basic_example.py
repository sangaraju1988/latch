"""Minimal usage example: an order-creation tool made retry-safe with latch."""

from latch import idempotent


@idempotent()
def create_order(order_id: str, amount: float) -> dict:
    print(f"Charging {amount} for order {order_id}...")  # simulates a real side effect
    return {"order_id": order_id, "amount": amount, "status": "created"}


if __name__ == "__main__":
    # First call executes and charges.
    result = create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
    print("First call result:", result)

    # Simulated retry after a timeout, same logical operation, same key.
    # This does NOT charge again — it returns the cached result.
    retry_result = create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
    print("Retry result:", retry_result)

    assert result == retry_result
    print("No duplicate charge on retry.")
