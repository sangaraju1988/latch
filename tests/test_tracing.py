import logging

import pytest

from latch import LoggingTracer, TraceEvent, Tracer


def test_subscribe_and_emit_delivers_event_to_callback():
    tracer = Tracer()
    received = []
    tracer.subscribe(received.append)

    tracer.emit("idempotent", "cache_hit", key="k1")

    assert len(received) == 1
    event = received[0]
    assert isinstance(event, TraceEvent)
    assert event.primitive == "idempotent"
    assert event.event == "cache_hit"
    assert event.metadata == {"key": "k1"}
    assert isinstance(event.timestamp, float)


def test_multiple_subscribers_all_receive_the_event():
    tracer = Tracer()
    a, b = [], []
    tracer.subscribe(a.append)
    tracer.subscribe(b.append)

    tracer.emit("saga", "saga_succeeded")

    assert len(a) == 1
    assert len(b) == 1


def test_unsubscribe_stops_delivery():
    tracer = Tracer()
    received = []

    def callback(event):
        received.append(event)

    tracer.subscribe(callback)
    tracer.emit("timeout", "completed", seconds=0.1)
    tracer.unsubscribe(callback)
    tracer.emit("timeout", "completed", seconds=0.2)

    assert len(received) == 1


def test_subscriber_exception_is_swallowed_not_propagated():
    tracer = Tracer()

    def bad_subscriber(event):
        raise RuntimeError("subscriber is broken")

    good_received = []
    tracer.subscribe(bad_subscriber)
    tracer.subscribe(good_received.append)

    # Must not raise -- a broken subscriber can't break the traced call.
    tracer.emit("circuit_breaker", "call_rejected")

    assert len(good_received) == 1


def test_no_subscribers_does_not_raise():
    tracer = Tracer()
    tracer.emit("budget_guardrail", "budget_exceeded", reason="test")


def test_logging_tracer_logs_events(caplog):
    tracer = LoggingTracer()
    with caplog.at_level(logging.INFO, logger="latch"):
        tracer.emit("idempotent", "cache_miss", key="k1")

    assert len(caplog.records) == 1
    assert "idempotent.cache_miss" in caplog.records[0].message


def test_logging_tracer_accepts_custom_logger():
    custom_logger = logging.getLogger("my-app.latch-events")
    records = []

    class Handler(logging.Handler):
        def emit(self, record):
            records.append(record)

    custom_logger.addHandler(Handler())
    custom_logger.setLevel(logging.INFO)

    tracer = LoggingTracer(logger=custom_logger)
    tracer.emit("saga", "step_started", step="charge")

    assert len(records) == 1
    assert "saga.step_started" in records[0].getMessage()


def test_logging_tracer_also_supports_additional_subscribers():
    tracer = LoggingTracer()
    received = []
    tracer.subscribe(received.append)

    tracer.emit("timeout", "timed_out", seconds=5.0)

    assert len(received) == 1


@pytest.mark.parametrize(
    "primitive,event,metadata",
    [
        ("idempotent", "stored", {"key": "k1"}),
        ("circuit_breaker", "state_changed", {"from_state": "closed", "to_state": "open"}),
        ("timeout", "timed_out", {"seconds": 10.0}),
        ("budget_guardrail", "call_recorded", {"call_count": 1, "total_cost": 0.5, "cost": 0.5}),
        ("saga", "compensation_failed", {"step": "ship", "exception": "RuntimeError('x')"}),
    ],
)
def test_trace_event_roundtrips_metadata_for_every_primitive_shape(primitive, event, metadata):
    tracer = Tracer()
    received = []
    tracer.subscribe(received.append)

    tracer.emit(primitive, event, **metadata)

    assert received[0].primitive == primitive
    assert received[0].event == event
    assert received[0].metadata == metadata
