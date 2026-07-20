import json
from types import SimpleNamespace

import pytest

from latch import InMemoryStore
from latch.adapters.langchain import resilient_tool
from latch.adapters.openai import dispatch_tool_call


def _fake_tool_call(call_id: str, name: str, arguments: dict) -> SimpleNamespace:
    """Duck-typed stand-in for openai.types.chat.ChatCompletionMessageToolCall
    -- dispatch_tool_call never imports openai, so this is enough."""
    return SimpleNamespace(
        id=call_id,
        function=SimpleNamespace(name=name, arguments=json.dumps(arguments)),
    )


class TestOpenAIAdapter:
    def test_dispatches_to_matching_tool_and_formats_response(self):
        def send_email(to: str, idempotency_key: str) -> dict:
            return {"to": to, "status": "sent", "key": idempotency_key}

        tool_call = _fake_tool_call("call_123", "send_email", {"to": "a@example.com"})

        message = dispatch_tool_call(tool_call, tools={"send_email": send_email})

        assert message["role"] == "tool"
        assert message["tool_call_id"] == "call_123"
        content = json.loads(message["content"])
        assert content == {"to": "a@example.com", "status": "sent", "key": "call_123"}

    def test_run_id_prefixes_idempotency_key(self):
        seen = {}

        def tool(idempotency_key: str) -> dict:
            seen["key"] = idempotency_key
            return {"ok": True}

        tool_call = _fake_tool_call("call_1", "tool", {})
        dispatch_tool_call(tool_call, tools={"tool": tool}, run_id="run-42")

        assert seen["key"] == "run-42:call_1"

    def test_unknown_tool_raises_keyerror(self):
        tool_call = _fake_tool_call("call_1", "does_not_exist", {})
        with pytest.raises(KeyError, match="does_not_exist"):
            dispatch_tool_call(tool_call, tools={})

    def test_tool_exception_propagates_unmodified(self):
        def flaky(idempotency_key: str):
            raise RuntimeError("downstream failure")

        tool_call = _fake_tool_call("call_1", "flaky", {})
        with pytest.raises(RuntimeError, match="downstream failure"):
            dispatch_tool_call(tool_call, tools={"flaky": flaky})

    def test_pass_idempotency_key_false_omits_kwarg(self):
        def tool(x: int) -> int:
            return x * 2

        tool_call = _fake_tool_call("call_1", "tool", {"x": 5})
        message = dispatch_tool_call(tool_call, tools={"tool": tool}, pass_idempotency_key=False)
        assert json.loads(message["content"]) == 10

    def test_dispatch_works_with_real_idempotent_decorator_dedupe(self):
        from latch import idempotent

        calls = []

        @idempotent()
        def create_order(order_id: str) -> dict:
            calls.append(order_id)
            return {"order_id": order_id, "status": "created"}

        tool_call = _fake_tool_call("call_1", "create_order", {"order_id": "A1"})

        first = dispatch_tool_call(tool_call, tools={"create_order": create_order})
        second = dispatch_tool_call(tool_call, tools={"create_order": create_order})

        assert first == second
        assert len(calls) == 1  # second dispatch was a cache hit, function not re-invoked


class TestLangChainAdapter:
    def test_wraps_with_idempotency_only(self):
        store = InMemoryStore()
        calls = []

        def create_order(order_id: str) -> dict:
            calls.append(order_id)
            return {"order_id": order_id}

        wrapped = resilient_tool(create_order, idempotency_store=store)

        r1 = wrapped(order_id="A1", idempotency_key="k1")
        r2 = wrapped(order_id="A1", idempotency_key="k1")

        assert r1 == r2 == {"order_id": "A1"}
        assert len(calls) == 1

    def test_no_layers_configured_returns_equivalent_callable(self):
        def add(a: int, b: int) -> int:
            return a + b

        wrapped = resilient_tool(add)
        assert wrapped(2, 3) == 5

    def test_all_layers_compose(self):
        from latch import BudgetGuardrail, CircuitBreaker

        store = InMemoryStore()
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        guardrail = BudgetGuardrail(max_calls=10, window_seconds=60)
        calls = []

        def paid_call(x: int) -> int:
            calls.append(x)
            return x * 2

        wrapped = resilient_tool(
            paid_call,
            idempotency_store=store,
            idempotency_ttl_seconds=60,
            breaker=breaker,
            timeout_seconds=5.0,
            guardrail=guardrail,
        )

        result = wrapped(x=21, idempotency_key="k1")
        assert result == 42
        assert guardrail.call_count == 1
        assert breaker.state.value == "closed"

        # Retry with same key is a cache hit at the idempotency layer, so
        # the underlying function body doesn't run again -- but the
        # budget guardrail sits *outside* idempotency in the stack (same
        # order as examples/resilient_tool_example.py), so each attempt
        # still counts against the budget regardless of cache hits.
        wrapped(x=21, idempotency_key="k1")
        assert guardrail.call_count == 2
        assert calls == [21]

    def test_langchain_structuredtool_integration(self):
        """Real integration against langchain_core, if it's installed
        (it's an optional dev-extra, not a required dependency -- this
        module never imports langchain_core itself)."""
        langchain_core = pytest.importorskip("langchain_core")
        from langchain_core.tools import StructuredTool

        calls = []

        def get_weather(city: str) -> str:
            """Look up the current weather for a city."""
            calls.append(city)
            return f"sunny in {city}"

        wrapped = resilient_tool(get_weather)
        tool = StructuredTool.from_function(func=wrapped, name="get_weather")

        result = tool.invoke({"city": "Austin"})
        assert result == "sunny in Austin"
        assert calls == ["Austin"]
        assert langchain_core is not None  # imported successfully

    def test_langchain_schema_inference_includes_idempotency_key(self):
        """Regression test: functools.wraps sets __wrapped__, which makes
        inspect.signature() auto-unwrap to the *original* function and
        silently lose the idempotency_key parameter @idempotent actually
        requires. LangChain's StructuredTool.from_function builds its
        pydantic schema from inspect.signature()/get_type_hints(), so
        without resilient_tool's signature patch, idempotency_key would
        be dropped before the tool function is ever called -- and
        `.invoke()` would raise IdempotencyKeyMissingError even though
        the caller passed it. This exercises the full stack (idempotency
        + circuit breaker + timeout + budget guardrail together, same as
        examples/langchain_adapter_example.py) to make sure the patch
        survives every layer of wrapping, not just idempotency alone."""
        pytest.importorskip("langchain_core")
        from latch import BudgetGuardrail, CircuitBreaker
        from langchain_core.tools import StructuredTool

        store = InMemoryStore()

        def charge_card(order_id: str, amount: float) -> dict:
            """Charge a customer's card for an order."""
            return {"order_id": order_id, "amount": amount, "status": "charged"}

        wrapped = resilient_tool(
            charge_card,
            idempotency_store=store,
            breaker=CircuitBreaker(failure_threshold=3, recovery_timeout=5.0),
            timeout_seconds=5.0,
            guardrail=BudgetGuardrail(max_calls=10, window_seconds=60),
        )
        tool = StructuredTool.from_function(func=wrapped, name="charge_card")

        assert "idempotency_key" in tool.args

        first = tool.invoke({"order_id": "A1", "amount": 42.0, "idempotency_key": "k1"})
        second = tool.invoke({"order_id": "A1", "amount": 42.0, "idempotency_key": "k1"})
        assert first == second == {"order_id": "A1", "amount": 42.0, "status": "charged"}
