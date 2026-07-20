"""Framework-integration helpers for latch.

Each module here targets one agent framework's tool-calling shape
(`latch.adapters.openai`, `latch.adapters.langchain`, ...). None of them
are imported by `latch/__init__.py` and none of them import the target
framework's SDK -- they operate on the plain, duck-typed shapes those
SDKs already use (a callable, an object with `.id`/`.function.name`/
`.function.arguments`, etc.), so `latch` keeps its zero-required-
dependency core even though these adapters exist. Import the one you need
directly, e.g. `from latch.adapters.openai import dispatch_tool_call`.

These are real, tested, importable modules -- not just inline doc
snippets -- but they are intentionally *not* a separate PyPI package. See
CLAUDE.md "Packaging decision (v0.3 adapters)" for why: a standalone
adapter package only makes sense once an adapter needs to hard-depend on
the target SDK, which none of these do yet.
"""

__all__: "list[str]" = []
