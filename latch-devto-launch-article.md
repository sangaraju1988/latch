---
title: I built a small library so my LLM agent stops double-charging people
published: false
tags: python, ai, opensource, llm
---

Short version: agents that call tools over a network eventually retry a call they shouldn't. If the tool isn't idempotent, that retry becomes a duplicate charge, a duplicate email, a duplicate order. This is a decades-old distributed-systems problem wearing a new agent costume, and there wasn't a small, pip-installable fix for the agent version of it. So I built one. It's called `latch`. It is not a novel idea, and I'd rather say that up front than have someone else point it out in the comments.

## The bug, live

Here's the whole thing in one file, no library involved:

```python
def charge_card(order_id, amount):
    time.sleep(0.4)  # the payment API is a little slow today
    ledger[order_id] += 1
    return {"order_id": order_id, "status": "charged"}

def agent_charge_with_naive_retry(order_id, amount, max_retries=2):
    for attempt in range(max_retries):
        thread = threading.Thread(target=lambda: charge_card(order_id, amount))
        thread.start()
        thread.join(timeout=0.2)  # the agent's own client-side timeout
        if thread.is_alive():
            continue  # "no response, let's retry"
        return  # got a response
```

The payment call takes 0.4s. The agent gives up waiting after 0.2s and retries. The first attempt is still running in the background — it wasn't cancelled, it can't be safely cancelled, Python threads don't work that way. So it finishes on its own time and charges the card. Then the retry charges it again. The agent's own view of the world is "everything's fine, got a response eventually." The ledger says otherwise.

This is [`examples/naive_agent_example.py`](https://github.com/sangaraju1988/latch/blob/main/examples/naive_agent_example.py) in the repo, if you want to run it and watch it happen rather than take my word for it.

None of this is exotic. It's the same class of problem as "what happens when a payment gateway's webhook fires twice," which every backend engineer who's touched Stripe has dealt with. The fix has a name — idempotency keys — and it's been standard practice in backend systems for a couple of decades. It just hadn't shown up as an off-the-shelf thing you `pip install` for the agent-tool-calling version of the same problem. That's the gap.

## What latch actually is

A small Python library, no required dependencies, that wraps a tool function so calling it twice with the same `idempotency_key` only executes it once:

```python
from latch import idempotent

@idempotent()
def create_order(order_id: str, amount: float) -> dict:
    return payments_api.charge(order_id, amount)

create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")
create_order(order_id="A1", amount=42.0, idempotency_key="run-7-step-3")  # cache hit, no second charge
```

`idempotency_key` is required, not auto-generated from the arguments. I went back and forth on this — auto-hashing the args is tempting and would make the decorator feel more magical. I didn't do it, because whether two calls with slightly different arguments should count as "the same operation" is a judgment call only the caller has the context to make. Guessing wrong silently is worse than making the caller type one extra kwarg.

Idempotency turned out to be the smallest piece of a slightly bigger problem, so three more decorators and one small orchestration class rode along on the same package over the following weeks:

| what | decorator | stops |
|---|---|---|
| idempotency | `@idempotent` | duplicate side effects on retry |
| circuit breaker | `@circuit_breaker` | hammering a dependency that's already down |
| timeout | `@with_timeout` | a hung call blocking the whole agent loop |
| budget guardrail | `@budget_guardrail` | a runaway loop burning unbounded cost |
| saga | `Saga` | a multi-step plan (charge → book flight → book hotel) left half-done when step 3 fails |

They're independent — use one, use all five, doesn't matter — but they compose on the same function:

```python
@budget_guardrail(max_calls=100, window_seconds=3600)
@circuit_breaker(failure_threshold=5, recovery_timeout=30.0)
@with_timeout(seconds=10)
@idempotent()
def charge_card(order_id: str, amount: float) -> dict:
    ...
```

There's also an opt-in tracing hook (`tracer=`, subscribe to what each primitive is doing — cache hits, circuit trips, saga rollbacks) and a small chaos-injection module for testing your own retry logic against simulated failure/latency instead of hoping it works. Neither is load-bearing to the pitch, so I'll leave the details to the [README](https://github.com/sangaraju1988/latch#readme).

## Does it actually help, or is that just a nice story

I didn't want to ship this on vibes, so there's a benchmark that runs the exact double-charge scenario above, twice, under identical injected latency — once with nothing, once with `@with_timeout` wrapping `@idempotent`:

```
$ python benchmarks/chaos_benchmark.py --seed 1 --operations 30

metric                             naive   protected
----------------------------------------------------
orders attempted                      30          30
orders reported successful            21          30
orders reported failed                 9           0
total real charges issued             65          30
orders double-charged                 21           0
idempotency cache hits               n/a          18
```

30 simulated orders, same seed, same latency curve on both sides. The naive column: 21 of 30 got double-charged, and the agent's own success/failure reporting was wrong for a third of the batch (9 reported as failed outright, despite the card sometimes still getting charged). The protected column: zero double charges, and the "failure" case just becomes a cache hit on the retry instead of a second execution. A second run at a different seed (seed 7, 20 orders) landed at 12 double-charged naive vs. 0 protected — same shape, not a cherry-picked seed.

## The part I actually want to talk about

Before I wrote a "ready for public use" README section, I made myself run a real audit against my own already-published package — not just re-reading the code, but fresh-install smoke tests, every snippet in the docs executed for real, and — this is the part that mattered — actual multi-threaded and `asyncio.gather` concurrency tests. My existing 111 tests were all sequential: call, retry, assert. Nothing had ever hit the decorator with two threads at once.

Turns out that matters. I found four real, silent bugs in code that was already on PyPI:

- `@idempotent` didn't dedupe functions that return `None` — `store.get() is not None` can't tell "nothing cached" from "cached `None`," so a fire-and-forget tool (a delete, a notification) never got deduped at all.
- Two decorated functions sharing a store could collide on the same key and silently return each other's cached result.
- Under real concurrency — not sequential retries, actual threads racing — the "check cache, then execute" sequence wasn't atomic. N threads calling the same key at the same instant could all see a cache miss and all execute.
- The circuit breaker's half-open state let unlimited concurrent calls through as "trial" calls, when the whole point of half-open is exactly one trial call.

Every one of these is the kind of bug that doesn't crash, doesn't throw, doesn't show up in a code review — it just quietly does less than it promises to. I'd rather find that myself before anyone else's production traffic does. Fixed all four, added 17 regression tests specifically targeting concurrency (not just logic), and wrote up exactly what was wrong in the changelog instead of quietly folding it into "misc fixes." If you're evaluating a library like this for anything real, "here's what I found wrong with my own code and how" is more useful to you than a changelog that only ever says "improvements."

## What it's not

It's Alpha software and I'd rather undersell it than have you find the ceiling the hard way:

- `Saga` has no persistence. Compensation runs in-process; if the process dies mid-saga, nothing resumes automatically. If you need crash-durable multi-step workflows, that's a job for Temporal or Step Functions, not this.
- The circuit breaker and budget guardrail hold state per-process. Run five replicas of your service, you get five independent circuits that don't talk to each other, unless you build that coordination yourself.
- A sync `@with_timeout` can't actually kill the underlying call — Python has no safe way to force-kill a thread. It unblocks the caller and raises, but the original call might still be running in the background. This is exactly the scenario `@idempotent` exists for, which is why they're meant to be stacked, not used as substitutes for each other.

None of that is a secret dealbreaker — it's just the actual scope, written down instead of implied away.

## Prior art, one more time

To be clear about credit: SagaLLM (arXiv 2503.11951) already applies the Saga pattern to multi-agent LLM planning, Robust Agent Compensation (ACM CAIS) covers agents learning to compensate for their own failures, and ReliabilityBench is a whole benchmark for exactly this space. Circuit breakers and timeouts are decades-old patterns with existing Python libraries (`pybreaker`, `tenacity`) that do them well in a general context. I'm not claiming to have invented any of this. What I think was actually missing was the boring part — a small, tested, zero-dependency, `pip install`-able version scoped specifically to the shape of an LLM tool call, that a working developer can drop into an agent loop this afternoon without reading a paper first. If that's wrong and something like this already exists and I missed it, I'd genuinely like to know — tell me in the comments.

## Try it

```bash
pip install latch-idempotent
```

- Repo: [github.com/sangaraju1988/latch](https://github.com/sangaraju1988/latch)
- PyPI: [pypi.org/project/latch-idempotent](https://pypi.org/project/latch-idempotent/)
- The double-charge demo above: [`examples/naive_agent_example.py`](https://github.com/sangaraju1988/latch/blob/main/examples/naive_agent_example.py) / [`examples/resilient_agent_example.py`](https://github.com/sangaraju1988/latch/blob/main/examples/resilient_agent_example.py)

If you're building anything that calls a tool with a real side effect and retries on failure — which, if you're building agents, you already are, whether you've thought about it that way or not — I'd be curious whether this is useful to you, or where it falls over. Issues and PRs are welcome, and honestly the "where does this break" feedback is worth more to me right now than stars.
