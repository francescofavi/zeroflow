"""Headline-feature showcase in a single runnable script.

Seven tiny self-contained workflows, one per feature: conditional
routing, loopback, AND-join, retry policy, error routing, HITL
pause/resume, custom events. Each one prints a one-line verdict.
Intended as a compact cheat-sheet to pair with ``01_quickstart.py``
and the larger pedagogical ``tour.py``.

Run with::

    uv run python examples/02_feature_matrix.py

Handlers are all defined at module level (ff_rules: no nested
functions). Per-demo shared state, when needed, lives in a
module-level dict named ``_<demo>_STATE``.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from zeroflow import (
    Event,
    Handler,
    HandlerResult,
    InMemoryWorkflowStore,
    WorkflowContext,
    WorkflowEngine,
    WorkflowResult,
)

POLL_THRESHOLD = 3
RETRY_UNTIL_ATTEMPT = 3
ROUTING_LARGE_CUTOFF = 100


# ---------------------------------------------------------------------------
# 1. Conditional routing — two branches picked by the handler's output label
# ---------------------------------------------------------------------------

_ROUTING_WORKFLOW: dict[str, Any] = {
    "workflow_name": "routing",
    "default_entry_node": "classify",
    "nodes": {
        "classify": {
            "handler": "classify",
            "outputs": {
                "small": [{"target_node": "cheap"}],
                "large": [{"target_node": "premium"}],
            },
        },
        "cheap": {"handler": "passthrough", "outputs": {"ok": []}},
        "premium": {"handler": "passthrough", "outputs": {"ok": []}},
    },
}


def _routing_classify(ctx: WorkflowContext) -> HandlerResult:
    size = int(ctx.state.input.get("size", 0))
    return HandlerResult(outputs=["large" if size >= ROUTING_LARGE_CUTOFF else "small"])


def _routing_passthrough(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"])


def demo_conditional_routing() -> WorkflowResult:
    handlers: dict[str, Handler] = {
        "classify": _routing_classify,
        "passthrough": _routing_passthrough,
    }
    engine = WorkflowEngine(_ROUTING_WORKFLOW, handlers=handlers)
    return engine.run(initial_input={"size": 250})


# ---------------------------------------------------------------------------
# 2. Loopback — iterate until a counter reaches the threshold
# ---------------------------------------------------------------------------

_LOOPBACK_WORKFLOW: dict[str, Any] = {
    "workflow_name": "loopback",
    "default_entry_node": "tick",
    "nodes": {
        "tick": {
            "handler": "tick",
            "outputs": {
                "again": [{"target_node": "tick", "is_loopback": True}],
                "done": [{"target_node": "finish"}],
            },
        },
        "finish": {"handler": "finish", "outputs": {"ok": []}},
    },
}


def _loopback_tick(ctx: WorkflowContext) -> HandlerResult:
    count = ctx.state.workflow.get("count", 0) + 1
    output = "done" if count >= POLL_THRESHOLD else "again"
    return HandlerResult(outputs=[output], workflow_updates={"count": count})


def _loopback_finish(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"])


def demo_loopback() -> WorkflowResult:
    handlers: dict[str, Handler] = {"tick": _loopback_tick, "finish": _loopback_finish}
    return WorkflowEngine(_LOOPBACK_WORKFLOW, handlers=handlers).run()


# ---------------------------------------------------------------------------
# 3. AND-join — wait for every declared predecessor before firing
# ---------------------------------------------------------------------------

_AND_JOIN_WORKFLOW: dict[str, Any] = {
    "workflow_name": "and_join",
    "default_entry_node": "fork",
    "nodes": {
        "fork": {
            "handler": "fork",
            "outputs": {"go": [{"target_node": "left"}, {"target_node": "right"}]},
        },
        "left": {"handler": "left", "outputs": {"ok": [{"target_node": "merge"}]}},
        "right": {"handler": "right", "outputs": {"ok": [{"target_node": "merge"}]}},
        "merge": {
            "handler": "merge",
            "join": {"mode": "and", "wait_for": ["left", "right"]},
            "outputs": {"ok": []},
        },
    },
}


def _and_fork(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["go"])


def _and_left(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"value": 2})


def _and_right(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"value": 3})


def _and_merge(ctx: WorkflowContext) -> HandlerResult:
    total = ctx.state.node["left"]["value"] + ctx.state.node["right"]["value"]
    return HandlerResult(outputs=["ok"], workflow_updates={"total": total})


def demo_and_join() -> WorkflowResult:
    handlers: dict[str, Handler] = {
        "fork": _and_fork,
        "left": _and_left,
        "right": _and_right,
        "merge": _and_merge,
    }
    return WorkflowEngine(_AND_JOIN_WORKFLOW, handlers=handlers).run()


# ---------------------------------------------------------------------------
# 4. Retry policy — flaky handler succeeds on the third attempt
# ---------------------------------------------------------------------------

_RETRIES_WORKFLOW: dict[str, Any] = {
    "workflow_name": "retries",
    "default_entry_node": "load",
    "nodes": {
        "load": {
            "handler": "load",
            "outputs": {"ok": []},
            "run_policy": {"max_retries": 3, "retry_sleep_seconds": 0.0},
        },
    },
}


def _retries_load(ctx: WorkflowContext) -> HandlerResult:
    counter = ctx.services["counter"]
    counter["n"] += 1
    if counter["n"] < RETRY_UNTIL_ATTEMPT:
        raise ConnectionError(f"transient (attempt {counter['n']})")
    return HandlerResult(outputs=["ok"], node_updates={"attempts": counter["n"]})


def demo_retries() -> WorkflowResult:
    services = {"counter": {"n": 0}}
    engine = WorkflowEngine(_RETRIES_WORKFLOW, handlers={"load": _retries_load}, services=services)
    return engine.run()


# ---------------------------------------------------------------------------
# 5. Error routing — raised exception routes to default_error_node
# ---------------------------------------------------------------------------

_ERROR_ROUTING_WORKFLOW: dict[str, Any] = {
    "workflow_name": "error_routing",
    "default_entry_node": "validate",
    "default_error_node": "log_error",
    "nodes": {
        "validate": {"handler": "validate", "outputs": {"ok": []}},
        "log_error": {"handler": "log_error", "outputs": {"ok": []}},
    },
}


def _error_validate(ctx: WorkflowContext) -> HandlerResult:
    if not isinstance(ctx.state.input.get("payload"), dict):
        raise ValueError("payload must be a dict")
    return HandlerResult(outputs=["ok"])


def _error_log(ctx: WorkflowContext) -> HandlerResult:
    err = ctx.state.workflow.get("__error__", {})
    return HandlerResult(outputs=["ok"], workflow_updates={"handled": err.get("code")})


def demo_error_routing() -> WorkflowResult:
    handlers: dict[str, Handler] = {"validate": _error_validate, "log_error": _error_log}
    return WorkflowEngine(_ERROR_ROUTING_WORKFLOW, handlers=handlers).run(
        initial_input={"payload": "oops"},
    )


# ---------------------------------------------------------------------------
# 6. HITL pause / resume — handler waits, a second call resumes with input
# ---------------------------------------------------------------------------

_HITL_WORKFLOW: dict[str, Any] = {
    "workflow_name": "hitl",
    "default_entry_node": "gate",
    "nodes": {
        "gate": {
            "handler": "gate",
            "outputs": {"approved": [{"target_node": "finalize"}]},
        },
        "finalize": {"handler": "finalize", "outputs": {"ok": []}},
    },
}


def _hitl_gate(ctx: WorkflowContext) -> HandlerResult:
    resume = ctx.state.workflow.get("__resume__")
    if resume is None:
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="Approve?")
    return HandlerResult(
        outputs=["approved"],
        node_updates={"reviewer": resume.get("reviewer", "?")},
    )


def _hitl_finalize(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(
        outputs=["ok"],
        workflow_updates={"approved_by": ctx.state.node["gate"]["reviewer"]},
    )


def demo_hitl_resume() -> WorkflowResult:
    handlers: dict[str, Handler] = {"gate": _hitl_gate, "finalize": _hitl_finalize}
    store = InMemoryWorkflowStore()
    engine = WorkflowEngine(_HITL_WORKFLOW, handlers=handlers, store=store)
    paused = engine.run(run_id="hitl-1")
    assert paused.waiting and paused.checkpoint is not None
    snapshot = engine.load_snapshot("hitl-1")
    return engine.run_from_checkpoint(snapshot, resume_input={"reviewer": "alice"})


# ---------------------------------------------------------------------------
# 7. Custom events — ctx.emit routed through the event callback
# ---------------------------------------------------------------------------

_CUSTOM_EVENTS_WORKFLOW: dict[str, Any] = {
    "workflow_name": "custom_events",
    "default_entry_node": "plan",
    "nodes": {"plan": {"handler": "plan", "outputs": {"ok": []}}},
}


def _custom_plan(ctx: WorkflowContext) -> HandlerResult:
    ctx.emit("plan_ready", {"tasks": ["a", "b", "c"]})
    observed: list[str] = ctx.services["observed"]
    return HandlerResult(outputs=["ok"], workflow_updates={"observed": list(observed)})


def _custom_record_plan_ready(observed: list[str], event: Event) -> None:
    if event.kind == "plan_ready":
        observed.append(f"{event.kind}:{event.data.get('tasks')}")


def demo_custom_events() -> WorkflowResult:
    observed: list[str] = []
    services = {"observed": observed}
    callback = functools.partial(_custom_record_plan_ready, observed)
    engine = WorkflowEngine(
        _CUSTOM_EVENTS_WORKFLOW,
        handlers={"plan": _custom_plan},
        services=services,
        event_callback=callback,
    )
    return engine.run()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


DEMOS: tuple[tuple[str, str, Callable[[], WorkflowResult]], ...] = (
    ("conditional_routing", "classify/small|large -> cheap|premium", demo_conditional_routing),
    ("loopback", "tick -(again)-> tick ; tick -(done)-> finish", demo_loopback),
    ("and_join", "fork -> {left,right} -AND-> merge", demo_and_join),
    ("retries", "load retries until attempt 3 succeeds", demo_retries),
    ("error_routing", "validate raises -> log_error captures __error__", demo_error_routing),
    ("hitl_resume", "gate pauses, second call resumes with __resume__", demo_hitl_resume),
    ("custom_events", "ctx.emit('plan_ready', ...) observed via callback", demo_custom_events),
)


def main() -> None:
    print("zeroflow — feature matrix (7 demos)\n")
    for label, shape, runner in DEMOS:
        result = runner()
        user_state = {k: v for k, v in result.state.workflow.items() if not k.startswith("__")}
        verdict = "ok" if result.success else f"failed:{result.error.code if result.error else '?'}"
        print(f"  [{label:20s}] {verdict:<18s} trace={result.trace}")
        print(f"     shape: {shape}")
        if user_state:
            print(f"     state: {user_state}")


if __name__ == "__main__":
    main()
