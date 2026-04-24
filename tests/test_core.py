"""Targeted tests for zeroflow.core.

Covers the behaviors that distinguish zeroflow from a bare workflow runner:
linear flow, conditional routing, loopback cycles, HITL pause/resume,
error routing, retry, cancel, checkpoints, custom events, AND-join and
wave semantics.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from zeroflow import (
    EVENT_CHECKPOINT,
    EVENT_STORE_SAVED,
    EVENT_WF_CANCELLED,
    EVENT_WF_WAITING,
    Event,
    HandlerResult,
    InMemoryWorkflowStore,
    JsonFileWorkflowStore,
    RunSnapshot,
    WorkflowContext,
    WorkflowEngine,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_events() -> tuple[list[Event], Callable[[Event], None]]:
    events: list[Event] = []

    def cb(evt: Event) -> None:
        events.append(evt)

    return events, cb


def _linear_wf() -> dict[str, Any]:
    return {
        "workflow_name": "linear",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": [{"target_node": "c"}]}},
            "c": {"handler": "c", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }


# ---------------------------------------------------------------------------
# Linear
# ---------------------------------------------------------------------------


def test_linear_runs_all_nodes_in_order() -> None:
    calls: list[str] = []

    def make(name: str) -> Callable[[WorkflowContext], HandlerResult]:
        def h(ctx: WorkflowContext) -> HandlerResult:
            calls.append(name)
            return HandlerResult(outputs=["ok"], node_updates={"from": name})

        return h

    engine = WorkflowEngine(
        _linear_wf(),
        handlers={"a": make("a"), "b": make("b"), "c": make("c"), "err": make("err")},
    )
    result = engine.run()

    assert result.success
    assert calls == ["a", "b", "c"]
    assert result.trace == ["a", "b", "c"]
    assert result.state.node["b"] == {"from": "b"}


def test_initial_input_is_readable() -> None:
    seen: dict[str, Any] = {}

    def a(ctx: WorkflowContext) -> HandlerResult:
        seen["initial"] = dict(ctx.state.input)
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "init",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    WorkflowEngine(wf, handlers={"a": a}).run(initial_input={"x": 42})
    assert seen["initial"] == {"x": 42}


def test_workflow_updates_merge_into_workflow_state() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], workflow_updates={"shared": {"k": 1}})

    def b(ctx: WorkflowContext) -> HandlerResult:
        assert ctx.state.workflow["shared"] == {"k": 1}
        return HandlerResult(outputs=["ok"], workflow_updates={"shared": {"k2": 2}})

    wf = {
        "workflow_name": "merge",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": a, "b": b}).run()
    assert result.success
    assert result.state.workflow["shared"] == {"k": 1, "k2": 2}


# ---------------------------------------------------------------------------
# Conditional routing
# ---------------------------------------------------------------------------


def test_conditional_routes_on_handler_output() -> None:
    wf = {
        "workflow_name": "branch",
        "default_entry_node": "decide",
        "nodes": {
            "decide": {
                "handler": "decide",
                "outputs": {
                    "left": [{"target_node": "L"}],
                    "right": [{"target_node": "R"}],
                },
            },
            "L": {"handler": "terminal", "outputs": {"ok": []}},
            "R": {"handler": "terminal", "outputs": {"ok": []}},
        },
    }
    for chosen, expected in [("left", ["decide", "L"]), ("right", ["decide", "R"])]:

        def decide(ctx: WorkflowContext, _c: str = chosen) -> HandlerResult:
            return HandlerResult(outputs=[_c])

        def terminal(ctx: WorkflowContext) -> HandlerResult:
            return HandlerResult(outputs=["ok"])

        engine = WorkflowEngine(wf, handlers={"decide": decide, "terminal": terminal})
        result = engine.run()
        assert result.trace == expected


def test_or_join_runs_target_once_even_with_multiple_declared_predecessors() -> None:
    """Branch that reconverges: only one path fires, target runs once."""
    wf = {
        "workflow_name": "or_join",
        "default_entry_node": "start",
        "nodes": {
            "start": {
                "handler": "start",
                "outputs": {
                    "left": [{"target_node": "L"}],
                    "right": [{"target_node": "R"}],
                },
            },
            "L": {"handler": "terminal", "outputs": {"ok": [{"target_node": "end"}]}},
            "R": {"handler": "terminal", "outputs": {"ok": [{"target_node": "end"}]}},
            "end": {"handler": "terminal", "outputs": {"ok": []}},
        },
    }
    calls: list[str] = []

    def start(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["left"])

    def terminal(ctx: WorkflowContext) -> HandlerResult:
        calls.append(ctx.node_name)
        return HandlerResult(outputs=["ok"])

    result = WorkflowEngine(wf, handlers={"start": start, "terminal": terminal}).run()

    assert result.success
    assert calls == ["L", "end"]
    assert "R" not in result.trace


def test_unknown_output_is_rejected_under_strict_outputs() -> None:
    """Strict outputs is the default: an unlisted output routes to the
    error node. The error node is then free to recover — the error is
    recorded in `state.workflow["__error__"]` and in the audit trail,
    not latched as a terminal failure."""
    seen: dict[str, Any] = {}

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["nonexistent"])

    def err(ctx: WorkflowContext) -> HandlerResult:
        seen["caught"] = dict(ctx.state.workflow["__error__"])
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "strict",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": a, "err": err}).run()
    assert result.trace == ["a", "err"]
    assert seen["caught"]["code"] == "UNDECLARED_OUTPUT"
    # audit trail records the failure of "a" even though the error node
    # recovered.
    failed = [s for s in result.audit_trail if s.node == "a"]
    assert failed and failed[0].status == "failed"


def test_undeclared_output_without_error_node_fails_the_run() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["nonexistent"])

    wf = {
        "workflow_name": "strict_nofallback",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    result = WorkflowEngine(wf, handlers={"a": a}).run()
    assert not result.success
    assert result.error is not None
    assert result.error.code == "UNDECLARED_OUTPUT"


def test_unknown_output_is_warning_when_strict_outputs_disabled() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["nonexistent"])

    wf = {
        "workflow_name": "loose",
        "default_entry_node": "a",
        "engine_policy": {"strict_outputs": False},
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    events, cb = _collect_events()
    result = WorkflowEngine(wf, handlers={"a": a}, event_callback=cb).run()
    assert result.success
    assert result.trace == ["a"]
    assert any(e.kind == "node:warning" for e in events)


# ---------------------------------------------------------------------------
# Cycles with loopback termination
# ---------------------------------------------------------------------------


def test_cycle_terminates_when_handler_chooses_exit_output() -> None:
    wf = {
        "workflow_name": "loop",
        "default_entry_node": "tick",
        "nodes": {
            "tick": {
                "handler": "tick",
                "outputs": {
                    "continue": [{"target_node": "tick", "is_loopback": True}],
                    "done": [{"target_node": "end"}],
                },
            },
            "end": {"handler": "end", "outputs": {"ok": []}},
        },
    }
    target_iters = 5

    def tick(ctx: WorkflowContext) -> HandlerResult:
        prev = ctx.state.node.get("tick", {}).get("n", 0)
        n = prev + 1
        out = "done" if n >= target_iters else "continue"
        return HandlerResult(outputs=[out], node_updates={"n": n})

    def end(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(
            outputs=["ok"], workflow_updates={"final": ctx.state.node["tick"]["n"]}
        )

    engine = WorkflowEngine(wf, handlers={"tick": tick, "end": end})
    result = engine.run()

    assert result.success
    assert result.trace == ["tick"] * target_iters + ["end"]
    assert result.state.workflow["final"] == target_iters


def test_forward_cycle_without_loopback_marker_is_rejected() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": [{"target_node": "a"}]}},
        },
    }
    with pytest.raises(ValueError, match="cycle"):
        WorkflowEngine(wf, handlers={})


def test_loopback_edges_schedule_in_next_wave() -> None:
    """Loopback moves the target to the next wave; forward edges stay in
    the current wave. Over two iterations the wave counter must grow."""
    wf = {
        "workflow_name": "loop_entry",
        "default_entry_node": "loop",
        "nodes": {
            "loop": {
                "handler": "loop",
                "outputs": {
                    "back": [{"target_node": "loop", "is_loopback": True}],
                    "out": [{"target_node": "end"}],
                },
            },
            "end": {"handler": "end", "outputs": {"ok": []}},
        },
    }
    waves: list[int] = []
    count = [0]

    def loop(ctx: WorkflowContext) -> HandlerResult:
        waves.append(ctx.wave)
        count[0] += 1
        return HandlerResult(outputs=["out"] if count[0] >= 2 else ["back"])

    def end(ctx: WorkflowContext) -> HandlerResult:
        waves.append(ctx.wave)
        return HandlerResult(outputs=["ok"])

    result = WorkflowEngine(wf, handlers={"loop": loop, "end": end}).run()
    assert result.success
    assert result.trace == ["loop", "loop", "end"]
    # loop#1 in wave 1, loop#2 in wave 2 (loopback), end stays in wave 2
    assert waves == [1, 2, 2]


# ---------------------------------------------------------------------------
# HITL
# ---------------------------------------------------------------------------


def test_waiting_handler_pauses_run_and_returns_checkpoint() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], node_updates={"step": 1})

    def gate(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(
            outputs=[],
            node_updates={"awaiting": True},
            waiting=True,
            waiting_prompt="Confirm to proceed",
        )

    def b(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], node_updates={"step": 3})

    wf = {
        "workflow_name": "hitl",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "gate"}]}},
            "gate": {"handler": "gate", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    events, cb = _collect_events()
    engine = WorkflowEngine(wf, handlers={"a": a, "gate": gate, "b": b}, event_callback=cb)
    first = engine.run()

    assert first.waiting
    assert first.waiting_prompt == "Confirm to proceed"
    assert first.checkpoint is not None
    assert any(e.kind == EVENT_WF_WAITING for e in events)
    assert first.trace == ["a", "gate"]


def test_resume_from_checkpoint_continues_the_run() -> None:
    state_box: dict[str, Any] = {}

    def gate(ctx: WorkflowContext) -> HandlerResult:
        if "__resume__" in ctx.state.workflow:
            return HandlerResult(outputs=["ok"], node_updates={"approved": True})
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="wait")

    def b(ctx: WorkflowContext) -> HandlerResult:
        state_box["b_saw"] = dict(ctx.state.node["gate"])
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "resume",
        "default_entry_node": "gate",
        "nodes": {
            "gate": {"handler": "gate", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    engine = WorkflowEngine(wf, handlers={"gate": gate, "b": b})
    paused = engine.run()
    assert paused.waiting
    assert paused.checkpoint is not None

    resumed = engine.run_from_checkpoint(paused.checkpoint, resume_input={"ok": True})
    assert resumed.success
    assert state_box["b_saw"] == {"approved": True}


def test_resume_rejects_checkpoint_with_mismatched_wf_hash() -> None:
    wf = {
        "workflow_name": "v1",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    engine = WorkflowEngine(wf, handlers={"a": lambda c: HandlerResult(outputs=["ok"])})
    stale = RunSnapshot.from_dict(
        {
            "run_id": "r",
            "workflow_name": "v1",
            "wf_hash": "deadbeef" * 8,
            "status": "waiting",
            "step": 0,
            "wave": 1,
            "ready_now": ["a"],
            "ready_next_wave": [],
            "state": {"input": {}, "workflow": {}, "node": {}},
            "trace": [],
            "tags": [],
            "audit_trail": [],
            "arrivals": {},
        }
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        engine.run_from_checkpoint(stale)


# ---------------------------------------------------------------------------
# Error routing
# ---------------------------------------------------------------------------


def test_handler_exception_routes_to_error_node_for_recovery() -> None:
    """When a handler raises and a `default_error_node` exists, the engine
    records the failure in the audit trail, exposes it via
    `state.workflow["__error__"]`, and runs the error node. The error
    node's return value determines whether the overall run succeeds."""
    seen: dict[str, Any] = {}

    def boom(ctx: WorkflowContext) -> HandlerResult:
        raise RuntimeError("boom")

    def err(ctx: WorkflowContext) -> HandlerResult:
        seen["caught"] = dict(ctx.state.workflow["__error__"])
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "err",
        "default_entry_node": "boom",
        "default_error_node": "err",
        "nodes": {
            "boom": {"handler": "boom", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"boom": boom, "err": err}).run()
    assert result.trace == ["boom", "err"]
    assert seen["caught"]["code"] == "HANDLER_EXCEPTION"
    assert "boom" in seen["caught"]["message"]
    boom_step = next(s for s in result.audit_trail if s.node == "boom")
    assert boom_step.status == "failed"
    assert boom_step.error is not None
    assert boom_step.error.code == "HANDLER_EXCEPTION"


def test_handler_explicit_error_routes_to_error_node() -> None:
    from zeroflow import WorkflowError

    seen: dict[str, Any] = {}

    def bad(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(
            outputs=[],
            error=WorkflowError(code="REJECTED", message="rejected input", node="bad"),
        )

    def err(ctx: WorkflowContext) -> HandlerResult:
        seen["caught"] = dict(ctx.state.workflow["__error__"])
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "err2",
        "default_entry_node": "bad",
        "default_error_node": "err",
        "nodes": {
            "bad": {"handler": "bad", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"bad": bad, "err": err}).run()
    assert result.trace == ["bad", "err"]
    assert seen["caught"]["code"] == "REJECTED"


def test_no_error_node_and_failure_returns_failed_result() -> None:
    def bad(ctx: WorkflowContext) -> HandlerResult:
        raise RuntimeError("oops")

    wf = {
        "workflow_name": "nofallback",
        "default_entry_node": "bad",
        "nodes": {"bad": {"handler": "bad", "outputs": {"ok": []}}},
    }
    result = WorkflowEngine(wf, handlers={"bad": bad}).run()
    assert not result.success
    assert result.error is not None
    assert "oops" in result.error.message


def test_retry_policy_reruns_then_succeeds() -> None:
    attempts = [0]

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        attempts[0] += 1
        if attempts[0] < 3:
            raise RuntimeError("transient")
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "retry",
        "default_entry_node": "flaky",
        "nodes": {
            "flaky": {
                "handler": "flaky",
                "outputs": {"ok": []},
                "run_policy": {"max_retries": 3, "retry_sleep_seconds": 0.01},
            }
        },
    }
    result = WorkflowEngine(wf, handlers={"flaky": flaky}).run()
    assert result.success
    assert attempts[0] == 3


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


def test_cancel_stops_run_before_next_node() -> None:
    engine_box: dict[str, Any] = {}

    def a(ctx: WorkflowContext) -> HandlerResult:
        engine_box["engine"].cancel()
        return HandlerResult(outputs=["ok"])

    def b(ctx: WorkflowContext) -> HandlerResult:
        raise AssertionError("should not run after cancel")

    wf = {
        "workflow_name": "cancel",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    events, cb = _collect_events()
    engine = WorkflowEngine(wf, handlers={"a": a, "b": b}, event_callback=cb)
    engine_box["engine"] = engine
    result = engine.run()

    assert result.cancelled
    assert result.trace == ["a"]
    assert any(e.kind == EVENT_WF_CANCELLED for e in events)


def test_cancel_flag_is_reset_between_runs() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "reset",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    engine = WorkflowEngine(wf, handlers={"a": a})
    engine.cancel()
    # Next run() must clear the flag and succeed.
    result = engine.run()
    assert result.success


# ---------------------------------------------------------------------------
# Custom events and checkpoints
# ---------------------------------------------------------------------------


def test_handler_can_emit_custom_events() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        ctx.emit("goal_normalized", {"goal": {"intent": "do_thing"}})
        ctx.emit("plan_ready", {"tasks": [1, 2, 3]})
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "cust",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    events, cb = _collect_events()
    WorkflowEngine(wf, handlers={"a": a}, event_callback=cb).run()

    kinds = [e.kind for e in events]
    assert "goal_normalized" in kinds
    assert "plan_ready" in kinds
    goal_evt = next(e for e in events if e.kind == "goal_normalized")
    assert goal_evt.node == "a"
    assert goal_evt.data["goal"] == {"intent": "do_thing"}


def test_checkpoint_emitted_after_each_node_when_store_configured() -> None:
    def make(name: str) -> Callable[[WorkflowContext], HandlerResult]:
        def h(ctx: WorkflowContext) -> HandlerResult:
            return HandlerResult(outputs=["ok"], node_updates={"who": name})

        return h

    wf = _linear_wf()
    events, cb = _collect_events()
    store = InMemoryWorkflowStore()
    WorkflowEngine(
        wf,
        handlers={n: make(n) for n in wf["nodes"]},
        event_callback=cb,
        store=store,
    ).run()

    checkpoints = [e for e in events if e.kind == EVENT_CHECKPOINT]
    # wf:start + a + b + c + wf:end (after each node + lifecycle writes)
    assert len(checkpoints) >= 3
    final_cp = checkpoints[-1]
    restored = RunSnapshot.from_dict(final_cp.data["state"])
    assert restored.trace == ["a", "b", "c"]
    assert restored.state.node["a"] == {"who": "a"}


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------


def test_services_are_forwarded_untouched_to_handlers() -> None:
    sentinel = object()

    def a(ctx: WorkflowContext) -> HandlerResult:
        assert ctx.services["mine"] is sentinel
        assert ctx.services["num"] == 42
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "svc",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    WorkflowEngine(wf, handlers={"a": a}, services={"mine": sentinel, "num": 42}).run()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_validation_rejects_unknown_target() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": [{"target_node": "ghost"}]}}},
    }
    with pytest.raises(ValueError, match="unknown target"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_missing_entry() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "missing",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    with pytest.raises(ValueError, match="entry node"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_missing_handler() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {"a": {"outputs": {"ok": []}}},
    }
    with pytest.raises(ValueError, match="missing 'handler'"):
        WorkflowEngine(wf, handlers={})


# ---------------------------------------------------------------------------
# AND-join
# ---------------------------------------------------------------------------


def _and_join_diamond_wf() -> dict[str, Any]:
    return {
        "workflow_name": "and_diamond",
        "default_entry_node": "A",
        "nodes": {
            "A": {
                "handler": "A",
                "outputs": {"go": [{"target_node": "B"}, {"target_node": "C"}]},
            },
            "B": {"handler": "B", "outputs": {"ok": [{"target_node": "D"}]}},
            "C": {"handler": "C", "outputs": {"ok": [{"target_node": "D"}]}},
            "D": {
                "handler": "D",
                "join": {"mode": "and", "wait_for": ["B", "C"]},
                "outputs": {"ok": []},
            },
        },
    }


def test_and_join_waits_for_all_declared_predecessors() -> None:
    calls: list[str] = []

    def make(name: str, out: str = "ok") -> Callable[[WorkflowContext], HandlerResult]:
        def h(ctx: WorkflowContext) -> HandlerResult:
            calls.append(name)
            return HandlerResult(outputs=[out], node_updates={"from": name})

        return h

    def d(ctx: WorkflowContext) -> HandlerResult:
        calls.append("D")
        # D must see both B and C node_updates when it finally runs.
        assert ctx.state.node["B"] == {"from": "B"}
        assert ctx.state.node["C"] == {"from": "C"}
        return HandlerResult(outputs=["ok"])

    result = WorkflowEngine(
        _and_join_diamond_wf(),
        handlers={"A": make("A", "go"), "B": make("B"), "C": make("C"), "D": d},
    ).run()

    assert result.success
    assert result.trace == ["A", "B", "C", "D"]
    assert calls.count("D") == 1


def test_and_join_arrivals_captured_in_checkpoint_on_pause() -> None:
    """When C pauses, the checkpoint records B's prior arrival on D but D
    is not yet scheduled. Also exercises the arrivals serialization."""

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["go"])

    def b(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    def c(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="wait")

    def d(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    paused = WorkflowEngine(
        _and_join_diamond_wf(),
        handlers={"A": a, "B": b, "C": c, "D": d},
    ).run()

    assert paused.waiting
    cp = paused.checkpoint
    assert cp is not None
    # The arrival is keyed "wave:target"; B arrived in wave 1.
    assert cp.arrivals.get("1:D") == ["B"]
    assert "D" not in cp.ready_now
    assert "D" not in cp.ready_next_wave

    # Round-trip through dict to ensure arrivals survives serialization.
    round_tripped = RunSnapshot.from_dict(cp.to_dict())
    assert round_tripped.arrivals == cp.arrivals


def test_and_join_resets_after_fire_to_support_loopback() -> None:
    """An AND-join target can run multiple times across loopback iterations."""
    wf = {
        "workflow_name": "loop_and",
        "default_entry_node": "tick",
        "nodes": {
            "tick": {
                "handler": "tick",
                "outputs": {"go": [{"target_node": "B"}, {"target_node": "C"}]},
            },
            "B": {"handler": "pass", "outputs": {"ok": [{"target_node": "merge"}]}},
            "C": {"handler": "pass", "outputs": {"ok": [{"target_node": "merge"}]}},
            "merge": {
                "handler": "merge",
                "join": {"mode": "and", "wait_for": ["B", "C"]},
                "outputs": {
                    "again": [{"target_node": "tick", "is_loopback": True}],
                    "done": [{"target_node": "end"}],
                },
            },
            "end": {"handler": "end", "outputs": {"ok": []}},
        },
    }
    counts = {"merge": 0}

    def tick(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["go"])

    def pass_(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    def merge(ctx: WorkflowContext) -> HandlerResult:
        counts["merge"] += 1
        out = "done" if counts["merge"] >= 2 else "again"
        return HandlerResult(outputs=[out])

    def end(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    result = WorkflowEngine(
        wf,
        handlers={"tick": tick, "pass": pass_, "merge": merge, "end": end},
    ).run()

    assert result.success
    assert counts["merge"] == 2
    assert result.trace == [
        "tick",
        "B",
        "C",
        "merge",
        "tick",
        "B",
        "C",
        "merge",
        "end",
    ]


def test_and_of_ors_via_intermediate_merge_nodes() -> None:
    """(CARD | PAYPAL) & (EXPRESS | STANDARD) via two OR-nodes + one AND-join."""
    wf = {
        "workflow_name": "checkout",
        "default_entry_node": "entry",
        "nodes": {
            "entry": {
                "handler": "entry",
                "outputs": {"go": [{"target_node": "pay"}, {"target_node": "ship"}]},
            },
            "pay": {
                "handler": "pay",
                "outputs": {
                    "card": [{"target_node": "PAY_CARD"}],
                    "paypal": [{"target_node": "PAY_PAYPAL"}],
                },
            },
            "ship": {
                "handler": "ship",
                "outputs": {
                    "express": [{"target_node": "SHIP_EXPRESS"}],
                    "standard": [{"target_node": "SHIP_STANDARD"}],
                },
            },
            "PAY_CARD": {"handler": "pass", "outputs": {"ok": [{"target_node": "PAY_DONE"}]}},
            "PAY_PAYPAL": {"handler": "pass", "outputs": {"ok": [{"target_node": "PAY_DONE"}]}},
            "SHIP_EXPRESS": {
                "handler": "pass",
                "outputs": {"ok": [{"target_node": "SHIP_DONE"}]},
            },
            "SHIP_STANDARD": {
                "handler": "pass",
                "outputs": {"ok": [{"target_node": "SHIP_DONE"}]},
            },
            "PAY_DONE": {"handler": "pass", "outputs": {"ok": [{"target_node": "CONFIRM"}]}},
            "SHIP_DONE": {"handler": "pass", "outputs": {"ok": [{"target_node": "CONFIRM"}]}},
            "CONFIRM": {
                "handler": "pass",
                "join": {"mode": "and", "wait_for": ["PAY_DONE", "SHIP_DONE"]},
                "outputs": {"ok": []},
            },
        },
    }

    def passthrough(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    handlers = {
        "pass": passthrough,
        "entry": lambda c: HandlerResult(outputs=["go"]),
        "pay": lambda c: HandlerResult(outputs=["paypal"]),
        "ship": lambda c: HandlerResult(outputs=["express"]),
    }

    result = WorkflowEngine(wf, handlers=handlers).run()

    assert result.success
    assert "CONFIRM" in result.trace
    assert "PAY_CARD" not in result.trace
    assert "SHIP_STANDARD" not in result.trace


def test_validation_rejects_and_join_without_wait_for() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "join": {"mode": "and"}, "outputs": {"ok": []}},
        },
    }
    with pytest.raises(ValueError, match="wait_for"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_wait_for_with_non_predecessor() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {
                "handler": "b",
                "join": {"mode": "and", "wait_for": ["a", "c"]},
                "outputs": {"ok": []},
            },
            "c": {"handler": "c", "outputs": {"ok": []}},
        },
    }
    with pytest.raises(ValueError, match="no declared edge"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_bad_join_mode() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "join": {"mode": "xor"}, "outputs": {"ok": []}},
        },
    }
    with pytest.raises(ValueError, match=r"join\.mode"):
        WorkflowEngine(wf, handlers={})


# ---------------------------------------------------------------------------
# Store integration
# ---------------------------------------------------------------------------


def test_in_memory_store_persists_snapshot_for_resume() -> None:
    wf = {
        "workflow_name": "store",
        "default_entry_node": "gate",
        "nodes": {
            "gate": {"handler": "gate", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }

    def gate(ctx: WorkflowContext) -> HandlerResult:
        if "__resume__" in ctx.state.workflow:
            return HandlerResult(outputs=["ok"])
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="go?")

    def b(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    store = InMemoryWorkflowStore()
    engine = WorkflowEngine(wf, handlers={"gate": gate, "b": b}, store=store)

    paused = engine.run(run_id="run-1")
    assert paused.waiting

    loaded = engine.load_snapshot("run-1")
    assert loaded.run_id == "run-1"
    assert loaded.status == "waiting"

    resumed = engine.run_from_checkpoint(loaded, resume_input={"yes": True})
    assert resumed.success


# ---------------------------------------------------------------------------
# Diamond forward edges — OR-join must not re-run an already-executed target
# ---------------------------------------------------------------------------


def test_diamond_forward_does_not_re_execute_merge_target() -> None:
    """A→B, A→D, D→B (all forward). B must run exactly once per wave."""
    calls: list[str] = []

    def make(name: str) -> Callable[[WorkflowContext], HandlerResult]:
        def h(ctx: WorkflowContext) -> HandlerResult:
            calls.append(name)
            return HandlerResult(outputs=["ok"])

        return h

    wf = {
        "workflow_name": "diamond",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": [{"target_node": "b"}, {"target_node": "d"}]},
            },
            "b": {"handler": "b", "outputs": {"ok": []}},
            "d": {"handler": "d", "outputs": {"ok": [{"target_node": "b"}]}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": make("a"), "b": make("b"), "d": make("d")}).run()

    assert result.success
    assert calls.count("b") == 1
    assert result.trace.count("b") == 1


def test_loopback_can_still_re_execute_node_in_next_wave() -> None:
    """Executed-this-wave tracking must reset across waves."""
    counter = {"n": 0}

    def loop_handler(ctx: WorkflowContext) -> HandlerResult:
        counter["n"] += 1
        if counter["n"] < 3:
            return HandlerResult(outputs=["again"])
        return HandlerResult(outputs=["done"])

    def sink(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "loop",
        "default_entry_node": "step",
        "nodes": {
            "step": {
                "handler": "step",
                "outputs": {
                    "again": [{"target_node": "step", "is_loopback": True}],
                    "done": [{"target_node": "end"}],
                },
            },
            "end": {"handler": "end", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"step": loop_handler, "end": sink}).run()

    assert result.success
    assert counter["n"] == 3
    assert result.trace == ["step", "step", "step", "end"]


# ---------------------------------------------------------------------------
# Engine policy — max_steps, workflow_timeout_seconds, require_json_state
# ---------------------------------------------------------------------------


def test_max_steps_policy_aborts_run() -> None:
    def spin(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["again"])

    wf = {
        "workflow_name": "spin",
        "default_entry_node": "s",
        "engine_policy": {"max_steps": 3},
        "nodes": {
            "s": {
                "handler": "s",
                "outputs": {"again": [{"target_node": "s", "is_loopback": True}]},
            },
        },
    }
    result = WorkflowEngine(wf, handlers={"s": spin}).run()

    assert not result.success
    assert result.error is not None
    assert result.error.code == "MAX_STEPS_EXCEEDED"


def test_workflow_timeout_policy_aborts_run() -> None:
    import time

    def slow(ctx: WorkflowContext) -> HandlerResult:
        time.sleep(0.05)
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "slow",
        "default_entry_node": "a",
        "engine_policy": {"workflow_timeout_seconds": 0.01},
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": slow, "b": slow}).run()

    assert not result.success
    assert result.error is not None
    assert result.error.code == "WORKFLOW_TIMEOUT"


def test_non_json_node_updates_fail_with_state_serialization_error() -> None:
    class Opaque:
        pass

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], node_updates={"obj": Opaque()})

    wf = {
        "workflow_name": "strict",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    result = WorkflowEngine(wf, handlers={"a": a}).run()

    assert not result.success
    assert result.error is not None
    assert result.error.code == "STATE_SERIALIZATION"


def test_non_json_node_updates_route_to_error_node() -> None:
    class Opaque:
        pass

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], node_updates={"obj": Opaque()})

    def err(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "strict_routed",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": a, "err": err}).run()

    assert result.success
    assert result.trace == ["a", "err"]
    assert result.state.workflow["__error__"]["code"] == "STATE_SERIALIZATION"


# ---------------------------------------------------------------------------
# Retry policy — retry_sleep, deterministic retry count
# ---------------------------------------------------------------------------


def test_retry_sleep_is_honoured_between_attempts() -> None:
    import time

    timestamps: list[float] = []

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        timestamps.append(time.perf_counter())
        if len(timestamps) < 2:
            raise RuntimeError("transient")
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "sleep",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "run_policy": {"max_retries": 1, "retry_sleep_seconds": 0.05},
                "outputs": {"ok": []},
            }
        },
    }
    result = WorkflowEngine(wf, handlers={"a": flaky}).run()

    assert result.success
    assert len(timestamps) == 2
    assert timestamps[1] - timestamps[0] >= 0.04


def test_retry_exhausted_routes_to_error_node() -> None:
    attempts: list[int] = []

    def always_fail(ctx: WorkflowContext) -> HandlerResult:
        attempts.append(1)
        raise RuntimeError("nope")

    def err(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "retry",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {
                "handler": "a",
                "run_policy": {"max_retries": 2, "retry_sleep_seconds": 0},
                "outputs": {"ok": []},
            },
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": always_fail, "err": err}).run()

    assert result.success  # error node recovered the run
    assert len(attempts) == 3  # 1 initial + 2 retries
    assert result.trace == ["a", "err"]


# ---------------------------------------------------------------------------
# Checkpoint event semantics — independent of persist_checkpoints
# ---------------------------------------------------------------------------


def test_checkpoint_event_fires_even_when_persistence_disabled() -> None:
    events, cb = _collect_events()

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "nopersist",
        "default_entry_node": "a",
        "engine_policy": {"persist_checkpoints": False},
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    store = InMemoryWorkflowStore()
    WorkflowEngine(wf, handlers={"a": a}, event_callback=cb, store=store).run()

    kinds = [e.kind for e in events]
    assert EVENT_CHECKPOINT in kinds
    assert EVENT_STORE_SAVED not in kinds
    assert store.list_metadata() == []  # nothing written


def test_checkpoint_event_fires_without_store() -> None:
    events, cb = _collect_events()

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "nostore",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    WorkflowEngine(wf, handlers={"a": a}, event_callback=cb).run()

    assert any(e.kind == EVENT_CHECKPOINT for e in events)


# ---------------------------------------------------------------------------
# Validation — JSON serializability & edge shape
# ---------------------------------------------------------------------------


def test_validation_rejects_non_json_serializable_workflow() -> None:
    class Opaque:
        pass

    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "config": {"opaque": Opaque()}, "outputs": {"ok": []}}},
    }
    with pytest.raises(ValueError, match="JSON-serializable"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_edge_without_target_node() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": [{}]}}},
    }
    with pytest.raises(ValueError, match="target_node"):
        WorkflowEngine(wf, handlers={})


def test_validation_rejects_edge_with_null_target_node() -> None:
    wf = {
        "workflow_name": "bad",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": [{"target_node": None}]}}},
    }
    with pytest.raises(ValueError, match="target_node"):
        WorkflowEngine(wf, handlers={})


# ---------------------------------------------------------------------------
# JsonFileWorkflowStore — disk I/O round-trip
# ---------------------------------------------------------------------------


def _simple_wf() -> dict[str, Any]:
    return {
        "workflow_name": "disk",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }


def test_json_file_store_round_trips_snapshot(tmp_path: Path) -> None:
    def h(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    store = JsonFileWorkflowStore(base_dir=tmp_path)
    engine = WorkflowEngine(_simple_wf(), handlers={"a": h, "b": h}, store=store)
    result = engine.run(run_id="disk-1")

    assert result.success
    snapshot_path = Path(tmp_path) / "disk-1" / "snapshot.json"
    metadata_path = Path(tmp_path) / "disk-1" / "metadata.json"
    events_path = Path(tmp_path) / "disk-1" / "events.jsonl"
    assert snapshot_path.exists()
    assert metadata_path.exists()
    assert events_path.exists()

    loaded = engine.load_snapshot("disk-1")
    assert loaded.run_id == "disk-1"
    assert loaded.workflow_name == "disk"
    assert loaded.status == "succeeded"


def test_json_file_store_pause_resume_across_instances(tmp_path: Path) -> None:
    def gate(ctx: WorkflowContext) -> HandlerResult:
        if "__resume__" in ctx.state.workflow:
            return HandlerResult(outputs=["ok"])
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="approve?")

    def sink(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "disk_hitl",
        "default_entry_node": "gate",
        "nodes": {
            "gate": {"handler": "gate", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }

    store_a = JsonFileWorkflowStore(base_dir=tmp_path)
    engine_a = WorkflowEngine(wf, handlers={"gate": gate, "b": sink}, store=store_a)
    first = engine_a.run(run_id="rescue")
    assert first.waiting

    store_b = JsonFileWorkflowStore(base_dir=tmp_path)
    engine_b = WorkflowEngine(wf, handlers={"gate": gate, "b": sink}, store=store_b)
    loaded = engine_b.load_snapshot("rescue")
    resumed = engine_b.run_from_checkpoint(loaded, resume_input={"ok": True})
    assert resumed.success


def test_json_file_store_load_missing_run_raises_keyerror(tmp_path: Path) -> None:
    store = JsonFileWorkflowStore(base_dir=tmp_path)
    with pytest.raises(KeyError):
        store.load_snapshot("nope")


def test_store_list_metadata_filters_by_workflow_name(tmp_path: Path) -> None:
    def h(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"])

    def _one_node_wf(name: str) -> dict[str, Any]:
        return {
            "workflow_name": name,
            "default_entry_node": "a",
            "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
        }

    for backend in (InMemoryWorkflowStore(), JsonFileWorkflowStore(base_dir=tmp_path)):
        WorkflowEngine(_one_node_wf("alpha"), handlers={"a": h}, store=backend).run(
            run_id=f"a-{id(backend)}"
        )
        WorkflowEngine(_one_node_wf("beta"), handlers={"a": h}, store=backend).run(
            run_id=f"b-{id(backend)}"
        )

        all_meta = backend.list_metadata()
        assert {m.workflow_name for m in all_meta} == {"alpha", "beta"}

        alpha_only = backend.list_metadata(workflow_name="alpha")
        assert {m.workflow_name for m in alpha_only} == {"alpha"}
