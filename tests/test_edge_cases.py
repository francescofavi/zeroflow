"""Edge-case coverage beyond the happy-path scenarios in ``test_core.py``.

Focuses on:

- validation failure modes that are otherwise hard to trigger
- engine construction helpers (``from_files``, ``workflow_hash``,
  ``load_snapshot`` without a store)
- ``HandlerResult.error`` and the "handler not registered" path
- retry / cancel / timeout interleavings inside the retry loop
- ``WorkflowContext`` niceties (``remaining_seconds``, ``node_state``,
  ``emit`` with no payload)
- ``EnginePolicy`` value validation and ``json_clone`` failure
- store scanning with stray filesystem entries
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from zeroflow import (
    Event,
    HandlerResult,
    InMemoryWorkflowStore,
    JsonFileWorkflowStore,
    WorkflowContext,
    WorkflowEngine,
)
from zeroflow.core.models import EnginePolicy, json_clone
from zeroflow.core.validation import validate_workflow_definition

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _single_node_wf(**overrides: Any) -> dict[str, Any]:
    wf: dict[str, Any] = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    wf.update(overrides)
    return wf


def _ok(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"])


# ---------------------------------------------------------------------------
# Validation edge cases
# ---------------------------------------------------------------------------


def test_validation_rejects_workflow_missing_all_top_level_keys() -> None:
    with pytest.raises(ValueError, match="missing required keys"):
        validate_workflow_definition({})


def test_validation_rejects_empty_nodes_dict() -> None:
    wf = {"workflow_name": "wf", "default_entry_node": "a", "nodes": {}}
    with pytest.raises(ValueError, match="non-empty dict"):
        validate_workflow_definition(wf)


def test_validation_rejects_nodes_not_a_dict() -> None:
    wf = {"workflow_name": "wf", "default_entry_node": "a", "nodes": []}
    with pytest.raises(ValueError, match="non-empty dict"):
        validate_workflow_definition(wf)


def test_validation_rejects_unknown_default_error_node() -> None:
    wf = _single_node_wf(default_error_node="ghost")
    with pytest.raises(ValueError, match="error node 'ghost' not in nodes"):
        validate_workflow_definition(wf)


def test_validation_rejects_node_missing_outputs_key() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a"}},
    }
    with pytest.raises(ValueError, match="missing 'outputs'"):
        validate_workflow_definition(wf)


def test_validation_rejects_outputs_value_that_is_not_a_dict() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": []}},
    }
    with pytest.raises(ValueError, match="outputs must be a dict"):
        validate_workflow_definition(wf)


def test_validation_rejects_output_targets_not_a_list() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": {"target_node": "a"}}}},
    }
    with pytest.raises(ValueError, match="must be a list"):
        validate_workflow_definition(wf)


def test_validation_rejects_edge_that_is_not_a_dict() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": ["a"]}}},
    }
    with pytest.raises(ValueError, match="edge must be a dict"):
        validate_workflow_definition(wf)


def test_validation_rejects_empty_target_node_string() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {"a": {"handler": "a", "outputs": {"ok": [{"target_node": ""}]}}},
    }
    with pytest.raises(ValueError, match="non-empty string"):
        validate_workflow_definition(wf)


def test_validation_rejects_state_contract_with_non_list_reads_from() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": []},
                "state_contract": {"reads_from": "not-a-list", "writes_to": []},
            }
        },
    }
    with pytest.raises(ValueError, match="state_contract"):
        validate_workflow_definition(wf)


def test_validation_rejects_and_join_with_empty_wait_for() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {
                "handler": "b",
                "join": {"mode": "and", "wait_for": []},
                "outputs": {"ok": []},
            },
        },
    }
    with pytest.raises(ValueError, match="non-empty wait_for"):
        validate_workflow_definition(wf)


def test_validation_accepts_explicit_or_join_without_wait_for() -> None:
    """``join.mode == "or"`` is the default; declaring it explicitly is fine
    and the ``wait_for`` check is skipped for OR-joins."""
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "merge"}]}},
            "merge": {
                "handler": "merge",
                "join": {"mode": "or"},  # explicit OR, no wait_for needed
                "outputs": {"ok": []},
            },
        },
    }
    validate_workflow_definition(wf)  # should not raise


def test_validation_rejects_and_join_wait_for_that_is_not_a_list() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {
                "handler": "b",
                "join": {"mode": "and", "wait_for": "a"},  # str, not list
                "outputs": {"ok": []},
            },
        },
    }
    with pytest.raises(ValueError, match="non-empty wait_for"):
        validate_workflow_definition(wf)


def test_validation_rejects_and_join_wait_for_that_names_unknown_node() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {
                "handler": "b",
                "join": {"mode": "and", "wait_for": ["ghost"]},
                "outputs": {"ok": []},
            },
        },
    }
    with pytest.raises(ValueError, match="unknown node 'ghost'"):
        validate_workflow_definition(wf)


# ---------------------------------------------------------------------------
# Engine construction and identity
# ---------------------------------------------------------------------------


def test_workflow_hash_property_is_stable_for_identical_definitions() -> None:
    wf = _single_node_wf()
    engine1 = WorkflowEngine(wf, handlers={"a": _ok})
    engine2 = WorkflowEngine(json.loads(json.dumps(wf)), handlers={"a": _ok})
    assert engine1.workflow_hash == engine2.workflow_hash
    assert len(engine1.workflow_hash) == 64  # sha256 hex


def test_workflow_hash_changes_when_definition_changes() -> None:
    wf_a = _single_node_wf()
    wf_b = _single_node_wf(workflow_name="renamed")
    h_a = WorkflowEngine(wf_a, handlers={"a": _ok}).workflow_hash
    h_b = WorkflowEngine(wf_b, handlers={"a": _ok}).workflow_hash
    assert h_a != h_b


def test_from_files_loads_json_workflow_definition(tmp_path: Path) -> None:
    path = tmp_path / "wf.json"
    path.write_text(json.dumps(_single_node_wf()), encoding="utf-8")
    engine = WorkflowEngine.from_files(path, handlers={"a": _ok})
    result = engine.run()
    assert result.success
    assert result.trace == ["a"]


def test_load_snapshot_without_store_raises_runtime_error() -> None:
    engine = WorkflowEngine(_single_node_wf(), handlers={"a": _ok})
    with pytest.raises(RuntimeError, match="no store configured"):
        engine.load_snapshot("any")


# ---------------------------------------------------------------------------
# Handler registration / HandlerResult.error edge cases
# ---------------------------------------------------------------------------


def test_unregistered_handler_routes_to_error_node() -> None:
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": "missing_type", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    engine = WorkflowEngine(wf, handlers={"err": _ok})
    result = engine.run()
    assert result.success
    assert result.trace == ["a", "err"]
    assert result.state.workflow["__error__"]["code"] == "HANDLER_NOT_REGISTERED"


def test_handler_key_set_to_none_produces_type_not_declared_error() -> None:
    """``validation`` requires the ``handler`` key, but accepts ``None`` as value.

    The engine must surface that as ``HANDLER_TYPE_NOT_DECLARED`` rather
    than crash when looking the handler up.
    """
    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": None, "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"err": _ok}).run()
    assert result.success
    assert result.state.workflow["__error__"]["code"] == "HANDLER_TYPE_NOT_DECLARED"


def test_unregistered_handler_without_error_node_fails_run() -> None:
    wf = _single_node_wf()
    wf["nodes"]["a"]["handler"] = "missing_type"
    engine = WorkflowEngine(wf, handlers={})
    result = engine.run()
    assert not result.success
    assert result.error is not None
    assert result.error.code == "HANDLER_NOT_REGISTERED"


def test_handler_result_with_explicit_error_skips_execution_and_routes() -> None:
    from zeroflow.core.models import WorkflowError

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(
            outputs=[],
            error=WorkflowError(code="CUSTOM_FAIL", message="handler-chose-to-fail"),
        )

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "default_error_node": "err",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": []}},
            "err": {"handler": "err", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": a, "err": _ok}).run()
    assert result.success  # run recovered via error node
    assert result.trace == ["a", "err"]
    assert result.state.workflow["__error__"]["code"] == "CUSTOM_FAIL"


def test_error_node_equal_to_failing_node_fails_the_run() -> None:
    """``default_error_node == failing node`` must not create an infinite loop."""

    def a(ctx: WorkflowContext) -> HandlerResult:
        raise ValueError("boom")

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "default_error_node": "a",  # same node
        "nodes": {"a": {"handler": "a", "outputs": {"ok": []}}},
    }
    result = WorkflowEngine(wf, handlers={"a": a}).run()
    assert not result.success
    assert result.error is not None
    assert result.error.code == "HANDLER_EXCEPTION"


# ---------------------------------------------------------------------------
# Retry / cancel / timeout interplay
# ---------------------------------------------------------------------------


def test_retry_policy_with_zero_max_retries_gives_single_attempt() -> None:
    calls = {"n": 0}

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        calls["n"] += 1
        raise RuntimeError("nope")

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": []},
                "run_policy": {"max_retries": 0, "retry_sleep_seconds": 0.0},
            }
        },
    }
    result = WorkflowEngine(wf, handlers={"a": flaky}).run()
    assert calls["n"] == 1
    assert not result.success


def test_retry_exhaustion_preserves_last_exception_type_in_error() -> None:
    def sequence() -> list[Exception]:
        return [ValueError("v1"), TypeError("t1"), KeyError("last")]

    queue = sequence()

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        raise queue.pop(0)

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": []},
                "run_policy": {"max_retries": 2, "retry_sleep_seconds": 0.0},
            }
        },
    }
    result = WorkflowEngine(wf, handlers={"a": flaky}).run()
    assert not result.success
    assert result.error is not None
    assert result.error.cause_type == "KeyError"


def test_workflow_timeout_expires_between_retry_attempts() -> None:
    """When the handler keeps raising and the workflow deadline expires
    during a retry sleep, the run ends with ``WORKFLOW_TIMEOUT`` — the
    timeout check inside the retry loop catches it before the next
    handler call."""

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        raise RuntimeError("still failing")

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": []},
                "run_policy": {"max_retries": 10, "retry_sleep_seconds": 0.2},
            }
        },
        "engine_policy": {"workflow_timeout_seconds": 0.3},
    }
    result = WorkflowEngine(wf, handlers={"a": flaky}).run()
    assert not result.success
    assert result.error is not None
    assert result.error.code == "WORKFLOW_TIMEOUT"


def test_cancel_during_retry_sleep_produces_cancelled_error() -> None:
    attempt_started = threading.Event()
    engine_ref: dict[str, WorkflowEngine] = {}

    def flaky(ctx: WorkflowContext) -> HandlerResult:
        attempt_started.set()
        raise RuntimeError("transient")

    def canceller() -> None:
        attempt_started.wait(timeout=1.0)
        # Fire cancel while engine is sleeping between retries.
        engine_ref["engine"].cancel()

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {
                "handler": "a",
                "outputs": {"ok": []},
                "run_policy": {"max_retries": 5, "retry_sleep_seconds": 0.5},
            }
        },
    }
    engine = WorkflowEngine(wf, handlers={"a": flaky})
    engine_ref["engine"] = engine

    thread = threading.Thread(target=canceller)
    thread.start()
    start = time.perf_counter()
    result = engine.run()
    elapsed = time.perf_counter() - start
    thread.join(timeout=1.0)

    assert not result.success
    assert result.error is not None
    # Cancellation observed during retry-sleep is reported via the handler
    # result's error code, not the ``cancelled=True`` wrapper (which only
    # fires when ``_check_stop_conditions`` sees the cancel flag between
    # nodes). The contract is: the run ends fast, with WORKFLOW_CANCELLED.
    assert result.error.code == "WORKFLOW_CANCELLED"
    # Should not have spun through all 5 retries (0.5s each).
    assert elapsed < 1.5


# ---------------------------------------------------------------------------
# WorkflowContext surface
# ---------------------------------------------------------------------------


def test_context_remaining_seconds_is_none_when_no_deadline() -> None:
    seen: dict[str, float | None] = {}

    def probe(ctx: WorkflowContext) -> HandlerResult:
        seen["remaining"] = ctx.remaining_seconds()
        return HandlerResult(outputs=["ok"])

    WorkflowEngine(_single_node_wf(), handlers={"a": probe}).run()
    assert seen["remaining"] is None


def test_context_remaining_seconds_counts_down_with_workflow_timeout() -> None:
    seen: dict[str, float | None] = {}

    def probe(ctx: WorkflowContext) -> HandlerResult:
        seen["remaining"] = ctx.remaining_seconds()
        assert not ctx.is_timed_out()
        return HandlerResult(outputs=["ok"])

    wf = _single_node_wf(engine_policy={"workflow_timeout_seconds": 10.0})
    WorkflowEngine(wf, handlers={"a": probe}).run()
    assert seen["remaining"] is not None
    assert 0.0 < seen["remaining"] <= 10.0


def test_context_node_state_property_returns_current_node_updates() -> None:
    seen: dict[str, dict[str, Any]] = {}

    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], node_updates={"v": 1})

    def b(ctx: WorkflowContext) -> HandlerResult:
        # b has no prior node state — node_state should be empty dict
        seen["b_before"] = ctx.node_state
        return HandlerResult(outputs=["ok"], node_updates={"v": 2})

    def c(ctx: WorkflowContext) -> HandlerResult:
        seen["c_reads_b"] = dict(ctx.state.node["b"])
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": [{"target_node": "c"}]}},
            "c": {"handler": "c", "outputs": {"ok": []}},
        },
    }
    WorkflowEngine(wf, handlers={"a": a, "b": b, "c": c}).run()
    assert seen["b_before"] == {}
    assert seen["c_reads_b"] == {"v": 2}


def test_context_emit_without_data_sends_event_with_defaults() -> None:
    events: list[Event] = []

    def a(ctx: WorkflowContext) -> HandlerResult:
        ctx.emit("custom:tick")  # no payload
        return HandlerResult(outputs=["ok"])

    WorkflowEngine(
        _single_node_wf(),
        handlers={"a": a},
        event_callback=events.append,
    ).run()

    custom = [e for e in events if e.kind == "custom:tick"]
    assert len(custom) == 1
    # emit() injects the node name into data even when caller passes nothing.
    assert custom[0].data == {"node": "a"}


def test_tags_from_multiple_nodes_accumulate_in_order() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], tags=["x"])

    def b(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], tags=["y", "z"])

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "a",
        "nodes": {
            "a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
            "b": {"handler": "b", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"a": a, "b": b}).run()
    assert result.tags == ["x", "y", "z"]


# ---------------------------------------------------------------------------
# EnginePolicy + json_clone
# ---------------------------------------------------------------------------


def test_engine_policy_rejects_zero_or_negative_max_steps() -> None:
    with pytest.raises(ValueError, match="max_steps"):
        EnginePolicy.from_dict({"max_steps": 0})
    with pytest.raises(ValueError, match="max_steps"):
        EnginePolicy.from_dict({"max_steps": -1})


def test_engine_policy_rejects_zero_or_negative_workflow_timeout() -> None:
    with pytest.raises(ValueError, match="workflow_timeout_seconds"):
        EnginePolicy.from_dict({"workflow_timeout_seconds": 0})
    with pytest.raises(ValueError, match="workflow_timeout_seconds"):
        EnginePolicy.from_dict({"workflow_timeout_seconds": -1.5})


def test_engine_policy_from_none_uses_defaults() -> None:
    policy = EnginePolicy.from_dict(None)
    assert policy.strict_outputs is True
    assert policy.max_steps is None
    assert policy.workflow_timeout_seconds is None
    assert policy.persist_checkpoints is True


def test_json_clone_raises_type_error_on_non_serializable_payload() -> None:
    with pytest.raises(TypeError, match="JSON-serializable"):
        json_clone({"bad": {1, 2, 3}})


# ---------------------------------------------------------------------------
# Scheduling: OR-join deduplication + loopback targeting entry
# ---------------------------------------------------------------------------


def test_or_join_deduplicates_target_enqueued_twice_in_same_wave() -> None:
    calls = {"m": 0}

    def fan(ctx: WorkflowContext) -> HandlerResult:
        # A single output whose edges both point to `merge` — merge
        # must still fire only once under OR-join semantics.
        return HandlerResult(outputs=["go"])

    def merge(ctx: WorkflowContext) -> HandlerResult:
        calls["m"] += 1
        return HandlerResult(outputs=["ok"])

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "fan",
        "nodes": {
            "fan": {
                "handler": "fan",
                "outputs": {
                    "go": [
                        {"target_node": "merge"},
                        {"target_node": "merge"},
                    ]
                },
            },
            "merge": {"handler": "merge", "outputs": {"ok": []}},
        },
    }
    result = WorkflowEngine(wf, handlers={"fan": fan, "merge": merge}).run()
    assert result.success
    assert calls["m"] == 1
    assert result.trace == ["fan", "merge"]


def test_loopback_edge_can_target_the_entry_node() -> None:
    waves_seen: list[int] = []

    def entry(ctx: WorkflowContext) -> HandlerResult:
        waves_seen.append(ctx.wave)
        iteration = ctx.state.workflow.get("i", 0) + 1
        if iteration >= 3:
            return HandlerResult(outputs=["done"], workflow_updates={"i": iteration})
        return HandlerResult(outputs=["again"], workflow_updates={"i": iteration})

    wf = {
        "workflow_name": "wf",
        "default_entry_node": "entry",
        "nodes": {
            "entry": {
                "handler": "entry",
                "outputs": {
                    "again": [{"target_node": "entry", "is_loopback": True}],
                    "done": [],
                },
            }
        },
    }
    result = WorkflowEngine(wf, handlers={"entry": entry}).run()
    assert result.success
    assert len(waves_seen) == 3
    assert waves_seen == sorted(set(waves_seen))  # strictly increasing waves


# ---------------------------------------------------------------------------
# JsonFileWorkflowStore scanning edge cases
# ---------------------------------------------------------------------------


def test_json_store_list_metadata_ignores_stray_files_and_bare_dirs(tmp_path: Path) -> None:
    store = JsonFileWorkflowStore(tmp_path)

    # Seed one real snapshot via a real run.
    engine = WorkflowEngine(_single_node_wf(), handlers={"a": _ok}, store=store)
    engine.run(run_id="real-run")

    # Stray sibling file at the store root — should be ignored.
    (tmp_path / "random.txt").write_text("not-a-run", encoding="utf-8")
    # Bare directory without a metadata.json — should also be ignored.
    (tmp_path / "half-written-run").mkdir()

    runs = store.list_metadata()
    assert [meta.workflow_name for meta in runs] == ["wf"]
    names = {meta.workflow_name for meta in runs}
    assert names == {"wf"}


def test_in_memory_store_load_unknown_run_raises_key_error() -> None:
    store = InMemoryWorkflowStore()
    with pytest.raises(KeyError, match="nope"):
        store.load_snapshot("nope")


# ---------------------------------------------------------------------------
# HITL edge cases
# ---------------------------------------------------------------------------


def test_waiting_without_prompt_is_accepted() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        if ctx.state.workflow.get("__resume__") is None:
            return HandlerResult(outputs=[], waiting=True)  # no prompt
        return HandlerResult(outputs=["ok"])

    engine = WorkflowEngine(_single_node_wf(), handlers={"a": a})
    paused = engine.run()
    assert paused.waiting
    assert paused.waiting_prompt is None
    assert paused.checkpoint is not None

    resumed = engine.run_from_checkpoint(paused.checkpoint, resume_input={"go": True})
    assert resumed.success


def test_resume_with_handler_that_does_not_re_invoke_pauses_again() -> None:
    def a(ctx: WorkflowContext) -> HandlerResult:
        # Each entry pauses again unless resume carries "final": True.
        resume = ctx.state.workflow.get("__resume__") or {}
        if not resume.get("final"):
            return HandlerResult(outputs=[], waiting=True, waiting_prompt="again?")
        return HandlerResult(outputs=["ok"])

    engine = WorkflowEngine(_single_node_wf(), handlers={"a": a})
    first = engine.run()
    assert first.waiting
    assert first.checkpoint is not None

    second = engine.run_from_checkpoint(first.checkpoint, resume_input={"final": False})
    assert second.waiting
    assert second.waiting_prompt == "again?"
    assert second.checkpoint is not None

    third = engine.run_from_checkpoint(second.checkpoint, resume_input={"final": True})
    assert third.success
