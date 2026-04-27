"""Round-trip tests for every public dataclass that ships ``to_dict``/``from_dict``.

A checkpoint is only useful if it can survive a JSON round-trip with zero
loss. Snapshots are already exercised end-to-end by the store tests, but
these tests pin each dataclass in isolation so regressions surface with
pinpoint accuracy instead of inside a full engine run.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from zeroflow import (
    Event,
    RunMetadata,
    RunSnapshot,
    StepRecord,
    WorkflowError,
    WorkflowState,
)

# ---------------------------------------------------------------------------
# WorkflowError
# ---------------------------------------------------------------------------


def _sample_error() -> WorkflowError:
    return WorkflowError(
        code="HANDLER_EXCEPTION",
        message="boom",
        node="compute",
        retryable=True,
        details={"attempt": 2, "nested": {"k": "v"}},
        cause_type="ValueError",
    )


def test_workflow_error_round_trips_through_dict() -> None:
    original = _sample_error()
    restored = WorkflowError.from_dict(original.to_dict())
    assert restored == original


def test_workflow_error_round_trips_through_json_dumps() -> None:
    original = _sample_error()
    restored = WorkflowError.from_dict(json.loads(json.dumps(original.to_dict())))
    assert restored == original


def test_workflow_error_defaults_are_preserved() -> None:
    original = WorkflowError(code="X", message="m")
    restored = WorkflowError.from_dict(original.to_dict())
    assert restored == original
    assert restored.node is None
    assert restored.retryable is False
    assert restored.details == {}
    assert restored.cause_type is None


# ---------------------------------------------------------------------------
# StepRecord
# ---------------------------------------------------------------------------


def _sample_step_record(*, with_error: bool) -> StepRecord:
    return StepRecord(
        step=3,
        wave=2,
        node="node_a",
        status="failed" if with_error else "succeeded",
        ts="2026-04-22T10:00:00+00:00",
        outputs=["ok", "retry"],
        error=_sample_error() if with_error else None,
        waiting=False,
        duration_ms=42,
    )


@pytest.mark.parametrize("with_error", [False, True])
def test_step_record_round_trips(*, with_error: bool) -> None:
    original = _sample_step_record(with_error=with_error)
    restored = StepRecord.from_dict(original.to_dict())
    assert restored == original


def test_step_record_round_trips_through_json_dumps() -> None:
    original = _sample_step_record(with_error=True)
    restored = StepRecord.from_dict(json.loads(json.dumps(original.to_dict())))
    assert restored == original


# ---------------------------------------------------------------------------
# WorkflowState
# ---------------------------------------------------------------------------


def test_workflow_state_round_trips_empty() -> None:
    original = WorkflowState(input={})
    restored = WorkflowState.from_dict(original.to_dict())
    assert restored.input == original.input
    assert restored.workflow == original.workflow
    assert restored.node == original.node


def test_workflow_state_round_trips_full_payload() -> None:
    original = WorkflowState(
        input={"goal": "refactor", "items": [1, 2, 3]},
        workflow={"count": 5, "nested": {"a": True}},
        node={"plan": {"tasks": ["x", "y"]}, "exec": {"done": True}},
    )
    restored = WorkflowState.from_dict(original.to_dict())
    assert restored.input == original.input
    assert restored.workflow == original.workflow
    assert restored.node == original.node


def test_workflow_state_to_dict_is_deep_copy() -> None:
    original = WorkflowState(input={"nested": {"k": "v"}})
    snapshot = original.to_dict()
    snapshot["input"]["nested"]["k"] = "mutated"
    assert original.input["nested"]["k"] == "v"


# ---------------------------------------------------------------------------
# RunSnapshot
# ---------------------------------------------------------------------------


def _sample_run_snapshot() -> RunSnapshot:
    return RunSnapshot(
        run_id="run-123",
        workflow_name="wf",
        wf_hash="a" * 64,
        status="running",
        step=4,
        wave=2,
        ready_now=["b", "c"],
        ready_next_wave=["d"],
        state=WorkflowState(input={"x": 1}, workflow={"y": 2}, node={"a": {"z": 3}}),
        trace=["a"],
        tags=["demo"],
        audit_trail=[_sample_step_record(with_error=False)],
        arrivals={"2:merge": ["a", "b"]},
        executed_this_wave=["a"],
        last_node="a",
        waiting_prompt=None,
        last_error=None,
        started_at="2026-04-22T10:00:00+00:00",
        updated_at="2026-04-22T10:01:00+00:00",
        deadline_ts=None,
    )


def test_run_snapshot_round_trips() -> None:
    original = _sample_run_snapshot()
    restored = RunSnapshot.from_dict(original.to_dict())
    assert restored.to_dict() == original.to_dict()


def test_run_snapshot_round_trips_through_json_dumps() -> None:
    original = _sample_run_snapshot()
    serialised = json.dumps(original.to_dict(), sort_keys=True)
    restored = RunSnapshot.from_dict(json.loads(serialised))
    assert restored.to_dict() == original.to_dict()


def test_run_snapshot_preserves_waiting_and_error_payload() -> None:
    original = _sample_run_snapshot()
    original.waiting_prompt = "Approve?"
    original.last_error = _sample_error()
    original.deadline_ts = 1_700_000_000.5
    restored = RunSnapshot.from_dict(original.to_dict())
    assert restored.waiting_prompt == "Approve?"
    assert restored.last_error == _sample_error()
    assert restored.deadline_ts == pytest.approx(1_700_000_000.5)


# ---------------------------------------------------------------------------
# RunMetadata
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("with_error", [False, True])
def test_run_metadata_to_dict_is_json_stable(*, with_error: bool) -> None:
    metadata = RunMetadata(
        workflow_name="wf",
        workflow_hash="b" * 64,
        status="failed" if with_error else "succeeded",
        started_at="2026-04-22T10:00:00+00:00",
        updated_at="2026-04-22T10:05:00+00:00",
        current_node="compute",
        waiting=False,
        last_error=_sample_error() if with_error else None,
    )
    payload = metadata.to_dict()
    reloaded = json.loads(json.dumps(payload, sort_keys=True))
    assert reloaded == payload


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------


def test_event_payload_is_json_serialisable() -> None:
    event = Event(
        run_id="r",
        step=1,
        wave=1,
        node="n",
        kind="checkpoint",
        ts="2026-04-22T10:00:00+00:00",
        data={"status": "running", "nested": {"k": [1, 2]}},
    )
    payload: dict[str, Any] = {
        "run_id": event.run_id,
        "step": event.step,
        "wave": event.wave,
        "node": event.node,
        "kind": event.kind,
        "ts": event.ts,
        "data": event.data,
    }
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
