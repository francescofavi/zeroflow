"""Zeroflow workflow engine.

Wave / loopback model (the mental model for scheduling):

- A **wave** is one logical execution cycle. A node runs at most once
  per wave.
- **Forward edges** route to the same wave. **Loopback edges**
  (`"is_loopback": true`) schedule the target for the next wave.
- Joins declared on the target decide when the target is enqueued:
  - `or` (default): first predecessor to arrive enqueues the target,
    later predecessors in the same wave dedupe.
  - `and`: the target waits until every node in `wait_for` has
    arrived. Arrivals reset after firing, so an AND-join target can
    run again in subsequent waves (e.g. on loopback).
- HITL waiting and error routing do not advance the wave.

The engine here only orchestrates the run. Supporting concerns live
next door:

- `models.py`      — data types and serialisation
- `store.py`       — snapshot/event persistence
- `validation.py`  — static workflow checks
- `errors.py`      — `WorkflowError` factories
- `nodes.py`       — read-only accessors over `workflow_def["nodes"]`
- `scheduling.py`  — queue manipulation, edge routing, OR/AND joins
- `events.py`      — `EventRecorder` (emit + audit + persist)
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from zeroflow.core import errors, nodes, scheduling
from zeroflow.core.events import EventRecorder
from zeroflow.core.models import (
    STATUS_CANCELLED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_WAITING,
    EnginePolicy,
    Event,
    EventCallback,
    Handler,
    HandlerResult,
    RunSnapshot,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    WorkflowState,
    clone_snapshot,
    json_clone,
    now_iso,
)
from zeroflow.core.store import WorkflowStore
from zeroflow.core.validation import validate_workflow_definition

_MS_PER_SECOND = 1000
_HASH_DISPLAY_LEN = 12


class WorkflowEngine:
    def __init__(
        self,
        workflow_def: dict[str, Any],
        handlers: dict[str, Handler],
        services: Mapping[str, Any] | None = None,
        event_callback: EventCallback | None = None,
        store: WorkflowStore | None = None,
    ) -> None:
        validate_workflow_definition(workflow_def)
        self._wf = workflow_def
        self._handlers = handlers
        self._services = services or {}
        self._store = store
        self._cancel_flag = threading.Event()
        self._name = workflow_def["workflow_name"]
        self._entry = workflow_def["default_entry_node"]
        self._error_node = workflow_def.get("default_error_node")
        self._nodes = workflow_def["nodes"]
        self._wf_hash = _hash_workflow(workflow_def)
        self._policy = EnginePolicy.from_dict(workflow_def.get("engine_policy"))
        self._recorder = EventRecorder(
            workflow_name=self._name,
            event_callback=event_callback,
            store=store,
            persist_checkpoints=self._policy.persist_checkpoints,
        )

    @classmethod
    def from_files(
        cls,
        workflow_json_path: str | Path,
        handlers: dict[str, Handler],
        services: Mapping[str, Any] | None = None,
        event_callback: EventCallback | None = None,
        store: WorkflowStore | None = None,
    ) -> WorkflowEngine:
        with Path(workflow_json_path).open(encoding="utf-8") as handle:
            workflow_def = json.load(handle)
        return cls(
            workflow_def,
            handlers,
            services=services,
            event_callback=event_callback,
            store=store,
        )

    def run(
        self,
        initial_input: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> WorkflowResult:
        self._cancel_flag.clear()
        snapshot = self._create_run_snapshot(initial_input, run_id)
        self._recorder.run_started(snapshot)
        return self._run_loop(snapshot)

    def run_from_checkpoint(
        self,
        checkpoint: RunSnapshot,
        resume_input: dict[str, Any] | None = None,
    ) -> WorkflowResult:
        self._cancel_flag.clear()
        snapshot = self._restore_run_snapshot(checkpoint, resume_input)
        self._recorder.run_resumed(snapshot)
        return self._run_loop(snapshot)

    def cancel(self) -> None:
        self._cancel_flag.set()

    @property
    def workflow_hash(self) -> str:
        return self._wf_hash

    def load_snapshot(self, run_id: str) -> RunSnapshot:
        if self._store is None:
            raise RuntimeError("no store configured")
        return self._store.load_snapshot(run_id)

    # Main loop

    def _run_loop(self, snapshot: RunSnapshot) -> WorkflowResult:
        while scheduling.has_pending_work(snapshot):
            stop_result = self._check_stop_conditions(snapshot)
            if stop_result is not None:
                return stop_result

            if scheduling.should_open_next_wave(snapshot):
                scheduling.open_next_wave(snapshot)
                self._recorder.save_progress(snapshot)
                continue

            node_name = scheduling.take_next_node(snapshot)
            stop_result = self._check_step_limit_after_take(snapshot)
            if stop_result is not None:
                return stop_result

            outcome, duration_ms = self._run_single_node(snapshot, node_name)
            terminal_result = self._resolve_node_outcome(snapshot, node_name, outcome, duration_ms)
            if terminal_result is not None:
                return terminal_result

        return self._finish_success(snapshot)

    def _run_single_node(
        self,
        snapshot: RunSnapshot,
        node_name: str,
    ) -> tuple[HandlerResult, int]:
        self._recorder.node_started(snapshot, node_name)
        start_ts = time.perf_counter()
        outcome = self._invoke_handler(snapshot, node_name)
        duration_ms = int((time.perf_counter() - start_ts) * _MS_PER_SECOND)
        return outcome, duration_ms

    def _resolve_node_outcome(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
        duration_ms: int,
    ) -> WorkflowResult | None:
        terminal = self._handle_node_error(snapshot, node_name, outcome, duration_ms)
        if terminal is not None:
            return terminal

        terminal = self._handle_invalid_outputs(snapshot, node_name, outcome, duration_ms)
        if terminal is not None:
            return terminal

        update_error = _first_non_json_error(outcome.workflow_updates, outcome.node_updates)
        if update_error is not None:
            return self._route_or_fail(
                snapshot,
                node_name,
                errors.state_serialization_error(node_name, update_error),
                outcome.outputs,
                duration_ms,
            )

        self._apply_node_result(snapshot, node_name, outcome)

        terminal = self._handle_waiting(snapshot, node_name, outcome, duration_ms)
        if terminal is not None:
            return terminal

        self._recorder.node_succeeded(snapshot, node_name, outcome, duration_ms)
        scheduling.schedule_outputs(snapshot, self._nodes, node_name, outcome.outputs)
        self._recorder.save_progress(snapshot)
        return None

    def _route_or_fail(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        error: WorkflowError,
        outputs: list[str],
        duration_ms: int,
    ) -> WorkflowResult | None:
        self._recorder.node_failed(snapshot, node_name, error, outputs, duration_ms)
        self._recorder.node_error(snapshot, node_name, error)
        if scheduling.route_to_error_node(snapshot, node_name, error, self._error_node):
            self._recorder.save_progress(snapshot)
            return None
        return self._finish_failure(snapshot, error)

    # Run lifecycle

    def _create_run_snapshot(
        self,
        initial_input: dict[str, Any] | None,
        run_id: str | None,
    ) -> RunSnapshot:
        now = now_iso()
        return RunSnapshot(
            run_id=run_id or uuid.uuid4().hex,
            workflow_name=self._name,
            wf_hash=self._wf_hash,
            status=STATUS_RUNNING,
            step=0,
            wave=1,
            ready_now=[self._entry],
            ready_next_wave=[],
            state=WorkflowState(input=json_clone(dict(initial_input or {}))),
            trace=[],
            tags=[],
            audit_trail=[],
            started_at=now,
            updated_at=now,
            deadline_ts=self._build_workflow_deadline(),
        )

    def _restore_run_snapshot(
        self,
        checkpoint: RunSnapshot,
        resume_input: dict[str, Any] | None,
    ) -> RunSnapshot:
        self._assert_checkpoint_compatibility(checkpoint)
        snapshot = clone_snapshot(checkpoint)
        snapshot.status = STATUS_RUNNING
        snapshot.updated_at = now_iso()
        if resume_input is not None:
            snapshot.state.workflow["__resume__"] = json_clone(dict(resume_input))
        return snapshot

    def _finish_success(self, snapshot: RunSnapshot) -> WorkflowResult:
        snapshot.status = STATUS_SUCCEEDED
        snapshot.updated_at = now_iso()
        self._recorder.workflow_finished(snapshot, success=True)
        self._recorder.save_progress(snapshot)
        return self._build_result(snapshot, success=True)

    def _finish_failure(
        self,
        snapshot: RunSnapshot,
        error: WorkflowError,
    ) -> WorkflowResult:
        snapshot.status = STATUS_FAILED
        snapshot.last_error = error
        snapshot.updated_at = now_iso()
        self._recorder.workflow_finished(snapshot, success=False, error=error)
        self._recorder.save_progress(snapshot)
        return self._build_result(snapshot, success=False, error=error)

    def _finish_waiting(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
    ) -> WorkflowResult:
        snapshot.status = STATUS_WAITING
        snapshot.waiting_prompt = outcome.waiting_prompt
        scheduling.prepend_unique(snapshot.ready_now, node_name)
        snapshot.updated_at = now_iso()
        self._recorder.workflow_waiting(snapshot, node_name, outcome.waiting_prompt)
        self._recorder.save_progress(snapshot)
        return self._build_result(
            snapshot,
            success=False,
            waiting=True,
            waiting_prompt=outcome.waiting_prompt,
            checkpoint=clone_snapshot(snapshot),
        )

    def _finish_cancelled(
        self,
        snapshot: RunSnapshot,
        error: WorkflowError,
    ) -> WorkflowResult:
        snapshot.status = STATUS_CANCELLED
        snapshot.last_error = error
        snapshot.updated_at = now_iso()
        self._recorder.workflow_cancelled(snapshot, error)
        self._recorder.save_progress(snapshot)
        return self._build_result(
            snapshot,
            success=False,
            error=error,
            cancelled=True,
            checkpoint=clone_snapshot(snapshot),
        )

    def _build_result(
        self,
        snapshot: RunSnapshot,
        *,
        success: bool,
        error: WorkflowError | None = None,
        waiting: bool = False,
        waiting_prompt: str | None = None,
        checkpoint: RunSnapshot | None = None,
        cancelled: bool = False,
    ) -> WorkflowResult:
        return WorkflowResult(
            run_id=snapshot.run_id,
            success=success,
            status=snapshot.status,
            trace=list(snapshot.trace),
            tags=list(snapshot.tags),
            state=WorkflowState.from_dict(snapshot.state.to_dict()),
            audit_trail=list(snapshot.audit_trail),
            error=error,
            waiting=waiting,
            waiting_prompt=waiting_prompt,
            checkpoint=checkpoint,
            cancelled=cancelled,
        )

    # Stop / policy checks

    def _check_stop_conditions(self, snapshot: RunSnapshot) -> WorkflowResult | None:
        if self._cancel_flag.is_set():
            return self._finish_cancelled(snapshot, errors.cancelled_error(snapshot.last_node))
        if self._workflow_has_timed_out(snapshot):
            return self._finish_failure(snapshot, errors.workflow_timeout_error(snapshot.last_node))
        return None

    def _check_step_limit_after_take(self, snapshot: RunSnapshot) -> WorkflowResult | None:
        if self._policy.max_steps is None:
            return None
        if snapshot.step <= self._policy.max_steps:
            return None
        return self._finish_failure(
            snapshot, errors.max_steps_error(self._policy.max_steps, snapshot.last_node)
        )

    def _workflow_has_timed_out(self, snapshot: RunSnapshot) -> bool:
        return snapshot.deadline_ts is not None and time.time() >= snapshot.deadline_ts

    def _build_workflow_deadline(self) -> float | None:
        if self._policy.workflow_timeout_seconds is None:
            return None
        return time.time() + self._policy.workflow_timeout_seconds

    def _assert_checkpoint_compatibility(self, checkpoint: RunSnapshot) -> None:
        if checkpoint.wf_hash != self._wf_hash:
            raise ValueError(
                f"workflow hash mismatch: "
                f"checkpoint={checkpoint.wf_hash[:_HASH_DISPLAY_LEN]}, "
                f"current={self._wf_hash[:_HASH_DISPLAY_LEN]}"
            )

    # Handler invocation

    def _invoke_handler(self, snapshot: RunSnapshot, node_name: str) -> HandlerResult:
        handler_type = nodes.handler_type_of(self._nodes, node_name)
        if handler_type is None:
            return HandlerResult(outputs=[], error=errors.handler_type_missing_error(node_name))

        handler = self._handlers.get(handler_type)
        if handler is None:
            return HandlerResult(
                outputs=[],
                error=errors.handler_not_registered_error(node_name, handler_type),
            )

        context = self._build_context(snapshot, node_name)
        return self._call_handler_with_retry(snapshot, node_name, handler_type, handler, context)

    def _build_context(self, snapshot: RunSnapshot, node_name: str) -> WorkflowContext:
        def emit_custom(kind: str, data: dict[str, Any] | None = None) -> None:
            payload = dict(data or {})
            payload.setdefault("node", node_name)
            event = Event(
                run_id=snapshot.run_id,
                step=snapshot.step,
                wave=snapshot.wave,
                node=node_name,
                kind=kind,
                ts=now_iso(),
                data=payload,
            )
            self._recorder.emit_raw(event)

        return WorkflowContext(
            workflow_name=self._name,
            workflow_hash=self._wf_hash,
            run_id=snapshot.run_id,
            node_name=node_name,
            node_config=dict(nodes.node_config(self._nodes, node_name)),
            step=snapshot.step,
            wave=snapshot.wave,
            state=WorkflowState.from_dict(snapshot.state.to_dict()),
            services=self._services,
            deadline_ts=snapshot.deadline_ts,
            emit=emit_custom,
        )

    def _call_handler_with_retry(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        handler_type: str,
        handler: Handler,
        context: WorkflowContext,
    ) -> HandlerResult:
        max_retries = nodes.node_max_retries(self._nodes, node_name)
        retry_sleep = nodes.node_retry_sleep(self._nodes, node_name)

        for attempt in range(max_retries + 1):
            if self._cancel_flag.is_set():
                return HandlerResult(outputs=[], error=errors.cancelled_error(node_name))
            if context.is_timed_out():
                return HandlerResult(outputs=[], error=errors.workflow_timeout_error(node_name))
            try:
                return handler(context)
            except Exception as exc:
                if attempt < max_retries:
                    self._recorder.node_retry(snapshot, node_name, attempt + 1, max_retries, exc)
                    if self._cancel_flag.wait(retry_sleep):
                        return HandlerResult(outputs=[], error=errors.cancelled_error(node_name))
                    continue
                return HandlerResult(
                    outputs=[],
                    error=errors.handler_exception_error(node_name, handler_type, exc),
                )

        raise AssertionError("unreachable")

    # Node outcome resolution

    def _handle_node_error(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
        duration_ms: int,
    ) -> WorkflowResult | None:
        if outcome.error is None:
            return None

        self._recorder.node_failed(snapshot, node_name, outcome.error, outcome.outputs, duration_ms)
        self._recorder.node_error(snapshot, node_name, outcome.error)
        if scheduling.route_to_error_node(snapshot, node_name, outcome.error, self._error_node):
            self._recorder.save_progress(snapshot)
            return None
        return self._finish_failure(snapshot, outcome.error)

    def _handle_invalid_outputs(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
        duration_ms: int,
    ) -> WorkflowResult | None:
        error = self._validate_declared_outputs(snapshot, node_name, outcome.outputs)
        if error is None:
            return None

        self._recorder.node_failed(snapshot, node_name, error, outcome.outputs, duration_ms)
        self._recorder.node_error(snapshot, node_name, error)
        if scheduling.route_to_error_node(snapshot, node_name, error, self._error_node):
            self._recorder.save_progress(snapshot)
            return None
        return self._finish_failure(snapshot, error)

    def _apply_node_result(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
    ) -> None:
        self._apply_workflow_updates(snapshot, outcome.workflow_updates)
        self._apply_node_updates(snapshot, node_name, outcome.node_updates)
        if outcome.tags:
            snapshot.tags.extend(outcome.tags)

    def _handle_waiting(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
        duration_ms: int,
    ) -> WorkflowResult | None:
        if not outcome.waiting:
            return None
        self._recorder.node_waiting(snapshot, node_name, outcome.outputs, duration_ms)
        return self._finish_waiting(snapshot, node_name, outcome)

    def _apply_workflow_updates(
        self,
        snapshot: RunSnapshot,
        updates: dict[str, Any] | None,
    ) -> None:
        if not updates:
            return
        _merge_dict(snapshot.state.workflow, copy.deepcopy(updates))

    def _apply_node_updates(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        updates: dict[str, Any] | None,
    ) -> None:
        if not updates:
            return
        _merge_dict(snapshot.state.node.setdefault(node_name, {}), copy.deepcopy(updates))

    def _validate_declared_outputs(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outputs: list[str],
    ) -> WorkflowError | None:
        declared = nodes.node_outputs(self._nodes, node_name)
        unknown = [name for name in outputs if name not in declared]
        if not unknown:
            return None
        if not self._policy.strict_outputs:
            self._recorder.node_warning(snapshot, node_name, unknown)
            return None
        return errors.undeclared_output_error(node_name, unknown)


# Small helpers used only by the engine


def _hash_workflow(workflow_def: dict[str, Any]) -> str:
    canonical = json.dumps(workflow_def, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _first_non_json_error(*updates: dict[str, Any] | None) -> TypeError | None:
    for payload in updates:
        if not payload:
            continue
        try:
            json.dumps(payload)
        except TypeError as exc:
            return exc
    return None


def _merge_dict(target: dict[str, Any], updates: dict[str, Any]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_dict(target[key], value)
            continue
        target[key] = value
