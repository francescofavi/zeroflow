"""Event emission, audit-trail append and snapshot persistence.

The engine composes an `EventRecorder` at construction time and
delegates every event, audit-trail entry and store write to it. This
isolates the I/O side of the engine (callback + store + JSON
payloads) from the control flow.
"""

from __future__ import annotations

from typing import Any

from zeroflow.core.models import (
    EVENT_CHECKPOINT,
    EVENT_NODE_END,
    EVENT_NODE_ERROR,
    EVENT_NODE_RETRY,
    EVENT_NODE_START,
    EVENT_NODE_WARNING,
    EVENT_STORE_SAVED,
    EVENT_WF_CANCELLED,
    EVENT_WF_END,
    EVENT_WF_RESUMED,
    EVENT_WF_START,
    EVENT_WF_WAITING,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    STATUS_WAITING,
    Event,
    EventCallback,
    HandlerResult,
    RunSnapshot,
    StepRecord,
    WorkflowError,
    now_iso,
)
from zeroflow.core.store import WorkflowStore


class EventRecorder:
    def __init__(
        self,
        workflow_name: str,
        event_callback: EventCallback | None,
        store: WorkflowStore | None,
        *,
        persist_checkpoints: bool,
    ) -> None:
        self._workflow_name = workflow_name
        self._callback = event_callback
        self._store = store
        self._persist_checkpoints = persist_checkpoints

    # Emission and persistence primitives

    def emit(
        self,
        snapshot: RunSnapshot,
        node: str | None,
        kind: str,
        data: dict[str, Any],
    ) -> None:
        event = Event(
            run_id=snapshot.run_id,
            step=snapshot.step,
            wave=snapshot.wave,
            node=node,
            kind=kind,
            ts=now_iso(),
            data=dict(data),
        )
        self.emit_raw(event)

    def emit_raw(self, event: Event) -> None:
        if self._callback is not None:
            self._callback(event)
        if self._store is not None and event.run_id:
            self._store.append_event(event.run_id, event)

    def save_progress(self, snapshot: RunSnapshot) -> None:
        if self._store is not None and self._persist_checkpoints:
            self._store.save_snapshot(snapshot)
            self.emit(
                snapshot,
                snapshot.last_node,
                EVENT_STORE_SAVED,
                {"run_id": snapshot.run_id, "status": snapshot.status},
            )
        self.emit(
            snapshot,
            snapshot.last_node,
            EVENT_CHECKPOINT,
            {"state": snapshot.to_dict()},
        )

    # Run lifecycle

    def run_started(self, snapshot: RunSnapshot) -> None:
        self.emit(snapshot, None, EVENT_WF_START, {"workflow": self._workflow_name})
        self.save_progress(snapshot)

    def run_resumed(self, snapshot: RunSnapshot) -> None:
        self.emit(
            snapshot,
            None,
            EVENT_WF_RESUMED,
            {
                "workflow": self._workflow_name,
                "resumed_from_step": snapshot.step,
                "wave": snapshot.wave,
            },
        )
        self.save_progress(snapshot)

    def workflow_waiting(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        prompt: str | None,
    ) -> None:
        self.emit(snapshot, node_name, EVENT_WF_WAITING, {"node": node_name, "prompt": prompt})

    def workflow_cancelled(self, snapshot: RunSnapshot, error: WorkflowError) -> None:
        self.emit(snapshot, None, EVENT_WF_CANCELLED, {"error": error.to_dict()})
        self.workflow_finished(snapshot, success=False, error=error, cancelled=True)

    def workflow_finished(
        self,
        snapshot: RunSnapshot,
        *,
        success: bool,
        error: WorkflowError | None = None,
        cancelled: bool = False,
    ) -> None:
        payload: dict[str, Any] = {
            "workflow": self._workflow_name,
            "success": success,
            "trace": list(snapshot.trace),
            "tags": list(snapshot.tags),
        }
        if error is not None:
            payload["error"] = error.to_dict()
        if cancelled:
            payload["cancelled"] = True
        self.emit(snapshot, None, EVENT_WF_END, payload)

    # Node-level records

    def node_started(self, snapshot: RunSnapshot, node_name: str) -> None:
        self.emit(snapshot, node_name, EVENT_NODE_START, {"node": node_name})

    def node_succeeded(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outcome: HandlerResult,
        duration_ms: int,
    ) -> None:
        self.emit(
            snapshot,
            node_name,
            EVENT_NODE_END,
            {"node": node_name, "outputs": list(outcome.outputs)},
        )
        self.append_audit(
            snapshot,
            node=node_name,
            status=STATUS_SUCCEEDED,
            outputs=outcome.outputs,
            duration_ms=duration_ms,
        )

    def node_failed(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        error: WorkflowError,
        outputs: list[str],
        duration_ms: int,
    ) -> None:
        self.append_audit(
            snapshot,
            node=node_name,
            status=STATUS_FAILED,
            outputs=outputs,
            error=error,
            duration_ms=duration_ms,
        )

    def node_waiting(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        outputs: list[str],
        duration_ms: int,
    ) -> None:
        self.append_audit(
            snapshot,
            node=node_name,
            status=STATUS_WAITING,
            outputs=outputs,
            waiting=True,
            duration_ms=duration_ms,
        )

    def node_error(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        error: WorkflowError,
    ) -> None:
        self.emit(
            snapshot,
            node_name,
            EVENT_NODE_ERROR,
            {"node": node_name, "error": error.to_dict()},
        )

    def node_retry(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        attempt: int,
        max_retries: int,
        exc: Exception,
    ) -> None:
        self.emit(
            snapshot,
            node_name,
            EVENT_NODE_RETRY,
            {
                "node": node_name,
                "attempt": attempt,
                "max_retries": max_retries,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )

    def node_warning(
        self,
        snapshot: RunSnapshot,
        node_name: str,
        unknown_outputs: list[str],
    ) -> None:
        self.emit(
            snapshot,
            node_name,
            EVENT_NODE_WARNING,
            {"node": node_name, "warning": sorted(unknown_outputs)},
        )

    # Audit trail

    def append_audit(
        self,
        snapshot: RunSnapshot,
        *,
        node: str,
        status: str,
        outputs: list[str],
        error: WorkflowError | None = None,
        waiting: bool = False,
        duration_ms: int | None = None,
    ) -> None:
        snapshot.audit_trail.append(
            StepRecord(
                step=snapshot.step,
                wave=snapshot.wave,
                node=node,
                status=status,
                ts=now_iso(),
                outputs=list(outputs),
                error=error,
                waiting=waiting,
                duration_ms=duration_ms,
            )
        )
