"""Data types, constants and serialization helpers for the engine.

Only data shapes live here: engine runtime logic is in `engine.py`,
store implementations in `store.py`, workflow validation in
`validation.py`.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

# Event kinds

EVENT_WF_START = "wf:start"
EVENT_WF_END = "wf:end"
EVENT_WF_WAITING = "wf:waiting"
EVENT_WF_CANCELLED = "wf:cancelled"
EVENT_WF_RESUMED = "wf:resumed"
EVENT_NODE_START = "node:start"
EVENT_NODE_END = "node:end"
EVENT_NODE_ERROR = "node:error"
EVENT_NODE_RETRY = "node:retry"
EVENT_NODE_WARNING = "node:warning"
EVENT_CHECKPOINT = "checkpoint"
EVENT_STORE_SAVED = "store:saved"

# Error codes

ERROR_HANDLER_EXCEPTION = "HANDLER_EXCEPTION"
ERROR_HANDLER_NOT_REGISTERED = "HANDLER_NOT_REGISTERED"
ERROR_HANDLER_TYPE_NOT_DECLARED = "HANDLER_TYPE_NOT_DECLARED"
ERROR_UNDECLARED_OUTPUT = "UNDECLARED_OUTPUT"
ERROR_MAX_STEPS_EXCEEDED = "MAX_STEPS_EXCEEDED"
ERROR_WORKFLOW_TIMEOUT = "WORKFLOW_TIMEOUT"
ERROR_CANCELLED = "WORKFLOW_CANCELLED"
ERROR_STATE_SERIALIZATION = "STATE_SERIALIZATION"

# Run statuses

STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_WAITING = "waiting"
STATUS_CANCELLED = "cancelled"

DEFAULT_RETRY_SLEEP = 0.1
DEFAULT_STRICT_OUTPUTS = True


# Serialization helpers shared across engine and stores


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def json_clone(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value))
    except TypeError as exc:
        raise TypeError(
            "workflow state must be JSON-serializable; received unsupported payload"
        ) from exc


def _read_optional_positive_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"engine_policy.{field_name} must be > 0")
    return parsed


def _read_optional_positive_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"engine_policy.{field_name} must be > 0")
    return parsed


# Data types


@dataclass(frozen=True)
class WorkflowError:
    code: str
    message: str
    node: str | None = None
    retryable: bool = False
    details: dict[str, Any] = field(default_factory=dict)
    cause_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "node": self.node,
            "retryable": self.retryable,
            "details": dict(self.details),
            "cause_type": self.cause_type,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowError:
        return cls(
            code=data["code"],
            message=data["message"],
            node=data.get("node"),
            retryable=bool(data.get("retryable", False)),
            details=dict(data.get("details", {})),
            cause_type=data.get("cause_type"),
        )


@dataclass(frozen=True)
class Event:
    run_id: str
    step: int
    wave: int
    node: str | None
    kind: str
    ts: str
    data: dict[str, Any]


@dataclass(frozen=True)
class StepRecord:
    step: int
    wave: int
    node: str
    status: str
    ts: str
    outputs: list[str] = field(default_factory=list)
    error: WorkflowError | None = None
    waiting: bool = False
    duration_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "wave": self.wave,
            "node": self.node,
            "status": self.status,
            "ts": self.ts,
            "outputs": list(self.outputs),
            "error": None if self.error is None else self.error.to_dict(),
            "waiting": self.waiting,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepRecord:
        raw_error = data.get("error")
        return cls(
            step=int(data["step"]),
            wave=int(data["wave"]),
            node=data["node"],
            status=data["status"],
            ts=data["ts"],
            outputs=list(data.get("outputs", [])),
            error=None if raw_error is None else WorkflowError.from_dict(raw_error),
            waiting=bool(data.get("waiting", False)),
            duration_ms=data.get("duration_ms"),
        )


@dataclass(frozen=True)
class NodeContract:
    reads_from: list[str] = field(default_factory=list)
    writes_to: list[str] = field(default_factory=list)


@dataclass
class WorkflowState:
    input: dict[str, Any]
    workflow: dict[str, Any] = field(default_factory=dict)
    node: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "input": json_clone(self.input),
            "workflow": json_clone(self.workflow),
            "node": json_clone(self.node),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowState:
        return cls(
            input=json_clone(data.get("input", {})),
            workflow=json_clone(data.get("workflow", {})),
            node=json_clone(data.get("node", {})),
        )


@dataclass(frozen=True)
class HandlerResult:
    outputs: list[str]
    workflow_updates: dict[str, Any] | None = None
    node_updates: dict[str, Any] | None = None
    tags: list[str] | None = None
    error: WorkflowError | None = None
    waiting: bool = False
    waiting_prompt: str | None = None


@dataclass
class EnginePolicy:
    strict_outputs: bool = DEFAULT_STRICT_OUTPUTS
    max_steps: int | None = None
    workflow_timeout_seconds: float | None = None
    persist_checkpoints: bool = True

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> EnginePolicy:
        raw = dict(data or {})
        return cls(
            strict_outputs=bool(raw.get("strict_outputs", DEFAULT_STRICT_OUTPUTS)),
            max_steps=_read_optional_positive_int(raw.get("max_steps"), "max_steps"),
            workflow_timeout_seconds=_read_optional_positive_float(
                raw.get("workflow_timeout_seconds"),
                "workflow_timeout_seconds",
            ),
            persist_checkpoints=bool(raw.get("persist_checkpoints", True)),
        )


@dataclass(frozen=True)
class RunMetadata:
    workflow_name: str
    workflow_hash: str
    status: str
    started_at: str
    updated_at: str
    current_node: str | None
    waiting: bool
    last_error: WorkflowError | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_name": self.workflow_name,
            "workflow_hash": self.workflow_hash,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "current_node": self.current_node,
            "waiting": self.waiting,
            "last_error": None if self.last_error is None else self.last_error.to_dict(),
        }


@dataclass
class RunSnapshot:
    run_id: str
    workflow_name: str
    wf_hash: str
    status: str
    step: int
    wave: int
    ready_now: list[str]
    ready_next_wave: list[str]
    state: WorkflowState
    trace: list[str]
    tags: list[str]
    audit_trail: list[StepRecord]
    arrivals: dict[str, list[str]] = field(default_factory=dict)
    executed_this_wave: list[str] = field(default_factory=list)
    last_node: str | None = None
    waiting_prompt: str | None = None
    last_error: WorkflowError | None = None
    started_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    deadline_ts: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "wf_hash": self.wf_hash,
            "status": self.status,
            "step": self.step,
            "wave": self.wave,
            "ready_now": list(self.ready_now),
            "ready_next_wave": list(self.ready_next_wave),
            "state": self.state.to_dict(),
            "trace": list(self.trace),
            "tags": list(self.tags),
            "audit_trail": [item.to_dict() for item in self.audit_trail],
            "arrivals": {key: list(value) for key, value in self.arrivals.items()},
            "executed_this_wave": list(self.executed_this_wave),
            "last_node": self.last_node,
            "waiting_prompt": self.waiting_prompt,
            "last_error": None if self.last_error is None else self.last_error.to_dict(),
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "deadline_ts": self.deadline_ts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunSnapshot:
        raw_error = data.get("last_error")
        return cls(
            run_id=data["run_id"],
            workflow_name=data["workflow_name"],
            wf_hash=data["wf_hash"],
            status=data["status"],
            step=int(data["step"]),
            wave=int(data["wave"]),
            ready_now=list(data.get("ready_now", [])),
            ready_next_wave=list(data.get("ready_next_wave", [])),
            state=WorkflowState.from_dict(data["state"]),
            trace=list(data.get("trace", [])),
            tags=list(data.get("tags", [])),
            audit_trail=[StepRecord.from_dict(item) for item in data.get("audit_trail", [])],
            arrivals={key: list(value) for key, value in data.get("arrivals", {}).items()},
            executed_this_wave=list(data.get("executed_this_wave", [])),
            last_node=data.get("last_node"),
            waiting_prompt=data.get("waiting_prompt"),
            last_error=None if raw_error is None else WorkflowError.from_dict(raw_error),
            started_at=data.get("started_at", now_iso()),
            updated_at=data.get("updated_at", now_iso()),
            deadline_ts=data.get("deadline_ts"),
        )

    def metadata(self) -> RunMetadata:
        return RunMetadata(
            workflow_name=self.workflow_name,
            workflow_hash=self.wf_hash,
            status=self.status,
            started_at=self.started_at,
            updated_at=self.updated_at,
            current_node=self.last_node,
            waiting=self.status == STATUS_WAITING,
            last_error=self.last_error,
        )


@dataclass(frozen=True)
class WorkflowResult:
    run_id: str
    success: bool
    status: str
    trace: list[str]
    tags: list[str]
    state: WorkflowState
    audit_trail: list[StepRecord]
    error: WorkflowError | None = None
    waiting: bool = False
    waiting_prompt: str | None = None
    checkpoint: RunSnapshot | None = None
    cancelled: bool = False


class EmitCallback(Protocol):
    """Callable signature of ``WorkflowContext.emit``.

    Used instead of a plain ``Callable[[str, dict[str, Any]], None]`` so
    that static checkers accept one-argument calls (``ctx.emit("kind")``),
    which the runtime supports via the ``data=None`` default.
    """

    def __call__(self, kind: str, data: dict[str, Any] | None = None) -> None: ...


@dataclass(frozen=True)
class WorkflowContext:
    workflow_name: str
    workflow_hash: str
    run_id: str
    node_name: str
    node_config: dict[str, Any]
    step: int
    wave: int
    state: WorkflowState
    services: Mapping[str, Any]
    deadline_ts: float | None
    emit: EmitCallback

    def is_timed_out(self) -> bool:
        if self.deadline_ts is None:
            return False
        return time.time() >= self.deadline_ts

    def remaining_seconds(self) -> float | None:
        if self.deadline_ts is None:
            return None
        return max(self.deadline_ts - time.time(), 0.0)

    @property
    def node_state(self) -> dict[str, Any]:
        return dict(self.state.node.get(self.node_name, {}))


Handler = Callable[["WorkflowContext"], HandlerResult]
EventCallback = Callable[[Event], None]


# Snapshot cloning — defined here because it depends on RunSnapshot


def clone_snapshot(snapshot: RunSnapshot) -> RunSnapshot:
    return RunSnapshot.from_dict(snapshot.to_dict())
