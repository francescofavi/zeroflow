"""Zeroflow core — the minimal, agnostic workflow engine.

Public surface::

    from zeroflow.core import (
        WorkflowEngine, WorkflowContext, HandlerResult,
        WorkflowResult, WorkflowState, WorkflowError,
        RunSnapshot, StepRecord, Event, EnginePolicy,
        InMemoryWorkflowStore, JsonFileWorkflowStore, WorkflowStore,
        EVENT_WF_START, EVENT_WF_END, EVENT_WF_WAITING,
        EVENT_WF_CANCELLED, EVENT_WF_RESUMED,
        EVENT_NODE_START, EVENT_NODE_END, EVENT_NODE_ERROR,
        EVENT_NODE_RETRY, EVENT_NODE_WARNING,
        EVENT_CHECKPOINT, EVENT_STORE_SAVED,
    )

This subpackage is self-contained: stdlib only, no HTTP framework, no
process pool, no database, no LLM coupling.
"""

from zeroflow.core.engine import WorkflowEngine
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
    EnginePolicy,
    Event,
    EventCallback,
    Handler,
    HandlerResult,
    RunMetadata,
    RunSnapshot,
    StepRecord,
    WorkflowContext,
    WorkflowError,
    WorkflowResult,
    WorkflowState,
)
from zeroflow.core.store import (
    InMemoryWorkflowStore,
    JsonFileWorkflowStore,
    WorkflowStore,
)

__all__ = [
    "EVENT_CHECKPOINT",
    "EVENT_NODE_END",
    "EVENT_NODE_ERROR",
    "EVENT_NODE_RETRY",
    "EVENT_NODE_START",
    "EVENT_NODE_WARNING",
    "EVENT_STORE_SAVED",
    "EVENT_WF_CANCELLED",
    "EVENT_WF_END",
    "EVENT_WF_RESUMED",
    "EVENT_WF_START",
    "EVENT_WF_WAITING",
    "EnginePolicy",
    "Event",
    "EventCallback",
    "Handler",
    "HandlerResult",
    "InMemoryWorkflowStore",
    "JsonFileWorkflowStore",
    "RunMetadata",
    "RunSnapshot",
    "StepRecord",
    "WorkflowContext",
    "WorkflowEngine",
    "WorkflowError",
    "WorkflowResult",
    "WorkflowState",
    "WorkflowStore",
]
