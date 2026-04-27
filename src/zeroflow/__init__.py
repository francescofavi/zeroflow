"""Zeroflow — agnostic workflow engine.

The public surface re-exports the core engine, which is the only piece
the vast majority of users need. Accessory layers (HTTP API,
multiprocess orchestrator, dashboards, demo UIs) live in
`zeroflow.extras` and must be imported explicitly.
"""

__version__ = "0.1.1"
__author__ = "Francesco Favi"
__email__ = "14098835+francescofavi@users.noreply.github.com"

from zeroflow.core import (
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
    InMemoryWorkflowStore,
    JsonFileWorkflowStore,
    RunMetadata,
    RunSnapshot,
    StepRecord,
    WorkflowContext,
    WorkflowEngine,
    WorkflowError,
    WorkflowResult,
    WorkflowState,
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
