"""WorkflowError factories.

Pure functions that build a `WorkflowError` for each engine-internal
failure mode. Extracted from the engine so the runtime reads as
orchestration, not string-formatting.
"""

from __future__ import annotations

from zeroflow.core.models import (
    ERROR_CANCELLED,
    ERROR_HANDLER_EXCEPTION,
    ERROR_HANDLER_NOT_REGISTERED,
    ERROR_HANDLER_TYPE_NOT_DECLARED,
    ERROR_MAX_STEPS_EXCEEDED,
    ERROR_STATE_SERIALIZATION,
    ERROR_UNDECLARED_OUTPUT,
    ERROR_WORKFLOW_TIMEOUT,
    WorkflowError,
)


def cancelled_error(node_name: str | None) -> WorkflowError:
    return WorkflowError(code=ERROR_CANCELLED, message="workflow was cancelled", node=node_name)


def workflow_timeout_error(node_name: str | None) -> WorkflowError:
    return WorkflowError(
        code=ERROR_WORKFLOW_TIMEOUT,
        message="workflow timeout exceeded",
        node=node_name,
    )


def max_steps_error(max_steps: int | None, node_name: str | None) -> WorkflowError:
    return WorkflowError(
        code=ERROR_MAX_STEPS_EXCEEDED,
        message=f"max steps exceeded: limit={max_steps}",
        node=node_name,
    )


def handler_type_missing_error(node_name: str) -> WorkflowError:
    return WorkflowError(
        code=ERROR_HANDLER_TYPE_NOT_DECLARED,
        message=f"node '{node_name}' missing handler declaration",
        node=node_name,
    )


def handler_not_registered_error(node_name: str, handler_type: str) -> WorkflowError:
    return WorkflowError(
        code=ERROR_HANDLER_NOT_REGISTERED,
        message=f"handler '{handler_type}' not registered",
        node=node_name,
        details={"handler": handler_type},
    )


def handler_exception_error(
    node_name: str,
    handler_type: str,
    exc: Exception,
) -> WorkflowError:
    return WorkflowError(
        code=ERROR_HANDLER_EXCEPTION,
        message=str(exc),
        node=node_name,
        details={"handler": handler_type},
        cause_type=type(exc).__name__,
    )


def undeclared_output_error(node_name: str, unknown_outputs: list[str]) -> WorkflowError:
    return WorkflowError(
        code=ERROR_UNDECLARED_OUTPUT,
        message=(
            f"handler '{node_name}' emitted undeclared outputs: "
            f"{', '.join(sorted(unknown_outputs))}"
        ),
        node=node_name,
        details={"outputs": sorted(unknown_outputs)},
    )


def state_serialization_error(node_name: str, exc: Exception) -> WorkflowError:
    return WorkflowError(
        code=ERROR_STATE_SERIALIZATION,
        message="workflow state is not JSON-serializable",
        node=node_name,
        details={"reason": str(exc)},
        cause_type=type(exc).__name__,
    )
