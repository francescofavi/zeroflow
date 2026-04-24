"""Smoke tests — version string and public API surface."""

from __future__ import annotations

import re

import zeroflow
import zeroflow.viz


def test_version_is_semver_like() -> None:
    assert isinstance(zeroflow.__version__, str)
    assert re.match(r"^\d+\.\d+\.\d+", zeroflow.__version__)


def test_top_level_all_matches_attributes() -> None:
    """Every name in ``zeroflow.__all__`` must be an actual attribute of the package."""
    for name in zeroflow.__all__:
        assert hasattr(zeroflow, name), f"zeroflow.__all__ lists {name!r} but attribute is missing"


def test_top_level_all_covers_expected_public_surface() -> None:
    """Regression guard: freeze the exact public surface so accidental drops are caught."""
    expected = {
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
    }
    assert set(zeroflow.__all__) == expected


def test_viz_all_matches_attributes() -> None:
    for name in zeroflow.viz.__all__:
        assert hasattr(zeroflow.viz, name), (
            f"zeroflow.viz.__all__ lists {name!r} but attribute is missing"
        )


def test_viz_all_covers_expected_public_surface() -> None:
    assert set(zeroflow.viz.__all__) == {"mermaid_to_html", "workflow_to_mermaid"}


def test_python_version_supported() -> None:
    import sys

    assert sys.version_info >= (3, 11), "zeroflow requires Python 3.11+"
