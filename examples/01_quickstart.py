"""Minimum runnable zeroflow workflow.

Three nodes, one conditional branch, one happy path. Run with::

    uv run python examples/01_quickstart.py
"""

from __future__ import annotations

from typing import Any

from zeroflow import HandlerResult, WorkflowContext, WorkflowEngine

LONG_GREETING_THRESHOLD = 12

WORKFLOW: dict[str, Any] = {
    "workflow_name": "quickstart",
    "default_entry_node": "greet",
    "nodes": {
        "greet": {
            "handler": "greet",
            "outputs": {"ok": [{"target_node": "classify"}]},
        },
        "classify": {
            "handler": "classify",
            "outputs": {
                "short": [{"target_node": "report"}],
                "long": [{"target_node": "report"}],
            },
        },
        "report": {
            "handler": "report",
            "outputs": {"ok": []},
        },
    },
}


def greet(ctx: WorkflowContext) -> HandlerResult:
    name = ctx.state.input.get("name", "world")
    return HandlerResult(outputs=["ok"], workflow_updates={"greeting": f"hello, {name}"})


def classify(ctx: WorkflowContext) -> HandlerResult:
    text = ctx.state.workflow.get("greeting", "")
    label = "long" if len(text) >= LONG_GREETING_THRESHOLD else "short"
    return HandlerResult(outputs=[label], workflow_updates={"length_bucket": label})


def report(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(
        outputs=["ok"],
        workflow_updates={
            "summary": f"{ctx.state.workflow['greeting']} ({ctx.state.workflow['length_bucket']})"
        },
    )


def main() -> None:
    engine = WorkflowEngine(
        WORKFLOW,
        handlers={"greet": greet, "classify": classify, "report": report},
    )
    result = engine.run(initial_input={"name": "zeroflow"})

    print("=== quickstart ===")
    print(f"  success:  {result.success}")
    print(f"  status:   {result.status}")
    print(f"  trace:    {result.trace}")
    print(f"  greeting: {result.state.workflow.get('greeting')!r}")
    print(f"  summary:  {result.state.workflow.get('summary')!r}")


if __name__ == "__main__":
    main()
