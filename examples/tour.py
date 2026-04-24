"""Seven-workflow guided tour, sized 2 / 3 / 5 / 7 / 10 / 15 / 30 nodes.

Single script, no arguments. Each workflow is self-contained, shows one
additional engine feature on top of the previous, and grows in node
count so the visualisation stresses a different scale each time. The
script runs all seven, prints a compact result, and writes one HTML
file per graph next to this file. The HTML pages render offline using
the ``mermaid.min.js`` bundle shipped inside the ``zeroflow.viz``
package — no CDN, no network.

| # | Nodes | Workflow           | Features on top of previous                    |
|---|-------|--------------------|------------------------------------------------|
| 1 |   2   | hello_chain        | minimum viable workflow                        |
| 2 |   3   | route_branch       | conditional routing (symbolic outputs)         |
| 3 |   5   | ingest_pipeline    | default_error_node + raised exception          |
| 4 |   7   | etl_line           | run_policy.max_retries with flaky handler      |
| 5 |  10   | poll_harvest       | is_loopback edge until a condition flips       |
| 6 |  15   | triple_diamond     | fan-out + AND-join over three parallel arms    |
| 7 |  30   | order_approval     | HITL pause + JSON store resume on a big graph  |

Run::

    uv run python examples/tour.py
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from zeroflow import (
    Handler,
    HandlerResult,
    JsonFileWorkflowStore,
    WorkflowContext,
    WorkflowEngine,
    WorkflowResult,
)
from zeroflow.viz import mermaid_to_html, workflow_to_mermaid

HERE = Path(__file__).parent
IMG_PREFIX = "tour"


# ---------------------------------------------------------------------------
# Tiny helpers used across the tour
# ---------------------------------------------------------------------------


def passthrough(label: str) -> Handler:
    """Factory for a trivial handler that emits ``"ok"`` and tags a marker.

    Used for filler nodes in the larger pipelines so the interesting
    logic (branching, joining, HITL) stays readable.
    """

    def _handler(ctx: WorkflowContext) -> HandlerResult:
        return HandlerResult(outputs=["ok"], workflow_updates={f"step_{label}": True})

    return _handler


def linear_chain(names: list[str]) -> dict[str, Any]:
    """Build a ``name[i] -> name[i+1]`` linear ``outputs`` block for a sequence."""
    node_block: dict[str, Any] = {}
    for index, name in enumerate(names):
        is_last = index == len(names) - 1
        targets = [] if is_last else [{"target_node": names[index + 1]}]
        node_block[name] = {"handler": name, "outputs": {"ok": targets}}
    return node_block


# ---------------------------------------------------------------------------
# 1. hello_chain — 2 nodes
# ---------------------------------------------------------------------------

WF_02: dict[str, Any] = {
    "workflow_name": "hello_chain",
    "default_entry_node": "greet",
    "nodes": linear_chain(["greet", "farewell"]),
}


def h02_greet(ctx: WorkflowContext) -> HandlerResult:
    name = ctx.state.input.get("name", "world")
    return HandlerResult(outputs=["ok"], workflow_updates={"hello": f"hello, {name}"})


def h02_farewell(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], workflow_updates={"bye": "goodbye"})


def run_02() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {"greet": h02_greet, "farewell": h02_farewell}
    return WF_02, WorkflowEngine(WF_02, handlers=handlers).run(initial_input={"name": "zeroflow"})


# ---------------------------------------------------------------------------
# 2. route_branch — 3 nodes
# ---------------------------------------------------------------------------

WF_03: dict[str, Any] = {
    "workflow_name": "route_branch",
    "default_entry_node": "classify",
    "nodes": {
        "classify": {
            "handler": "classify",
            "outputs": {
                "small": [{"target_node": "cheap_path"}],
                "large": [{"target_node": "premium_path"}],
            },
        },
        "cheap_path": {"handler": "cheap_path", "outputs": {"ok": []}},
        "premium_path": {"handler": "premium_path", "outputs": {"ok": []}},
    },
}

WF_03_LARGE_SIZE_CUTOFF = 100


def h03_classify(ctx: WorkflowContext) -> HandlerResult:
    size = int(ctx.state.input.get("size", 0))
    label = "large" if size >= WF_03_LARGE_SIZE_CUTOFF else "small"
    return HandlerResult(outputs=[label], workflow_updates={"size": size, "label": label})


def run_03() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {
        "classify": h03_classify,
        "cheap_path": passthrough("cheap"),
        "premium_path": passthrough("premium"),
    }
    return WF_03, WorkflowEngine(WF_03, handlers=handlers).run(initial_input={"size": 250})


# ---------------------------------------------------------------------------
# 3. ingest_pipeline — 5 nodes
# ---------------------------------------------------------------------------

WF_05: dict[str, Any] = {
    "workflow_name": "ingest_pipeline",
    "default_entry_node": "validate",
    "default_error_node": "log_error",
    "nodes": {
        "validate": {"handler": "validate", "outputs": {"ok": [{"target_node": "transform"}]}},
        "transform": {"handler": "transform", "outputs": {"ok": [{"target_node": "persist"}]}},
        "persist": {"handler": "persist", "outputs": {"ok": [{"target_node": "notify"}]}},
        "notify": {"handler": "notify", "outputs": {"ok": []}},
        "log_error": {"handler": "log_error", "outputs": {"ok": []}},
    },
}


def h05_validate(ctx: WorkflowContext) -> HandlerResult:
    payload = ctx.state.input.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    return HandlerResult(outputs=["ok"], node_updates={"parsed": payload})


def h05_log_error(ctx: WorkflowContext) -> HandlerResult:
    err = ctx.state.workflow.get("__error__", {})
    return HandlerResult(
        outputs=["ok"],
        workflow_updates={"status": "errored", "error_code": err.get("code")},
    )


def run_05() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {
        "validate": h05_validate,
        "transform": passthrough("transform"),
        "persist": passthrough("persist"),
        "notify": passthrough("notify"),
        "log_error": h05_log_error,
    }
    return WF_05, WorkflowEngine(WF_05, handlers=handlers).run(initial_input={"payload": "oops"})


# ---------------------------------------------------------------------------
# 4. etl_line — 7 nodes (with retry policy on ``load``)
# ---------------------------------------------------------------------------

_ETL_SEQUENCE = ["extract", "parse", "validate_row", "enrich", "load", "verify", "report"]

WF_07_LOAD_SUCCESS_ATTEMPT = 3

WF_07: dict[str, Any] = {
    "workflow_name": "etl_line",
    "default_entry_node": "extract",
    "nodes": linear_chain(_ETL_SEQUENCE),
}
WF_07["nodes"]["load"]["run_policy"] = {"max_retries": 3, "retry_sleep_seconds": 0.0}


def h07_load(ctx: WorkflowContext) -> HandlerResult:
    counter = ctx.services["attempts"]
    counter["n"] += 1
    if counter["n"] < WF_07_LOAD_SUCCESS_ATTEMPT:
        raise ConnectionError(f"transient load failure on attempt {counter['n']}")
    return HandlerResult(outputs=["ok"], node_updates={"attempts": counter["n"]})


def run_07() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {name: passthrough(name) for name in _ETL_SEQUENCE}
    handlers["load"] = h07_load
    services = {"attempts": {"n": 0}}
    return WF_07, WorkflowEngine(WF_07, handlers=handlers, services=services).run()


# ---------------------------------------------------------------------------
# 5. poll_harvest — 10 nodes (one loopback on ``poll``)
# ---------------------------------------------------------------------------

_POLL_SEQUENCE = [
    "init",
    "connect",
    "poll",
    "download",
    "unpack",
    "parse",
    "validate",
    "enrich",
    "persist",
    "summary",
]

WF_10: dict[str, Any] = {
    "workflow_name": "poll_harvest",
    "default_entry_node": "init",
    "nodes": linear_chain(_POLL_SEQUENCE),
}
# Overwrite poll so it can either loop back to itself or advance.
WF_10["nodes"]["poll"]["outputs"] = {
    "ready": [{"target_node": "download"}],
    "again": [{"target_node": "poll", "is_loopback": True}],
}

POLL_THRESHOLD = 3


def h10_poll(ctx: WorkflowContext) -> HandlerResult:
    attempts = ctx.state.workflow.get("polls", 0) + 1
    output = "ready" if attempts >= POLL_THRESHOLD else "again"
    return HandlerResult(outputs=[output], workflow_updates={"polls": attempts})


def run_10() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {name: passthrough(name) for name in _POLL_SEQUENCE}
    handlers["poll"] = h10_poll
    return WF_10, WorkflowEngine(WF_10, handlers=handlers).run()


# ---------------------------------------------------------------------------
# 6. triple_diamond — 15 nodes (fan-out + AND-join over 3 arms, then tail)
# ---------------------------------------------------------------------------

WF_15: dict[str, Any] = {
    "workflow_name": "triple_diamond",
    "default_entry_node": "start",
    "nodes": {
        "start": {"handler": "start", "outputs": {"ok": [{"target_node": "split"}]}},
        "split": {
            "handler": "split",
            "outputs": {
                "go": [
                    {"target_node": "a_fetch"},
                    {"target_node": "b_fetch"},
                    {"target_node": "c_fetch"},
                ]
            },
        },
        "a_fetch": {"handler": "a_fetch", "outputs": {"ok": [{"target_node": "a_parse"}]}},
        "b_fetch": {"handler": "b_fetch", "outputs": {"ok": [{"target_node": "b_parse"}]}},
        "c_fetch": {"handler": "c_fetch", "outputs": {"ok": [{"target_node": "c_parse"}]}},
        "a_parse": {"handler": "a_parse", "outputs": {"ok": [{"target_node": "merge"}]}},
        "b_parse": {"handler": "b_parse", "outputs": {"ok": [{"target_node": "merge"}]}},
        "c_parse": {"handler": "c_parse", "outputs": {"ok": [{"target_node": "merge"}]}},
        "merge": {
            "handler": "merge",
            "join": {"mode": "and", "wait_for": ["a_parse", "b_parse", "c_parse"]},
            "outputs": {"ok": [{"target_node": "analyze"}]},
        },
        "analyze": {"handler": "analyze", "outputs": {"ok": [{"target_node": "score"}]}},
        "score": {"handler": "score", "outputs": {"ok": [{"target_node": "decide"}]}},
        "decide": {"handler": "decide", "outputs": {"ok": [{"target_node": "publish"}]}},
        "publish": {"handler": "publish", "outputs": {"ok": [{"target_node": "archive"}]}},
        "archive": {"handler": "archive", "outputs": {"ok": [{"target_node": "notify"}]}},
        "notify": {"handler": "notify", "outputs": {"ok": []}},
    },
}


def h15_split(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["go"], workflow_updates={"step_split": True})


def h15_a_fetch(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"value": 3})


def h15_b_fetch(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"value": 4})


def h15_c_fetch(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"value": 5})


def h15_parse_from(source: str) -> Handler:
    def _handler(ctx: WorkflowContext) -> HandlerResult:
        value = ctx.state.node[source]["value"]
        return HandlerResult(outputs=["ok"], node_updates={"squared": value * value})

    return _handler


def h15_merge(ctx: WorkflowContext) -> HandlerResult:
    a = ctx.state.node["a_parse"]["squared"]
    b = ctx.state.node["b_parse"]["squared"]
    c = ctx.state.node["c_parse"]["squared"]
    return HandlerResult(outputs=["ok"], workflow_updates={"sum_of_squares": a + b + c})


def run_15() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {
        "start": passthrough("start"),
        "split": h15_split,
        "a_fetch": h15_a_fetch,
        "b_fetch": h15_b_fetch,
        "c_fetch": h15_c_fetch,
        "a_parse": h15_parse_from("a_fetch"),
        "b_parse": h15_parse_from("b_fetch"),
        "c_parse": h15_parse_from("c_fetch"),
        "merge": h15_merge,
        "analyze": passthrough("analyze"),
        "score": passthrough("score"),
        "decide": passthrough("decide"),
        "publish": passthrough("publish"),
        "archive": passthrough("archive"),
        "notify": passthrough("notify"),
    }
    return WF_15, WorkflowEngine(WF_15, handlers=handlers).run()


# ---------------------------------------------------------------------------
# 7. order_approval — 30 nodes (HITL + AND-join + branching + error + retry)
# ---------------------------------------------------------------------------

WF_30: dict[str, Any] = {
    "workflow_name": "order_approval",
    "default_entry_node": "receive",
    "default_error_node": "error_sink",
    "nodes": {
        # intake
        "receive": {"handler": "receive", "outputs": {"ok": [{"target_node": "deduplicate"}]}},
        "deduplicate": {
            "handler": "deduplicate",
            "outputs": {"ok": [{"target_node": "classify_type"}]},
        },
        "classify_type": {
            "handler": "classify_type",
            "outputs": {"ok": [{"target_node": "extract_payload"}]},
        },
        "extract_payload": {
            "handler": "extract_payload",
            "outputs": {"ok": [{"target_node": "extract_metadata"}]},
        },
        "extract_metadata": {
            "handler": "extract_metadata",
            "outputs": {"ok": [{"target_node": "validate_schema"}]},
        },
        "validate_schema": {
            "handler": "validate_schema",
            "outputs": {"ok": [{"target_node": "normalize_currency"}]},
        },
        "normalize_currency": {
            "handler": "normalize_currency",
            "outputs": {"ok": [{"target_node": "normalize_dates"}]},
        },
        "normalize_dates": {
            "handler": "normalize_dates",
            "outputs": {"ok": [{"target_node": "fork_checks"}]},
        },
        # fan-out to three parallel checks
        "fork_checks": {
            "handler": "fork_checks",
            "outputs": {
                "go": [
                    {"target_node": "risk_score"},
                    {"target_node": "compliance_check"},
                    {"target_node": "fraud_check"},
                ]
            },
        },
        "risk_score": {
            "handler": "risk_score",
            "outputs": {"ok": [{"target_node": "join_checks"}]},
        },
        "compliance_check": {
            "handler": "compliance_check",
            "outputs": {"ok": [{"target_node": "join_checks"}]},
        },
        "fraud_check": {
            "handler": "fraud_check",
            "outputs": {"ok": [{"target_node": "join_checks"}]},
        },
        "join_checks": {
            "handler": "join_checks",
            "join": {
                "mode": "and",
                "wait_for": ["risk_score", "compliance_check", "fraud_check"],
            },
            "outputs": {"ok": [{"target_node": "decide"}]},
        },
        # gate: auto-approve, send to human, or reject
        "decide": {
            "handler": "decide",
            "outputs": {
                "auto_ok": [{"target_node": "payment_init"}],
                "manual": [{"target_node": "human_review"}],
                "deny": [{"target_node": "rejection_record"}],
            },
        },
        # HITL
        "human_review": {
            "handler": "human_review",
            "outputs": {
                "approved": [{"target_node": "payment_init"}],
                "rejected": [{"target_node": "rejection_record"}],
            },
        },
        # happy path
        "payment_init": {
            "handler": "payment_init",
            "outputs": {"ok": [{"target_node": "payment_capture"}]},
            "run_policy": {"max_retries": 2, "retry_sleep_seconds": 0.0},
        },
        "payment_capture": {
            "handler": "payment_capture",
            "outputs": {"ok": [{"target_node": "fulfill_prepare"}]},
        },
        "fulfill_prepare": {
            "handler": "fulfill_prepare",
            "outputs": {"ok": [{"target_node": "fulfill_pack"}]},
        },
        "fulfill_pack": {
            "handler": "fulfill_pack",
            "outputs": {"ok": [{"target_node": "fulfill_ship"}]},
        },
        "fulfill_ship": {
            "handler": "fulfill_ship",
            "outputs": {"ok": [{"target_node": "invoice_generate"}]},
        },
        "invoice_generate": {
            "handler": "invoice_generate",
            "outputs": {"ok": [{"target_node": "receipt_email"}]},
        },
        "receipt_email": {
            "handler": "receipt_email",
            "outputs": {"ok": [{"target_node": "audit_log"}]},
        },
        "audit_log": {
            "handler": "audit_log",
            "outputs": {"ok": [{"target_node": "analytics_push"}]},
        },
        "analytics_push": {
            "handler": "analytics_push",
            "outputs": {"ok": [{"target_node": "archive"}]},
        },
        "archive": {"handler": "archive", "outputs": {"ok": [{"target_node": "final_summary"}]}},
        # rejection path
        "rejection_record": {
            "handler": "rejection_record",
            "outputs": {"ok": [{"target_node": "rejection_notify"}]},
        },
        "rejection_notify": {
            "handler": "rejection_notify",
            "outputs": {"ok": [{"target_node": "rejection_archive"}]},
        },
        "rejection_archive": {
            "handler": "rejection_archive",
            "outputs": {"ok": [{"target_node": "final_summary"}]},
        },
        # error sink + common tail
        "error_sink": {
            "handler": "error_sink",
            "outputs": {"ok": [{"target_node": "final_summary"}]},
        },
        "final_summary": {"handler": "final_summary", "outputs": {"ok": []}},
    },
}


def h30_receive(ctx: WorkflowContext) -> HandlerResult:
    order = ctx.state.input.get("order", {})
    return HandlerResult(outputs=["ok"], workflow_updates={"order_id": order.get("id", "?")})


def h30_fork_checks(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["go"], workflow_updates={"step_fork_checks": True})


WF_30_MANUAL_REVIEW_AMOUNT = 1000.0
WF_30_PAYMENT_SUCCESS_ATTEMPT = 2


def h30_decide(ctx: WorkflowContext) -> HandlerResult:
    amount = float(ctx.state.input.get("order", {}).get("amount", 0))
    if amount <= 0:
        output = "deny"
    elif amount >= WF_30_MANUAL_REVIEW_AMOUNT:
        output = "manual"
    else:
        output = "auto_ok"
    return HandlerResult(outputs=[output], workflow_updates={"decision": output})


def h30_human_review(ctx: WorkflowContext) -> HandlerResult:
    resume = ctx.state.workflow.get("__resume__")
    if resume is None:
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="Approve order?")
    output = "approved" if resume.get("approved") else "rejected"
    return HandlerResult(
        outputs=[output],
        node_updates={"reviewer": resume.get("reviewer", "unknown")},
    )


def h30_payment_init(ctx: WorkflowContext) -> HandlerResult:
    counter = ctx.services["payment_attempts"]
    counter["n"] += 1
    if counter["n"] < WF_30_PAYMENT_SUCCESS_ATTEMPT:
        raise ConnectionError(f"payment gateway transient failure (attempt {counter['n']})")
    return HandlerResult(outputs=["ok"], node_updates={"attempts": counter["n"]})


def h30_final_summary(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(
        outputs=["ok"],
        workflow_updates={
            "final_decision": ctx.state.workflow.get("decision"),
            "reviewer": ctx.state.node.get("human_review", {}).get("reviewer"),
        },
    )


_WF_30_PASS_NODES = [
    "deduplicate",
    "classify_type",
    "extract_payload",
    "extract_metadata",
    "validate_schema",
    "normalize_currency",
    "normalize_dates",
    "risk_score",
    "compliance_check",
    "fraud_check",
    "join_checks",
    "payment_capture",
    "fulfill_prepare",
    "fulfill_pack",
    "fulfill_ship",
    "invoice_generate",
    "receipt_email",
    "audit_log",
    "analytics_push",
    "archive",
    "rejection_record",
    "rejection_notify",
    "rejection_archive",
    "error_sink",
]


def run_30() -> tuple[dict[str, Any], WorkflowResult]:
    handlers: dict[str, Handler] = {name: passthrough(name) for name in _WF_30_PASS_NODES}
    handlers.update(
        {
            "receive": h30_receive,
            "fork_checks": h30_fork_checks,
            "decide": h30_decide,
            "human_review": h30_human_review,
            "payment_init": h30_payment_init,
            "final_summary": h30_final_summary,
        }
    )
    services = {"payment_attempts": {"n": 0}}
    run_id = "tour-order-1"
    store_dir = Path(tempfile.mkdtemp(prefix="zeroflow-tour-order-"))

    first = WorkflowEngine(
        WF_30,
        handlers=handlers,
        store=JsonFileWorkflowStore(store_dir),
        services=services,
    )
    first.run(
        initial_input={"order": {"id": "ORD-42", "amount": 1500}},
        run_id=run_id,
    )

    second = WorkflowEngine(
        WF_30,
        handlers=handlers,
        store=JsonFileWorkflowStore(store_dir),
        services=services,
    )
    checkpoint = second.load_snapshot(run_id)
    resumed = second.run_from_checkpoint(
        checkpoint,
        resume_input={"approved": True, "reviewer": "alice"},
    )
    return WF_30, resumed


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


TourEntry = tuple[int, str, Callable[[], tuple[dict[str, Any], WorkflowResult]]]

TOUR: list[TourEntry] = [
    (2, "hello_chain", run_02),
    (3, "route_branch", run_03),
    (5, "ingest_pipeline", run_05),
    (7, "etl_line", run_07),
    (10, "poll_harvest", run_10),
    (15, "triple_diamond", run_15),
    (30, "order_approval", run_30),
]


def _print_result(node_count: int, label: str, result: WorkflowResult) -> None:
    user_state = {k: v for k, v in result.state.workflow.items() if not k.startswith("__")}
    print(f"\n=== {node_count:02d} nodes — {label} ===")
    print(f"  success:     {result.success}")
    print(f"  status:      {result.status}")
    print(f"  trace:       {result.trace}")
    if result.tags:
        print(f"  tags:        {result.tags}")
    if user_state:
        print(f"  workflow:    {user_state}")
    if result.error is not None:
        print(f"  error:       {result.error.code} — {result.error.message}")


def _render_html(
    node_count: int,
    label: str,
    workflow_def: dict[str, Any],
    result: WorkflowResult,
) -> None:
    mermaid = workflow_to_mermaid(
        workflow_def,
        done_nodes=result.trace,
        failed=not result.success,
    )
    target = HERE / f"{IMG_PREFIX}_{node_count:02d}_{label}.html"
    mermaid_to_html(
        mermaid,
        target,
        title=f"{node_count:02d} nodes — {label}",
    )
    print(f"  html:        {target}")


def main() -> None:
    print(
        f"zeroflow tour — 7 workflows (2/3/5/7/10/15/30 nodes), images next to {Path(__file__).name}"
    )
    for node_count, label, runner in TOUR:
        workflow_def, result = runner()
        actual_nodes = len(workflow_def["nodes"])
        assert actual_nodes == node_count, (
            f"{label} declared {actual_nodes} nodes, expected {node_count}"
        )
        _print_result(node_count, label, result)
        _render_html(node_count, label, workflow_def, result)


if __name__ == "__main__":
    main()
