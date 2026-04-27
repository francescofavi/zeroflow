<p align="center">
  <img src="https://raw.githubusercontent.com/francescofavi/zeroflow/main/logo.png" alt="zeroflow logo" width="200">
</p>

# zeroflow

[![CI](https://img.shields.io/github/actions/workflow/status/francescofavi/zeroflow/ci.yml?branch=main&label=CI&cacheSeconds=0)](https://github.com/francescofavi/zeroflow/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![Python versions](https://img.shields.io/pypi/pyversions/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?cacheSeconds=0)](https://github.com/francescofavi/zeroflow/blob/main/LICENSE)
[![Status](https://img.shields.io/pypi/status/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![Typed](https://img.shields.io/badge/typed-PEP%20561-blue.svg?cacheSeconds=0)](https://peps.python.org/pep-0561/)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen.svg?cacheSeconds=0)]()
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg?cacheSeconds=0)](https://docs.astral.sh/ruff/)

**Workflow engine that fits in one import: graph routing, retries, OR/AND joins, HITL pause-resume, checkpointing, error routing. Pure stdlib, zero runtime dependencies.**

`zeroflow` runs JSON-described graphs in-process. You declare the workflow as a
plain `dict`, register one Python handler per node, and call `engine.run()`.
It handles plain DAGs as a sub-case, and also covers what DAGs cannot express
on their own: explicit loopbacks, retries, OR/AND joins, and human-in-the-loop
pause/resume — without standing up a scheduler, a database, or a worker pool.

**What you get out of the box**

- **Graph routing** — conditional branches, loopbacks, OR and AND joins. Forward-acyclic by construction; cycles are allowed only on edges explicitly tagged as loopbacks.
- **Retry policy per node** — fixed-delay retries on raised exceptions.
- **HITL pause/resume** — handlers can freeze the run and return a resumable checkpoint.
- **Checkpointing to disk** — every completed node is a valid resume point, with a SHA-256 hash lock against incompatible workflow revisions.
- **Error routing and full event stream** — including custom events emitted from handlers.
- **Typed, JSON-serialisable, deterministic** — single-threaded FIFO scheduler, `py.typed` shipped.

**How it works**

A workflow is a JSON-serialisable `dict`. Each node points to a handler — a
plain Python function that returns a `HandlerResult`. The engine walks the
graph serially, persists checkpoints through a pluggable `WorkflowStore`, and
returns a `WorkflowResult`. No broker, no scheduler, no web UI.

**When you should use it**

- Running a DAG in-process when you do not want Airflow / Prefect / Dagster infrastructure.
- Embedding workflow logic inside agents, CLIs, jobs, AWS Lambda, or notebooks.
- LLM agent loops shaped like *plan → execute → critique → retry* (where a plain DAG is not enough — you need loopbacks and retries).
- Order/approval flows, data pipelines, or migration runners that need retries and resume without an orchestrator.
- Cases where you want to start in-process now and wrap the engine inside a heavier orchestrator later, instead of replacing it.

Supports **Python 3.12, 3.13, 3.14**.

---

## Why the existing tools don't fit

There is already an orchestrator for everything, and each one comes with a
cost that makes it the wrong shape for a workflow that has to live inside
another process.

- **Heavy orchestrators** — Airflow, Prefect, Dagster, Temporal. They bring
  real workflow semantics, but each requires its own scheduler, database,
  worker pool, and web UI. The right answer for scheduled production DAGs at
  scale; the wrong answer for embedding a small graph inside a CLI, an agent,
  or a Lambda.
- **Task queues** — Celery, RQ. They solve "send work to a worker": dispatch
  an independent task to a broker-backed pool. They do not walk a graph with
  conditional branches, retries on the same node, OR/AND joins, or
  pause/resume across processes.
- **LLM-graph engines** — LangGraph and similar. They bake LLM semantics into
  the engine itself. Useful when the workflow IS an LLM agent; the wrong
  abstraction when most nodes are not LLMs.
- **Hand-rolled state machines.** Fine when you need one or two of retries,
  conditional branching, pause-and-resume, error recovery, or progress
  tracking. Once you need all of them consistently, home-grown code gets
  fragile.

`zeroflow` sits below all of these. It is the in-process engine layer with no
infrastructure attached: graph routing, retries, joins, HITL, checkpointing,
error routing — and nothing else. When you outgrow in-process execution, you
wrap `zeroflow` inside one of the heavier tools instead of replacing it.

## Capabilities in detail

The intro lists what you get; this section pins each capability to the
concrete API surface and the operational guarantee behind it.

- **Resumable HITL.** A handler that returns
  `HandlerResult(waiting=True, waiting_prompt=...)` freezes the run mid-graph
  and emits a resumable checkpoint. A new process can load that checkpoint
  through the store and call `run_from_checkpoint(...)` — runs survive process
  restarts, machine reboots, and week-long human approvals.
- **Checkpoint after every completed node.** Each successful node completion
  emits an `EVENT_CHECKPOINT` payload on the event stream and writes a
  snapshot through the configured store. Either form is a valid resume point.
- **OR + AND joins.** `or` (default) and `and` (with a `wait_for` barrier
  listing the predecessor nodes) on the same target node. Multiple incoming
  branches fan back into a single continuation point without bespoke gating
  code in handlers.
- **Retry policy per node.** `run_policy.max_retries` paired with
  `run_policy.retry_sleep_seconds`. Engages on raised Python exceptions only —
  explicit `HandlerResult(error=...)` returns are routed through
  `default_error_node` instead.
- **Error routing.** Unhandled exceptions and explicit `error` results land
  on `default_error_node`, with the failing payload available at
  `state.workflow["__error__"]`. The error-handling node owns retry-vs-abort
  policy, so handlers stay free of try/except scaffolding.
- **Workflow hash lock.** The workflow definition is SHA-256 hashed at
  construction. Resuming a checkpoint against a different graph fails fast
  with `workflow hash mismatch: ...` rather than silently routing through
  stale wiring.
- **Custom events from handlers.** `ctx.emit(kind, data)` lets a handler add
  domain-specific events to the trace. The full event stream — system events
  plus custom emissions — is captured in `WorkflowResult.trace`.
- **Pluggable store.** Persistence is exposed as a `WorkflowStore` Protocol.
  An in-memory implementation and a JSON-on-disk implementation ship in the
  box; you can implement the protocol with any backend without touching
  engine internals.
- **Deterministic, serial execution.** Single-threaded FIFO scheduler. No
  hidden concurrency, no race conditions to reason about — the same workflow
  with the same input produces the same trace.
- **Static validation at construction.** Shape, references,
  forward-acyclicity, and JSON serialisability are all enforced before the
  first node runs. Bad workflows fail at `WorkflowEngine(...)`, not three
  hours into a long run.
- **Optional Mermaid visualisation.** `mermaid_to_html(...)` writes a
  self-contained HTML diagram of the graph. Fully offline — uses a vendored
  `mermaid.min.js` bundle, no CDN at runtime.
- **Typed public surface.** `py.typed` shipped, modern generics,
  `from __future__ import annotations` everywhere. The IDE understands
  handler signatures and `WorkflowResult` shape.

## Installation

```bash
pip install zeroflow
# or
uv add zeroflow
```

Supports **Python 3.12, 3.13, 3.14**. No runtime dependencies.

## Quick start

```python
from zeroflow import HandlerResult, WorkflowContext, WorkflowEngine

workflow = {
    "workflow_name": "plan_exec_review",
    "default_entry_node": "plan",
    "default_error_node": "handle_error",
    "nodes": {
        "plan": {
            "handler": "plan",
            "outputs": {"ok": [{"target_node": "exec"}]},
        },
        "exec": {
            "handler": "exec",
            "outputs": {
                "ok": [{"target_node": "review"}],
                "retry": [{"target_node": "exec", "is_loopback": True}],
            },
            "run_policy": {"max_retries": 2},
        },
        "review": {"handler": "review", "outputs": {"ok": []}},
        "handle_error": {"handler": "handle_error", "outputs": {"ok": []}},
    },
}


def plan(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"], node_updates={"tasks": ["a", "b"]})


def exec_(ctx: WorkflowContext) -> HandlerResult:
    tasks = ctx.state.node["plan"]["tasks"]
    return HandlerResult(outputs=["ok"], node_updates={"results": tasks})


def review(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"])


def handle_error(ctx: WorkflowContext) -> HandlerResult:
    return HandlerResult(outputs=["ok"])


engine = WorkflowEngine(
    workflow,
    handlers={"plan": plan, "exec": exec_, "review": review, "handle_error": handle_error},
)
result = engine.run(initial_input={"goal": "refactor module X"})

print(result.success, result.trace)
```

More depth — state model, HITL, stores, cancel, events — lives in
[`docs/USER_GUIDE.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/USER_GUIDE.md).

## Comparison with alternatives

| Library | Shape | Scope | Pause / resume | Runtime deps | When to prefer |
|---------|-------|-------|----------------|--------------|----------------|
| **zeroflow** | in-process engine | graph routing + retries + HITL + checkpoints | yes (resumable across processes via store) | none | embeddable workflow with no infrastructure |
| [Airflow](https://airflow.apache.org/) | full orchestrator | scheduler + DB + workers + web UI | yes | heavy | scheduled DAGs at scale, cron-like with UI |
| [Prefect](https://www.prefect.io/) | full orchestrator | hosted or self-hosted, agents | yes | medium-heavy | production orchestration with dashboard |
| [Dagster](https://dagster.io/) | full orchestrator | asset-centric data pipelines, webserver | yes | medium-heavy | data assets / lineage focus |
| [Temporal](https://temporal.io/) | distributed runtime | durable cross-language workflows + dedicated server | yes | very heavy | distributed/durable workflows across services |
| [Celery](https://docs.celeryq.dev/) / [RQ](https://python-rq.org/) | task queue | dispatch work to workers | partial (tasks, not graphs) | medium (broker) | "send work to a worker", not "walk this graph" |
| [LangGraph](https://github.com/langchain-ai/langgraph) | LLM graph engine | LLM-centric agent loops | yes | LLM/agent stack | the workflow IS an LLM agent |
| [`graphlib.TopologicalSorter`](https://docs.python.org/3/library/graphlib.html) | stdlib helper | topological ordering only | no | stdlib | you only need ordering, no execution |
| Hand-rolled state machine | bespoke | whatever you write | as much as you write | none | trivially small workflows where retries/resume aren't needed |

## Known limits and open issues

Where the project is deliberately limited, where it enforces a hard
constraint, and what is not yet shipped — one list, grouped by axis
(`design:` intentional trade-off, `limit:` hard constraint visible in
the code, `open:` tracked roadmap item).

- *design:* Serial execution — no parallel node fan-out; HITL pauses
  the whole run, not the waiting branch.
- *design:* Fixed-delay retries only (`run_policy.retry_sleep_seconds`)
  — no exponential backoff, no jitter.
- *design:* Workflow-wide timeout only (`engine_policy.workflow_timeout_seconds`)
  — no per-node timeout.
- *limit:* State must be JSON-serialisable after every node; non-JSON
  payloads raise `STATE_SERIALIZATION`.
- *limit:* Exactly one `default_entry_node`, at most one
  `default_error_node`; forward cycles rejected unless the closing
  edge is tagged `"is_loopback": true`.
- *limit:* Resume rejected when the workflow SHA-256 hash has changed
  (`"workflow hash mismatch: ..."`).
- *open:* Advanced grammar (`zf/1`) not yet implemented: `subflow`,
  per-node `timeout_ms`, exponential backoff, `map`/fan-out, multiple
  entry points, branch-local HITL, per-node `on_error`.
- *open:* No JSON Schema / offline validator CLI shipped yet;
  `mermaid_to_html` writes browser-rendered HTML that requires a
  modern browser to paint the diagram (no server-side raster image).

## Anti-patterns — how NOT to use this project

Usage patterns that reliably cause trouble. Full per-pattern
explanation in [`docs/ANTI_PATTERNS.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/ANTI_PATTERNS.md).

- Do not expect parallel execution — branches run one at a time.
- Do not put non-JSON values (`set`, custom classes, `datetime`) into
  `workflow_updates` / `node_updates` / `initial_input`.
- Do not create forward cycles — tag the closing edge with
  `"is_loopback": true` instead.
- Do not return `HandlerResult(error=...)` if you want retries —
  retries engage only on raised Python exceptions.
- Do not mutate `ctx.state` inside a handler — it is a defensive copy;
  propagate changes through the returned `HandlerResult`.
- Do not resume a checkpoint after changing the workflow structure —
  the SHA-256 hash lock rejects the resume.
- Do not request anything other than `.html` / `.htm` from
  `mermaid_to_html` — the function writes a browser-rendered page, not
  a raster image.
- Do not call `engine.cancel()` between runs expecting it to persist
  — the flag is reset at the top of `run()`.

## Running tests

```bash
uv sync
uv run pytest
```

## Running examples

Three runnable scripts live in [`examples/`](https://github.com/francescofavi/zeroflow/tree/main/examples),
ordered by depth.

```bash
uv run python examples/01_quickstart.py
uv run python examples/02_feature_matrix.py
uv run python examples/tour.py
```

- [`01_quickstart.py`](https://github.com/francescofavi/zeroflow/blob/main/examples/01_quickstart.py) — minimum runnable workflow (3 nodes, one conditional branch).
- [`02_feature_matrix.py`](https://github.com/francescofavi/zeroflow/blob/main/examples/02_feature_matrix.py) — seven tiny demos, one per headline feature (conditional routing, loopback, AND-join, retry, error routing, HITL, custom events).
- [`tour.py`](https://github.com/francescofavi/zeroflow/blob/main/examples/tour.py) — pedagogical guided tour: seven workflows of growing size (2, 3, 5, 7, 10, 15 and 30 nodes). Writes one offline HTML file per graph next to the script (plus one shared `mermaid.min.js` sibling — see "Third-party notices" below).

## Development

Contributor setup, quality pipeline, commit conventions and release
process are documented in
[`docs/DEVELOPMENT.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/DEVELOPMENT.md).

## Documentation map

### User documentation

- [`README.md`](https://github.com/francescofavi/zeroflow/blob/main/README.md) — this file.
- [`docs/USER_GUIDE.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/USER_GUIDE.md) — self-sufficient user guide.
- [`docs/ANTI_PATTERNS.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/ANTI_PATTERNS.md) — how NOT to use the library.

### Developer documentation

- [`docs/ARCHITECTURE.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/ARCHITECTURE.md) — module map, data flow, design decisions.
- [`docs/API_REFERENCE.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/API_REFERENCE.md) — every public symbol, verbatim signatures.
- [`docs/DEVELOPMENT.md`](https://github.com/francescofavi/zeroflow/blob/main/docs/DEVELOPMENT.md) — contributor setup, quality pipeline, release process.

### Adjacent files

- [`CHANGELOG.md`](https://github.com/francescofavi/zeroflow/blob/main/CHANGELOG.md) — release notes.

## Contributing

This repository is maintained as a personal portfolio project. Pull requests are generally not accepted, but exceptional contributions may be considered.

For bug reports and feature requests, please use [GitHub Issues](https://github.com/francescofavi/zeroflow/issues).

## License

[MIT](https://github.com/francescofavi/zeroflow/blob/main/LICENSE)

## Third-party notices

`zeroflow.viz.mermaid_to_html` renders diagrams using the
[mermaid](https://github.com/mermaid-js/mermaid) JavaScript bundle,
which is vendored inside the package at
`src/zeroflow/viz/mermaid.min.js`. Mermaid is distributed under the
MIT license — Copyright (c) 2014–2022 Knut Sveidqvist. The full
upstream license text is shipped alongside the bundle as
`src/zeroflow/viz/mermaid.min.js.LICENSE.txt`.
