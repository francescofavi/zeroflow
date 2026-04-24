# zeroflow

[![CI](https://img.shields.io/github/actions/workflow/status/francescofavi/zeroflow/ci.yml?branch=main&label=CI&cacheSeconds=0)](https://github.com/francescofavi/zeroflow/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![Python versions](https://img.shields.io/pypi/pyversions/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?cacheSeconds=0)](https://github.com/francescofavi/zeroflow/blob/main/LICENSE)
[![Status](https://img.shields.io/pypi/status/zeroflow.svg?cacheSeconds=0)](https://pypi.org/project/zeroflow/)
[![Typed](https://img.shields.io/badge/typed-PEP%20561-blue.svg?cacheSeconds=0)](https://peps.python.org/pep-0561/)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen.svg?cacheSeconds=0)]()
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg?cacheSeconds=0)](https://docs.astral.sh/ruff/)

Agnostic **workflow engine** for Python. Describe the graph as a plain
JSON-serialisable `dict`, register one handler per node, call
`engine.run()`. The engine handles conditional routing, loopbacks,
OR/AND joins, retry policy, HITL (human-in-the-loop) pause/resume,
checkpointing to disk, error routing and a full event stream.

**Pure stdlib, zero runtime dependencies. Python 3.11 → 3.14.**

---

## Problem

Small and medium workflows come up constantly: an LLM agent loop
("plan → execute → critique → retry"), an order/approval flow, a data
pipeline, a migration runner. People build them as ad-hoc state
machines, bolting on retries, conditional branching, pause-and-resume,
recovery on error, progress tracking. Doing one or two of those by
hand is fine. Doing all of them consistently is where home-grown code
gets fragile.

The mainstream alternatives all impose a cost. Heavy orchestrators
(Airflow, Prefect, Dagster, Temporal) want a scheduler, a database, a
worker pool, a web UI. Task queues (Celery, RQ) solve "send work to a
worker", not "execute this graph". LLM-graph engines (LangGraph) bake
LLM semantics into the engine — wrong shape when most steps are not
LLMs.

## Solution

`zeroflow` is the **engine layer** extracted: a workflow is a JSON
dict, a handler is a Python function, `engine.run()` returns a
`WorkflowResult`. Nothing else — no scheduler, no broker, no database,
no web UI. When you outgrow in-process execution, you wrap zeroflow
inside a real orchestrator instead of replacing it.

## What it gives you

- **Zero runtime dependencies.** Only standard library modules. Nothing to
  audit, nothing to update weekly.
- **Embeddable.** Drops into agents, CLIs, jobs, Lambdas, notebooks —
  no infrastructure.
- **Deterministic, serial.** Single-threaded FIFO scheduler; no hidden
  concurrency, no race conditions to reason about.
- **Checkpoint after every completed node.** Any `EVENT_CHECKPOINT`
  payload, and any snapshot persisted to the store, is a valid resume
  point.
- **First-class HITL.** A handler returning
  `HandlerResult(waiting=True, waiting_prompt=...)` freezes the run
  and returns a resumable checkpoint. A new process can load it and
  call `run_from_checkpoint(...)`.
- **OR + AND joins built in.** `or` (default) and `and` (with a
  `wait_for` barrier) on the same target.
- **Workflow hash lock.** The workflow definition is SHA-256 hashed;
  checkpoints are rejected on mismatch, so you cannot resume against
  an incompatible revision of the graph.
- **Retry policy per node** — `max_retries` + `retry_sleep_seconds`,
  engages on raised exceptions only.
- **Error routing** through `default_error_node` and
  `state.workflow["__error__"]`.
- **Custom events** from handlers (`ctx.emit(kind, data)`).
- **Pluggable store** via `WorkflowStore` Protocol; in-memory and
  JSON-on-disk reference implementations ship.
- **Static validation at construction** — shape, references,
  forward-acyclicity, JSON serialisability.
- **Optional Mermaid visualisation** with fully offline HTML
  rendering using a vendored `mermaid.min.js` bundle.
- **Typed public surface.** `py.typed` shipped, modern generics,
  `from __future__ import annotations` everywhere.

## Installation

```bash
pip install zeroflow
# or
uv add zeroflow
```

Supports **Python 3.11, 3.12, 3.13, 3.14**. No runtime dependencies.

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
- *open:* Advanced grammar roadmap (`zf/1`) tracked in
  [`WORKFLOW_GRAMMAR.md`](https://github.com/francescofavi/zeroflow/blob/main/WORKFLOW_GRAMMAR.md)
  not yet implemented: `subflow`, per-node `timeout_ms`, exponential
  backoff, `map`/fan-out, multiple entry points, branch-local HITL,
  per-node `on_error`.
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
- [`WORKFLOW_GRAMMAR.md`](https://github.com/francescofavi/zeroflow/blob/main/WORKFLOW_GRAMMAR.md) — roadmap for the next workflow grammar (`zf/1`).

## Contributing

`zeroflow` is a personal portfolio project. Issues and small,
well-scoped pull requests are welcome on
[GitHub](https://github.com/francescofavi/zeroflow). Larger changes
that expand scope beyond the "smallest coherent workflow engine"
charter may not be merged — see the *Known limits and open issues*
section above for what is intentionally out of scope.

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
