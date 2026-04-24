# USER GUIDE

## Purpose

Explain what `zeroflow` is and how to use it, top to bottom, without
requiring prior exposure to the codebase, to the workflow-engine
domain, or to related tools. A reader arriving here cold should be
able to install the library, write a workflow, run it, pause it,
resume it, recover from errors, and know the exact boundaries of what
the engine does and does not do.

## Scope

Includes:

- What the project is and what problem it solves.
- Concrete strengths, and the merged *known limits and open issues*
  view (design trade-offs, hard limits, unresolved roadmap items).
- Architectural choices that affect you as a user.
- Installation, first run, the common workflows, and the troubleshooting
  entries that are actually emitted by the code.

Excludes:

- Internal module layout and runtime mechanics (see
  [`ARCHITECTURE.md`](ARCHITECTURE.md)).
- Per-symbol API reference (see [`API_REFERENCE.md`](API_REFERENCE.md)).
- Migration to the upcoming `zf/1` grammar (see
  `WORKFLOW_GRAMMAR.md` at the repo root).
- Deep per-component limit analysis, observed anti-patterns and an
  exhaustive comparison with alternative tools. Those are tracked
  internally and are not part of the public guide.

---

## 1. Purpose of the project

`zeroflow` is an **in-process workflow engine**. You describe a
graph of steps as a JSON-serialisable Python `dict`. Each step — a
*node* — has:

- a `handler` name (a string the engine maps to one of the Python
  functions you register);
- declared *outputs* — symbolic labels (`"ok"`, `"retry"`, `"bad"`, …)
  that route to other nodes;
- optionally, a per-node `run_policy` (retries), a `join` spec
  (AND-merge), and a `config` block passed through to the handler.

You then call `engine.run(initial_input=...)`. The engine walks the
graph, invokes handlers in order, merges their state updates, emits
lifecycle events, and returns a `WorkflowResult` — or a resumable
checkpoint if a handler said "wait for a human".

The problem this solves: people write workflow-ish code all the time
— retries, conditional branching, pause-and-resume, recovery on error,
progress tracking. Doing any *one* of those by hand is fine. Doing
*all* of them is how fragile state-machine code gets born. And the
mainstream libraries that do all of them (Airflow, Prefect, Dagster,
Temporal) want their own scheduler, database, workers and web UI.
zeroflow is the smallest coherent piece: **just the engine**, for when
the rest of the stack is overkill.

### Who it serves

- Authors of Python agents / LLM loops who want "plan → execute →
  review → retry" without adopting LangGraph semantics.
- Authors of approval flows, migrations, ETL jobs, ops automation
  scripts that need retries and resume-from-crash.
- Library authors who need a workflow primitive without locking their
  users to an orchestrator.

---

## 2. Strengths

- **Zero runtime dependencies.** Only standard library modules
  (`collections.abc`, `copy`, `dataclasses`, `datetime`, `hashlib`,
  `html`, `importlib.resources`, `json`, `pathlib`, `threading`,
  `time`, `typing`, `uuid`) are used in the runtime package. Nothing
  to audit, nothing to version-pin, nothing to keep updating.
- **Embeddable.** The package is pure Python, `py.typed` shipped,
  works anywhere Python 3.11–3.14 runs. No background daemons, no
  sidecar processes, no required network.
- **Deterministic serial execution.** Single-threaded FIFO scheduler:
  given the same workflow, same handlers and same input, the trace is
  reproducible. No hidden concurrency.
- **Checkpoint after every completed node.** The `EVENT_CHECKPOINT`
  event fires after every node finishes. If you wire in a
  `WorkflowStore`, the snapshot is also persisted — a process crash
  loses at most the currently-executing node.
- **First-class human-in-the-loop.** `HandlerResult(waiting=True,
  waiting_prompt="…")` freezes the run, returns a checkpoint, and
  makes the resume payload visible to the resumed handler at
  `ctx.state.workflow["__resume__"]`.
- **Wave model with OR + AND joins.** Fan-in collapses to a single
  target execution by default (OR-join). Declare `{"join": {"mode":
  "and", "wait_for": [...]}}` on a target for barrier semantics.
  Joins reset after firing so they work across loopback iterations.
- **Error routing.** A failed handler is a routing event, not a
  crash: the engine records the error in
  `state.workflow["__error__"]` and runs the `default_error_node` if
  declared. The error node's own outcome decides whether the run
  succeeds.
- **Workflow-hash lock.** The workflow dict is SHA-256 hashed;
  checkpoints carry that hash, and a resume against a diverged
  workflow is rejected with a `ValueError`.
- **Fully typed surface.** Modern generics (`dict[str, Any]`, `str |
  None`), `py.typed` marker, no `typing.Optional` / `typing.Dict`.

---

## 3. Known limits and open issues

Where the project is deliberately limited, where it enforces a hard
constraint, and what is not yet shipped — one flat list, grouped by
axis (`design:` intentional trade-off, `limit:` hard constraint visible
in code, `open:` tracked roadmap item). Per-component specifics are
tracked internally and out of scope for this guide.

- *design:* Serial execution — nodes run one at a time, no parallel
  fan-out; HITL pauses the whole run, not the waiting branch.
- *design:* Fixed-delay retries only (`run_policy.retry_sleep_seconds`)
  — no exponential backoff, no jitter.
- *design:* Workflow-wide timeout only
  (`engine_policy.workflow_timeout_seconds`) — no per-node timeout.
- *limit:* State must be JSON-serialisable after every node; non-JSON
  updates raise `STATE_SERIALIZATION`.
- *limit:* Exactly one `default_entry_node`, at most one
  `default_error_node`; forward cycles rejected unless the closing
  edge is tagged `"is_loopback": true`.
- *limit:* Resume rejected when the workflow SHA-256 hash has changed
  (`"workflow hash mismatch: ..."`).
- *open:* Advanced grammar roadmap (`zf/1`) tracked in
  `WORKFLOW_GRAMMAR.md` not yet implemented: `subflow`, per-node
  `timeout_ms`, exponential backoff, `map`/fan-out, multiple entry
  points, branch-local HITL, per-node `on_error`; retries engage only
  on raised Python exceptions, not on `HandlerResult(error=...)`.
- *open:* No offline validator / JSON Schema shipped;
  `mermaid_to_html` writes browser-rendered HTML (using the vendored
  `mermaid.min.js` bundle) rather than a server-side raster image — a
  modern browser is required to paint the diagram.

---

## 4. Main architectural choices

Each choice is reflected in how the library looks from the outside.

### Workflow-as-data

The graph is a plain JSON-serialisable Python `dict`. It can be
round-tripped through `json.dumps` / `json.loads`, stored in a file,
diffed, versioned, shipped across processes. You can also load a
workflow straight from a JSON file:

```python
from zeroflow import WorkflowEngine

engine = WorkflowEngine.from_files("workflows/order.json", handlers=HANDLERS)
```

Consequence for users: you write the graph once; you do not also write
a Python DSL to register nodes and edges. Authoring is a JSON task.

### Symbolic outputs, not return values

Handlers do not return payloads that the engine interprets. They emit
labels (`"ok"`, `"bad"`, `"retry"`, `"approved"`, …) and the
`outputs.<label>: [...]` edges decide routing. Structured data flows
through two separate channels: `workflow_updates` (shared, merged into
`state.workflow`) and `node_updates` (per-node, merged into
`state.node[<node_name>]`).

Consequence for users: routing is explicit and visible in the
workflow file. No implicit conditionals hiding in handler return
values.

### Wave scheduler

Time inside a run advances in *waves*. Each wave is a set of nodes
scheduled together; a node runs at most once per wave. Normal edges
route to the same wave; edges marked `"is_loopback": true` push the
target into the *next* wave. Forward cycles (edges that would revisit
a node in the current wave) are rejected at construction time.

Consequence for users: loops are explicit, predictable, and checkpoint
cleanly. The wave counter is visible on every `ctx` and every
`StepRecord`, so "how many iterations so far?" is just `ctx.wave`.

### Handler isolation

Every handler receives a `WorkflowContext` built by the engine. The
context holds a *defensive copy* of the run state — mutating
`ctx.state.workflow` inside a handler has no effect on the engine.
Updates flow back through the `HandlerResult` the handler returns.

Consequence for users: handlers are pure functions from
`WorkflowContext` to `HandlerResult`. No accidental coupling to engine
internals, no "who mutated my state" surprises.

### Errors as routable events

If a handler raises, returns `HandlerResult(error=...)`, or emits an
output not listed in its `outputs` dict (with `strict_outputs=True`,
the default), the engine:

1. appends a `failed` row to the audit trail;
2. emits a `node:error` event;
3. writes the error payload into `state.workflow["__error__"]`;
4. if a `default_error_node` is declared, resets the ready queues and
   runs it; otherwise finalises the run as failed.

Consequence for users: error handling is part of the graph. The error
node can read `state.workflow["__error__"]`, decide whether to recover,
and its outcome determines `result.success`.

### Pluggable store, JSON-serialisable state

`WorkflowStore` is a `Protocol` with four methods
(`save_snapshot`, `load_snapshot`, `append_event`, `list_metadata`).
Two reference stores ship: `InMemoryWorkflowStore` for tests and
ephemeral use, `JsonFileWorkflowStore` for crash-safe checkpoints on
disk. Because every snapshot must round-trip through JSON, custom
stores can be SQLite-, Redis-, S3- or anything-backed without any
concession from the engine.

Consequence for users: you can ship a workflow to disk, kill the
process, start a new one, and resume.

### Workflow hash lock

The dict you pass to `WorkflowEngine(...)` is hashed with SHA-256.
The hash is written into every `RunSnapshot`. When you later call
`run_from_checkpoint(...)`, the engine compares the checkpoint's hash
to the current workflow's hash and refuses the resume if they differ.

Consequence for users: you cannot silently resume a checkpoint against
a workflow whose structure has changed. Make an intentional migration
or start a new run.

---

## 5. Getting started

### Installation

```bash
pip install zeroflow
# or
uv add zeroflow
```

Python 3.11 or newer required. No runtime dependencies.

### A minimum workflow

```python
from zeroflow import HandlerResult, WorkflowContext, WorkflowEngine

WORKFLOW = {
    "workflow_name": "hello",
    "default_entry_node": "greet",
    "nodes": {
        "greet": {"handler": "greet", "outputs": {"ok": []}},
    },
}


def greet(ctx: WorkflowContext) -> HandlerResult:
    name = ctx.state.input.get("name", "world")
    return HandlerResult(outputs=["ok"], workflow_updates={"msg": f"hello, {name}"})


engine = WorkflowEngine(WORKFLOW, handlers={"greet": greet})
result = engine.run(initial_input={"name": "ada"})

print(result.success)                     # True
print(result.trace)                       # ["greet"]
print(result.state.workflow["msg"])       # "hello, ada"
```

---

## 6. Common workflows

### Conditional routing and error node

```python
WORKFLOW = {
    "workflow_name": "order",
    "default_entry_node": "validate",
    "default_error_node": "reject",
    "nodes": {
        "validate": {
            "handler": "validate",
            "outputs": {
                "ok": [{"target_node": "charge"}],
                "bad": [{"target_node": "reject"}],
            },
        },
        "charge": {"handler": "charge", "outputs": {"ok": []}},
        "reject": {"handler": "reject", "outputs": {"ok": []}},
    },
}
```

`validate` emits `"ok"` or `"bad"`. `"bad"` routes to `reject`. If
`validate` raises instead of returning a clean outcome, the engine
populates `state.workflow["__error__"]` and runs `reject` anyway.

### Retries on a flaky node

```python
"charge": {
    "handler": "charge",
    "outputs": {"ok": []},
    "run_policy": {"max_retries": 3, "retry_sleep_seconds": 0.5},
},
```

The handler runs up to `max_retries + 1` times. A Python exception in
any attempt triggers a retry after `retry_sleep_seconds`. A
`HandlerResult(error=...)` does **not** retry — the engine treats an
explicit `WorkflowError` as a final outcome and routes to the error
node (if any).

### Loopback — iterate until done

```python
"tick": {
    "handler": "tick",
    "outputs": {
        "again": [{"target_node": "tick", "is_loopback": True}],
        "done":  [{"target_node": "summary"}],
    },
},
```

`"is_loopback": true` reschedules `tick` for the *next* wave. The
handler chooses `"again"` or `"done"` per iteration. The wave counter
is visible on `ctx.wave` and on every `StepRecord`.

### OR-join (default fan-in)

Two branches can share a target without any join spec:

```python
"merge": {"handler": "merge", "outputs": {"ok": []}},
# two predecessors:
"left":  {"handler": "left",  "outputs": {"ok": [{"target_node": "merge"}]}},
"right": {"handler": "right", "outputs": {"ok": [{"target_node": "merge"}]}},
```

If both branches fire in the same wave, `merge` runs **once** — the
second predecessor dedupes.

### AND-join (barrier fan-in)

```python
"merge": {
    "handler": "merge",
    "join": {"mode": "and", "wait_for": ["credit", "stock", "fraud"]},
    "outputs": {"ok": []},
}
```

The engine queues `merge` only after every node in `wait_for` has
arrived. Arrivals reset after firing, so an AND-join target can run
again on loopback iterations.

### HITL pause / resume

```python
def review(ctx: WorkflowContext) -> HandlerResult:
    if "__resume__" not in ctx.state.workflow:
        return HandlerResult(outputs=[], waiting=True, waiting_prompt="Approve?")
    return HandlerResult(outputs=["ok"], node_updates={"approved": True})
```

First call — the engine freezes the run and returns a
`WorkflowResult` with `waiting=True`, `waiting_prompt` set, and a
`checkpoint` attached. Second call — from the same or a different
process:

```python
resumed = engine.run_from_checkpoint(
    checkpoint, resume_input={"approved": True, "by": "alice"}
)
```

`resume_input` is merged into `state.workflow["__resume__"]`; the
waiting handler reads it and decides.

### Persistent checkpoints (JSON on disk)

```python
from pathlib import Path
from zeroflow import JsonFileWorkflowStore, WorkflowEngine

store = JsonFileWorkflowStore(base_dir=Path("./runs"))
engine = WorkflowEngine(WORKFLOW, handlers=HANDLERS, store=store)
paused = engine.run(run_id="order-123")
# …later, possibly in a new process…
engine2 = WorkflowEngine(WORKFLOW, handlers=HANDLERS, store=store)
snapshot = engine2.load_snapshot("order-123")
final = engine2.run_from_checkpoint(snapshot, resume_input={"approved": True})
```

After each completed node the store writes:

- `runs/order-123/snapshot.json` — the full resumable snapshot;
- `runs/order-123/metadata.json` — a compact summary for listing;
- `runs/order-123/events.jsonl` — one line per emitted event.

### Cancellation

```python
engine = WorkflowEngine(WORKFLOW, handlers=HANDLERS)
# …from another thread or from a handler…
engine.cancel()
```

The cancel flag is checked at the top of the scheduling loop and
inside the retry back-off wait. The run returns with
`result.cancelled = True` and a resumable checkpoint. The flag resets
automatically on the next `run()` or `run_from_checkpoint()`.

### Custom events

Inside a handler:

```python
def plan(ctx: WorkflowContext) -> HandlerResult:
    ctx.emit("plan_ready", {"tasks": ["a", "b"]})
    return HandlerResult(outputs=["ok"])
```

An observer passed as `event_callback=` to `WorkflowEngine(...)`
receives the emitted `Event`, plus every lifecycle event the engine
itself generates.

### Visualising a workflow

```python
from zeroflow.viz import workflow_to_mermaid, mermaid_to_html

mermaid = workflow_to_mermaid(WORKFLOW)     # Mermaid flowchart TD
print(mermaid)
mermaid_to_html(mermaid, "graph.html")      # offline HTML render
```

`mermaid_to_html` writes a self-contained HTML page that renders the
diagram in any modern browser using the `mermaid.min.js` bundle
vendored inside `zeroflow.viz`. No CDN, no network, no subprocess.

- By default, the function copies `mermaid.min.js` next to
  `graph.html` once per output directory; the HTML references it via
  `<script src="mermaid.min.js">`. A single JS file is shared by
  every sibling HTML — compact for batch renders (e.g. the tour).
- Pass `embed_js=True` to inline the bundle inside the HTML. The page
  becomes a single ~3 MB self-contained file and no sibling `.js` is
  written.
- Accepted output extensions: `.html`, `.htm`. Anything else raises
  `ValueError`.

Execution overlay:

```python
workflow_to_mermaid(
    WORKFLOW,
    done_nodes=result.trace[:-1],
    active_node=result.trace[-1],
    failed=not result.success,
)
```

Loopback edges render as dotted arrows so the wave boundary is
visible.

---

## 7. Troubleshooting

All entries below are derived from real messages raised by the code —
nothing invented. File references use `src/zeroflow/...` paths.

### Construction-time errors (`ValueError`)

Raised from `validate_workflow_definition` in
`src/zeroflow/core/validation.py`:

| Message pattern | Cause | Fix |
|-----------------|-------|-----|
| `workflow definition must be JSON-serializable: ...` | A value in the dict cannot survive `json.dumps`. | Replace non-JSON values with JSON primitives. |
| `missing required keys: ...` | `workflow_name`, `default_entry_node` or `nodes` is absent. | Add the missing top-level key. |
| `'nodes' must be a non-empty dict` | `nodes` is missing, `None`, or empty. | Provide at least one node. |
| `entry node '<X>' not in nodes` | `default_entry_node` refers to a node that doesn't exist. | Rename or add the entry node. |
| `error node '<X>' not in nodes` | `default_error_node` refers to a node that doesn't exist. | Rename or drop the field. |
| `node '<X>' missing 'handler'` | The node dict has no `handler` key. | Add `"handler": "<name>"`. |
| `node '<X>' missing 'outputs'` | The node dict has no `outputs` key. | Add `"outputs": {}` even if empty. |
| `node '<X>' outputs must be a dict` | `outputs` is not a dict. | Replace with `{"<label>": [ ... ]}`. |
| `node '<X>' output '<o>' must be a list` | Edge collection for an output is not a list. | Use a list of edge dicts. |
| `node '<X>' output '<o>' edge must be a dict` | An entry in the edge list is not a dict. | Replace with `{"target_node": "..."}`. |
| `node '<X>' output '<o>' edge missing 'target_node'` | An edge dict has no `target_node`. | Add `"target_node": "<name>"`. |
| `node '<X>' output '<o>' target_node must be a non-empty string` | `target_node` is empty or not a string. | Use a node name. |
| `node '<X>' output '<o>' references unknown target '<Y>'` | Dangling edge target. | Declare the target node or fix the edge. |
| `node '<X>' state_contract.reads_from and writes_to must be lists` | `state_contract` block has the wrong shape. | Use two list fields. |
| `node '<X>' join.mode must be 'or' or 'and', got '<v>'` | Invalid join mode. | Use `"or"` or `"and"`. |
| `node '<X>' join.mode='and' requires a non-empty wait_for list` | AND-join without `wait_for`. | Provide `"wait_for": [...]` with at least one predecessor. |
| `node '<X>' wait_for references unknown node '<Y>'` | `wait_for` lists a non-existent node. | Fix the name. |
| `node '<X>' wait_for includes '<Y>' but no declared edge reaches '<X>'` | `wait_for` lists a node that has no edge to the AND-target. | Add the edge or drop the entry. |
| `cycle detected in forward edges: a -> b -> a. Mark the closing edge with 'is_loopback': true if the loop is intentional.` | Forward cycle. | Add `"is_loopback": true` on the closing edge. |
| `engine_policy.max_steps must be > 0` | `max_steps` is `0` or negative. | Use a positive integer or drop the key. |
| `engine_policy.workflow_timeout_seconds must be > 0` | Timeout is non-positive. | Use a positive number or drop the key. |

### Run-time errors (on `WorkflowResult`)

These appear as `WorkflowError(code=..., message=...)` on
`result.error` or on `state.workflow["__error__"]`:

| `code` | Condition | Where |
|--------|-----------|-------|
| `HANDLER_TYPE_NOT_DECLARED` | Node's `handler` key is `None`. | `src/zeroflow/core/engine.py` |
| `HANDLER_NOT_REGISTERED` | The handler name is not in the `handlers` dict passed to `WorkflowEngine`. | `src/zeroflow/core/engine.py` |
| `HANDLER_EXCEPTION` | Handler raised. The original exception class name is available at `error.cause_type`. | `src/zeroflow/core/engine.py` |
| `UNDECLARED_OUTPUT` | Handler returned an output label not listed in `outputs`. Fires under `strict_outputs=True` only. | `src/zeroflow/core/engine.py` |
| `STATE_SERIALIZATION` | `workflow_updates` or `node_updates` contain non-JSON values. | `src/zeroflow/core/engine.py` |
| `MAX_STEPS_EXCEEDED` | `snapshot.step` went past `engine_policy.max_steps`. | `src/zeroflow/core/engine.py` |
| `WORKFLOW_TIMEOUT` | `time.time() >= snapshot.deadline_ts`. | `src/zeroflow/core/engine.py` |
| `WORKFLOW_CANCELLED` | `engine.cancel()` was called. `result.cancelled` is also `True`. | `src/zeroflow/core/engine.py` |

### Resume errors

| Message | Cause | Fix |
|---------|-------|-----|
| `ValueError("workflow hash mismatch: checkpoint=..., current=...")` | The workflow dict has changed since the checkpoint was taken. | Either revert the workflow, migrate the snapshot, or start a new run. |
| `RuntimeError("no store configured")` | `engine.load_snapshot(run_id)` called on an engine built without `store=`. | Pass a `WorkflowStore` to the engine. |
| `KeyError("run '<id>' not found")` | No snapshot exists under `run_id` in the store. | Check the `run_id`; check the store's `base_dir`. |

### Visualisation errors

Raised from `mermaid_to_html` in `src/zeroflow/viz/viz.py`:

| Cause | Message |
|-------|---------|
| `output_path` extension not in `.html` / `.htm` | `ValueError: unsupported output extension '<ext>'; use one of .html, .htm` |

All other failure modes (invalid Mermaid syntax, missing JS, etc.)
surface as runtime errors inside the browser when the HTML is opened
— Python does not know the render failed, because rendering happens
client-side against the vendored `mermaid.min.js`.
