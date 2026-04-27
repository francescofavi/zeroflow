# ANTI_PATTERNS

## Purpose

User-facing guide to common ways `zeroflow` is misused, with the
correct pattern for each. Every entry corresponds to behaviour the
library actually penalises (validation error, runtime error, silent
divergence) — nothing speculative.

## Scope

- How NOT to author workflows the engine will accept but mishandle.
- How NOT to write handlers.
- How NOT to use checkpoints, stores, or visualisation.

For deeper architecture rationale see `ARCHITECTURE.md`. For limits
see `USER_GUIDE.md` §3.

---

## 1. Do not expect parallel execution

**What it looks like.** Two outgoing edges from the same output and
the assumption that both targets execute simultaneously:

```python
"split": {
    "handler": "split",
    "outputs": {
        "go": [{"target_node": "left"}, {"target_node": "right"}],
    },
},
```

**Why it's wrong.** The scheduler is **single-threaded**. Both
targets are queued, but they run one after the other on the calling
thread. There is no parallel fan-out, no asyncio, no thread pool. A
slow `left` blocks `right`.

**Do instead.** Treat fan-out as ordered scheduling. If you need real
parallelism, run zeroflow inside an external worker pool (one
process / thread per run) — the engine itself stays serial.

---

## 2. Do not put non-JSON values into state

**What it looks like.**

```python
return HandlerResult(
    outputs=["ok"],
    workflow_updates={"connection": db_handle, "started_at": datetime.now()},
)
```

**Why it's wrong.** Every value that lands in `state.input`,
`state.workflow` or `state.node[...]` must survive `json.dumps`. The
engine validates after every handler — non-JSON payloads raise
`STATE_SERIALIZATION` and route through the standard failure path. A
`set`, a custom class, a `datetime`, a `Path`, raw bytes — all fail.

**Do instead.** Convert before assignment.

```python
return HandlerResult(
    outputs=["ok"],
    workflow_updates={
        "connection_id": db_handle.id,
        "started_at": datetime.now(UTC).isoformat(),
    },
)
```

Pass live objects through `services` instead of state.

---

## 3. Do not create forward cycles

**What it looks like.**

```python
"a": {"handler": "a", "outputs": {"ok": [{"target_node": "b"}]}},
"b": {"handler": "b", "outputs": {"ok": [{"target_node": "a"}]}},  # cycle
```

**Why it's wrong.** Construction fails immediately with
`ValueError: cycle detected in forward edges: a -> b -> a. Mark the
closing edge with 'is_loopback': true if the loop is intentional.`

**Do instead.** Tag the closing edge:

```python
"b": {
    "handler": "b",
    "outputs": {"ok": [{"target_node": "a", "is_loopback": True}]},
},
```

Loopback edges schedule the target into the *next* wave, so cycles
are explicit and checkpointable.

---

## 4. Do not return `HandlerResult(error=...)` if you want retries

**What it looks like.**

```python
def fetch(ctx):
    try:
        return HandlerResult(outputs=["ok"], workflow_updates={"data": call_api()})
    except Exception as exc:
        return HandlerResult(outputs=[], error=WorkflowError(code="API_DOWN", message=str(exc)))
```

**Why it's wrong.** A returned `WorkflowError` is **final**. The
engine treats it as an explicit "no more attempts" and routes
through `default_error_node` (or terminates the run). Retries are
never engaged.

**Do instead.** Let the exception propagate so the retry loop sees
it:

```python
def fetch(ctx):
    return HandlerResult(outputs=["ok"], workflow_updates={"data": call_api()})

# ...
"fetch": {
    "handler": "fetch",
    "outputs": {"ok": [{"target_node": "next"}]},
    "run_policy": {"max_retries": 3, "retry_sleep_seconds": 0.5},
},
```

Use `HandlerResult(error=...)` only when you mean "give up now,
route to error node".

---

## 5. Do not mutate `ctx.state` inside a handler

**What it looks like.**

```python
def update(ctx):
    ctx.state.workflow["count"] = ctx.state.workflow.get("count", 0) + 1
    return HandlerResult(outputs=["ok"])
```

**Why it's wrong.** `ctx.state` is a **defensive copy**, built from
`WorkflowState.from_dict(snapshot.state.to_dict())`. Mutations have
no effect on the engine's state. The next handler will not see the
change.

**Do instead.** Return updates through `HandlerResult`:

```python
def update(ctx):
    next_count = ctx.state.workflow.get("count", 0) + 1
    return HandlerResult(outputs=["ok"], workflow_updates={"count": next_count})
```

Updates are deep-merged into `state.workflow` / `state.node` after
the handler returns.

---

## 6. Do not resume after changing the workflow structure

**What it looks like.**

```python
# Run 1 — the workflow has nodes A, B, C
engine = WorkflowEngine(WORKFLOW_V1, handlers=HANDLERS, store=store)
result = engine.run(run_id="job-42")  # waits at B

# ...edit WORKFLOW_V1 to add a new node D between A and B...

engine2 = WorkflowEngine(WORKFLOW_V2, handlers=HANDLERS, store=store)
snapshot = engine2.load_snapshot("job-42")
engine2.run_from_checkpoint(snapshot, resume_input={...})
```

**Why it's wrong.** Every snapshot carries the SHA-256 hash of the
workflow dict it was generated under. `run_from_checkpoint` rejects
the resume:

```
ValueError: workflow hash mismatch: checkpoint=abc123..., current=def456...
```

This protects you from silently resuming on top of a graph whose
shape no longer matches.

**Do instead.** Either revert the workflow to the original shape and
resume, or start a fresh run with the new shape. There is no
automated migration: the assumption is that a structural change is
significant enough to require a deliberate decision.

---

## 7. Do not request anything other than `.html` / `.htm` from `mermaid_to_html`

**What it looks like.**

```python
mermaid_to_html(diagram, "graph.png")  # ValueError
mermaid_to_html(diagram, "graph.svg")  # ValueError
```

**Why it's wrong.** `mermaid_to_html` writes a **browser-rendered**
HTML page; rendering happens client-side using the vendored
`mermaid.min.js` bundle. There is no server-side raster step, no
subprocess, no CLI. The function is strict about its output
extension.

**Do instead.** Stay with `.html` / `.htm`:

```python
mermaid_to_html(diagram, "graph.html")
```

If you need a raster image, open the generated HTML in a browser and
export from there, or use a separate tool.

---

## 8. Do not call `engine.cancel()` between runs expecting it to persist

**What it looks like.**

```python
engine.cancel()
result = engine.run()  # the cancel was reset before this call started
```

**Why it's wrong.** The cancel flag is a `threading.Event` that the
engine **resets** at the top of `run()` and `run_from_checkpoint()`.
A pre-set flag is cleared before the run begins; the run executes
normally.

**Do instead.** Call `cancel()` while a run is in progress (from
another thread or from a handler). To stop a future run, do not
start it.

---

## 9. Do not share one `WorkflowEngine` across concurrent runs

**What it looks like.**

```python
engine = WorkflowEngine(WORKFLOW, handlers=HANDLERS)
threading.Thread(target=engine.run).start()
threading.Thread(target=engine.run).start()
```

**Why it's wrong.** The engine holds run-specific state on
construction (`_cancel_flag`, the recorder), but reuses it across
`run()` calls. Concurrent invocations on the same instance share the
cancel flag and the recorder; events and cancellation will not be
isolated per run.

**Do instead.** Build one `WorkflowEngine` per run, or serialise
calls. The engine is cheap to construct (validation runs once, no
sockets, no threads).

---

## 10. Do not assume `EVENT_STORE_SAVED` fires without a store

**What it looks like.**

```python
def callback(event):
    if event.kind == EVENT_STORE_SAVED:
        ...

engine = WorkflowEngine(WORKFLOW, handlers=HANDLERS, event_callback=callback)  # no store=
```

**Why it's wrong.** `EVENT_STORE_SAVED` is emitted only when a store
is configured *and* `engine_policy.persist_checkpoints=True`.
`EVENT_CHECKPOINT` always fires; `EVENT_STORE_SAVED` only when a
disk write actually happened.

**Do instead.** Listen for `EVENT_CHECKPOINT` if you only need
"a node finished and the snapshot is current". Listen for
`EVENT_STORE_SAVED` only when you need to react to the disk write
itself (e.g. trigger a downstream observer).
