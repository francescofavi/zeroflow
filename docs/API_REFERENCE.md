# API REFERENCE

## Purpose

Document every public symbol exposed by the zeroflow package,
verbatim from the source, with behaviour, arguments, return values,
exceptions and limits.

## Scope

- `zeroflow.*` (re-exported from `zeroflow/__init__.py`).
- `zeroflow.viz.*` (the optional visualisation module).

Excludes internal helpers (prefixed `_`) and internal modules under
`zeroflow.core.*` not re-exported from the top level. For a
walkthrough of the run loop see [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## Public surface — summary

Re-exported from `zeroflow.__all__`:

- Class: `WorkflowEngine`
- Data types: `WorkflowContext`, `WorkflowState`, `HandlerResult`,
  `WorkflowResult`, `WorkflowError`, `RunSnapshot`, `StepRecord`,
  `RunMetadata`, `Event`, `EnginePolicy`.
- Store Protocol + reference implementations: `WorkflowStore`,
  `InMemoryWorkflowStore`, `JsonFileWorkflowStore`.
- Type aliases: `Handler`, `EventCallback`.
- Event-kind constants: `EVENT_WF_START`, `EVENT_WF_END`,
  `EVENT_WF_WAITING`, `EVENT_WF_CANCELLED`, `EVENT_WF_RESUMED`,
  `EVENT_NODE_START`, `EVENT_NODE_END`, `EVENT_NODE_ERROR`,
  `EVENT_NODE_RETRY`, `EVENT_NODE_WARNING`, `EVENT_CHECKPOINT`,
  `EVENT_STORE_SAVED`.

Exposed by `zeroflow.viz`:

- `workflow_to_mermaid(...)`.
- `mermaid_to_html(...)`.

---

## `WorkflowEngine`

Source: `src/zeroflow/core/engine.py`.

### Constructor

```python
WorkflowEngine(
    workflow_def: dict[str, Any],
    handlers: dict[str, Handler],
    services: Mapping[str, Any] | None = None,
    event_callback: EventCallback | None = None,
    store: WorkflowStore | None = None,
) -> None
```

Validates the workflow definition, hashes it, parses
`engine_policy`, and prepares the internal `EventRecorder`.

Arguments:

- `workflow_def` — a JSON-serialisable dict. Shape rules live in
  `validate_workflow_definition`; any violation raises `ValueError`
  at this point.
- `handlers` — mapping from handler-type string to a Python callable
  with signature `Callable[[WorkflowContext], HandlerResult]`.
  Handlers are resolved at run time, not at construction, so a
  missing handler name surfaces later as `HANDLER_NOT_REGISTERED`.
- `services` — optional `Mapping[str, Any]` surfaced on
  `ctx.services`. Defaults to `{}` internally.
- `event_callback` — optional `Callable[[Event], None]`. Receives
  every lifecycle event plus anything a handler emits via
  `ctx.emit(...)`.
- `store` — optional object implementing the `WorkflowStore`
  Protocol.

Raises: `ValueError` on any shape violation in `workflow_def`
(unknown targets, missing `handler`, bad `join` spec, forward cycle
without `is_loopback`, non-JSON payload, bad `engine_policy`
values, etc. — see [`USER_GUIDE.md`](USER_GUIDE.md) §7 for the full
table).

### `WorkflowEngine.from_files`

```python
@classmethod
def from_files(
    cls,
    workflow_json_path: str | Path,
    handlers: dict[str, Handler],
    services: Mapping[str, Any] | None = None,
    event_callback: EventCallback | None = None,
    store: WorkflowStore | None = None,
) -> WorkflowEngine
```

Reads `workflow_json_path` with UTF-8 encoding, parses it with
`json.load`, and forwards to the main constructor.

### `run`

```python
def run(
    self,
    initial_input: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> WorkflowResult
```

Runs the workflow from the entry node.

Arguments:

- `initial_input` — placed at `state.input` after being passed
  through `json_clone`; defaults to `{}`.
- `run_id` — optional string used as the snapshot identifier and as
  the directory name in the store. Defaults to `uuid.uuid4().hex`.

Returns: `WorkflowResult` with `success`, `status`, `trace`, `tags`,
`state`, `audit_trail`, and — when applicable — `error`, `waiting`,
`waiting_prompt`, `checkpoint`, `cancelled`.

Side effects:

- Resets the cancel flag (`self._cancel_flag.clear()`).
- Emits `wf:start` and a first `checkpoint` before executing any
  node.
- Emits lifecycle events for every node, wave transition, retry,
  warning, error, waiting, cancellation.
- If a store is configured, persists a snapshot after every
  completed node plus lifecycle transitions.

### `run_from_checkpoint`

```python
def run_from_checkpoint(
    self,
    checkpoint: RunSnapshot,
    resume_input: dict[str, Any] | None = None,
) -> WorkflowResult
```

Resumes a previously-paused run.

Arguments:

- `checkpoint` — a `RunSnapshot`, typically the one found on
  `WorkflowResult.checkpoint` or loaded via `engine.load_snapshot`.
- `resume_input` — optional dict placed at
  `state.workflow["__resume__"]` for the resumed node.

Raises: `ValueError("workflow hash mismatch: ...")` if
`checkpoint.wf_hash != self.workflow_hash`.

### `cancel`

```python
def cancel(self) -> None
```

Sets the thread-safe cancel flag. Safe to call from any thread,
including from inside a handler. The next iteration of `_run_loop`
finalises the run as `cancelled` and returns a resumable checkpoint.
The flag is reset automatically at the top of the next `run()` /
`run_from_checkpoint()`.

### `load_snapshot`

```python
def load_snapshot(self, run_id: str) -> RunSnapshot
```

Delegates to `self._store.load_snapshot(run_id)`. Raises
`RuntimeError("no store configured")` if the engine was built
without `store=`. Raises `KeyError(f"run '{run_id}' not found")`
from the store implementation if the snapshot is not present.

### `workflow_hash` (property)

```python
@property
def workflow_hash(self) -> str
```

Returns the SHA-256 hex digest of the workflow definition. Stable
for a given dict content (keys sorted before hashing).

---

## Data types

All dataclasses live in `src/zeroflow/core/models.py`.

### `WorkflowError` (frozen dataclass)

```python
@dataclass(frozen=True)
class WorkflowError:
    code: str
    message: str
    node: str | None = None
    retryable: bool = False
    details: dict[str, Any] = field(default_factory=dict)
    cause_type: str | None = None
```

Thrown-around value object describing why something failed.
`to_dict()` returns a JSON-safe dict; `from_dict(data)` is the
inverse.

`code` is one of: `HANDLER_EXCEPTION`, `HANDLER_NOT_REGISTERED`,
`HANDLER_TYPE_NOT_DECLARED`, `UNDECLARED_OUTPUT`,
`MAX_STEPS_EXCEEDED`, `WORKFLOW_TIMEOUT`, `WORKFLOW_CANCELLED`,
`STATE_SERIALIZATION` (string constants are exposed as the `ERROR_*`
names in `models.py` but are not re-exported).

### `Event` (frozen dataclass)

```python
@dataclass(frozen=True)
class Event:
    run_id: str
    step: int
    wave: int
    node: str | None
    kind: str
    ts: str
    data: dict[str, Any]
```

Every callback invocation receives one of these. `kind` is either
one of the `EVENT_*` constants or any custom string supplied to
`ctx.emit(kind, data)`. `ts` is an ISO-8601 UTC timestamp produced
by `now_iso()`.

### `StepRecord` (frozen dataclass)

```python
@dataclass(frozen=True)
class StepRecord:
    step: int
    wave: int
    node: str
    status: str
    ts: str
    outputs: list[str] = field(default_factory=list)
    error: WorkflowError | None = None
    waiting: bool = False
    duration_ms: int | None = None
```

One row per attempted node, appended to `snapshot.audit_trail`.
`status` is `"succeeded"`, `"failed"` or `"waiting"`. Serialises to
a dict via `to_dict()` / `from_dict`.

### `WorkflowState` (mutable dataclass)

```python
@dataclass
class WorkflowState:
    input: dict[str, Any]
    workflow: dict[str, Any] = field(default_factory=dict)
    node: dict[str, dict[str, Any]] = field(default_factory=dict)
```

Three-tier state container:

- `input` — the `initial_input` the run was started with (read-only
  by convention).
- `workflow` — shared between nodes. Receives `workflow_updates`
  via deep-merge. The engine writes `__error__` here on failure
  and `__resume__` on resume.
- `node` — per-node state dict. `node[<name>]` receives the
  corresponding `node_updates`.

`to_dict()` / `from_dict(data)` deep-clone through `json_clone`.

### `HandlerResult` (frozen dataclass)

```python
@dataclass(frozen=True)
class HandlerResult:
    outputs: list[str]
    workflow_updates: dict[str, Any] | None = None
    node_updates: dict[str, Any] | None = None
    tags: list[str] | None = None
    error: WorkflowError | None = None
    waiting: bool = False
    waiting_prompt: str | None = None
```

What a handler returns. `outputs` drives routing. `workflow_updates`
/ `node_updates` are deep-merged into state. `tags` extend
`snapshot.tags`. `error` is a final failure (no retry). `waiting =
True` pauses the run and returns a checkpoint.

### `EnginePolicy` (mutable dataclass)

```python
@dataclass
class EnginePolicy:
    strict_outputs: bool = True
    max_steps: int | None = None
    workflow_timeout_seconds: float | None = None
    persist_checkpoints: bool = True

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> EnginePolicy: ...
```

Parsed from the `engine_policy` block of the workflow dict. Shape:

- `strict_outputs` — reject (True, default) or warn (False) when a
  handler emits an undeclared output.
- `max_steps` — positive integer cap on total node invocations; any
  value ≤ 0 raises `ValueError` on parsing.
- `workflow_timeout_seconds` — positive float; any value ≤ 0 raises
  `ValueError`.
- `persist_checkpoints` — when False, the engine still emits
  `checkpoint` events but does not call `store.save_snapshot`.

### `RunMetadata` (frozen dataclass)

```python
@dataclass(frozen=True)
class RunMetadata:
    workflow_name: str
    workflow_hash: str
    status: str
    started_at: str
    updated_at: str
    current_node: str | None
    waiting: bool
    last_error: WorkflowError | None = None
```

Compact descriptor of a run. Returned from
`WorkflowStore.list_metadata(...)`.

### `RunSnapshot` (mutable dataclass)

```python
@dataclass
class RunSnapshot:
    run_id: str
    workflow_name: str
    wf_hash: str
    status: str
    step: int
    wave: int
    ready_now: list[str]
    ready_next_wave: list[str]
    state: WorkflowState
    trace: list[str]
    tags: list[str]
    audit_trail: list[StepRecord]
    arrivals: dict[str, list[str]] = field(default_factory=dict)
    executed_this_wave: list[str] = field(default_factory=list)
    last_node: str | None = None
    waiting_prompt: str | None = None
    last_error: WorkflowError | None = None
    started_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    deadline_ts: float | None = None
```

Full resumable state of a run. `arrivals[f"{wave}:{target}"]` is the
AND-join bookkeeping. `executed_this_wave` is used to dedupe OR-join
re-enqueues within a wave. `metadata()` returns a `RunMetadata`
view.

`to_dict()` / `from_dict(data)` are the JSON round-trip pair used by
every store implementation.

### `WorkflowResult` (frozen dataclass)

```python
@dataclass(frozen=True)
class WorkflowResult:
    run_id: str
    success: bool
    status: str
    trace: list[str]
    tags: list[str]
    state: WorkflowState
    audit_trail: list[StepRecord]
    error: WorkflowError | None = None
    waiting: bool = False
    waiting_prompt: str | None = None
    checkpoint: RunSnapshot | None = None
    cancelled: bool = False
```

Returned by `run()` and `run_from_checkpoint()`. `checkpoint` is
populated when `waiting=True` or `cancelled=True`.

### `WorkflowContext` (frozen dataclass)

```python
@dataclass(frozen=True)
class WorkflowContext:
    workflow_name: str
    workflow_hash: str
    run_id: str
    node_name: str
    node_config: dict[str, Any]
    step: int
    wave: int
    state: WorkflowState
    services: Mapping[str, Any]
    deadline_ts: float | None
    emit: EmitCallback
```

Handed to each handler. `state` is a *defensive copy* — mutations
do not feed back to the engine; use `HandlerResult` for that.

`emit` is typed as an `EmitCallback` Protocol (declared in
`zeroflow.core.models`, not re-exported from the top-level package)
with signature:

```python
class EmitCallback(Protocol):
    def __call__(self, kind: str, data: dict[str, Any] | None = None) -> None: ...
```

The Protocol makes ``ctx.emit("kind")`` (single-argument call) valid
under strict static checkers — the runtime supports it via the
``data=None`` default.

Helpers:

- `is_timed_out() -> bool` — True if `deadline_ts` has passed.
- `remaining_seconds() -> float | None` — seconds left until
  `deadline_ts`, clamped to ≥ 0; `None` if no deadline.
- `node_state` (property) — `dict(state.node.get(self.node_name, {}))`.

---

## Type aliases

```python
Handler = Callable[["WorkflowContext"], HandlerResult]
EventCallback = Callable[[Event], None]
```

---

## Stores

Source: `src/zeroflow/core/store.py`.

### `WorkflowStore` (Protocol)

```python
class WorkflowStore(Protocol):
    def save_snapshot(self, snapshot: RunSnapshot) -> None: ...
    def load_snapshot(self, run_id: str) -> RunSnapshot: ...
    def append_event(self, run_id: str, event: Event) -> None: ...
    def list_metadata(self, workflow_name: str | None = None) -> list[RunMetadata]: ...
```

Implement all four methods for a custom backend.

### `InMemoryWorkflowStore` (dataclass)

```python
@dataclass
class InMemoryWorkflowStore:
    _snapshots: dict[str, RunSnapshot] = field(default_factory=dict)
    _events: dict[str, list[Event]] = field(default_factory=dict)
```

- `save_snapshot(snapshot)` — stores `clone_snapshot(snapshot)` under
  `snapshot.run_id`.
- `load_snapshot(run_id)` — returns a `clone_snapshot(...)` of the
  stored snapshot. Raises `KeyError(f"run '{run_id}' not found")`.
- `append_event(run_id, event)` — appends to the in-memory event
  list.
- `list_metadata(workflow_name=None)` — returns metadata for every
  stored snapshot; filters by `workflow_name` when given.

### `JsonFileWorkflowStore` (dataclass)

```python
@dataclass
class JsonFileWorkflowStore:
    base_dir: Path | str
```

`__post_init__` converts `base_dir` to `Path` and creates the
directory (`parents=True, exist_ok=True`).

Per `run_id`, writes three files under `base_dir / run_id`:

- `snapshot.json` — `json.dumps(snapshot.to_dict(), indent=2,
  sort_keys=True)`.
- `metadata.json` — `json.dumps(snapshot.metadata().to_dict(),
  indent=2, sort_keys=True)`.
- `events.jsonl` — one `json.dumps(event_payload, sort_keys=True)`
  per line, appended with UTF-8 encoding.

`load_snapshot(run_id)` raises `KeyError(f"run '{run_id}' not
found")` if `snapshot.json` is missing. `list_metadata(...)` scans
every sub-directory under `base_dir` that contains a
`metadata.json`.

---

## Event kinds

String constants re-exported from `zeroflow`:

| Constant | Literal value | Emitted when |
|----------|---------------|--------------|
| `EVENT_WF_START` | `"wf:start"` | `run()` after the initial snapshot is built. |
| `EVENT_WF_END` | `"wf:end"` | Run finalises for any reason. Payload carries `success`, `trace`, `tags`, optional `error`, optional `cancelled=True`. |
| `EVENT_WF_WAITING` | `"wf:waiting"` | Handler returned `waiting=True`. |
| `EVENT_WF_CANCELLED` | `"wf:cancelled"` | Cancel flag observed in the loop. |
| `EVENT_WF_RESUMED` | `"wf:resumed"` | `run_from_checkpoint` after restore. |
| `EVENT_NODE_START` | `"node:start"` | Before a handler is invoked (after retries, per attempt the event is not re-emitted). |
| `EVENT_NODE_END` | `"node:end"` | Handler succeeded. Payload carries `outputs`. |
| `EVENT_NODE_ERROR` | `"node:error"` | Handler failed or emitted an invalid output. Payload carries `error.to_dict()`. |
| `EVENT_NODE_RETRY` | `"node:retry"` | Between retry attempts. Payload carries `attempt`, `max_retries`, `error` string. |
| `EVENT_NODE_WARNING` | `"node:warning"` | Strict-outputs is off and an undeclared output was emitted. Payload carries `warning: sorted_unknown_outputs`. |
| `EVENT_CHECKPOINT` | `"checkpoint"` | Fires after every completed node + every lifecycle transition. Payload carries `state: snapshot.to_dict()`. Independent of `persist_checkpoints`. |
| `EVENT_STORE_SAVED` | `"store:saved"` | Emitted only when `persist_checkpoints=True` and a store is configured, after each `store.save_snapshot(...)`. |

---

## `zeroflow.viz`

Source: `src/zeroflow/viz/viz.py` (re-exported from `src/zeroflow/viz/__init__.py`).

### `workflow_to_mermaid`

```python
def workflow_to_mermaid(
    workflow_def: dict[str, Any],
    *,
    done_nodes: list[str] | None = None,
    active_node: str | None = None,
    failed: bool = False,
    fenced: bool = True,
) -> str
```

Renders the workflow definition as a Mermaid `flowchart TD`.

- Entry node renders with stadium shape `node(["label"])`.
- Error node renders with hexagon shape `node{{"label"}}`.
- Other nodes render as rectangles `node["label"]`.
- Styles (`pending`/`active`/`done`/`error`) are applied as Mermaid
  class definitions at the end of the diagram.
- Loopback edges (`is_loopback: true`) use dotted arrows (`-.->|out|`);
  forward edges use solid arrows (`-->|out|`).
- `fenced=True` wraps the result with ```` ```mermaid ``` ```` fences.

Limitation: node names are used verbatim as both Mermaid IDs and
labels. Names containing Mermaid metacharacters (`[]`, `()`, `{}`,
`|`, `"`) may break the rendered diagram — rename the node.

### `mermaid_to_html`

```python
def mermaid_to_html(
    mermaid: str,
    output_path: str | Path,
    *,
    title: str | None = None,
    embed_js: bool = False,
) -> Path
```

Writes a self-contained HTML file that renders the Mermaid source
in any modern browser using the `mermaid.min.js` bundle vendored
inside the `zeroflow.viz` package. **Fully offline** — no CDN, no
network, no subprocess, no CLI dependency.

- Accepts either raw Mermaid or the fenced ```` ```mermaid ``` ````
  form produced by `workflow_to_mermaid`; the fence is stripped
  before the source is embedded in the HTML.
- The Mermaid source is HTML-escaped and inserted into a
  `<pre class="mermaid">` block; an initializer
  `mermaid.initialize({ startOnLoad: true })` is appended so the
  browser renders automatically on page load.
- `output_path` extension must be `.html` or `.htm`. Any other
  extension raises `ValueError`.
- `title` controls both the `<title>` tag and the page `<h1>`. If
  omitted it defaults to `Path(output_path).stem`. The value is
  HTML-escaped.

Behaviour — `embed_js`:

- `False` (default): writes `mermaid.min.js` next to `output_path`
  (only if the sibling file does not already exist) and the
  generated HTML loads it via `<script src="mermaid.min.js">`. One
  JS file is shared by every HTML sibling rendered into the same
  directory — compact for batch runs.
- `True`: inlines the full `mermaid.min.js` bundle inside a
  `<script>` tag. Produces a single self-contained file at the cost
  of size (~3 MB per HTML). No sibling `.js` is written.

Side effects:

- The parent directory of `output_path` is created if missing
  (`parents=True, exist_ok=True`).
- When `embed_js=False`, a sibling `mermaid.min.js` may be written
  (once per directory, not overwritten).

Raises:

- `ValueError` — `output_path` has an unsupported extension.

Returns: `Path(output_path)` after the HTML has been written.

**Licensing note.** The vendored `mermaid.min.js` bundle is
distributed under the MIT license by the mermaid-js project. The
upstream license text ships inside the package as
`zeroflow/viz/mermaid.min.js.LICENSE.txt`.

---

## Handler contract (detail)

Every handler is a `Callable[[WorkflowContext], HandlerResult]`.

Reading state:

- `ctx.state.input` — original `initial_input`.
- `ctx.state.workflow` — shared state; the `__error__` key is
  populated on error routing; `__resume__` is populated on
  `run_from_checkpoint(resume_input=...)`.
- `ctx.state.node[<name>]` — per-node state.
- `ctx.node_config` — the `config` block declared on this node in
  the workflow dict.
- `ctx.services` — dict injected at `WorkflowEngine(...)` time.
- `ctx.wave`, `ctx.step`, `ctx.run_id`, `ctx.workflow_hash`,
  `ctx.workflow_name`, `ctx.node_name`.
- `ctx.is_timed_out()`, `ctx.remaining_seconds()`, `ctx.deadline_ts`.

Producing outcomes (all fields of `HandlerResult`):

- `outputs: list[str]` — the symbolic labels that drive routing. At
  least one element per successful invocation; empty list when
  `waiting=True` or when the run is expected to end here.
- `workflow_updates`, `node_updates: dict[str, Any] | None` —
  deep-merged into state. Must be JSON-serialisable.
- `tags: list[str] | None` — appended to `snapshot.tags`.
- `error: WorkflowError | None` — final failure (no retry).
- `waiting: bool`, `waiting_prompt: str | None` — pause the run
  and return a resumable checkpoint.

Emitting custom events:

```python
ctx.emit("plan_ready", {"tasks": [...]})
```

Payload is merged with `{"node": ctx.node_name}` (the node name is
set if the dict does not already contain it) and routed through the
`EventRecorder` like any engine event.

---

## Configuration reference (workflow dict)

Complete shape the engine expects. Unknown keys are ignored.

```json
{
  "workflow_name": "...",
  "default_entry_node": "...",
  "default_error_node": "...",
  "engine_policy": {
    "strict_outputs": true,
    "max_steps": 1000,
    "workflow_timeout_seconds": 60.0,
    "persist_checkpoints": true
  },
  "nodes": {
    "<name>": {
      "handler": "<handler_type>",
      "config": {"...": "..."},
      "state_contract": {
        "reads_from": ["..."],
        "writes_to": ["..."]
      },
      "run_policy": {
        "max_retries": 0,
        "retry_sleep_seconds": 0.1
      },
      "join": {
        "mode": "and",
        "wait_for": ["..."]
      },
      "outputs": {
        "<output_label>": [
          {"target_node": "...", "is_loopback": false}
        ]
      }
    }
  }
}
```
