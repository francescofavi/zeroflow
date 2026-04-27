# ARCHITECTURE

## Purpose

Explain how the zeroflow package is laid out, how data flows through
a run, and which design decisions are baked into the code.

## Scope

- Module-by-module roles.
- Data flow from `WorkflowEngine(...)` construction to
  `WorkflowResult`.
- Design decisions visible in the source.
- External dependencies, configuration sources, extensibility points.

Excludes user-facing behaviour (see
[`USER_GUIDE.md`](USER_GUIDE.md)) and per-symbol signatures (see
[`API_REFERENCE.md`](API_REFERENCE.md)).

---

## 1. Overview

zeroflow is a single Python package (`src/zeroflow/`) split into two
concentric rings:

- **`zeroflow.core`** — the engine proper. Stdlib-only. No HTTP, no
  process pool, no database, no LLM coupling. This is what `zeroflow`
  actually *is*.
- **`zeroflow` top-level** — a re-export facade that keeps the public
  API flat (`from zeroflow import WorkflowEngine, ...`) plus the
  optional visualisation module `zeroflow.viz`.

The package follows a clear layering:

```
zeroflow/__init__.py              ← public re-exports
zeroflow/viz/
    __init__.py                   ← public re-exports for zeroflow.viz
    viz.py                        ← Mermaid source + offline HTML render
    mermaid.min.js                ← vendored mermaid.js bundle (MIT)
    mermaid.min.js.LICENSE.txt    ← upstream MIT notice for the bundle
zeroflow/core/
    __init__.py                   ← internal re-exports
    engine.py                     ← run loop orchestration
    models.py                     ← data types + constants + serialisation helpers
    errors.py                     ← WorkflowError factories
    events.py                     ← EventRecorder (emit + audit + persist)
    nodes.py                      ← read-only accessors over workflow_def["nodes"]
    scheduling.py                 ← queue manipulation, edge routing, joins
    store.py                      ← WorkflowStore Protocol + reference stores
    validation.py                 ← static workflow-definition checks
```

Each `core/*.py` file holds one responsibility. The engine file
(`engine.py`) is the only one that composes runtime behaviour; every
other module is either pure data, pure helpers, or pure side effects
confined to `events.py` and `store.py`.

---

## 2. Module map

### `zeroflow/__init__.py`

Thin facade. Defines `__version__`, `__author__`, `__email__` and
re-exports every public symbol from `zeroflow.core`. Hatchling reads
`__version__` via `[tool.hatch.version] path =
"src/zeroflow/__init__.py"`.

### `zeroflow/viz/`

Optional, self-contained package. The public re-exports live in
`zeroflow/viz/__init__.py` (`mermaid_to_html`, `workflow_to_mermaid`);
the implementation lives in `zeroflow/viz/viz.py`. The package also
ships two non-Python data files:

- `mermaid.min.js` — the vendored `mermaid-js/mermaid` bundle used at
  render time by the HTML output of `mermaid_to_html`.
- `mermaid.min.js.LICENSE.txt` — the upstream MIT license of that
  bundle (Copyright (c) 2014–2022 Knut Sveidqvist), redistributed
  verbatim to satisfy the MIT terms.

Both are part of the wheel because hatchling ships every non-excluded
file inside `packages = ["src/zeroflow"]`.

Two public functions:

- `workflow_to_mermaid(workflow_def, *, done_nodes, active_node, failed, fenced)` — returns
  a Mermaid `flowchart TD` string. Pure stdlib; no I/O.
- `mermaid_to_html(mermaid, output_path, *, title, embed_js)` —
  writes a self-contained HTML page that renders the diagram in a
  browser using the `mermaid.min.js` bundle vendored inside the
  package at `src/zeroflow/viz/mermaid.min.js`. Fully offline — no
  network, no subprocess, no external CLI. By default `mermaid.min.js`
  is copied once next to the output HTML so several HTML siblings
  can share one ~3 MB bundle; `embed_js=True` inlines the bundle
  inside the HTML for a single self-contained file. Output extension
  must be `.html` / `.htm`; anything else is rejected with a
  `ValueError`.

Both functions depend only on `core` data shapes (they read a dict)
and stdlib. Not re-exported from `zeroflow.__init__` on purpose — it
is an optional surface.

### `zeroflow/core/__init__.py`

Re-exports from `engine`, `models`, `store`. Acts as the stable
import surface for the engine: `from zeroflow.core import
WorkflowEngine, HandlerResult, ...`.

### `zeroflow/core/engine.py`

The scheduler. Defines `WorkflowEngine` and a handful of module-level
helpers (`_hash_workflow`, `_first_non_json_error`, `_merge_dict`).
Two module constants: `_MS_PER_SECOND = 1000`, `_HASH_DISPLAY_LEN = 12`.

Internally split into five logical blocks, in this order:

1. `__init__` + `from_files` — construction and validation.
2. `run` / `run_from_checkpoint` / `cancel` / `load_snapshot` /
   `workflow_hash` — public entry points.
3. `_run_loop` — the scheduling loop that drives one wave at a time.
4. `_run_single_node` / `_resolve_node_outcome` — per-node
   invocation, error classification, state merge, waiting handling.
5. `_create_run_snapshot` / `_restore_run_snapshot` / `_finish_*` —
   lifecycle transitions between statuses (`running`, `succeeded`,
   `failed`, `waiting`, `cancelled`).

The engine holds exactly these instance fields: `_wf`, `_handlers`,
`_services`, `_store`, `_cancel_flag` (a `threading.Event`),
`_name`, `_entry`, `_error_node`, `_nodes`, `_wf_hash`, `_policy`
(an `EnginePolicy`), `_recorder` (an `EventRecorder`).

### `zeroflow/core/models.py`

All data shapes and their (de)serialisation logic.

- Event-kind string constants (`EVENT_WF_*`, `EVENT_NODE_*`,
  `EVENT_CHECKPOINT`, `EVENT_STORE_SAVED`).
- Error-code string constants (`ERROR_HANDLER_EXCEPTION`,
  `ERROR_HANDLER_NOT_REGISTERED`, `ERROR_HANDLER_TYPE_NOT_DECLARED`,
  `ERROR_UNDECLARED_OUTPUT`, `ERROR_MAX_STEPS_EXCEEDED`,
  `ERROR_WORKFLOW_TIMEOUT`, `ERROR_CANCELLED`,
  `ERROR_STATE_SERIALIZATION`).
- Run-status constants (`STATUS_RUNNING`, `STATUS_SUCCEEDED`,
  `STATUS_FAILED`, `STATUS_WAITING`, `STATUS_CANCELLED`).
- Default constants: `DEFAULT_RETRY_SLEEP = 0.1`,
  `DEFAULT_STRICT_OUTPUTS = True`.
- Data types (all `@dataclass`): `WorkflowError`, `Event`,
  `StepRecord`, `NodeContract`, `WorkflowState`, `HandlerResult`,
  `EnginePolicy`, `RunMetadata`, `RunSnapshot`, `WorkflowResult`,
  `WorkflowContext`.
- Type aliases: `Handler`, `EventCallback`.
- Serialisation helpers: `now_iso()`, `json_clone()`,
  `clone_snapshot()`, plus private `_read_optional_positive_int`
  and `_read_optional_positive_float` for policy parsing.

### `zeroflow/core/errors.py`

Factory functions for `WorkflowError`, one per engine-internal
failure mode. No side effects. Engine calls them to produce the
payload it then routes to the error node.

### `zeroflow/core/events.py`

`EventRecorder` composes three concerns that used to clutter the
engine: (1) emit the event to the callback, (2) append to the audit
trail, (3) persist the snapshot. Every event the engine generates
goes through this class; custom events from `ctx.emit(...)` go
through `emit_raw`.

### `zeroflow/core/nodes.py`

Stateless readers over the validated `workflow_def["nodes"]` map.
One function per piece of information (`handler_type_of`,
`node_config`, `node_outputs`, `output_edges`, `node_run_policy`,
`node_max_retries`, `node_retry_sleep`, `target_uses_and_join`,
`target_wait_for`, `join_is_satisfied`). No mutation, no runtime
state.

### `zeroflow/core/scheduling.py`

All queue manipulation and routing logic:

- `has_pending_work`, `should_open_next_wave`, `open_next_wave`,
  `take_next_node`, `reset_ready_queues`.
- `schedule_outputs` + `_schedule_target` + `_schedule_and_join_target`
  implement edge resolution and AND-join arrival tracking.
- `route_to_error_node` moves the engine onto the error node.
- `prepend_unique`, `append_unique`, `_append_target`,
  `_arrival_key` are small helpers.

Reads the `nodes` map through `core.nodes`. Mutates only the
`RunSnapshot` it is given.

### `zeroflow/core/store.py`

`WorkflowStore` is a `typing.Protocol` with four methods
(`save_snapshot`, `load_snapshot`, `append_event`,
`list_metadata`). Two reference implementations ship:

- `InMemoryWorkflowStore` — dict-backed, deep-copies on save/load via
  `clone_snapshot`.
- `JsonFileWorkflowStore` — one directory per `run_id` under
  `base_dir`, with `snapshot.json`, `metadata.json`, `events.jsonl`.

### `zeroflow/core/validation.py`

Validation logic for the workflow definition dict. Runs exactly
once inside `WorkflowEngine(...)` via
`validate_workflow_definition(workflow_def)`. Checks:

- JSON-serialisability of the whole dict.
- Presence of `workflow_name`, `default_entry_node`, `nodes`.
- Shape of `nodes` (non-empty dict).
- Entry / error node existence.
- Per-node shape: `handler`, `outputs` (dict of lists of edge dicts
  with `target_node` that references an existing node).
- Per-node `state_contract` shape (if present).
- Join declarations (`join.mode` ∈ {`or`, `and`}, `wait_for`
  non-empty for AND, every `wait_for` entry actually has an edge
  reaching the target).
- Forward acyclicity: iterative DFS with `WHITE/GRAY/BLACK`
  colouring, skipping edges marked `is_loopback`.

---

## 3. Data flow — one full run

```
caller ──▶ WorkflowEngine(workflow_def, handlers, services, event_callback, store)
             │
             │  validate_workflow_definition   ← shape errors surface here
             │  _hash_workflow                 ← SHA-256 over the JSON-sorted dict
             │  EnginePolicy.from_dict         ← engine_policy parsing
             │  EventRecorder(...)             ← emits + audits + persists
             ▼
caller ──▶ engine.run(initial_input=..., run_id=...)
             │
             │  _create_run_snapshot            ← status=running, wave=1, ready_now=[entry]
             │  recorder.run_started            ← emits wf:start + save_progress
             ▼
           _run_loop
             │
             │  while has_pending_work:
             │    _check_stop_conditions        ← cancel? workflow timeout? → finalise
             │    should_open_next_wave?        ← yes → open_next_wave + save_progress
             │    take_next_node                ← pop from ready_now; step++; trace += node
             │    _check_step_limit_after_take  ← max_steps exceeded? → finalise
             │
             │    _run_single_node
             │       recorder.node_started       ← node:start
             │       _invoke_handler
             │         nodes.handler_type_of     ← handler string
             │         self._handlers[type]      ← resolve Python callable
             │         _build_context            ← WorkflowContext with defensive state copy
             │         _call_handler_with_retry  ← max_retries + retry_sleep_seconds
             │                                      (retry only on raised exceptions)
             │       duration_ms                 ← int(elapsed * 1000)
             │
             │    _resolve_node_outcome
             │       _handle_node_error          ← HandlerResult(error=...) or retry-exhausted
             │       _handle_invalid_outputs     ← strict_outputs + undeclared output
             │       _first_non_json_error       ← STATE_SERIALIZATION
             │       _apply_node_result          ← deep-merge workflow_updates + node_updates, extend tags
             │       _handle_waiting             ← waiting=True → finalise as waiting
             │       recorder.node_succeeded     ← node:end + audit row
             │       scheduling.schedule_outputs ← walk outputs, enqueue targets by wave + join rules
             │       recorder.save_progress      ← checkpoint event + store write
             │
             ▼
           _finish_success / _finish_failure / _finish_waiting / _finish_cancelled
             │
             │  sets status + updated_at
             │  recorder.workflow_finished       ← wf:end (or wf:waiting / wf:cancelled)
             │  recorder.save_progress
             ▼
caller ◀──  WorkflowResult
```

### Snapshot lifecycle on waiting / resume

When a handler returns `waiting=True`:

1. `snapshot.status = "waiting"`, `waiting_prompt` set.
2. The waiting node is re-inserted at the front of `ready_now` via
   `scheduling.prepend_unique`, so the resumed run re-executes that
   handler first.
3. `recorder.workflow_waiting` emits `wf:waiting` and writes a
   checkpoint.
4. `_build_result(..., waiting=True, checkpoint=clone_snapshot(...))`
   returns to the caller.

On `run_from_checkpoint(checkpoint, resume_input=...)`:

1. `_assert_checkpoint_compatibility` rejects on hash mismatch.
2. `clone_snapshot` deep-copies the caller's checkpoint.
3. `status = "running"`, `updated_at` refreshed.
4. `resume_input`, if given, is placed at
   `state.workflow["__resume__"]`.
5. The run loop resumes from the persisted `ready_now` / wave /
   arrivals.

### Error classification in `_resolve_node_outcome`

The engine checks four mutually-exclusive conditions *in this order*:

1. `outcome.error is not None` → `_handle_node_error` →
   `route_or_fail`.
2. Strict-outputs violation → `_handle_invalid_outputs` →
   `route_or_fail` (or warning + continue if strict is off).
3. Non-JSON `workflow_updates` / `node_updates` →
   `state_serialization_error` → `route_or_fail`.
4. Handler returned `waiting=True` → `_handle_waiting` → finalise.

`route_or_fail` first appends a `failed` audit row and emits
`node:error`, then delegates to `scheduling.route_to_error_node` —
which resets the queues, writes the error into
`state.workflow["__error__"]`, and either enqueues the error node or
returns `False` (run ends as failed).

### Wave opening and join resolution

`scheduling.schedule_outputs` iterates over declared edges. For each
edge it calls `_schedule_target`, which:

- returns immediately if the target is `None` or if the target has
  already executed this wave (forward edges only);
- for AND-join targets, updates the `arrivals` map keyed by
  `"<wave>:<target>"` and enqueues only when `wait_for ⊆ arrivals`;
- for OR-join targets (default), simply appends to `ready_now`
  (forward edge) or `ready_next_wave` (loopback edge) with dedupe.

`open_next_wave` is called when `ready_now` is empty but
`ready_next_wave` is not: it promotes the next-wave queue to the
current one, clears `executed_this_wave`, increments the wave
counter, and calls `save_progress` so the checkpoint reflects the
wave transition.

---

## 4. Key decisions (all derivable from code)

- **Separation of concerns across five core modules.** The engine
  file does not format error messages, does not touch the store
  directly, and does not decide scheduling by itself. Each concern
  lives in its own file (`errors.py`, `events.py`, `scheduling.py`,
  `store.py`, `validation.py`). Makes every file fit in one head.
- **`EventRecorder` composition.** Rather than scattering
  `callback(event)` + `store.append_event(event)` +
  `snapshot.audit_trail.append(...)` throughout the engine, one
  recorder object holds those three concerns. The engine calls
  `recorder.node_succeeded(...)`, `recorder.save_progress(...)`, etc.
- **Defensive state copy.** `WorkflowContext.state` is constructed
  via `WorkflowState.from_dict(snapshot.state.to_dict())`, so
  handlers see a *copy* of the state. Mutating `ctx.state` has no
  effect on the engine.
- **Workflow hash lock.** The full workflow dict is hashed (sorted
  JSON + SHA-256). Every snapshot carries that hash.
  `run_from_checkpoint` refuses to resume if the hash differs.
- **JSON serialisability enforced on every update.** Both at
  construction (the full workflow) and on every node boundary (via
  `_first_non_json_error` over `workflow_updates` + `node_updates`).
  Guarantees that any snapshot is resumable from disk.
- **Retries only on raised exceptions.** A handler returning
  `HandlerResult(error=...)` is a final failure and does **not**
  retry; only a raised Python exception triggers the retry loop.
- **Static validation at construction.** `validate_workflow_definition`
  runs in `WorkflowEngine.__init__`, so shape errors surface before
  any side effect. Forward cycles are caught by the iterative DFS in
  `_require_acyclic_forward_graph`.
- **Serial-first.** The scheduler is a simple FIFO over two lists
  (`ready_now`, `ready_next_wave`). No executors, no `asyncio`, no
  threads beyond the cancel flag.
- **Arrival tracking keyed by wave.** `arrivals[f"{wave}:{target}"] =
  sorted_list` lets AND-joins reset cleanly across loopback
  iterations and survive JSON round-trips.
- **Optional viz lives outside `core`.** `zeroflow.viz` sits at the
  package root, not inside `core`, because it is a separate optional
  facet and we do not want `core` depending on `urllib.request` even
  transitively.

---

## 5. External dependencies

### Runtime

Only Python standard-library modules:

- Engine (`zeroflow.core`): `collections.abc`, `copy`, `dataclasses`,
  `datetime`, `hashlib`, `json`, `pathlib`, `threading`, `time`,
  `typing`, `uuid`.
- Visualisation (`zeroflow.viz`): `html`, `importlib.resources`,
  `pathlib`, `typing`. No HTTP client, no subprocess.

Declared in `pyproject.toml` as `dependencies = []`. One non-Python
data file ships inside the wheel: `src/zeroflow/viz/mermaid.min.js`
(the vendored Mermaid bundle, MIT-licensed) plus its license notice
`mermaid.min.js.LICENSE.txt`.

### Development (`[dependency-groups].dev`)

- `pytest>=8.3.5`, `pytest-cov>=6.1.1`, `pytest-mock>=3.14.0`
- `ruff>=0.11.8`, `mypy>=1.15.0`
- `bandit>=1.7.10`, `vulture>=2.13`
- `pre-commit>=4.2.0`

### Build

- `hatchling` — declared under `[build-system]` with
  `build-backend = "hatchling.build"`.

---

## 6. Configuration sources

zeroflow is configured *per workflow*, not via global state. There is
no config file, no environment variables read by the package itself,
and no feature flags.

### Configuration embedded in the workflow dict

- `workflow_name` — free-form string, required.
- `default_entry_node` — required; must match a key in `nodes`.
- `default_error_node` — optional; must match a key in `nodes` if
  present.
- `engine_policy` — optional dict, parsed by `EnginePolicy.from_dict`:
  - `strict_outputs` (bool, default `True`)
  - `max_steps` (positive int or `None`)
  - `workflow_timeout_seconds` (positive float or `None`)
  - `persist_checkpoints` (bool, default `True`)
- `nodes.<name>.handler` — required, string.
- `nodes.<name>.config` — optional dict, forwarded verbatim to the
  handler through `ctx.node_config`.
- `nodes.<name>.run_policy.max_retries` — int ≥ 0, default `0`.
- `nodes.<name>.run_policy.retry_sleep_seconds` — float, default
  `0.1` (`DEFAULT_RETRY_SLEEP`).
- `nodes.<name>.join.mode` — `"or"` (default) or `"and"`.
- `nodes.<name>.join.wait_for` — required for `"and"`.
- `nodes.<name>.outputs.<label>[].target_node` — required.
- `nodes.<name>.outputs.<label>[].is_loopback` — bool, default
  `False`.
- `nodes.<name>.state_contract.reads_from` /
  `nodes.<name>.state_contract.writes_to` — optional lists; validated
  for shape only.

### Configuration injected at construction

- `handlers: dict[str, Handler]` — required.
- `services: Mapping[str, Any] | None` — optional; forwarded to
  handlers as `ctx.services`.
- `event_callback: EventCallback | None` — optional observer.
- `store: WorkflowStore | None` — optional persistent backend.

### Per-run configuration

- `run(initial_input=..., run_id=...)`.
- `run_from_checkpoint(checkpoint, resume_input=...)`.

---

## 7. Component boundaries and responsibilities

| Module | Owns | Does not own |
|--------|------|--------------|
| `engine.py` | Run loop, retry loop, state merge, lifecycle transitions. | Error message strings (errors.py), queue mechanics (scheduling.py), event persistence (events.py). |
| `models.py` | Data shapes + (de)serialisation. | Any scheduling or validation logic. |
| `errors.py` | `WorkflowError` construction. | Deciding whether to emit (engine does that). |
| `events.py` | Emit + audit-trail append + store write. | Deciding what kind of event to emit (engine does that). |
| `nodes.py` | Read-only views over the validated workflow dict. | Mutation, arrivals tracking, join resolution beyond the simple predicate. |
| `scheduling.py` | `ready_now` / `ready_next_wave` / `arrivals` / `executed_this_wave`. | Handler invocation, error classification. |
| `store.py` | Persist snapshots / metadata / events. | Event payload shape (that is on `events.py`). |
| `validation.py` | Static workflow-definition checks. | Runtime checks (strict_outputs, JSON state updates) which live in the engine. |
| `viz/` | Mermaid source generation (`workflow_to_mermaid`) + offline HTML render (`mermaid_to_html`, using the vendored `mermaid.min.js`). | Anything in `core`. |

---

## 8. Concurrency / IO model

- **Single-threaded.** Nodes run one at a time on the thread calling
  `engine.run()`.
- **`threading.Event` cancel flag.** The only threading primitive.
  Set from any thread via `engine.cancel()`; the run loop polls it at
  the top of each iteration and inside the retry back-off via
  `_cancel_flag.wait(retry_sleep)`.
- **Blocking I/O only.** `JsonFileWorkflowStore` does synchronous
  filesystem writes; `mermaid_to_html` does synchronous filesystem
  writes (one `.html`, optionally one `mermaid.min.js` sibling). No
  `asyncio`, no HTTP, no subprocess.
- **No parallel fan-out.** Branches execute in insertion order
  within a wave.

---

## 9. Extensibility points

- **`WorkflowStore` Protocol.** Implement `save_snapshot`,
  `load_snapshot`, `append_event`, `list_metadata` to plug in a
  custom backend. The engine does not care what it is.
- **`event_callback`.** Receive every `Event` the engine produces
  plus any custom event from `ctx.emit(...)`. Useful for
  OpenTelemetry / Prometheus / log-shipping glue, dashboards, etc.
- **`services` dict.** Arbitrary `Mapping[str, Any]` passed to
  `WorkflowEngine(...)` and surfaced on every `ctx.services`.
  Standard place to inject DB handles, HTTP clients, LLM clients,
  feature flags, etc.
- **Custom events via `ctx.emit(kind, data)`.** Any string `kind` the
  handler emits flows through the `EventRecorder` like a first-class
  event. The node name is auto-attached if the data dict does not
  already carry one.
- **Handlers themselves.** The `handler` key is a string; registration
  happens at `WorkflowEngine(...)` time. Handlers can be any Python
  callable with the shape `Callable[[WorkflowContext], HandlerResult]`.
- **`WorkflowEngine.from_files`.** Static constructor that reads the
  workflow dict from a JSON file. Lets you author workflows outside
  Python.
