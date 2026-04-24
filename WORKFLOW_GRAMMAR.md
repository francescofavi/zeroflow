# Workflow Grammar — Roadmap

Open proposals for the zeroflow workflow grammar. Everything listed
here is **not yet implemented**. Items that graduate to the shipping
format are removed from this file and documented in `README.md` and
`CHANGELOG.md`.

The current shipping format is documented in `README.md`. This file
only tracks where we want the grammar to go next.

## Design principles (do not break)

- The JSON describes orchestration, not business rules.
- Handlers emit symbolic outcomes; the engine routes on those
  outcomes.
- No LLM-specific concepts in the core.
- No arbitrary expression language in the workflow file.
- Join policy belongs to the target node.
- Error behaviour must be explicit policy, not a side effect.
- Zero runtime dependencies; standard library only.
- The format must be versioned from day one of the `zf/1` rewrite.

## Target format — `zf/1`

A versioned, stricter workflow schema meant to replace the current
implicit format. The current format stays as a compatibility layer,
normalised into `zf/1` on construction.

### Top-level

```json
{
  "version": "zf/1",
  "name": "workflow_name",
  "entry": "node_id",
  "error": "error_node_id",
  "defaults": {
    "retry": {
      "max_attempts": 1,
      "backoff": { "mode": "fixed", "delay_ms": 0 }
    },
    "timeout_ms": null,
    "strict_outputs": true,
    "on_error": "workflow"
  },
  "nodes": {
    "node_id": {}
  }
}
```

### Node kinds

Four built-in kinds, no more:

- `task` — runs a handler.
- `wait` — HITL pause; resume payload exposed via `resume_as`.
- `subflow` — runs another workflow inline (new capability).
- `end` — terminal node carrying a `result`.

### Common node shape

```json
{
  "kind": "task",
  "label": "Human label",
  "description": "Optional",
  "with": {},
  "join": { "mode": "or" },
  "retry": {
    "max_attempts": 1,
    "backoff": { "mode": "fixed", "delay_ms": 0 },
    "retry_on": ["Exception"]
  },
  "timeout_ms": null,
  "on_error": "workflow",
  "on": {},
  "meta": { "tags": ["optional"] }
}
```

### `task`

```json
{
  "kind": "task",
  "handler": "plan",
  "with": { "mode": "fast" },
  "on": {
    "ok": "next",
    "retry": { "to": "same", "loop": true },
    "reject": ["audit", "stop"]
  }
}
```

### `wait`

```json
{
  "kind": "wait",
  "prompt": "Approve deployment?",
  "resume_as": "decision",
  "on": { "approved": "deploy", "rejected": "abort" }
}
```

### `subflow`

```json
{
  "kind": "subflow",
  "workflow": "child_workflow_name_or_ref",
  "input": { "source": "build" },
  "on": { "ok": "publish", "error": "handle_error" }
}
```

### `end`

```json
{ "kind": "end", "result": "success" }
```

### Routing (`on`)

- A string → one target.
- An array → multiple targets.
- An object route carries metadata (`{"to": "x", "loop": true}`).

### Join

Stays on the target node:

```json
{ "join": { "mode": "and", "wait_for": ["a", "b"] } }
```

- `or` is default.
- `and` requires explicit `wait_for`.
- Join affects scheduling, not threading.

### Validation rules

- `version`, `name`, `entry`, `nodes` are required.
- `entry`, `error`, and every target in `on` must exist.
- `on` uses symbolic outcomes only.
- `join.mode` is `or` or `and`.
- `join.wait_for` is required only for `and`.
- `end` must not define `on`.
- `wait` must not define `handler`.
- `task` must define `handler`.
- `subflow` must define `workflow`.
- `retry.max_attempts >= 1`.
- `strict_outputs=true` → undeclared emitted outcomes are errors.
- Forward cycles are rejected unless the route is marked `loop: true`.

## Not in `zf/1`, maybe later

- **`map` / fan-out** — dynamic fan-out over a collection. Waits for
  a parallelism story.
- **Parallelism primitive** — current engine is deliberately serial.
- **Per-node `timeout_ms`** — today only workflow-level timeout exists.
- **Exponential backoff** — today only fixed `retry_sleep_seconds`.
- **Multiple entry points** — currently a single `default_entry_node`.
- **Branch-local HITL** — today a `waiting` handler freezes the whole
  run, not just its branch.

## Migration from the current format

| Current                                          | `zf/1`                                     |
| ------------------------------------------------ | ------------------------------------------ |
| `workflow_name`                                  | `name`                                     |
| `default_entry_node`                             | `entry`                                    |
| `default_error_node`                             | `error`                                    |
| `config`                                         | `with`                                     |
| `run_policy.max_retries = N`                     | `retry.max_attempts = N + 1`               |
| `run_policy.retry_sleep_seconds`                 | `retry.backoff.delay_ms` (ms)              |
| `outputs.<out>: [ {target_node: x} ]`            | `on.<out>: "x"`                            |
| `outputs.<out>: [ {target_node: x, is_loopback}]`| `on.<out>: { "to": "x", "loop": true }`    |
| legacy node without `kind`                       | `kind: "task"`, `handler: <node_id>`       |

## Suggested implementation order

1. Ship `zf/1` as an alternative input format accepted by
   `WorkflowEngine`.
2. Add a normaliser: on construction, the engine runs on a single
   internal representation regardless of which format was passed.
3. Add `subflow` as a first-class node kind.
4. Add per-node `timeout_ms` and `retry.backoff` semantics.
5. Export a JSON Schema document for `zf/1`.
6. Ship a graph linter / static analyser CLI.
7. Ship a DOT / Mermaid exporter.

## JSON Schema draft (preview)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://zeroflow.dev/schema/zf-1.json",
  "type": "object",
  "required": ["version", "name", "entry", "nodes"],
  "properties": {
    "version": { "const": "zf/1" },
    "name": { "type": "string", "minLength": 1 },
    "entry": { "type": "string", "minLength": 1 },
    "error": { "type": "string" },
    "defaults": { "$ref": "#/$defs/defaults" },
    "nodes": {
      "type": "object",
      "minProperties": 1,
      "additionalProperties": { "$ref": "#/$defs/node" }
    }
  }
}
```

The full `$defs` block will ship alongside the implementation.
