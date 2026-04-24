# zeroflow Documentation

> Zero-dependency workflow engine with HITL pause/resume, checkpointing,
> loopbacks and error routing.

## Quick links

| Document | What you will find |
|----------|-------------------|
| [`USER_GUIDE.md`](USER_GUIDE.md) | First steps, workflow definition format, running and resuming workflows |
| [`API_REFERENCE.md`](API_REFERENCE.md) | Every public symbol: signatures, arguments, return values, exceptions |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Module map, data flow, run loop, design decisions |
| [`ANTI_PATTERNS.md`](ANTI_PATTERNS.md) | Patterns that reliably cause trouble — and what to do instead |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | How to build, test, lint and contribute to the project |

## One-line pitch

zeroflow executes a directed graph of Python callables in-process.
It requires no scheduler, no database and no worker pool.
A run can pause mid-flight (human-in-the-loop), be serialised to JSON,
resumed later, and even recovered after a process crash.

## Installation

```bash
pip install zeroflow
```

Requires Python ≥ 3.11.

## Minimal example

```python
from zeroflow import WorkflowEngine, WorkflowContext

workflow = {
    "nodes": {
        "greet": {"handler": "say_hello"},
    },
    "edges": [],
    "initial": "greet",
}

def say_hello(ctx: WorkflowContext) -> dict:
    return {"message": f"Hello, {ctx.workflow_state.get('name', 'world')}!"}

engine = WorkflowEngine(workflow, handlers={"say_hello": say_hello})
result = engine.run({"name": "zeroflow"})
print(result.workflow_state["message"])  # Hello, zeroflow!
```

See [`examples/01_quickstart.py`](../examples/01_quickstart.py) for a runnable version.

---

*For internal project notes (functional analysis, snapshots, quality reports,
etc.) see `.internal_docs/` in the repository root.*
