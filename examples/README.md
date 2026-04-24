# Examples

Runnable zeroflow examples, ordered by depth. Each script is self-contained and prints a human-readable summary when run.

## Entries

- [`01_quickstart.py`](01_quickstart.py) — minimum runnable workflow (3 nodes, one conditional branch). Start here.
- [`02_feature_matrix.py`](02_feature_matrix.py) — seven tiny demos, one per headline feature: conditional routing, loopback, AND-join, retry policy, error routing, HITL pause/resume, custom events. Compact cheat-sheet.
- [`tour.py`](tour.py) — pedagogical walkthrough: seven workflows sized 2 / 3 / 5 / 7 / 10 / 15 / 30 nodes, each layering one feature on top of the previous one. Writes one offline HTML diagram per workflow next to the script, plus a shared `mermaid.min.js` sibling rendered offline via the vendored bundle.

## Run

```bash
uv run python examples/01_quickstart.py
uv run python examples/02_feature_matrix.py
uv run python examples/tour.py
```

No arguments. No environment setup beyond `uv sync`. No network access required.
