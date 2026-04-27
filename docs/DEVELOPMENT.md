# DEVELOPMENT

## Purpose

Contributor setup guide for working on `zeroflow` locally: how to
build, test, lint, and release. Audience: developers who want to
modify the source, run the test suite, or open a PR.

## Scope

- Prerequisites and environment.
- Quality pipeline (ruff / mypy / bandit / vulture / pytest).
- Pre-commit hooks.
- Commit and PR conventions.
- Release process.

For library *usage* see `USER_GUIDE.md`. For the public API see
`API_REFERENCE.md`. For module-level architecture see
`ARCHITECTURE.md`.

---

## 1. Prerequisites

- **Python**: 3.12, 3.13, or 3.14.
- **uv**: recommended package manager (`pipx install uv` or
  [official installer](https://docs.astral.sh/uv/getting-started/installation/)).
- **git**: for cloning and committing.

The project ships a `uv.lock`-less `pyproject.toml` deliberately —
zeroflow is a library, not an application, so resolution stays
flexible for downstream consumers. `uv sync` resolves freshly each
time.

---

## 2. Clone and setup

```bash
git clone https://github.com/francescofavi/zeroflow.git
cd zeroflow
uv sync
```

`uv sync` creates `.venv/`, installs the project in editable mode,
and pulls every dev dependency declared under
`[dependency-groups].dev` in `pyproject.toml`.

To activate the environment manually (most `uv run` commands do not
require this):

```bash
source .venv/bin/activate
```

---

## 3. Running tests

```bash
uv run pytest -q
```

Run with coverage:

```bash
uv run pytest --cov=src/zeroflow --cov-report=term-missing
```

Run a single test:

```bash
uv run pytest tests/test_core.py::test_linear_runs_all_nodes_in_order -v
```

### Test layout

```
tests/
├── conftest.py                 # shared fixtures
├── test_core.py                # engine + scheduling + retry + HITL
├── test_edge_cases.py          # boundary conditions, error paths
├── test_serialization.py       # to_dict / from_dict round-trips
├── test_version.py             # __version__ contract
├── test_viz.py                 # mermaid + html
└── test_examples.py            # smoke test for examples/
```

Add a test alongside the change that introduces the behaviour, never
later. Tests must run cleanly under default settings (no env vars,
no network, no clock manipulation).

---

## 4. Quality pipeline

Each tool has its own `pyproject.toml` configuration. Run them
individually:

```bash
uv run ruff check                # lint
uv run ruff format --check       # formatting (no diff)
uv run ruff format               # formatting (apply)
uv run mypy                      # static types (src/ only)
uv run bandit -r src/zeroflow    # security
uv run vulture src --min-confidence 80   # dead code
```

The full battery should be green before opening a PR. CI runs
ruff + mypy + pytest on every push.

### Ruff configuration

Ruff is configured under `[tool.ruff]` and `[tool.ruff.lint]` in
`pyproject.toml`. The selected rules cover PEP 8, pyflakes, isort,
naming, modern Python syntax (`UP`), bugbear, comprehensions,
simplify, ruff-specific rules, tryceratops, and the boolean-trap
checker (`FBT`).

### Mypy configuration

`mypy_path = ["src"]`, `files = ["src/zeroflow"]`. Tests and
examples are not type-checked by default — they exist as runnable
fixtures, not as a typing target. `strict_optional = True`,
`warn_return_any = True`.

---

## 5. Pre-commit hooks

```bash
uv run pre-commit install
```

The hooks defined in `.pre-commit-config.yaml` run on every commit:

- **ruff** — lint + format.
- **mypy** — static types on `src/`.
- **trailing-whitespace**, **end-of-file-fixer**, **check-yaml**,
  **check-toml** — file hygiene.

If a hook fails, fix the issue and commit again. Do **not** bypass
hooks with `--no-verify` — the hook is reporting a real problem.

---

## 6. Commit conventions

zeroflow follows [Conventional Commits](https://www.conventionalcommits.org/).
The `release-please` action parses the log to bump versions and
generate `CHANGELOG.md`.

| Prefix | When |
|--------|------|
| `feat:` | A new user-visible feature. Triggers a minor bump. |
| `fix:` | A bug fix. Triggers a patch bump. |
| `docs:` | Documentation only. No version bump. |
| `style:` | Formatting / lint fixes that do not change behaviour. No bump. |
| `refactor:` | Internal restructure with no behaviour change. No bump. |
| `test:` | Test-only change. No bump. |
| `chore:` | Tooling, CI, dependencies. No bump. |
| `perf:` | Performance improvement. Patch bump. |

Breaking changes go in the body with `BREAKING CHANGE:` to trigger
a major bump.

CI validates the format on PRs. Reject anything that does not match.

---

## 7. Running examples

```bash
uv run python examples/01_quickstart.py
uv run python examples/02_feature_matrix.py
uv run python examples/tour.py
```

`tour.py` writes `.html` files next to itself plus a shared
`mermaid.min.js` sibling; open the `.html` files in any modern
browser. The other two scripts print to stdout only.

The smoke test `tests/test_examples.py` runs every example via
`subprocess.run` with a timeout — keep it green.

---

## 8. Release process

zeroflow uses `release-please` for version bumps and changelog
generation, plus a manually-dispatched `publish.yml` workflow that
publishes to PyPI via Trusted Publishing (OIDC).

### How a release happens

1. Conventional Commits land on `main`. `release-please` opens (or
   updates) a "release PR" that bumps the version and edits
   `CHANGELOG.md`.
2. A maintainer reviews and merges the release PR. `release-please`
   creates a git tag (`vX.Y.Z`) and a GitHub Release.
3. A maintainer manually dispatches `.github/workflows/publish.yml`
   from the GitHub UI, choosing the tag.
4. `publish.yml` runs `uv build` and uploads the artefacts to PyPI
   using OIDC. No API token is configured anywhere — PyPI verifies
   the GitHub Actions identity directly.

### Why manual dispatch

The publish step is intentionally separated from the tag creation so
that:

- A bad release can be re-tagged without immediately reaching PyPI.
- The maintainer makes one final review before a public artefact is
  published.

### Local pre-release sanity check

Before merging a release PR, run:

```bash
uv build
uv run twine check dist/*
```

This catches malformed `README.md`, missing classifiers, etc., so
the PyPI upload does not fail.

---

## 9. Project layout

```
zeroflow/
├── README.md                   # public landing page
├── pyproject.toml              # build, deps, ruff / mypy / pytest config
├── src/zeroflow/               # source code
│   ├── __init__.py             # public re-exports + version
│   ├── core/                   # engine + models + stores + validation
│   └── viz/                    # mermaid + html (vendored mermaid.min.js)
├── tests/                      # pytest suite
├── examples/                   # runnable scripts
├── docs/                       # public documentation (this folder)
├── .github/                    # CI + release-please + publish workflows
└── CHANGELOG.md                # generated by release-please
```

The `src/` layout is mandatory: it prevents accidental imports of
the source tree without going through `pip install -e .`.

---

## 10. Where to ask

This is a personal-portfolio project. Issues and discussions live on
[GitHub](https://github.com/francescofavi/zeroflow). Pull requests
are welcome but may not be merged if they expand scope beyond the
"smallest coherent workflow engine" charter — see `USER_GUIDE.md`
§3 (Known limits and open issues) for what is intentionally out of
scope.
