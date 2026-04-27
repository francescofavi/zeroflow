"""Smoke tests for shipped example scripts.

Every script listed in ``SCRIPTS`` is invoked as a subprocess via the
current interpreter and must exit with code 0 within ``TIMEOUT_SECONDS``.
Asserts no runtime regression broke a headline usage shown in the docs.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"
TIMEOUT_SECONDS = 60

SCRIPTS: tuple[str, ...] = (
    "01_quickstart.py",
    "02_feature_matrix.py",
    "tour.py",
)


@pytest.mark.parametrize("script", SCRIPTS)
def test_example_script_runs_to_completion(script: str, tmp_path: Path) -> None:
    path = EXAMPLES_DIR / script
    assert path.exists(), f"missing example script: {path}"

    completed = subprocess.run(
        [sys.executable, str(path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    assert completed.returncode == 0, (
        f"{script} exited with {completed.returncode}\n"
        f"--- stdout ---\n{completed.stdout}\n"
        f"--- stderr ---\n{completed.stderr}"
    )
    assert completed.stdout.strip(), f"{script} produced no stdout"
