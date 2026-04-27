"""Workflow snapshot/event stores.

Two concrete stores ship with zeroflow: in-memory (for tests and
ephemeral runs) and JSON-on-disk (for crash-safe checkpointing).
Custom stores implement the `WorkflowStore` Protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from zeroflow.core.models import (
    Event,
    RunMetadata,
    RunSnapshot,
    WorkflowError,
    clone_snapshot,
)


class WorkflowStore(Protocol):
    def save_snapshot(self, snapshot: RunSnapshot) -> None: ...
    def load_snapshot(self, run_id: str) -> RunSnapshot: ...
    def append_event(self, run_id: str, event: Event) -> None: ...
    def list_metadata(self, workflow_name: str | None = None) -> list[RunMetadata]: ...


@dataclass
class InMemoryWorkflowStore:
    _snapshots: dict[str, RunSnapshot] = field(default_factory=dict)
    _events: dict[str, list[Event]] = field(default_factory=dict)

    def save_snapshot(self, snapshot: RunSnapshot) -> None:
        self._snapshots[snapshot.run_id] = clone_snapshot(snapshot)

    def load_snapshot(self, run_id: str) -> RunSnapshot:
        snapshot = self._snapshots.get(run_id)
        if snapshot is None:
            raise KeyError(f"run '{run_id}' not found")
        return clone_snapshot(snapshot)

    def append_event(self, run_id: str, event: Event) -> None:
        self._events.setdefault(run_id, []).append(event)

    def list_metadata(self, workflow_name: str | None = None) -> list[RunMetadata]:
        items = [snapshot.metadata() for snapshot in self._snapshots.values()]
        if workflow_name is None:
            return items
        return [item for item in items if item.workflow_name == workflow_name]


@dataclass
class JsonFileWorkflowStore:
    base_dir: Path | str

    def __post_init__(self) -> None:
        self.base_dir = Path(self.base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_snapshot(self, snapshot: RunSnapshot) -> None:
        run_dir = self._run_dir(snapshot.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "snapshot.json").write_text(
            json.dumps(snapshot.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        (run_dir / "metadata.json").write_text(
            json.dumps(snapshot.metadata().to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def load_snapshot(self, run_id: str) -> RunSnapshot:
        path = self._run_dir(run_id) / "snapshot.json"
        if not path.exists():
            raise KeyError(f"run '{run_id}' not found")
        return RunSnapshot.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def append_event(self, run_id: str, event: Event) -> None:
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "events.jsonl"
        payload = {
            "run_id": event.run_id,
            "step": event.step,
            "wave": event.wave,
            "node": event.node,
            "kind": event.kind,
            "ts": event.ts,
            "data": event.data,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    def list_metadata(self, workflow_name: str | None = None) -> list[RunMetadata]:
        items: list[RunMetadata] = []
        for run_dir in Path(self.base_dir).iterdir():
            if not run_dir.is_dir():
                continue
            metadata_path = run_dir / "metadata.json"
            if not metadata_path.exists():
                continue
            raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            error_raw = raw.get("last_error")
            metadata = RunMetadata(
                workflow_name=raw["workflow_name"],
                workflow_hash=raw["workflow_hash"],
                status=raw["status"],
                started_at=raw["started_at"],
                updated_at=raw["updated_at"],
                current_node=raw.get("current_node"),
                waiting=bool(raw.get("waiting", False)),
                last_error=None if error_raw is None else WorkflowError.from_dict(error_raw),
            )
            if workflow_name is None or metadata.workflow_name == workflow_name:
                items.append(metadata)
        return items

    def _run_dir(self, run_id: str) -> Path:
        return Path(self.base_dir) / run_id
