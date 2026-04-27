"""Queue manipulation, edge routing, and join resolution.

Everything here mutates (or reads) a `RunSnapshot` and reads the
validated workflow `nodes` map. The engine delegates all scheduling
decisions to this module so the run loop stays readable.
"""

from __future__ import annotations

from typing import Any

from zeroflow.core.models import RunSnapshot, WorkflowError, now_iso
from zeroflow.core.nodes import (
    join_is_satisfied,
    output_edges,
    target_uses_and_join,
)


def has_pending_work(snapshot: RunSnapshot) -> bool:
    return bool(snapshot.ready_now or snapshot.ready_next_wave)


def should_open_next_wave(snapshot: RunSnapshot) -> bool:
    return not snapshot.ready_now and bool(snapshot.ready_next_wave)


def open_next_wave(snapshot: RunSnapshot) -> None:
    snapshot.ready_now = list(snapshot.ready_next_wave)
    snapshot.ready_next_wave = []
    snapshot.wave += 1
    snapshot.executed_this_wave = []
    snapshot.updated_at = now_iso()


def take_next_node(snapshot: RunSnapshot) -> str:
    node_name = snapshot.ready_now.pop(0)
    snapshot.last_node = node_name
    snapshot.trace.append(node_name)
    snapshot.step += 1
    if node_name not in snapshot.executed_this_wave:
        snapshot.executed_this_wave.append(node_name)
    snapshot.updated_at = now_iso()
    return node_name


def reset_ready_queues(snapshot: RunSnapshot) -> None:
    snapshot.ready_now = []
    snapshot.ready_next_wave = []
    snapshot.arrivals = {}
    snapshot.executed_this_wave = []


def schedule_outputs(
    snapshot: RunSnapshot,
    nodes: dict[str, Any],
    node_name: str,
    outputs: list[str],
) -> None:
    for output_name in outputs:
        for edge in output_edges(nodes, node_name, output_name):
            _schedule_target(
                snapshot,
                nodes,
                source=node_name,
                target=edge.get("target_node"),
                is_loopback=bool(edge.get("is_loopback")),
            )


def route_to_error_node(
    snapshot: RunSnapshot,
    node_name: str,
    error: WorkflowError,
    default_error_node: str | None,
) -> bool:
    """Reset the queues and point them at the error node if one is
    declared and different from the failing node. Returns True when the
    error node has been scheduled, False when the engine should finalise
    the run as failed."""
    snapshot.last_error = error
    snapshot.state.workflow["__error__"] = error.to_dict()
    reset_ready_queues(snapshot)

    if default_error_node is None or node_name == default_error_node:
        return False

    snapshot.ready_now = [default_error_node]
    return True


def prepend_unique(items: list[str], value: str) -> None:
    if value in items:
        items.remove(value)
    items.insert(0, value)


# Internal helpers


def _schedule_target(
    snapshot: RunSnapshot,
    nodes: dict[str, Any],
    *,
    source: str,
    target: str | None,
    is_loopback: bool,
) -> None:
    if target is None:
        return
    if not is_loopback and target in snapshot.executed_this_wave:
        return
    if target_uses_and_join(nodes, target):
        _schedule_and_join_target(snapshot, nodes, source, target, is_loopback=is_loopback)
        return
    _append_target(snapshot, target, is_loopback=is_loopback)


def _schedule_and_join_target(
    snapshot: RunSnapshot,
    nodes: dict[str, Any],
    source: str,
    target: str,
    *,
    is_loopback: bool,
) -> None:
    arrival_key = _arrival_key(snapshot.wave, target, is_loopback=is_loopback)
    arrived = set(snapshot.arrivals.get(arrival_key, []))
    arrived.add(source)

    if join_is_satisfied(nodes, target, arrived):
        snapshot.arrivals.pop(arrival_key, None)
        _append_target(snapshot, target, is_loopback=is_loopback)
        return

    snapshot.arrivals[arrival_key] = sorted(arrived)


def _append_target(snapshot: RunSnapshot, target: str, *, is_loopback: bool) -> None:
    queue = snapshot.ready_next_wave if is_loopback else snapshot.ready_now
    append_unique(queue, target)


def append_unique(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def _arrival_key(current_wave: int, target: str, *, is_loopback: bool) -> str:
    wave = current_wave + 1 if is_loopback else current_wave
    return f"{wave}:{target}"
