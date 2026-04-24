"""Static validation of workflow definitions.

Runs once at `WorkflowEngine(...)` construction. Catches shape errors,
unknown targets, bad join declarations, and forward cycles without an
explicit `is_loopback` marker. Runtime validation (strict_outputs,
retries, timeouts) is handled by the engine itself.
"""

from __future__ import annotations

import json
from typing import Any

_DFS_WHITE = 0
_DFS_GRAY = 1
_DFS_BLACK = 2


def validate_workflow_definition(workflow_def: dict[str, Any]) -> None:
    _require_json_serializable(workflow_def)
    _require_top_level_keys(workflow_def)
    _require_valid_nodes_map(workflow_def)
    _require_valid_entry_node(workflow_def)
    _require_valid_error_node(workflow_def)
    _require_valid_node_shapes(workflow_def)
    _require_valid_joins(workflow_def["nodes"])
    _require_acyclic_forward_graph(workflow_def["nodes"])


def _require_json_serializable(workflow_def: dict[str, Any]) -> None:
    try:
        json.dumps(workflow_def, sort_keys=True)
    except TypeError as exc:
        raise ValueError(f"workflow definition must be JSON-serializable: {exc}") from exc


def _require_top_level_keys(workflow_def: dict[str, Any]) -> None:
    required_keys = ("workflow_name", "default_entry_node", "nodes")
    missing_keys = [key for key in required_keys if key not in workflow_def]
    if missing_keys:
        raise ValueError(f"missing required keys: {', '.join(missing_keys)}")


def _require_valid_nodes_map(workflow_def: dict[str, Any]) -> None:
    nodes = workflow_def["nodes"]
    if not isinstance(nodes, dict) or not nodes:
        raise ValueError("'nodes' must be a non-empty dict")


def _require_valid_entry_node(workflow_def: dict[str, Any]) -> None:
    entry_node = workflow_def["default_entry_node"]
    if entry_node not in workflow_def["nodes"]:
        raise ValueError(f"entry node '{entry_node}' not in nodes")


def _require_valid_error_node(workflow_def: dict[str, Any]) -> None:
    error_node = workflow_def.get("default_error_node")
    if error_node is not None and error_node not in workflow_def["nodes"]:
        raise ValueError(f"error node '{error_node}' not in nodes")


def _require_valid_node_shapes(workflow_def: dict[str, Any]) -> None:
    nodes = workflow_def["nodes"]
    for node_name, node_def in nodes.items():
        _require_node_handler(node_name, node_def)
        _require_node_outputs(node_name, node_def, nodes)
        _require_valid_state_contract(node_name, node_def)


def _require_node_handler(node_name: str, node_def: dict[str, Any]) -> None:
    if "handler" not in node_def:
        raise ValueError(f"node '{node_name}' missing 'handler'")


def _require_node_outputs(
    node_name: str,
    node_def: dict[str, Any],
    all_nodes: dict[str, Any],
) -> None:
    if "outputs" not in node_def:
        raise ValueError(f"node '{node_name}' missing 'outputs'")
    outputs = node_def["outputs"]
    if not isinstance(outputs, dict):
        raise ValueError(f"node '{node_name}' outputs must be a dict")
    for output_name, targets in outputs.items():
        if not isinstance(targets, list):
            raise ValueError(f"node '{node_name}' output '{output_name}' must be a list")
        for edge in targets:
            if not isinstance(edge, dict):
                raise ValueError(f"node '{node_name}' output '{output_name}' edge must be a dict")
            if "target_node" not in edge:
                raise ValueError(
                    f"node '{node_name}' output '{output_name}' edge missing 'target_node'"
                )
            target = edge["target_node"]
            if not isinstance(target, str) or not target:
                raise ValueError(
                    f"node '{node_name}' output '{output_name}' target_node must be a non-empty string"
                )
            if target not in all_nodes:
                raise ValueError(
                    f"node '{node_name}' output '{output_name}' references unknown target '{target}'"
                )


def _require_valid_state_contract(node_name: str, node_def: dict[str, Any]) -> None:
    contract = node_def.get("state_contract")
    if contract is None:
        return
    reads_from = contract.get("reads_from", [])
    writes_to = contract.get("writes_to", [])
    if not isinstance(reads_from, list) or not isinstance(writes_to, list):
        raise ValueError(
            f"node '{node_name}' state_contract.reads_from and writes_to must be lists"
        )


def _require_valid_joins(nodes: dict[str, Any]) -> None:
    incoming = _build_incoming_map(nodes)
    for node_name, node_def in nodes.items():
        join = node_def.get("join")
        if join is None:
            continue
        _require_valid_join_mode(node_name, join)
        _require_valid_and_join_wait_for(node_name, join, incoming, nodes)


def _build_incoming_map(nodes: dict[str, Any]) -> dict[str, set[str]]:
    incoming: dict[str, set[str]] = {node_name: set() for node_name in nodes}
    for source_name, node_def in nodes.items():
        for targets in node_def.get("outputs", {}).values():
            for edge in targets:
                target = edge.get("target_node")
                if target is not None:
                    incoming[target].add(source_name)
    return incoming


def _require_valid_join_mode(node_name: str, join: dict[str, Any]) -> None:
    mode = join.get("mode", "or")
    if mode not in ("or", "and"):
        raise ValueError(f"node '{node_name}' join.mode must be 'or' or 'and', got {mode!r}")


def _require_valid_and_join_wait_for(
    node_name: str,
    join: dict[str, Any],
    incoming: dict[str, set[str]],
    nodes: dict[str, Any],
) -> None:
    if join.get("mode", "or") != "and":
        return
    wait_for = join.get("wait_for", [])
    if not isinstance(wait_for, list) or not wait_for:
        raise ValueError(f"node '{node_name}' join.mode='and' requires a non-empty wait_for list")
    for predecessor in wait_for:
        if predecessor not in nodes:
            raise ValueError(f"node '{node_name}' wait_for references unknown node '{predecessor}'")
        if predecessor not in incoming[node_name]:
            raise ValueError(
                f"node '{node_name}' wait_for includes '{predecessor}' but no declared edge reaches '{node_name}'"
            )


def _require_acyclic_forward_graph(nodes: dict[str, Any]) -> None:
    colors = dict.fromkeys(nodes, _DFS_WHITE)
    for node_name in nodes:
        if colors[node_name] == _DFS_WHITE:
            _visit_forward_graph(node_name, nodes, colors)


def _visit_forward_graph(
    start_node: str,
    nodes: dict[str, Any],
    colors: dict[str, int],
) -> None:
    stack: list[tuple[str, list[str]]] = [(start_node, [start_node])]
    colors[start_node] = _DFS_GRAY

    while stack:
        node_name, path = stack[-1]
        advanced = False
        for targets in nodes[node_name].get("outputs", {}).values():
            for edge in targets:
                target = edge.get("target_node")
                if target is None or edge.get("is_loopback"):
                    continue
                if colors[target] == _DFS_GRAY:
                    cycle = " -> ".join([*path, target])
                    raise ValueError(
                        f"cycle detected in forward edges: {cycle}. "
                        "Mark the closing edge with 'is_loopback': true if the loop is intentional."
                    )
                if colors[target] == _DFS_WHITE:
                    colors[target] = _DFS_GRAY
                    stack.append((target, [*path, target]))
                    advanced = True
                    break
            if advanced:
                break
        if not advanced:
            colors[node_name] = _DFS_BLACK
            stack.pop()
