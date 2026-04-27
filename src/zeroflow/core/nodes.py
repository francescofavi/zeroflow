"""Readers for the workflow definition dict.

Every function takes the already-validated `nodes` map (the
`workflow_def["nodes"]` sub-tree) plus a node name, and returns a
typed view of what the JSON says about that node. No mutation, no
runtime state.
"""

from __future__ import annotations

from typing import Any, cast

from zeroflow.core.models import DEFAULT_RETRY_SLEEP


def node_definition(nodes: dict[str, Any], node_name: str) -> dict[str, Any]:
    return cast(dict[str, Any], nodes[node_name])


def handler_type_of(nodes: dict[str, Any], node_name: str) -> str | None:
    raw = node_definition(nodes, node_name).get("handler")
    return None if raw is None else str(raw)


def node_config(nodes: dict[str, Any], node_name: str) -> dict[str, Any]:
    return cast(dict[str, Any], node_definition(nodes, node_name).get("config", {}))


def node_outputs(nodes: dict[str, Any], node_name: str) -> dict[str, list[dict[str, Any]]]:
    return cast(
        dict[str, list[dict[str, Any]]],
        node_definition(nodes, node_name).get("outputs", {}),
    )


def output_edges(
    nodes: dict[str, Any],
    node_name: str,
    output_name: str,
) -> list[dict[str, Any]]:
    return node_outputs(nodes, node_name).get(output_name, [])


def node_run_policy(nodes: dict[str, Any], node_name: str) -> dict[str, Any]:
    return cast(dict[str, Any], node_definition(nodes, node_name).get("run_policy", {}))


def node_max_retries(nodes: dict[str, Any], node_name: str) -> int:
    return int(node_run_policy(nodes, node_name).get("max_retries", 0))


def node_retry_sleep(nodes: dict[str, Any], node_name: str) -> float:
    return float(node_run_policy(nodes, node_name).get("retry_sleep_seconds", DEFAULT_RETRY_SLEEP))


def target_uses_and_join(nodes: dict[str, Any], node_name: str) -> bool:
    join = cast(dict[str, Any], node_definition(nodes, node_name).get("join", {}))
    return join.get("mode") == "and"


def target_wait_for(nodes: dict[str, Any], node_name: str) -> set[str]:
    join = cast(dict[str, Any], node_definition(nodes, node_name).get("join", {}))
    return set(join.get("wait_for", []))


def join_is_satisfied(
    nodes: dict[str, Any],
    node_name: str,
    arrivals: set[str],
) -> bool:
    wait_for = target_wait_for(nodes, node_name)
    return bool(wait_for and arrivals >= wait_for)
