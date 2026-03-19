#!/usr/bin/env python3
"""Evaluate whether a UI component, route, or DAG node is safe to delete."""

from __future__ import annotations

import argparse
import json
import sys

from review_maps_common import (
    DAG_FILE,
    OPS_FILE,
    SPEC_FILE,
    build_forward_trace,
    build_graph,
    build_reverse_trace,
    load_json,
    normalize_flask_path,
    reachable_nodes,
)


def _fail(message: str) -> None:
    print(json.dumps({"error": message}, indent=2))
    raise SystemExit(2)


def classify_from_node_entries(entries: list[dict]) -> str:
    if any(entry["incoming_route_edges"] for entry in entries):
        return "protected-route-dependency"
    if any(entry["status"] == "canonical" for entry in entries):
        return "protected-canonical"
    if any(entry["status"] == "display_only_ref" for entry in entries):
        return "protected-canonical"
    if all(entry["status"] == "cli_only" for entry in entries):
        return "cli_only"
    if all(entry["status"] == "worker_only" for entry in entries):
        return "worker_only"
    if all(entry["status"] in {"legacy", "unmapped", "doc_only", "test_only"} for entry in entries):
        if any(entry["status"] == "legacy" for entry in entries):
            return "legacy-delete-candidate"
        return "needs-manual-review"
    return "needs-manual-review"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ui-id")
    group.add_argument("--route")
    group.add_argument("--node")
    parser.add_argument("--deep", action="store_true", help="Include transitive reachable DAG nodes.")
    args = parser.parse_args()

    spec = load_json(SPEC_FILE)
    dag = load_json(DAG_FILE)
    ops = load_json(OPS_FILE)
    forward = build_forward_trace()
    reverse = build_reverse_trace()
    reverse_by_id = {entry["id"]: entry for entry in reverse["nodes"]}
    graph = build_graph(dag.get("edges", []))

    if args.ui_id:
        component = next((c for c in forward["components"] if c["id"] == args.ui_id), None)
        if component is None:
            _fail(f"unknown ui_spec component: {args.ui_id}")
        node_ids = list(component["direct_dag_nodes"] if not args.deep else component["reachable_dag_nodes"])
        entries = [reverse_by_id[node_id] for node_id in node_ids if node_id in reverse_by_id]
        status = "protected-canonical" if component["status"] == "canonical" and (component["operations"] or component["api_endpoints"]) else (
            "legacy-delete-candidate" if component["status"] == "legacy" and not component["operations"] else (
                "needs-manual-review" if not entries else classify_from_node_entries(entries)
            )
        )
        result = {
            "target_type": "ui_id",
            "target": args.ui_id,
            "status": status,
            "component": component,
            "resolved_nodes": node_ids,
            "node_evidence": entries,
        }
    elif args.route:
        route = normalize_flask_path(args.route)
        route_entries = [entry for entry in reverse["nodes"] if route in entry["route_paths"]]
        if not route_entries:
            _fail(f"unknown route: {args.route}")
        node_ids = [entry["id"] for entry in route_entries]
        if args.deep:
            seen = set(node_ids)
            for node_id in list(node_ids):
                for child in reachable_nodes([node_id], graph):
                    if child not in seen:
                        seen.add(child)
                        node_ids.append(child)
            route_entries = [reverse_by_id[node_id] for node_id in node_ids if node_id in reverse_by_id]
        result = {
            "target_type": "route",
            "target": route,
            "status": classify_from_node_entries(route_entries),
            "resolved_nodes": node_ids,
            "node_evidence": route_entries,
        }
    else:
        node = next((entry for entry in reverse["nodes"] if entry["id"] == args.node), None)
        if node is None:
            _fail(f"unknown dag node: {args.node}")
        node_ids = [node["id"]]
        entries = [node]
        if args.deep:
            node_ids.extend(reachable_nodes([node["id"]], graph))
            node_ids = sorted(set(node_ids))
            entries = [reverse_by_id[node_id] for node_id in node_ids if node_id in reverse_by_id]
        result = {
            "target_type": "node",
            "target": args.node,
            "status": classify_from_node_entries(entries),
            "resolved_nodes": node_ids,
            "node_evidence": entries,
        }

    result["metadata"] = {
        "input_hash": forward["metadata"]["input_hash"],
        "generator_version": forward["metadata"]["generator_version"],
        "operations_count": len(ops.get("operations", [])),
        "node_count": len(dag.get("nodes", [])),
        "component_count": len(spec.get("components", [])),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
