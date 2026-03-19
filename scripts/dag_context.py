#!/usr/bin/env python3
"""Emit a compact architecture summary from dag.json and operations.json."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DAG_FILE = ROOT / "docs" / "architecture" / "dag.json"
OPERATIONS_FILE = ROOT / "docs" / "architecture" / "operations.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    dag = load_json(DAG_FILE)
    operations = load_json(OPERATIONS_FILE)

    nodes = dag.get("nodes", [])
    edges = dag.get("edges", [])
    by_layer: dict[int, list[str]] = defaultdict(list)
    for node in nodes:
        by_layer[int(node["layer"])].append(node["id"])

    known_violations = [
        edge for edge in edges
        if edge.get("notes")
    ]
    route_nodes = sorted(node["id"] for node in nodes if node["id"].startswith("routes."))
    op_refs = Counter()
    for op in operations.get("operations", []):
        for step in op.get("dag", []):
            file_path = step.get("file")
            if not file_path:
                continue
            module = file_path.replace("src/", "").replace(".py", "").replace("/", ".")
            op_refs[module] += 1

    print("# Architecture Context")
    print()
    print(f"- Nodes: {len(nodes)}")
    print(f"- Edges: {len(edges)}")
    print(f"- Route nodes: {len(route_nodes)}")
    print(f"- Operations: {len(operations.get('operations', []))}")
    print(f"- Known documented violations: {len(known_violations)}")
    print()
    print("## Layers")
    for layer in sorted(by_layer):
        labels = ", ".join(sorted(by_layer[layer]))
        print(f"- L{layer}: {labels}")
    print()
    print("## Route Coverage By Operations")
    for route in route_nodes:
        refs = op_refs.get(route, 0)
        print(f"- {route}: operation refs={refs}")
    print()
    print("## Documented Violations")
    if not known_violations:
        print("- none")
    else:
        for edge in known_violations:
            print(f"- {edge['from']} -> {edge['to']}: {edge['notes']}")


if __name__ == "__main__":
    main()
