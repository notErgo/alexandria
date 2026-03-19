#!/usr/bin/env python3
"""Shared helpers for UI/DAG review mapping artifacts."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from collections import defaultdict, deque
from pathlib import Path


GENERATOR_VERSION = "2026-03-19.2"
ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"
OPS_FILE = ROOT / "docs" / "architecture" / "operations.json"
DAG_FILE = ROOT / "docs" / "architecture" / "dag.json"
CLI_FILE = ROOT / "cli.py"
RUN_WEB_FILE = ROOT / "run_web.py"
ROUTES_DIR = ROOT / "src" / "routes"
TESTS_DIR = ROOT / "tests"
DOCS_DIR = ROOT / "docs"
TEMPLATES_DIR = ROOT / "templates"
TEMPLATES = [
    "ops.html",
    "miner_data.html",
    "review.html",
    "dashboard.html",
    "index.html",
]
INPUT_FILES = [SPEC_FILE, OPS_FILE, DAG_FILE, CLI_FILE]


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_flask_path(path: str) -> str:
    return re.sub(r"<(?:[^:>]+:)?([^>]+)>", r"<\1>", path)


def endpoint_pattern_matches(spec_endpoint: str, actual_route: str) -> bool:
    marker_segment = "__SEGMENT__"
    marker_ellipsis = "__ELLIPSIS__"
    pattern = re.sub(r"<[^>]+>", marker_segment, spec_endpoint)
    pattern = pattern.replace("...", marker_ellipsis)
    escaped = re.escape(pattern)
    escaped = escaped.replace(marker_segment, r"[^/]+")
    escaped = escaped.replace(marker_ellipsis, r".+")
    return re.fullmatch(rf"{escaped}", actual_route) is not None


def compute_input_hash(paths: list[Path] | None = None) -> str:
    sha = hashlib.sha256()
    for path in paths or INPUT_FILES:
        rel = str(path.relative_to(ROOT))
        sha.update(rel.encode("utf-8"))
        sha.update(b"\0")
        sha.update(path.read_bytes())
        sha.update(b"\0")
    return sha.hexdigest()


def build_metadata() -> dict:
    return {
        "generator_version": GENERATOR_VERSION,
        "input_files": [str(path.relative_to(ROOT)) for path in INPUT_FILES],
        "input_hash": compute_input_hash(),
    }


def classify_template(template: str | None) -> str:
    if template == "ops.html":
        return "canonical"
    if template == "miner_data.html":
        return "legacy"
    if template == "review.html":
        return "active_standalone"
    return "active"


def build_graph(edges: list[dict]) -> dict[str, list[str]]:
    graph: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        graph[edge["from"]].append(edge["to"])
    return graph


def reverse_graph(edges: list[dict]) -> dict[str, list[str]]:
    graph: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        graph[edge["to"]].append(edge["from"])
    return graph


def reachable_nodes(start_nodes: list[str], graph: dict[str, list[str]]) -> list[str]:
    seen: set[str] = set()
    queue = deque(start_nodes)
    while queue:
        node = queue.popleft()
        for child in graph.get(node, []):
            if child in seen:
                continue
            seen.add(child)
            queue.append(child)
    return sorted(seen)


def collect_routes() -> list[dict]:
    routes: list[dict] = []
    route_pat = re.compile(r"@\w+\.route\(\s*['\"]([^'\"]+)['\"]")
    for path in [RUN_WEB_FILE, *sorted(ROUTES_DIR.glob("*.py"))]:
        text = path.read_text(encoding="utf-8")
        for raw in route_pat.findall(text):
            routes.append(
                {
                    "path": normalize_flask_path(raw.strip()),
                    "file": str(path.relative_to(ROOT)),
                }
            )
    return routes


def module_path_to_node_id(file_path: str) -> str:
    return file_path.replace("src/", "").replace(".py", "").replace("/", ".")


def collect_cli_refs(path_to_node: dict[str, str]) -> dict[str, dict]:
    text = CLI_FILE.read_text(encoding="utf-8")
    tree = ast.parse(text)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}

    command_to_handler: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Attribute)
            and isinstance(test.left.value, ast.Name)
            and test.left.value.id == "args"
            and test.left.attr == "command"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and isinstance(test.comparators[0].value, str)
        ):
            continue
        handler = None
        for stmt in node.body:
            if (
                isinstance(stmt, ast.Expr)
                and isinstance(stmt.value, ast.Call)
                and isinstance(stmt.value.func, ast.Name)
            ):
                handler = stmt.value.func.id
                break
        if handler:
            command_to_handler[test.comparators[0].value] = handler

    refs_by_node: dict[str, dict] = defaultdict(lambda: {"commands": [], "handlers": []})
    for command, handler_name in command_to_handler.items():
        fn = functions.get(handler_name)
        if fn is None:
            continue
        node_ids: set[str] = set()
        for inner in ast.walk(fn):
            if isinstance(inner, ast.ImportFrom) and inner.module:
                module_path = f"src/{inner.module.replace('.', '/')}.py"
                node_id = path_to_node.get(module_path)
                if node_id:
                    node_ids.add(node_id)
            elif isinstance(inner, ast.Import):
                for alias in inner.names:
                    module_path = f"src/{alias.name.replace('.', '/')}.py"
                    node_id = path_to_node.get(module_path)
                    if node_id:
                        node_ids.add(node_id)
        for node_id in node_ids:
            refs_by_node[node_id]["commands"].append(command)
            refs_by_node[node_id]["handlers"].append(handler_name)

    for node_id, payload in refs_by_node.items():
        payload["commands"] = sorted(set(payload["commands"]))
        payload["handlers"] = sorted(set(payload["handlers"]))
    return dict(refs_by_node)


def collect_text_refs(base: Path, patterns: list[str]) -> dict[str, list[str]]:
    refs: dict[str, list[str]] = {pattern: [] for pattern in patterns}
    files = sorted(path for path in base.rglob("*") if path.is_file())
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = str(path.relative_to(ROOT))
        for pattern in patterns:
            if pattern in text:
                refs[pattern].append(rel)
    return refs


def build_forward_trace() -> dict:
    spec = load_json(SPEC_FILE)
    operations = load_json(OPS_FILE)
    dag = load_json(DAG_FILE)

    nodes = dag.get("nodes", [])
    path_to_node = {node.get("path"): node["id"] for node in nodes if node.get("path")}
    graph = build_graph(dag.get("edges", []))

    operations_by_component: dict[str, list[dict]] = defaultdict(list)
    unresolved_files: list[dict] = []
    for operation in operations.get("operations", []):
        direct_nodes: list[str] = []
        route_nodes: list[str] = []
        for step in operation.get("dag", []):
            file_path = step.get("file")
            if not file_path:
                continue
            node_id = path_to_node.get(file_path)
            if not node_id:
                unresolved_files.append({"operation": operation["id"], "file": file_path})
                continue
            direct_nodes.append(node_id)
            if node_id.startswith("routes."):
                route_nodes.append(node_id)

        operation_entry = {
            "id": operation["id"],
            "name": operation["name"],
            "route_nodes": sorted(set(route_nodes)),
            "direct_dag_nodes": sorted(set(direct_nodes)),
            "reachable_dag_nodes": reachable_nodes(sorted(set(route_nodes)), graph),
        }
        for component_id in operation.get("components", []):
            operations_by_component[component_id].append(operation_entry)

    components = []
    for component in spec.get("components", []):
        linked_ops = operations_by_component.get(component["id"], [])
        route_nodes = sorted({node for op in linked_ops for node in op["route_nodes"]})
        direct_nodes = sorted({node for op in linked_ops for node in op["direct_dag_nodes"]})
        reachable = sorted({node for op in linked_ops for node in op["reachable_dag_nodes"]})
        display_only = bool(component.get("display_only"))
        components.append(
            {
                "id": component["id"],
                "name": component["name"],
                "template": component.get("template"),
                "status": classify_template(component.get("template")),
                "display_only": display_only,
                "api_endpoints": component.get("api_endpoints", []),
                "operations": linked_ops,
                "route_nodes": route_nodes,
                "direct_dag_nodes": direct_nodes,
                "reachable_dag_nodes": reachable,
                "coverage_status": (
                    "operation_traced" if linked_ops else
                    "display_only" if display_only else
                    "unmapped_canonical" if component.get("template") == "ops.html" else
                    "unmapped_noncanonical"
                ),
            }
        )

    return {
        "metadata": build_metadata(),
        "note": "Derived mapping from ui_spec.json -> operations.json -> dag.json. dag.json remains UI-agnostic.",
        "components": components,
        "unresolved_operation_files": unresolved_files,
    }


def build_reverse_trace() -> dict:
    spec = load_json(SPEC_FILE)
    operations = load_json(OPS_FILE)
    dag = load_json(DAG_FILE)
    routes = collect_routes()
    route_paths_by_file: dict[str, list[str]] = defaultdict(list)
    for route in routes:
        route_paths_by_file[route["file"]].append(route["path"])

    nodes = dag.get("nodes", [])
    edges = dag.get("edges", [])
    path_to_node = {node.get("path"): node["id"] for node in nodes if node.get("path")}
    graph = build_graph(edges)
    reverse = reverse_graph(edges)
    route_nodes = {node["id"] for node in nodes if node["id"].startswith("routes.")}
    worker_reachable = set(reachable_nodes(["scrapers.scrape_worker"], graph))
    cli_refs = collect_cli_refs(path_to_node)

    operation_entries: list[dict] = []
    component_to_ops: dict[str, list[str]] = defaultdict(list)
    for operation in operations.get("operations", []):
        direct_nodes: set[str] = set()
        route_node_ids: set[str] = set()
        for step in operation.get("dag", []):
            file_path = step.get("file")
            if not file_path:
                continue
            node_id = path_to_node.get(file_path)
            if not node_id:
                continue
            direct_nodes.add(node_id)
            if node_id in route_nodes:
                route_node_ids.add(node_id)
        op_entry = {
            "id": operation["id"],
            "components": list(operation.get("components", [])),
            "route_nodes": sorted(route_node_ids),
            "direct_dag_nodes": sorted(direct_nodes),
            "reachable_dag_nodes": reachable_nodes(sorted(route_node_ids), graph),
        }
        operation_entries.append(op_entry)
        for component_id in operation.get("components", []):
            component_to_ops[component_id].append(operation["id"])

    endpoint_components_by_node: dict[str, set[str]] = defaultdict(set)
    component_lookup = {component["id"]: component for component in spec.get("components", [])}
    for component in spec.get("components", []):
        if component_to_ops.get(component["id"]):
            continue
        for endpoint in component.get("api_endpoints", []):
            endpoint_path = normalize_flask_path(endpoint.split("?", 1)[0])
            for route in routes:
                if endpoint_pattern_matches(endpoint_path, route["path"]):
                    node_id = path_to_node.get(route["file"])
                    if node_id:
                        endpoint_components_by_node[node_id].add(component["id"])
                        for child in reachable_nodes([node_id], graph):
                            endpoint_components_by_node[child].add(component["id"])

    test_refs = collect_text_refs(TESTS_DIR, [node["id"] for node in nodes])
    doc_refs = collect_text_refs(DOCS_DIR, [node["id"] for node in nodes])

    component_status = {component["id"]: classify_template(component.get("template")) for component in spec.get("components", [])}

    node_entries = []
    for node in nodes:
        node_id = node["id"]
        component_ids = sorted({
            component_id
            for op in operation_entries
            if node_id in op["reachable_dag_nodes"] or node_id in op["direct_dag_nodes"]
            for component_id in op["components"]
        })
        canonical_components = sorted([cid for cid in component_ids if component_status.get(cid) == "canonical"])
        legacy_components = sorted([cid for cid in component_ids if component_status.get(cid) == "legacy"])
        standalone_components = sorted([cid for cid in component_ids if component_status.get(cid) == "active_standalone"])
        display_only_components = sorted(endpoint_components_by_node.get(node_id, set()))
        operation_ids = sorted({
            op["id"]
            for op in operation_entries
            if node_id in op["direct_dag_nodes"] or node_id in op["reachable_dag_nodes"]
        })
        incoming = sorted(reverse.get(node_id, []))
        incoming_route_edges = sorted(parent for parent in incoming if parent in route_nodes)
        cli_entry = cli_refs.get(node_id, {"commands": [], "handlers": []})
        worker_only = node_id in worker_reachable
        tests = sorted(test_refs.get(node_id, []))
        docs = sorted(doc_refs.get(node_id, []))

        if canonical_components or standalone_components or operation_ids:
            status = "canonical"
        elif display_only_components:
            status = "display_only_ref"
        elif incoming_route_edges:
            status = "protected_route_dependency"
        elif cli_entry["commands"]:
            status = "cli_only"
        elif worker_only:
            status = "worker_only"
        elif legacy_components:
            status = "legacy"
        elif tests and not docs:
            status = "test_only"
        elif docs and not tests:
            status = "doc_only"
        elif not any([canonical_components, display_only_components, legacy_components, operation_ids, incoming, tests, docs, cli_entry["commands"], worker_only]):
            status = "unmapped"
        else:
            status = "needs_manual_review"

        node_entries.append(
            {
                "id": node_id,
                "path": node.get("path"),
                "layer": node.get("layer"),
                "route_paths": sorted(route_paths_by_file.get(node.get("path", ""), [])),
                "status": status,
                "canonical_components": canonical_components,
                "standalone_components": standalone_components,
                "legacy_components": legacy_components,
                "display_only_components": display_only_components,
                "operation_ids": operation_ids,
                "incoming_edges": incoming,
                "incoming_route_edges": incoming_route_edges,
                "cli_commands": cli_entry["commands"],
                "cli_handlers": cli_entry["handlers"],
                "worker_reachable": worker_only,
                "test_refs": tests,
                "doc_refs": docs,
            }
        )

    return {
        "metadata": build_metadata(),
        "note": "Reverse mapping from DAG nodes back to UI, operation, CLI, worker, test, and docs references.",
        "nodes": node_entries,
    }
