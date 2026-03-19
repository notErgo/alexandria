#!/usr/bin/env python3
"""Generate a UI component to runtime mapping from ui_spec, routes, and operation contracts."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from template_source import load_template_source


ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"
OPS_FILE = ROOT / "docs" / "architecture" / "operations.json"
RUN_WEB_FILE = ROOT / "run_web.py"
ROUTES_DIR = ROOT / "src" / "routes"
TEMPLATES = [
    "ops.html",
    "miner_data.html",
    "review.html",
    "dashboard.html",
    "index.html",
]


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


def collect_interpreter_imports() -> dict[str, list[str]]:
    imports_by_file: dict[str, list[str]] = {}
    for path in [RUN_WEB_FILE, *sorted(ROUTES_DIR.glob("*.py"))]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("interpreters"):
                modules.add(node.module)
        imports_by_file[str(path.relative_to(ROOT))] = sorted(modules)
    return imports_by_file


def collect_template_gaps() -> dict[str, list[dict]]:
    gaps: dict[str, list[dict]] = {}
    for name in TEMPLATES:
        try:
            text = load_template_source(name)
        except FileNotFoundError:
            continue
        ui_only = []
        for match in re.finditer(r'(<[^>]+data-ui-id="([^"]+)"[^>]*>)', text):
            tag, ui_id = match.group(1), match.group(2)
            if 'data-spec-id=' in tag:
                continue
            ui_only.append({"ui_id": ui_id, "tag": tag[:200]})
        gaps[name] = ui_only
    return gaps


def classify_component(component: dict, route_to_file: list[dict]) -> str:
    template = component.get("template")
    route = component.get("route")
    comp_id = component.get("id")
    if template == "miner_data.html":
        return "legacy-template-drift"
    if template == "review.html":
        return "active-standalone"
    if template == "ops.html":
        return "canonical-ops"
    if route in {"/dashboard", "/data-explorer", "/review"}:
        return "active-standalone"
    if comp_id == "2.5.2.2.1":
        return "canonical-ops-mispointed-template"
    return "active"


def main() -> None:
    spec = load_json(SPEC_FILE)
    operations = load_json(OPS_FILE)
    routes = collect_routes()
    imports_by_file = collect_interpreter_imports()
    template_gaps = collect_template_gaps()

    op_by_component: dict[str, list[str]] = {}
    for op in operations.get("operations", []):
        for component in op.get("components", []):
            op_by_component.setdefault(component, []).append(op["id"])

    mapped_components = []
    for component in spec.get("components", []):
        endpoints = component.get("api_endpoints", [])
        route_files = []
        interpreters = set()
        for endpoint in endpoints:
            endpoint_path = normalize_flask_path(endpoint.split("?", 1)[0])
            for route in routes:
                if endpoint_pattern_matches(endpoint_path, route["path"]):
                    route_files.append(route["file"])
                    for module in imports_by_file.get(route["file"], []):
                        interpreters.add(module)
        mapped_components.append(
            {
                "id": component["id"],
                "name": component["name"],
                "template": component.get("template"),
                "route": component.get("route"),
                "api_endpoints": endpoints,
                "operation_refs": sorted(op_by_component.get(component["id"], [])),
                "route_files": sorted(set(route_files)),
                "interpreter_modules": sorted(interpreters),
                "status": classify_component(component, routes),
            }
        )

    extra_spec_anchors = []
    all_spec_ids = {c["id"] for c in spec.get("components", [])}
    for template_name in TEMPLATES:
        try:
            text = load_template_source(template_name)
        except FileNotFoundError:
            continue
        anchors = re.findall(r'data-spec-id="([^"]+)"', text)
        for anchor in sorted(set(anchors) - all_spec_ids):
            extra_spec_anchors.append({"template": template_name, "spec_id": anchor})

    result = {
        "note": "Generated from ui_spec.json, operations.json, route decorators, and route-file interpreter imports.",
        "components": mapped_components,
        "template_ui_only_gaps": template_gaps,
        "extra_template_spec_anchors": extra_spec_anchors,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
