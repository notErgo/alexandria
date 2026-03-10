"""
Spec drift tests for Display Ritual IDs.
"""

import json
import re
import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"
TEMPLATES_DIR = ROOT / "templates"
ROUTES_DIR = ROOT / "src" / "routes"
RUN_WEB_FILE = ROOT / "run_web.py"

# IDs in scope for the Display Ritual rollout.
REQUIRED_COMPONENT_IDS = {
    "1.0",
    "1.1",
    "2.0",
    "2.1",
    "2.1.1",
    "2.1.4",
    "2.1.5",
    "2.2",
    "2.2.2",
    "2.3",
    "2.3.2",
    "3.0",
    "3.1",
    "3.2",
    "5.0",
    "5.1",
    "5.2",
    "6.0",
    "MD5.3",
    "MD5.4",
}


def load_spec() -> dict:
    with SPEC_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def anchors_in_template(template_name: str) -> set[str]:
    path = TEMPLATES_DIR / template_name
    if not path.exists():
        return set()
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r'data-spec-id="([^"]+)"', text))


def normalize_flask_path(path: str) -> str:
    """Normalize Flask converters: <int:id> -> <id>."""
    return re.sub(r"<(?:[^:>]+:)?([^>]+)>", r"<\1>", path)


def collect_route_paths() -> set[str]:
    """Collect all route decorator paths from run_web.py and src/routes/*.py."""
    paths: set[str] = set()
    route_pat = re.compile(r"@\w+\.route\(\s*['\"]([^'\"]+)['\"]")
    for path in list(ROUTES_DIR.glob("*.py")) + [RUN_WEB_FILE]:
        text = path.read_text(encoding="utf-8")
        for raw in route_pat.findall(text):
            paths.add(normalize_flask_path(raw.strip()))
    return paths


def collect_page_route_templates() -> dict[str, set[str]]:
    """
    Parse run_web.py for page routes that return render_template('...').
    Returns route path -> template file name.
    """
    text = RUN_WEB_FILE.read_text(encoding="utf-8")
    tree = ast.parse(text)
    page_routes: dict[str, set[str]] = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue

        routes: list[str] = []
        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue
            if not isinstance(deco.func, ast.Attribute):
                continue
            if not isinstance(deco.func.value, ast.Name) or deco.func.value.id != "app":
                continue
            if deco.func.attr != "route":
                continue
            if deco.args and isinstance(deco.args[0], ast.Constant) and isinstance(deco.args[0].value, str):
                routes.append(normalize_flask_path(deco.args[0].value))

        if not routes:
            continue

        templates: set[str] = set()
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            if not isinstance(inner.func, ast.Name) or inner.func.id != "render_template":
                continue
            if inner.args and isinstance(inner.args[0], ast.Constant) and isinstance(inner.args[0].value, str):
                templates.add(inner.args[0].value)

        for route in routes:
            page_routes.setdefault(route, set()).update(templates)

    return page_routes


def endpoint_pattern_matches(spec_endpoint: str, actual_route: str) -> bool:
    """Support placeholders and /.../ shorthand in spec API paths."""
    marker_segment = "__SEGMENT__"
    marker_ellipsis = "__ELLIPSIS__"

    pattern = re.sub(r"<[^>]+>", marker_segment, spec_endpoint)
    pattern = pattern.replace("...", marker_ellipsis)
    escaped = re.escape(pattern)
    escaped = escaped.replace(marker_segment, r"[^/]+")
    escaped = escaped.replace(marker_ellipsis, r".+")
    return re.fullmatch(rf"{escaped}", actual_route) is not None


class TestUiSpec:
    def test_spec_file_exists(self):
        assert SPEC_FILE.exists(), f"ui_spec.json not found at {SPEC_FILE}"

    def test_spec_file_valid_json(self):
        data = load_spec()
        assert isinstance(data.get("components"), list)
        assert len(data["components"]) > 0

    def test_all_ids_are_unique(self):
        data = load_spec()
        ids = [c["id"] for c in data["components"]]
        assert len(ids) == len(set(ids))

    def test_all_components_have_required_fields(self):
        data = load_spec()
        for comp in data["components"]:
            assert "id" in comp
            assert "name" in comp
            assert "source" in comp
            assert comp["source"] in ("DATA", "CONFIG", "n/a")

    def test_all_spec_templates_exist(self):
        data = load_spec()
        missing = []
        for comp in data["components"]:
            template = comp.get("template")
            if not template:
                continue
            if not (TEMPLATES_DIR / template).exists():
                missing.append(f"{comp['id']}: template '{template}' does not exist")
        assert not missing, "Missing templates:\n" + "\n".join(f"- {m}" for m in missing)

    def test_spec_api_endpoints_exist_in_routes(self):
        data = load_spec()
        all_routes = collect_route_paths()
        api_routes = sorted(r for r in all_routes if r.startswith("/api/"))
        missing = []
        for comp in data["components"]:
            for endpoint in comp.get("api_endpoints", []):
                endpoint_path = normalize_flask_path(endpoint.split("?", 1)[0])
                if not endpoint_path.startswith("/api/"):
                    continue
                if not any(endpoint_pattern_matches(endpoint_path, r) for r in api_routes):
                    missing.append(f"{comp['id']}: API endpoint '{endpoint}' not found in routes")
        assert not missing, "Spec API drift:\n" + "\n".join(f"- {m}" for m in missing)

    def test_spec_route_template_mapping_matches_run_web(self):
        data = load_spec()
        page_routes = collect_page_route_templates()
        mismatches = []
        for comp in data["components"]:
            route = comp.get("route")
            template = comp.get("template")
            if not route or not template:
                continue
            normalized = normalize_flask_path(route)
            actual_templates = page_routes.get(normalized)
            if actual_templates is None:
                mismatches.append(f"{comp['id']}: route '{route}' not found in run_web.py")
                continue
            if template not in actual_templates:
                mismatches.append(
                    f"{comp['id']}: route '{route}' renders {sorted(actual_templates)}, spec says '{template}'"
                )
        assert not mismatches, "Spec route/template drift:\n" + "\n".join(f"- {m}" for m in mismatches)

    def test_all_templated_components_have_dom_anchors(self):
        data = load_spec()
        missing = []
        for comp in data["components"]:
            comp_id = comp["id"]
            template = comp.get("template")
            if not template:
                continue
            anchors = anchors_in_template(template)
            if comp_id not in anchors:
                missing.append(f"{comp_id} missing data-spec-id in {template}")
        assert not missing, "Missing anchors:\n" + "\n".join(f"- {m}" for m in missing)

    def test_required_ids_exist_in_spec(self):
        """Every ID in REQUIRED_COMPONENT_IDS must exist in ui_spec.json."""
        data = load_spec()
        spec_ids = {c["id"] for c in data["components"]}
        missing = REQUIRED_COMPONENT_IDS - spec_ids
        assert not missing, f"REQUIRED IDs not in spec: {missing}"

    def test_dag_spec_ids_are_valid(self):
        """All spec_ids values in dag.json must exist in ui_spec.json."""
        dag_file = ROOT / "docs" / "architecture" / "dag.json"
        if not dag_file.exists():
            import pytest
            pytest.skip("dag.json not found")
        dag = json.loads(dag_file.read_text(encoding="utf-8"))
        spec_ids = {c["id"] for c in load_spec()["components"]}
        invalid = []
        for node in dag.get("nodes", []):
            for sid in node.get("spec_ids", []):
                if sid not in spec_ids:
                    invalid.append(f"dag node '{node['id']}' has spec_id '{sid}' not in ui_spec.json")
        assert not invalid, "\n".join(invalid)

    def test_pipeline_ui_params_wired(self):
        """Every entry in pipeline.PIPELINE_UI_PARAMS must have a matching
        element id in ops.html.  This catches backend params that were added
        but never exposed in the UI (the 'include_crawl dead-end' class of bug).
        """
        import sys
        sys.path.insert(0, str(ROOT / 'src'))
        from routes.pipeline import PIPELINE_UI_PARAMS

        ops_html = (TEMPLATES_DIR / 'ops.html').read_text(encoding='utf-8')
        missing = []
        for param, element_id in PIPELINE_UI_PARAMS.items():
            if f'id="{element_id}"' not in ops_html:
                missing.append(
                    f"pipeline param '{param}' expects element id='{element_id}' in ops.html — not found"
                )
        assert not missing, (
            "Pipeline UI contract broken:\n" + "\n".join(f"- {m}" for m in missing)
        )

    def test_source_badges_present_for_data_and_config(self):
        data = load_spec()
        by_id = {c["id"]: c for c in data["components"]}
        missing = []
        for comp_id in sorted(REQUIRED_COMPONENT_IDS):
            comp = by_id.get(comp_id)
            if not comp:
                continue
            source = comp.get("source")
            if source not in {"DATA", "CONFIG"}:
                continue
            template = comp.get("template")
            if not template:
                continue
            path = TEMPLATES_DIR / template
            if not path.exists():
                missing.append(f"{comp_id} template missing: {template}")
                continue
            text = path.read_text(encoding="utf-8")
            expected_class = f"source-badge-{source.lower()}"
            pattern = rf'{re.escape(comp_id)}(.|\n){{0,350}}{re.escape(expected_class)}'
            if not re.search(pattern, text):
                missing.append(f"{comp_id} missing {expected_class} in {template}")
        assert not missing, "Missing source badges:\n" + "\n".join(f"- {m}" for m in missing)
