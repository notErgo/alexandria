import json
import re
from pathlib import Path

from scripts.template_source import load_template_source


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_SPEC_FILE = ROOT / "static" / "data" / "workflow_spec.json"
UI_SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"
ROUTES_DIR = ROOT / "src" / "routes"
RUN_WEB_FILE = ROOT / "run_web.py"


def load_workflow_spec() -> dict:
    return json.loads(WORKFLOW_SPEC_FILE.read_text(encoding="utf-8"))


def load_ui_spec() -> dict:
    return json.loads(UI_SPEC_FILE.read_text(encoding="utf-8"))


def normalize_flask_path(path: str) -> str:
    return re.sub(r"<(?:[^:>]+:)?([^>]+)>", r"<\1>", path)


def collect_route_paths() -> set[str]:
    paths: set[str] = set()
    route_pat = re.compile(r"@\w+\.route\(\s*['\"]([^'\"]+)['\"]")
    for path in list(ROUTES_DIR.glob("*.py")) + [RUN_WEB_FILE]:
        text = path.read_text(encoding="utf-8")
        for raw in route_pat.findall(text):
            paths.add(normalize_flask_path(raw.strip()))
    return paths


def workflow_nav_targets() -> set[tuple[str, str]]:
    html = load_template_source("ops.html")
    return set(re.findall(r"activatePipelineSubTab\('([^']+)','([^']+)'\)", html))


class TestWorkflowSpec:
    def test_workflow_spec_exists(self):
        assert WORKFLOW_SPEC_FILE.exists(), f"workflow spec missing: {WORKFLOW_SPEC_FILE}"

    def test_workflow_spec_ids_exist_in_ui_spec(self):
        workflow = load_workflow_spec()
        ui_ids = {component["id"] for component in load_ui_spec()["components"]}
        missing = []
        for stage in workflow.get("stages", []):
            if stage["spec_id"] not in ui_ids:
                missing.append(f"stage spec_id missing from ui_spec: {stage['spec_id']}")
            for step in stage.get("steps", []):
                if step["spec_id"] not in ui_ids:
                    missing.append(f"step spec_id missing from ui_spec: {step['spec_id']}")
        assert not missing, "\n".join(missing)

    def test_workflow_nav_targets_exist(self):
        workflow = load_workflow_spec()
        routes = collect_route_paths()
        sub_tabs = workflow_nav_targets()
        missing = []
        for stage in workflow.get("stages", []):
            for node in [stage] + list(stage.get("steps", [])):
                nav = node.get("nav") or {}
                route = nav.get("route")
                if route and route not in routes:
                    missing.append(f"{node['spec_id']} references unknown route {route}")
                tab = nav.get("tab")
                sub_tab = nav.get("sub_tab")
                if sub_tab and (tab, sub_tab) not in sub_tabs:
                    missing.append(f"{node['spec_id']} references unknown sub-tab {tab}:{sub_tab}")
        assert not missing, "\n".join(missing)
