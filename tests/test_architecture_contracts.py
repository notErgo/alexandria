"""Architecture contract tests for dag.json and operations.json."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DAG_FILE = ROOT / "docs" / "architecture" / "dag.json"
OPERATIONS_FILE = ROOT / "docs" / "architecture" / "operations.json"
SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class TestArchitectureContracts:
    def test_dag_node_files_exist(self):
        dag = load_json(DAG_FILE)
        missing = []
        for node in dag.get("nodes", []):
            path = node.get("path")
            if path and not (ROOT / path).exists():
                missing.append(f"dag node '{node['id']}' path missing: {path}")
        assert not missing, "\n".join(missing)

    def test_dag_edges_reference_existing_nodes(self):
        dag = load_json(DAG_FILE)
        node_ids = {node["id"] for node in dag.get("nodes", [])}
        broken = []
        for edge in dag.get("edges", []):
            if edge["from"] not in node_ids:
                broken.append(f"unknown edge source: {edge['from']}")
            if edge["to"] not in node_ids:
                broken.append(f"unknown edge target: {edge['to']}")
        assert not broken, "\n".join(broken)

    def test_dag_nodes_do_not_own_ui_spec_ids(self):
        dag = load_json(DAG_FILE)
        offenders = [
            node["id"]
            for node in dag.get("nodes", [])
            if node.get("spec_ids")
        ]
        assert not offenders, "dag.json should not own UI spec IDs:\n" + "\n".join(offenders)

    def test_operation_stage_spec_ids_exist_in_ui_spec(self):
        operations = load_json(OPERATIONS_FILE)
        spec = load_json(SPEC_FILE)
        valid_ids = {component["id"] for component in spec.get("components", [])}
        invalid = []
        for stage in operations.get("pipeline_stages", []):
            for spec_id in stage.get("spec_ids", []):
                if spec_id not in valid_ids:
                    invalid.append(f"pipeline stage '{stage['id']}' references missing spec_id '{spec_id}'")
        assert not invalid, "\n".join(invalid)

    def test_operation_dag_files_resolve_to_dag_nodes(self):
        operations = load_json(OPERATIONS_FILE)
        dag = load_json(DAG_FILE)
        known_paths = {node.get("path") for node in dag.get("nodes", []) if node.get("path")}
        missing = []
        for operation in operations.get("operations", []):
            for step in operation.get("dag", []):
                file_path = step.get("file")
                if file_path and file_path not in known_paths:
                    missing.append(f"operation '{operation['id']}' references non-DAG file '{file_path}'")
        assert not missing, "\n".join(missing)

    def test_display_only_components_protect_declared_api_endpoints(self):
        spec = load_json(SPEC_FILE)
        reverse = load_json(ROOT / "docs" / "review" / "dag_ui_reverse_trace.json")
        protected = []
        for component in spec.get("components", []):
            if not component.get("display_only") or not component.get("api_endpoints"):
                continue
            for node in reverse.get("nodes", []):
                if component["id"] in node.get("display_only_components", []):
                    protected.append((component["id"], node["id"]))
        expected = [
            component["id"]
            for component in spec.get("components", [])
            if component.get("display_only") and component.get("api_endpoints")
        ]
        assert protected or not expected
