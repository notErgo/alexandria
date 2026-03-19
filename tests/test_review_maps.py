from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.review_maps_common import INPUT_FILES, compute_input_hash


ROOT = Path(__file__).resolve().parent.parent
FORWARD_ARTIFACT = ROOT / "docs" / "review" / "ui_dag_trace_map.json"
REVERSE_ARTIFACT = ROOT / "docs" / "review" / "dag_ui_reverse_trace.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class TestReviewMaps:
    def test_forward_trace_artifact_hash_is_current(self):
        data = load_json(FORWARD_ARTIFACT)
        assert data["metadata"]["input_hash"] == compute_input_hash(INPUT_FILES)

    def test_reverse_trace_artifact_hash_is_current(self):
        data = load_json(REVERSE_ARTIFACT)
        assert data["metadata"]["input_hash"] == compute_input_hash(INPUT_FILES)

    def test_reverse_trace_covers_all_dag_nodes(self):
        reverse = load_json(REVERSE_ARTIFACT)
        dag = load_json(ROOT / "docs" / "architecture" / "dag.json")
        assert {node["id"] for node in dag["nodes"]} == {node["id"] for node in reverse["nodes"]}
        assert all(node["status"] for node in reverse["nodes"])

    def test_forward_trace_reports_unmapped_canonical_components_consistently(self):
        forward = load_json(FORWARD_ARTIFACT)
        actual = sum(1 for component in forward["components"] if component["coverage_status"] == "unmapped_canonical")
        assert actual == 0

    def test_delete_gate_protects_canonical_component(self):
        payload = json.loads(
            subprocess.check_output(
                [sys.executable, "scripts/delete_gate.py", "--ui-id", "2.4.1.1"],
                cwd=ROOT,
                text=True,
            )
        )
        assert payload["status"] == "protected-canonical"

    def test_delete_gate_protects_route_dependency(self):
        payload = json.loads(
            subprocess.check_output(
                [sys.executable, "scripts/delete_gate.py", "--node", "scrapers.llm_crawler"],
                cwd=ROOT,
                text=True,
            )
        )
        assert payload["status"] == "protected-route-dependency"

    def test_delete_gate_rejects_invalid_target(self):
        proc = subprocess.run(
            [sys.executable, "scripts/delete_gate.py", "--ui-id", "does.not.exist"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert proc.returncode != 0
        payload = json.loads(proc.stdout)
        assert "unknown ui_spec component" in payload["error"]
