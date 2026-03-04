"""
Spec drift tests for Display Ritual IDs.
"""

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SPEC_FILE = ROOT / "static" / "data" / "ui_spec.json"
TEMPLATES_DIR = ROOT / "templates"

# IDs in scope for the Display Ritual rollout.
REQUIRED_COMPONENT_IDS = {
    "1.0",
    "1.1",
    "2.0",
    "2.1",
    "2.1.1",
    "2.1.2",
    "2.1.3",
    "2.1.4",
    "2.1.5",
    "2.2",
    "2.2.2",
    "2.3",
    "2.3.2",
    "2.3.3",
    "2.4",
    "2.4.1",
    "3.0",
    "3.1",
    "3.2",
    "3.3",
    "5.0",
    "5.1",
    "5.2",
    "6.0",
    "6.1",
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

    def test_required_components_have_dom_anchors(self):
        data = load_spec()
        by_id = {c["id"]: c for c in data["components"]}
        missing = []
        for comp_id in sorted(REQUIRED_COMPONENT_IDS):
            comp = by_id.get(comp_id)
            if not comp:
                missing.append(f"{comp_id} missing from ui_spec.json")
                continue
            template = comp.get("template")
            if not template:
                missing.append(f"{comp_id} has no template in ui_spec.json")
                continue
            anchors = anchors_in_template(template)
            if comp_id not in anchors:
                missing.append(f"{comp_id} missing data-spec-id in {template}")
        assert not missing, "Missing anchors:\n" + "\n".join(f"- {m}" for m in missing)

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
