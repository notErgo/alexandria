# Display Ritual — Option A Implementation Plan

**Project:** Bitcoin Miner Data Platform (localhost:5004)
**Goal:** Persistent component ID badges in all panel headers so users can reference spec IDs
(e.g. `[2.1.3]`) without opening the browser console.
**Reference:** `UI_SPEC.md` (component IDs), `workflow/documentation/ui_spec_standard.md` (convention)
**Agent:** Codex (worktree: `miners-codex`, branch: `codex/parallel-work`)

---

## Background

`UI_SPEC.md` assigns stable IDs to every page, tab, panel, table, and background script:

| Level | Format | Example |
|-------|--------|---------|
| Page | `X.0` | `2.0` = Ops page |
| Tab/Section | `X.Y` | `2.1` = Companies tab |
| Panel/Table/Form | `X.Y.Z` | `2.1.3` = Scrape Queue table |
| Background script | `S.N` | `S.1` = ScrapeWorker |

Each component also has a **source** classification: `DATA`, `CONFIG`, or `n/a`.

**Option A** adds these two pieces of information as visible inline badges on every
panel/table/section header in the templates. The result: users can say "2.1.3" in a
bug report and both human and agent know exactly which UI element is being discussed.

---

## Deliverables

| # | File | Action |
|---|------|--------|
| 1 | `static/css/style.css` | Add `.spec-id`, `.source-badge-*` CSS rules |
| 2 | `static/data/ui_spec.json` | Machine-readable component map (new file) |
| 3 | `templates/ops.html` | Add badges to all tab headers and panel headers |
| 4 | `templates/base.html` | No change needed (badges use existing CSS vars) |
| 5 | `templates/landing.html` | Add `[1.0]` / `[1.1]` page and section badges |
| 6 | `templates/review.html` | Add `[3.0]`–`[3.7]` badges |
| 7 | `templates/miner_data.html` | Add `[5.0]`–`[5.2]` badges |
| 8 | `templates/dashboard.html` | Add `[6.0]`–`[6.1]` badges |
| 9 | `tests/test_ui_spec.py` | Spec drift test: every ID in JSON has a DOM anchor |

---

## Phase 1 — CSS (style.css)

Add after the existing `.btn-danger` block:

```css
/* ── Display Ritual: component ID badges (Option A) ─────────────────────── */
.spec-id {
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 0.68rem;
    color: var(--theme-text-muted);
    background: var(--theme-bg-tertiary);
    border: 1px solid var(--theme-border);
    border-radius: 4px;
    padding: 1px 5px;
    margin-right: 0.45rem;
    vertical-align: middle;
    user-select: none;
}

.source-badge {
    font-size: 0.62rem;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    border-radius: 4px;
    padding: 1px 5px;
    vertical-align: middle;
    margin-left: 0.3rem;
    user-select: none;
}
.source-badge-data   { background: #3b82f620; color: #3b82f6; }
.source-badge-config { background: #f9731620; color: #f97316; }
.source-badge-na     { background: #6b728020; color: #6b7280; }
```

**Constraint:** no inline styles — all badge styles must go in style.css.

---

## Phase 2 — ui_spec.json (new file)

Create `static/data/ui_spec.json`. This file is the machine-readable index of all
component IDs. It enables:
- LLM agents to look up IDs without scraping HTML
- The drift test (Phase 5) to verify DOM anchors exist for every registered ID

```json
{
  "version": "1.1",
  "generated_from": "UI_SPEC.md",
  "components": [
    {"id": "1.0", "name": "Landing Page", "source": "n/a", "route": "/"},
    {"id": "1.1", "name": "Scorecard table", "source": "DATA", "template": "landing.html"},

    {"id": "2.0", "name": "Ops Page", "source": "n/a", "route": "/ops"},
    {"id": "2.1", "name": "Companies tab", "source": "n/a", "template": "ops.html"},
    {"id": "2.1.1", "name": "Companies table", "source": "CONFIG", "template": "ops.html"},
    {"id": "2.1.2", "name": "Regime editor panel", "source": "CONFIG", "template": "ops.html"},
    {"id": "2.1.3", "name": "Scrape Queue table", "source": "DATA", "template": "ops.html"},
    {"id": "2.1.4", "name": "Danger Zone purge form", "source": "n/a", "template": "ops.html"},
    {"id": "2.1.5", "name": "Add Company form", "source": "n/a", "template": "ops.html"},
    {"id": "2.1.6", "name": "Sync Config button", "source": "n/a", "template": "ops.html"},
    {"id": "2.1.7", "name": "Scrape trigger button", "source": "n/a", "template": "ops.html"},

    {"id": "2.2", "name": "Registry tab", "source": "n/a", "template": "ops.html"},
    {"id": "2.2.1", "name": "Filter bar", "source": "n/a", "template": "ops.html"},
    {"id": "2.2.2", "name": "Registry table", "source": "DATA", "template": "ops.html"},

    {"id": "2.3", "name": "Explorer tab", "source": "n/a", "template": "ops.html"},
    {"id": "2.3.1", "name": "Filter bar", "source": "n/a", "template": "ops.html"},
    {"id": "2.3.2", "name": "Coverage heatmap", "source": "DATA", "template": "ops.html"},
    {"id": "2.3.3", "name": "Cell detail panel", "source": "DATA", "template": "ops.html"},
    {"id": "2.3.4", "name": "Cell save action", "source": "n/a", "template": "ops.html"},
    {"id": "2.3.5", "name": "Cell gap action", "source": "n/a", "template": "ops.html"},
    {"id": "2.3.6", "name": "Re-extract action", "source": "n/a", "template": "ops.html"},

    {"id": "2.4", "name": "Metric Rules tab", "source": "n/a", "template": "ops.html"},
    {"id": "2.4.1", "name": "Rules table", "source": "CONFIG", "template": "ops.html"},

    {"id": "3.0", "name": "Review Queue Page", "source": "n/a", "route": "/review"},
    {"id": "3.1", "name": "Filter bar", "source": "n/a", "template": "review.html"},
    {"id": "3.2", "name": "Review table", "source": "DATA", "template": "review.html"},
    {"id": "3.3", "name": "Doc panel", "source": "DATA", "template": "review.html"},
    {"id": "3.4", "name": "Approve action", "source": "n/a", "template": "review.html"},
    {"id": "3.5", "name": "Reject action", "source": "n/a", "template": "review.html"},
    {"id": "3.6", "name": "Re-extract action", "source": "n/a", "template": "review.html"},
    {"id": "3.7", "name": "Bulk approve", "source": "n/a", "template": "review.html"},

    {"id": "5.0", "name": "Miner Data Page", "source": "n/a", "route": "/miner-data"},
    {"id": "5.1", "name": "Reports table", "source": "DATA", "template": "miner_data.html"},
    {"id": "5.2", "name": "Doc panel", "source": "DATA", "template": "miner_data.html"},

    {"id": "6.0", "name": "Dashboard Page", "source": "n/a", "route": "/dashboard"},
    {"id": "6.1", "name": "Metric panels", "source": "DATA", "template": "dashboard.html"},

    {"id": "S.1", "name": "ScrapeWorker", "source": "n/a", "type": "background"},
    {"id": "S.2", "name": "IRScraper", "source": "n/a", "type": "background"},
    {"id": "S.3", "name": "ArchiveIngestor", "source": "n/a", "type": "background"},
    {"id": "S.4", "name": "EdgarConnector", "source": "n/a", "type": "background"},
    {"id": "S.5", "name": "ManifestScanner", "source": "n/a", "type": "background"},
    {"id": "S.6", "name": "Extraction pipeline", "source": "n/a", "type": "background"}
  ]
}
```

---

## Phase 3 — Template Badge Markup

### Badge HTML pattern

```html
<!-- Tab/section header badge (X.Y level) -->
<h3 class="section-title">
  <span class="spec-id">2.1</span>Companies
</h3>

<!-- Panel/table header badge (X.Y.Z level) with source badge -->
<h4 class="card-title">
  <span class="spec-id">2.1.3</span>Scrape Queue
  <span class="source-badge source-badge-data">DATA</span>
</h4>
```

### Placement rules

1. `spec-id` badge goes at the START of the heading text, before the label.
2. `source-badge` goes at the END, after the label, only for DATA and CONFIG panels
   (skip for `n/a` — stateless UI elements like buttons and filter bars).
3. Badges are `<span>` inside the existing heading element — do NOT add new heading elements.
4. Do not add badges to `<button>` elements or nav tabs (`.ops-tab`) — only to section
   and panel headings (h3, h4, card titles, table section labels).

### ops.html — all required badge placements

| Location | Current heading text | Badge to add |
|----------|----------------------|--------------|
| Companies tab section | "Companies" tab label area / pane heading | `[2.1]` on the pane `<h3>` if present |
| Companies table card | table heading | `[2.1.1]` + CONFIG badge |
| Regime editor | regime editor heading | `[2.1.2]` + CONFIG badge |
| Scrape Queue | scrape queue heading | `[2.1.3]` + DATA badge |
| Danger Zone | danger zone heading | `[2.1.4]` (no source badge — n/a) |
| Add Company | add company form heading | `[2.1.5]` (no source badge — n/a) |
| Registry tab | Registry pane heading | `[2.2]` |
| Registry table | registry table heading | `[2.2.2]` + DATA badge |
| Explorer tab | Explorer pane heading | `[2.3]` |
| Coverage heatmap | heatmap heading | `[2.3.2]` + DATA badge |
| Cell detail | cell detail panel heading | `[2.3.3]` + DATA badge |
| Metric Rules tab | rules pane heading | `[2.4]` |
| Rules table | rules table heading | `[2.4.1]` + CONFIG badge |

### review.html — required badge placements

| Location | Badge |
|----------|-------|
| Page heading | `[3.0]` |
| Filter bar section | `[3.1]` (n/a — no source badge) |
| Review table | `[3.2]` + DATA badge |
| Doc panel | `[3.3]` + DATA badge |

### landing.html — required badge placements

| Location | Badge |
|----------|-------|
| Page heading | `[1.0]` |
| Scorecard table | `[1.1]` + DATA badge |

### miner_data.html — required badge placements

| Location | Badge |
|----------|-------|
| Page heading | `[5.0]` |
| Reports table | `[5.1]` + DATA badge |
| Doc panel | `[5.2]` + DATA badge |

### dashboard.html — required badge placements

| Location | Badge |
|----------|-------|
| Page heading | `[6.0]` |
| Metric panels | `[6.1]` + DATA badge |

---

## Phase 4 — DOM Anchor Convention

Every component with a badge MUST also have a stable `data-spec-id` attribute on the
enclosing panel/section container. This is what the drift test (Phase 5) checks.

```html
<!-- Panel container pattern -->
<div class="card" data-spec-id="2.1.3">
  <h4 class="card-title">
    <span class="spec-id">2.1.3</span>Scrape Queue
    <span class="source-badge source-badge-data">DATA</span>
  </h4>
  ...
</div>
```

Rules:
- `data-spec-id` goes on the outermost container of the panel (card div, section div).
- For tab panes: `data-spec-id="2.1"` on the `<div class="ops-pane" id="pane-companies">`.
- For page-level IDs (X.0): `data-spec-id="2.0"` on the `<main>` or outermost content container.
- Background scripts (S.N): no DOM anchor needed — they are not rendered.

---

## Phase 5 — Spec Drift Test (test_ui_spec.py)

**Write this test BEFORE implementing the HTML changes.** The test must fail first,
then pass once badges and anchors are in place.

```python
"""
tests/test_ui_spec.py

Spec drift test: every component ID in ui_spec.json that has a template
must have a corresponding data-spec-id anchor in that template.

Run: pytest tests/test_ui_spec.py -v
"""
import json
import re
from pathlib import Path

SPEC_FILE = Path(__file__).parent.parent / "static" / "data" / "ui_spec.json"
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"


def load_spec():
    with open(SPEC_FILE) as f:
        return json.load(f)


def anchors_in_template(template_name: str) -> set:
    """Return all data-spec-id values found in a template file."""
    path = TEMPLATES_DIR / template_name
    if not path.exists():
        return set()
    text = path.read_text()
    return set(re.findall(r'data-spec-id="([^"]+)"', text))


class TestSpecDrift:

    def test_spec_file_exists(self):
        assert SPEC_FILE.exists(), f"ui_spec.json not found at {SPEC_FILE}"

    def test_spec_file_valid_json(self):
        data = load_spec()
        assert "components" in data
        assert isinstance(data["components"], list)
        assert len(data["components"]) > 0

    def test_all_ids_are_unique(self):
        data = load_spec()
        ids = [c["id"] for c in data["components"]]
        assert len(ids) == len(set(ids)), f"Duplicate IDs: {[x for x in ids if ids.count(x) > 1]}"

    def test_all_components_have_required_fields(self):
        data = load_spec()
        for comp in data["components"]:
            assert "id" in comp, f"Missing 'id' in {comp}"
            assert "name" in comp, f"Missing 'name' in {comp}"
            assert "source" in comp, f"Missing 'source' in {comp}"
            assert comp["source"] in ("DATA", "CONFIG", "n/a"), \
                f"Invalid source '{comp['source']}' in {comp['id']}"

    def test_template_components_have_dom_anchors(self):
        """
        Every component with a 'template' field must have a data-spec-id
        attribute in that template. Background scripts (type=background)
        and page-level IDs without templates are excluded.
        """
        data = load_spec()
        missing = []

        # Cache template anchors per file (avoid re-reading same file)
        anchor_cache: dict[str, set] = {}

        for comp in data["components"]:
            template = comp.get("template")
            if not template:
                # No template = background script or page-level without template anchor
                continue

            if template not in anchor_cache:
                anchor_cache[template] = anchors_in_template(template)

            if comp["id"] not in anchor_cache[template]:
                missing.append(f"{comp['id']} ({comp['name']}) in {template}")

        assert not missing, (
            f"{len(missing)} component(s) missing data-spec-id anchor in template:\n"
            + "\n".join(f"  - {m}" for m in missing)
        )

    def test_source_badge_present_for_data_and_config(self):
        """
        Every DATA or CONFIG component with a template must have a source-badge
        span in that template.
        """
        data = load_spec()
        missing = []

        badge_cache: dict[str, str] = {}

        for comp in data["components"]:
            template = comp.get("template")
            if not template or comp["source"] == "n/a":
                continue

            if template not in badge_cache:
                path = TEMPLATES_DIR / template
                badge_cache[template] = path.read_text() if path.exists() else ""

            text = badge_cache[template]
            expected_class = f"source-badge-{comp['source'].lower()}"
            # Check that the badge class appears near the spec-id
            spec_id_pattern = re.escape(comp["id"])
            # Look for the spec-id badge followed by the source badge within 500 chars
            pattern = rf'spec-id">{re.escape(comp["id"])}</span>.{{0,300}}{re.escape(expected_class)}'
            if not re.search(pattern, text, re.DOTALL):
                missing.append(
                    f"{comp['id']} ({comp['name']}) in {template} — "
                    f"missing .{expected_class}"
                )

        assert not missing, (
            f"{len(missing)} component(s) missing source-badge in template:\n"
            + "\n".join(f"  - {m}" for m in missing)
        )
```

**Test-first order:**
1. Create `static/data/ui_spec.json` (Phase 2) — lets `test_spec_file_exists` pass.
2. Run `pytest tests/test_ui_spec.py` — `test_template_components_have_dom_anchors` fails (expected).
3. Implement template badges and `data-spec-id` anchors (Phase 3 + 4).
4. Run again — all tests pass.

---

## Phase 6 — Acceptance Criteria (T2 Smoke Tests)

After implementation, manually verify each page:

### ops.html

- [ ] `[2.1]` badge visible in Companies tab pane heading
- [ ] `[2.1.1]` + CONFIG badge visible in Companies table card header
- [ ] `[2.1.2]` + CONFIG badge visible in Regime editor header
- [ ] `[2.1.3]` + DATA badge visible in Scrape Queue header
- [ ] `[2.1.4]` badge visible in Danger Zone header (no source badge)
- [ ] `[2.2]` badge visible in Registry tab pane heading
- [ ] `[2.2.2]` + DATA badge visible in Registry table header
- [ ] `[2.3]` badge visible in Explorer tab pane heading
- [ ] `[2.3.2]` + DATA badge visible in heatmap section header
- [ ] `[2.3.3]` + DATA badge visible in Cell Detail panel header
- [ ] `[2.4]` badge visible in Metric Rules tab pane heading
- [ ] `[2.4.1]` + CONFIG badge visible in Rules table header

### review.html

- [ ] `[3.0]` badge visible in page heading
- [ ] `[3.2]` + DATA badge visible in Review table header

### landing.html

- [ ] `[1.0]` badge visible in page heading
- [ ] `[1.1]` + DATA badge visible in Scorecard table header

### miner_data.html

- [ ] `[5.0]` badge visible in page heading
- [ ] `[5.1]` + DATA badge visible in Reports table header

### Cross-cutting

- [ ] Badges are visible in both dark and light theme
- [ ] Badges do not break layout (verify on narrow viewport ~1024px)
- [ ] Badge text is unselectable (user-select: none prevents accidental copy)
- [ ] No badge text wraps onto a second line

---

## Phase 7 — Unit Test Requirements (T3)

### test_ui_spec.py (Phase 5 above)

5 tests:
1. `test_spec_file_exists` — ui_spec.json present
2. `test_spec_file_valid_json` — valid JSON with components array
3. `test_all_ids_are_unique` — no duplicate IDs
4. `test_all_components_have_required_fields` — id/name/source present, source in allowed set
5. `test_template_components_have_dom_anchors` — all templated components have `data-spec-id`
6. `test_source_badge_present_for_data_and_config` — DATA/CONFIG components have source badge

**Target: 6 passing tests added to test suite.**

---

## Constraints and Rules

1. **No emojis** — not in badge text, not in comments, not anywhere.
2. **No inline styles** — all `.spec-id` and `.source-badge-*` rules go in `style.css`.
3. **No new heading elements** — badges are `<span>` inside existing headings.
4. **No changes to routing or Python code** — this is pure HTML/CSS/JSON.
5. **Tests written before HTML changes** — the drift test must fail first.
6. **No modification to `UI_SPEC.md`** — it is the source of truth; `ui_spec.json` is derived from it.
7. **Stable IDs** — do not renumber. If a new panel is added, append at the end of its parent section.

---

## Checklist Gate (Definition of Done)

- [ ] `pytest tests/test_ui_spec.py` — 6/6 pass
- [ ] `pytest tests/` — full suite still passes (no regressions)
- [ ] T2 smoke checklist above fully checked
- [ ] Both dark and light themes verified
- [ ] Committed to `codex/parallel-work` with message format:
  ```
  Add Option A display ritual: component ID badges in panel headers

  - .spec-id and .source-badge-* CSS in style.css
  - static/data/ui_spec.json machine-readable component map
  - data-spec-id anchors on all panel containers
  - tests/test_ui_spec.py: 6-test drift suite
  - Badges on ops.html, review.html, landing.html, miner_data.html, dashboard.html
  ```
