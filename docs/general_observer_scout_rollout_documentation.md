# Observer/Scout Rollout Plan

**Status:** Complete  
**Last Updated:** 2026-03-04  
**Scope:** UI spec hardening first, then multi-agent cleanup for `/ops` consolidation and review workflow improvements.

## Objective
Execute a fast cleanup with parallel agents while preventing further UI spec drift.  
Phase 0 must complete before scout branches start feature work.

## Roles
| Role | Mission | Branch Pattern | Output |
|---|---|---|---|
| Observer | Own canonical spec alignment and merge gates | `observer/ui-spec-hardening` | Green spec gate suite and reviewed scout merges |
| Scout A | Fix UI spec mismatches and standardization bugs | `scout-a/spec-corrections` | Corrected `UI_SPEC.md`, `ui_spec.json`, route mappings |
| Scout B | Consolidate acquisition controls into `/ops` | `scout-b/ops-acquisition` | Unified Archive, IR, and EDGAR controls in one pane |
| Scout C | Improve evidence visibility and keyword highlighting | `scout-c/evidence-keywords` | Deterministic source highlighting plus dictionary packs |
| Scout D | Add LLM progress window and logs | `scout-d/llm-telemetry` | Live extraction progress panel with structured run logs |

## Mandatory Agent Context Pack
Before any scout writes code, the scout prompt must include these files and directives:
1. Global architecture and anti-pattern canon:
   - `/Users/workstation/Documents/Hermeneutic/CLAUDE.md`
   - `/Users/workstation/Documents/Hermeneutic/CODEX.md`
2. Project context:
   - `/Users/workstation/Documents/Hermeneutic/OffChain/miners/CLAUDE.md`
   - `/Users/workstation/Documents/Hermeneutic/OffChain/miners/UI_SPEC.md`
3. Required instruction:
   - “Comply with global anti-pattern rules and preserve existing route/API behavior unless this task explicitly changes it.”
4. Required verification:
   - Run `./venv/bin/pytest -q tests/test_ui_spec.py` before handoff.

## Phase 0 Gate (Must Pass First)
1. Resolve current spec mismatches between `run_web.py`, `UI_SPEC.md`, and `static/data/ui_spec.json`.
2. Enforce automated checks for:
   - template file existence for every spec component with `template`.
   - API endpoint existence for every spec `api_endpoints` entry.
   - route-to-template mapping consistency for all page routes.
3. Run:
```bash
# Working directory: /Users/workstation/Documents/Hermeneutic/OffChain/miners
./venv/bin/pytest -q tests/test_ui_spec.py
```
Expected output:
```text
.........                                                               [100%]
9 passed in <time>s
```

## Scout Task Briefs

### Scout A: Spec Corrections
1. Remove non-standardized UI panels that are not represented in `UI_SPEC.md`.
2. Ensure all visible major components expose a stable `data-spec-id`.
3. Keep `UI_SPEC.md` and `static/data/ui_spec.json` synchronized in the same PR.
4. Add a short validation section in PR description:
   - files changed,
   - which IDs were added or removed,
   - result of `tests/test_ui_spec.py`.

### Scout B: Ops Acquisition Consolidation
1. Create one `/ops` acquisition section with explicit actions:
   - `Acquire: Archive`
   - `Acquire: IR`
   - `Acquire: EDGAR`
2. Standardize status language:
   - `queued`, `running`, `complete`, `failed`.
3. Keep existing endpoints; do not break CLI flow.
4. Ensure actions update queue and registry views without page reload.

### Scout C: Evidence and Dictionary Packs
1. Unify source panel highlighting across Explorer and Review.
2. Add global keyword dictionary storage with selectable packs:
   - `btc_activity`
   - `miners_deployed`
   - `ai_hpc_compute`
3. Ensure highlighted terms are visible in:
   - document preview panel,
   - parsed source view.
4. Add tests for highlight behavior on at least one Review path and one Explorer path.

### Scout D: LLM Telemetry
1. Add extraction run panel in `/ops` that shows:
   - run status,
   - reports processed versus total,
   - current ticker or report,
   - recent log events.
2. Avoid manual refresh workflow by polling or server-sent updates.
3. Keep logs structured and filterable by run id.
4. Include failure states with actionable operator messages.

## Merge Policy
1. Observer merges first.
2. Scouts rebase on Observer branch before opening PR.
3. Required checks before merge:
   - `./venv/bin/pytest -q tests/test_ui_spec.py`
   - any tests added by scout pass in branch.
4. If two scouts touch the same template, Observer performs integration merge and resolves conflicts.
5. Observer rejects any scout PR that does not cite the Context Pack files in the PR description.

## Parallel Deployment Checklist
Use this section as the direct operator runbook for launching scouts in parallel.

### 1) Branch Setup
```bash
# Working directory: /Users/workstation/Documents/Hermeneutic/OffChain/miners
git checkout -b observer/ui-spec-hardening
git checkout -b scout-b/ops-acquisition
git checkout -b scout-c/evidence-keywords
git checkout -b scout-d/llm-telemetry
```

### 2) File Ownership Matrix (Conflict Control)
| Agent | Primary files | Must avoid touching |
|---|---|---|
| Scout B | `templates/ops.html`, `static/js/operations.js`, `/api/ingest*` trigger UI integration, ops docs | `static/js/review_panel.js`, `static/js/doc_panel.js`, LLM telemetry backend |
| Scout C | `static/js/doc_panel.js`, `static/js/review_panel.js`, source-view highlight logic, keyword dictionary files | major `/ops` layout edits, worker/progress backend |
| Scout D | progress/log endpoints and telemetry UI, extraction run status components | review/explorer highlight internals, broad `/ops` refactor unrelated to telemetry |

### 3) Shared Guardrails for All Scouts
1. Load the Mandatory Agent Context Pack before coding.
2. Keep endpoint compatibility unless task explicitly requires endpoint change.
3. Run before handoff:
```bash
./venv/bin/pytest -q tests/test_ui_spec.py
```
4. If `UI_SPEC.md` or `static/data/ui_spec.json` changes, update both in same branch.

### 4) Integration Order (Observer)
Recommended merge order to minimize conflicts:
1. Scout B (layout/control-plane baseline in `/ops`)
2. Scout C (evidence/highlight behavior)
3. Scout D (telemetry panel and status plumbing)

If Scout D depends on layout hooks from Scout B, Observer rebases Scout D on merged B before final test.

### 5) Observer Merge Command Pattern
```bash
# Observer branch
git checkout observer/ui-spec-hardening

# For each scout branch in order:
git merge --no-ff scout-b/ops-acquisition
./venv/bin/pytest -q tests/test_ui_spec.py

git merge --no-ff scout-c/evidence-keywords
./venv/bin/pytest -q tests/test_ui_spec.py

git merge --no-ff scout-d/llm-telemetry
./venv/bin/pytest -q tests/test_ui_spec.py
```

### 6) PR Template Snippet (Required)
Each scout PR description must include:
1. `Context Pack Loaded:` list of required files.
2. `Files Touched:` explicit list.
3. `Behavior Changes:` bullet list.
4. `Validation:` output summary of `tests/test_ui_spec.py` and any new tests.
5. `Spec Impact:` whether IDs/routes/endpoints changed, and corresponding `UI_SPEC.md` + `ui_spec.json` updates.

## Change Control
1. Any component ID change requires updates in both:
   - `UI_SPEC.md`
   - `static/data/ui_spec.json`
2. Any route or endpoint rename requires:
   - route implementation change,
   - spec update,
   - test confirmation in `tests/test_ui_spec.py`.
3. Purge and destructive controls remain hidden by default and require explicit user expansion.

## Current Baseline Notes
Phase 0 in this worktree has aligned:
1. Review doc panel endpoint to `/api/review/<id>/document`.
2. Data Explorer template mapping to `index.html`.
3. UI spec tests extended beyond DOM anchors to include route and API consistency checks.
