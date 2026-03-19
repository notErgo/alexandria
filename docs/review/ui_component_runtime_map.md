# UI Component Runtime Map

This review note maps current UI surfaces to route files, operation contracts, and interpreter/runtime modules.

Use the generated JSON from:

```bash
python3 scripts/ui_component_map.py
```

That output is the working artifact for later "delete this" decisions.

## Current Recommendation

Do this mapping pass before the deletion pass.

Reason:

- current UI drift is substantial enough that route reachability alone is not a safe delete signal
- several live `/ops` controls still have `data-ui-id` but no `data-spec-id`
- some `ui_spec.json` components still point at `miner_data.html` even though `/miner-data` is no longer the canonical UI
- the projection/forward-fill feature still exists in code, but only on the legacy `miner_data.html` surface

## Canonical Interpreter Paths Exposed In The UI

### Canonical `/ops` paths

- `interpreters.interpret_pipeline`
  - main extraction flow via `/api/operations/interpret`
  - surfaced by component `2.4.1.1`
- `interpreters.llm_interpreter`
  - used by prompt generation, review re-extract, explorer re-extract, and prompt-preview routes
- `interpreters.gap_fill`
  - live via `/api/operations/gap-fill`
  - also auto-run from pipeline flow for non-monthly reporters
- `interpreters.qc_check`
  - used by QC/health flows

### Legacy-or-drift paths

- legacy projection/export controls
  - were previously only present in `miner_data.html`
  - were not present in canonical `ops.html`
  - have now been removed rather than restored

## Highest-Value Drift Findings

### 1. Canonical `/ops` review timeline is now explicitly mapped

- the `2.5.2.*` review-timeline components that are actually present in `/ops` now point to `ops.html`
- visible `/ops` tabs and sub-tabs now have matching `data-spec-id` coverage
- the Data/Documents sub-tab is now tracked as component `2.6.3`
- `python3 scripts/ui_component_map.py` now reports:
  - no `ops.html` UI-only gaps
  - no extra untracked spec anchors

### 2. Legacy-only review controls were removed

- the legacy review-only controls that previously lived under `2.5.2.4.6` through `2.5.2.4.8`
  have been removed from `miner_data.html`
- the corresponding browser-only glue was removed from `static/js/miner_data.js`
- the canonical review surface is now the only spec-mapped review timeline

### 3. The projection/forward-fill feature was legacy and has been removed

- the forward-fill helper and legacy long/final export endpoints were removed
- `miner_data.html` and `static/js/miner_data.js` no longer expose projection/export controls
- `templates/ops.html` still does not expose this feature

### 4. The remaining legacy surface is no longer projection/export

- the projection/export cluster has been deleted
- future cleanup can focus on any remaining legacy-only `miner_data.html` behavior

## Practical Delete Workflow

Before deleting any UI or interpreter code:

1. Run `python3 scripts/ui_component_map.py`
2. Identify the component ID
3. Confirm whether it is:
   - `canonical-ops`
   - `active-standalone`
   - `legacy-template-drift`
   - `canonical-ops-mispointed-template`
4. If a component is only represented on `miner_data.html` and not in canonical `/ops`, treat it as a legacy candidate rather than current product behavior

## Suggested Next Implementation Pass

1. Re-run the generated mapping artifacts after each delete pass.
2. Use delete-gate evidence to target the next legacy-only `miner_data.html` cluster.
