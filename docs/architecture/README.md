# Architecture DAG

Machine-readable module dependency graph for `OffChain/miners`.

## Files

| File | Format | Purpose |
|------|--------|---------|
| `dag.json` | JSON | Module import graph — nodes with layer metadata, edges with optional notes |
| `dag.dot` | Graphviz DOT | Visual render source for `dag.json` |
| `operations.json` | JSON | Operation contracts — SSOT read priority, pipeline stage DAG, per-operation code paths (UI → route → DB). Validated by architecture contract tests. |
| `../review/runtime_inventory.json` | JSON | Optional generated inventory for runtime topology review work |

## Conventions

- **Edge direction**: `A -> B` means "module A imports module B"
- **Layer** = proximity to IO. L0 is furthest (pure domain), L5 is closest (HTTP routes)
- **Valid edges** must go from higher layer to lower layer (or same layer for peers)
- **Violations**: any edge where `from.layer < to.layer` is an upward dependency (bad)

## Rendering

```bash
# SVG
dot -Tsvg docs/architecture/dag.dot -o docs/architecture/dag.svg

# PNG
dot -Tpng docs/architecture/dag.dot -o docs/architecture/dag.png
```

Graphviz install: `brew install graphviz`

## Validation

```bash
python3 scripts/check_dag.py
python3 scripts/check_dag.py --strict
pytest -q tests/test_architecture_contracts.py
```

`scripts/check_dag.py` validates graph integrity, cycles, layer violations, stale references, and unreachable nodes.

## Review Workflow

For architecture and cleanup work, start here before opening large source files:

1. Read `dag.json` for module/layer topology.
2. Read `operations.json` for UI-to-route-to-DB behavior contracts.
3. Run `python3 scripts/dag_context.py` for a compact summary suitable for review notes or LLM context.
4. Run `python3 scripts/ui_dag_trace.py` to resolve `ui_spec` IDs into operation IDs, route nodes, and downstream DAG nodes.
5. Run `python3 scripts/dag_ui_reverse_trace.py` to resolve DAG nodes and routes back to UI, CLI, worker, test, and docs consumers.
6. Open source files only for the specific path or subsystem you are tracing.

`dag.json` owns module dependency metadata only. UI component IDs live in `ui_spec.json`; operation/UI mappings live in `operations.json`.

Checked-in review artifacts under `docs/review/` are governed snapshots. Their embedded input hash
must match the current `ui_spec.json`, `operations.json`, `dag.json`, and `cli.py`.

## Known Issues (as of 2026-03-19)

| Module | Issue |
|--------|-------|
| `parsers.press_release_parser` | Imports `parsers.annual_report_parser` — shared helpers should move to `parsers.utils` |
| `routes.review` | Directly imports `interpreters.llm_interpreter` — bypasses `app_globals` boundary |
| `routes.config` | Directly imports `interpreters.llm_interpreter` — bypasses `app_globals` boundary |
| `routes.llm_prompts` | Directly imports `interpreters.llm_interpreter` — bypasses `app_globals` boundary |
| `routes.operations` | Imports `routes.reports` and `routes.companies` — same-layer L5→L5 coupling |
| `routes.pipeline` | Imports `routes.reports` and `routes.companies` — same-layer L5→L5 coupling |
| `routes.pipeline` | Imports `scrapers.llm_crawler` directly — L5→L1 skip-layer (bypasses L2–L4) |
| `config` | `load_companies()` performs file IO — belongs in L1, not L0 |
