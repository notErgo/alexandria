# Architecture DAG

Machine-readable module dependency graph for `OffChain/miners`.

## Files

| File | Format | Purpose |
|------|--------|---------|
| `dag.json` | JSON | Module import graph — nodes with layer metadata, edges with optional notes |
| `dag.dot` | Graphviz DOT | Visual render source for `dag.json` |
| `operations.json` | JSON | Operation contracts — SSOT read priority, pipeline stage DAG, per-operation code paths (UI → route → DB). Read by `tests/test_ui_spec.py` validators. |

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

TODO: write `scripts/check_dag.py` to validate this DAG programmatically. It should:
1. Load `dag.json`
2. Run topological sort — fail if a cycle exists
3. For each edge, assert `from.layer >= to.layer` — report violations
4. Find unreachable nodes (no path from any L5 route) — report dead ends
5. Find nodes with no outgoing edges other than L0 (leaf nodes) — informational

## Known Issues (as of 2026-03-09)

| Module | Issue |
|--------|-------|
| `parsers.press_release_parser` | Imports `parsers.annual_report_parser` — shared helpers should move to `parsers.utils` |
| `routes.miner` | Directly imports `interpreters.regex_interpreter` — bypasses `app_globals` boundary |
| `routes.review` | Directly imports `interpreters.llm_interpreter` and `regex_interpreter` — bypasses `app_globals` boundary |
| `routes.config` | Directly imports `interpreters.llm_interpreter` — bypasses `app_globals` boundary |
| `routes.llm_prompts` | Directly imports `interpreters.llm_interpreter` — bypasses `app_globals` boundary |
| `routes.operations` | Imports `routes.reports` and `routes.companies` — same-layer L5→L5 coupling |
| `routes.pipeline` | Imports `routes.reports` and `routes.companies` — same-layer L5→L5 coupling |
| `routes.pipeline` | Imports `scrapers.llm_crawler` directly — L5→L1 skip-layer (bypasses L2–L4) |
| `config` | `load_companies()` performs file IO — belongs in L1, not L0 |
