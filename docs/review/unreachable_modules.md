# Unreachable Module Disposition Matrix

Generated from the current `python3 scripts/check_dag.py` unreachable set.

## Summary

The current unreachable list is not a dead-code list.

- `interpreters.gap_fill`, `scrapers.crawl_context`, and `scrapers.observer_swarm` are active runtime code paths that the DAG misses because it does not fully model function-local imports or non-route internal entrypoints.
- `analysis.coverage`, `infra.logging_config`, and `interpreters.broad_interpreter` are active through CLI or script entrypoints that are outside the DAG's route-only reachability model.
- `scrapers.primitive_feedback`, `scrapers.primitive_registry`, and `scrapers.source_contract` are active observer-swarm support modules and are not deletion candidates.
- The only strong delete candidates from this pass are `parsers.document_parser` and `scrapers.edgar_context_builder`.
- `interpreters.table_interpreter` and `interpreters.unit_normalizer` are retained for now as test-backed extraction utilities, but they should be revisited once the extraction strategy is simplified.

## Disposition Matrix

| Module | Runtime refs | Script refs | Test refs | Disposition | Rationale |
|---|---:|---:|---:|---|---|
| `analysis.coverage` | CLI diagnose path | `generate_report.py` | yes | `keep` | Used by `cli.py diagnose`; unreachable only because CLI is outside the DAG. |
| `infra.logging_config` | `run_web.py`, `cli.py` | backfill/report scripts | no direct test | `keep` | Entry-point logging support, not dead code. DAG does not model top-level app/scripts. |
| `interpreters.broad_interpreter` | `cli.py` broad extract path | none | none | `keep` | Reachable from CLI; not a route path, but still a supported operator workflow. |
| `interpreters.gap_fill` | `routes.pipeline`, `routes.operations` | none | yes | `reintegrate in DAG` | Live runtime module missed by DAG due to function-local imports. |
| `interpreters.table_interpreter` | none found | none | yes | `keep, script/test scoped` | Test-backed extraction utility; no current route/CLI path, but still coherent and not yet safe to delete. |
| `interpreters.unit_normalizer` | via `table_interpreter` | none | yes | `keep, script/test scoped` | Dependency of `table_interpreter`; evaluate with it as one unit. |
| `parsers.document_parser` | none found | none | none | `delete candidate` | Pure dispatcher with no current runtime, script, or test use. |
| `scrapers.crawl_context` | `scrapers.llm_crawler` | none | yes | `reintegrate in DAG` | Active crawl path missed because reachability is route-only and `llm_crawler` sits below routes. |
| `scrapers.edgar_context_builder` | none found | none | yes | `delete candidate` | Test-only builder with no current runtime or script integration. |
| `scrapers.observer_swarm` | `routes.operations` observer endpoints | `run_observer_swarm.py`, `run_scout_worker.py` | yes | `reintegrate in DAG` | Active runtime subsystem; current unreachable status is a graph modeling gap. |
| `scrapers.primitive_feedback` | via `observer_swarm` | `run_primitive_feedback_loop.py` | yes | `keep` | Active observer-swarm support module. |
| `scrapers.primitive_registry` | via `observer_swarm` and `primitive_feedback` | none | yes | `keep` | Active registry module for observer-swarm primitive management. |
| `scrapers.source_contract` | via `observer_swarm` | prompt/feedback scripts consume artifacts | yes | `keep` | Active contract/merge layer for observer-swarm outputs. |

## Evidence Notes

### Reintegrate in DAG

- `interpreters.gap_fill`
  - Runtime refs in `src/routes/pipeline.py` and `src/routes/operations.py`
  - Current false positive caused by function-local imports
- `scrapers.crawl_context`
  - Runtime ref in `src/scrapers/llm_crawler.py`
  - Current false positive caused by route-only reachability assumption
- `scrapers.observer_swarm`
  - Runtime ref in `src/routes/operations.py`
  - Also used by dedicated operator scripts

### Keep

- `analysis.coverage`
  - Imported by `cli.py`
  - Used by `scripts/generate_report.py`
- `infra.logging_config`
  - Imported by `run_web.py`, `cli.py`, and multiple maintenance scripts
- `interpreters.broad_interpreter`
  - Imported by `cli.py` for broad extraction
- `scrapers.primitive_feedback`, `scrapers.primitive_registry`, `scrapers.source_contract`
  - Observer-swarm subsystem support; not route-leaf code, but still active

### Keep, Script/Test Scoped

- `interpreters.table_interpreter`
  - Used in tests only
  - Looks like a preserved extraction path rather than accidental dead code
- `interpreters.unit_normalizer`
  - Used by `table_interpreter`
  - Should be reviewed together with `table_interpreter`

### Delete Candidates

- `parsers.document_parser`
  - No runtime refs found
  - No script refs found
  - No test refs found
  - Only appears in docs/contracts
- `scrapers.edgar_context_builder`
  - Test-backed only
  - No runtime or script integration found
  - Safe next candidate for explicit removal review

## Immediate Follow-Ups

1. Update `dag.json` or its maintenance rules so function-local imports and lower-layer internal runtime paths stop producing false positives.
2. Create a deletion PR shortlist containing only:
   - `parsers.document_parser`
   - `scrapers.edgar_context_builder`
3. Defer any decision on `table_interpreter` and `unit_normalizer` until the extraction strategy review decides whether table-first extraction remains in scope.
