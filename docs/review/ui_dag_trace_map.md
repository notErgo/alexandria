# UI To DAG Trace Map

Use the generated trace artifact to bridge UI components to route nodes and downstream modules
without reintroducing UI ownership into `dag.json`.

Command:

```bash
python3 scripts/ui_dag_trace.py
```

Optional file output:

```bash
python3 scripts/ui_dag_trace.py --write docs/review/ui_dag_trace_map.json
```

## What it resolves

- UI component ID from `static/data/ui_spec.json`
- linked operation IDs from `docs/architecture/operations.json`
- route nodes resolved from operation `dag` steps
- direct DAG node set touched by the operation
- reachable downstream DAG nodes from the route entrypoint

## Current intended use

- start with a visible UI component
- look up its trace record
- confirm whether it has an operation contract
- confirm which route node owns it
- inspect the downstream module set before deleting or rewriting

## Important current result

The canonical `/ops` review timeline now resolves cleanly through the trace layer, and the
previous `miner_data.html`-only review controls have been removed. Remaining legacy cleanup work is
now concentrated in the projection/export surface rather than the review-control surface.
