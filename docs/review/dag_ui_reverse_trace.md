# DAG To UI Reverse Trace

Generate the governed reverse trace with:

```bash
python3 scripts/dag_ui_reverse_trace.py --write docs/review/dag_ui_reverse_trace.json
```

The JSON snapshot embeds an input hash over:

- `static/data/ui_spec.json`
- `docs/architecture/operations.json`
- `docs/architecture/dag.json`
- `cli.py`

Use it to answer:

- what canonical UI depends on this route or module
- whether a node is legacy-only, CLI-only, display-only, or route-protected
- whether a candidate can be deleted safely

For per-target decisions use:

```bash
python3 scripts/delete_gate.py --ui-id 2.4.1.1
python3 scripts/delete_gate.py --route /api/operations/interpret
python3 scripts/delete_gate.py --node scrapers.llm_crawler
```
