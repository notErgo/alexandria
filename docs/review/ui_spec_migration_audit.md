# UI Spec Migration Audit

Generate the current canonical-component migration snapshot with:

```bash
python3 scripts/ui_spec_migration_audit.py --write docs/review/ui_spec_migration_audit.json
```

The output splits canonical components into:

- `operation_traced`
- `display_only`
- `unmapped_canonical`

`unmapped_canonical` is further split into:

- components with declared API endpoints
- components without endpoints

Use that split to decide the next pass:

- endpoint-backed items should usually become operation-traced
- no-endpoint structural items are candidates for `display_only`
- anything surprising stays unmapped until the contract is clarified
