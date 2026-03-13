"""Tests for v46 migration: prompt_instructions and quarterly_prompt columns on metric_schema."""
import pytest


def test_v46_migration_adds_prompt_instructions_column(db):
    cols = {row[1] for row in db._get_connection().execute("PRAGMA table_info(metric_schema)").fetchall()}
    assert 'prompt_instructions' in cols


def test_v46_migration_adds_quarterly_prompt_column(db):
    cols = {row[1] for row in db._get_connection().execute("PRAGMA table_info(metric_schema)").fetchall()}
    assert 'quarterly_prompt' in cols


def test_v46_seeds_production_btc_prompt_instructions(db):
    row = db._get_connection().execute(
        "SELECT prompt_instructions FROM metric_schema WHERE key='production_btc'"
    ).fetchone()
    assert row is not None, "production_btc row must exist"
    instr = row[0]
    assert instr and len(instr) > 20
    assert "Return ONLY this JSON" not in instr
    assert "You are a financial data extractor" not in instr


def test_v46_seeds_quarterly_prompt(db):
    row = db._get_connection().execute(
        "SELECT quarterly_prompt FROM metric_schema WHERE key='production_btc'"
    ).fetchone()
    assert row is not None
    assert row[0] and len(row[0]) > 10


def test_update_metric_schema_prompt_instructions(db):
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = next(r['id'] for r in rows if r['key'] == 'production_btc')
    result = db.update_metric_schema(row_id, prompt_instructions="Test instructions")
    assert result is True
    updated = db._get_connection().execute(
        "SELECT prompt_instructions FROM metric_schema WHERE id=?", (row_id,)
    ).fetchone()
    assert updated[0] == "Test instructions"


def test_update_metric_schema_prompt_instructions_null_clears(db):
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    row_id = next(r['id'] for r in rows if r['key'] == 'production_btc')
    db.update_metric_schema(row_id, prompt_instructions="Something")
    db.update_metric_schema(row_id, prompt_instructions="")
    updated = db._get_connection().execute(
        "SELECT prompt_instructions FROM metric_schema WHERE id=?", (row_id,)
    ).fetchone()
    assert updated[0] is None


def test_get_metric_schema_includes_new_fields(db):
    rows = db.get_metric_schema('BTC-miners', active_only=False)
    assert len(rows) > 0
    row = rows[0]
    assert 'prompt_instructions' in row
    assert 'quarterly_prompt' in row
