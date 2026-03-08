"""Tests for MinerDB metric_schema CRUD methods."""
import sqlite3
import pytest


class TestMetricSchemaDBI:

    def test_get_metric_schema_returns_seeded_rows(self, db):
        rows = db.get_metric_schema('BTC-miners')
        assert len(rows) == 13

    def test_add_analyst_metric_creates_row(self, db):
        db.add_analyst_metric('energy_cost_btc', 'Energy Cost per BTC', 'USD', 'BTC-miners')
        rows = db.get_metric_schema('BTC-miners')
        assert len(rows) == 14
        new = next(r for r in rows if r['key'] == 'energy_cost_btc')
        assert new['analyst_defined'] == 1
        assert new['has_extraction_pattern'] == 0

    def test_add_duplicate_metric_raises_error(self, db):
        with pytest.raises(sqlite3.IntegrityError):
            db.add_analyst_metric('production_btc', 'Dupe', 'BTC', 'BTC-miners')


class TestMetricRulesDB:

    def test_get_metric_rules_returns_seeded_rows(self, db):
        rows = db.get_metric_rules()
        assert len(rows) == 13
        metrics = {r['metric'] for r in rows}
        assert 'production_btc' in metrics
        assert 'hashrate_eh' in metrics

    def test_upsert_creates_new_rule(self, db):
        db.upsert_metric_rule('custom_metric', 0.05, 0.50, 3)
        rows = db.get_metric_rules(metric='custom_metric')
        assert len(rows) == 1
        assert rows[0]['agreement_threshold'] == pytest.approx(0.05)

    def test_upsert_updates_existing_rule(self, db):
        db.upsert_metric_rule('production_btc', 0.03, 0.40, 3)
        rows = db.get_metric_rules(metric='production_btc')
        assert rows[0]['agreement_threshold'] == pytest.approx(0.03)

    def test_delete_metric_rule_removes_row(self, db):
        db.delete_metric_rule('production_btc')
        rows = db.get_metric_rules(metric='production_btc')
        assert rows == []

    def test_delete_nonexistent_rule_is_noop(self, db):
        db.delete_metric_rule('nonexistent_metric_xyz')  # should not raise
