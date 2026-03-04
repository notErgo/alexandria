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
