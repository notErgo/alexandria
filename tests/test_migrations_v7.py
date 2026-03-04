"""Tests for schema v7 migration — new tables and columns.

All tests in this file must FAIL before _migrate_v7 is implemented,
and PASS after. This is the test-first gate for Phase I.
"""
import sqlite3
import pytest


class TestMigrationV7:

    # ── companies: new columns ───────────────────────────────────────────────

    def test_companies_has_sector_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'sector' in cols

    def test_companies_has_scraper_mode_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'scraper_mode' in cols

    def test_companies_has_scraper_issues_log_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'scraper_issues_log' in cols

    def test_companies_has_scraper_status_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'scraper_status' in cols

    def test_companies_has_last_scrape_at_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'last_scrape_at' in cols

    def test_companies_has_last_scrape_error_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'last_scrape_error' in cols

    def test_companies_has_probe_completed_at_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(companies)")]
        assert 'probe_completed_at' in cols

    # ── asset_manifest: new column ───────────────────────────────────────────

    def test_asset_manifest_has_mutation_log_column(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(asset_manifest)")]
        assert 'mutation_log' in cols

    # ── regime_config: table exists and has required columns ─────────────────

    def test_regime_config_table_exists(self, db):
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='regime_config'"
            ).fetchone()
        assert row is not None

    def test_regime_config_has_required_columns(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(regime_config)")]
        for col in ('id', 'ticker', 'cadence', 'start_date', 'end_date', 'notes', 'created_at'):
            assert col in cols, f"Missing column: {col}"

    # ── metric_schema: table exists, columns, and UNIQUE constraint ──────────

    def test_metric_schema_table_exists(self, db):
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='metric_schema'"
            ).fetchone()
        assert row is not None

    def test_metric_schema_has_required_columns(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(metric_schema)")]
        for col in ('id', 'key', 'label', 'unit', 'sector',
                    'has_extraction_pattern', 'analyst_defined', 'created_at'):
            assert col in cols, f"Missing column: {col}"

    def test_metric_schema_unique_key_sector(self, db):
        with db._get_connection() as conn:
            conn.execute(
                "INSERT INTO metric_schema (key, label, unit, sector) VALUES ('test_metric', 'Test', '', 'BTC-miners')"
            )
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO metric_schema (key, label, unit, sector) VALUES ('test_metric', 'Test Dupe', '', 'BTC-miners')"
                )

    # ── scrape_queue: table exists and has required columns ──────────────────

    def test_scrape_queue_table_exists(self, db):
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='scrape_queue'"
            ).fetchone()
        assert row is not None

    def test_scrape_queue_has_required_columns(self, db):
        with db._get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(scrape_queue)")]
        for col in ('id', 'ticker', 'mode', 'status',
                    'created_at', 'started_at', 'completed_at', 'error_msg'):
            assert col in cols, f"Missing column: {col}"

    # ── metric_schema seed ───────────────────────────────────────────────────

    def test_metric_schema_seed(self, db):
        with db._get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM metric_schema WHERE sector='BTC-miners'"
            ).fetchone()[0]
        assert count == 13, f"Expected 13 seeded metrics, got {count}"
