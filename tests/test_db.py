"""Tests for MinerDB schema, constraints, and CRUD operations."""
import json
import pytest
import sqlite3
from helpers import make_report, make_data_point, make_review_item
from infra.db import MinerDB


class TestSchema:
    def test_schema_tables_exist(self, db):
        with db._get_connection() as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert {'companies', 'reports', 'data_points', 'patterns', 'review_queue'} <= tables

    def test_wal_mode_enabled(self, db):
        with db._get_connection() as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == 'wal'

    def test_foreign_key_enforcement(self, db):
        with pytest.raises(sqlite3.IntegrityError):
            with db._get_connection() as conn:
                conn.execute(
                    """INSERT INTO reports(ticker, report_date, source_type)
                       VALUES('NONEXISTENT', '2024-01-01', 'archive_pdf')"""
                )


class TestCompanyCRUD:
    def test_insert_and_retrieve_company(self, db):
        # MARA is pre-seeded by sync_companies_from_config on DB init.
        # insert_company is idempotent (INSERT OR IGNORE) — the call
        # below is a no-op but the row still exists with the canonical name.
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })
        result = db.get_company('MARA')
        assert result is not None
        # Canonical name comes from companies.json sync, not the insert above.
        assert result['ticker'] == 'MARA'

    def test_get_company_returns_none_for_unknown(self, db):
        assert db.get_company('UNKNOWN') is None

    def test_insert_company_idempotent(self, db):
        c = {'ticker': 'RIOT', 'name': 'Riot', 'tier': 1,
             'ir_url': 'https://example.com', 'pr_base_url': None,
             'cik': '0001167419', 'active': 1}
        db.insert_company(c)
        db.insert_company(c)  # second insert is ignored
        companies = db.get_companies()
        assert len([x for x in companies if x['ticker'] == 'RIOT']) == 1

    def test_sync_companies_prefers_scraper_mode_over_legacy_scrape_mode(self, tmp_path):
        db = MinerDB(str(tmp_path / 'test.db'))
        config_path = tmp_path / 'companies.json'
        config_path.write_text(json.dumps([
            {
                'ticker': 'ZZZZ',
                'name': 'Mode Priority Co',
                'tier': 2,
                'ir_url': 'https://example.com/investors',
                'active': True,
                'scrape_mode': 'skip',
                'scraper_mode': 'rss',
                'rss_url': 'https://example.com/feed.xml',
            }
        ]))
        db.sync_companies_from_config(str(config_path))
        company = db.get_company('ZZZZ')
        assert company is not None
        assert company['scraper_mode'] == 'rss'

    def test_sync_companies_does_not_overwrite_existing_mode_from_legacy_key(self, tmp_path):
        db = MinerDB(str(tmp_path / 'test.db'))
        config_path = tmp_path / 'companies.json'
        config_path.write_text(json.dumps([
            {
                'ticker': 'ZZZZ',
                'name': 'Mode Sticky Co',
                'tier': 2,
                'ir_url': 'https://example.com/investors',
                'active': True,
                'scraper_mode': 'rss',
                'rss_url': 'https://example.com/feed.xml',
            }
        ]))
        db.sync_companies_from_config(str(config_path))
        assert db.get_company('ZZZZ')['scraper_mode'] == 'rss'

        # Legacy scrape_mode in old config should not clobber the existing mode.
        config_path.write_text(json.dumps([
            {
                'ticker': 'ZZZZ',
                'name': 'Mode Sticky Co',
                'tier': 2,
                'ir_url': 'https://example.com/investors',
                'active': True,
                'scrape_mode': 'skip',
            }
        ]))
        db.sync_companies_from_config(str(config_path))
        assert db.get_company('ZZZZ')['scraper_mode'] == 'rss'


class TestReportCRUD:
    def test_insert_report_returns_id(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        assert isinstance(report_id, int)
        assert report_id > 0

    def test_report_exists_true(self, db_with_company):
        db_with_company.insert_report(make_report())
        assert db_with_company.report_exists('MARA', '2024-09-01', 'archive_pdf')

    def test_report_exists_false(self, db_with_company):
        assert not db_with_company.report_exists('MARA', '2024-09-01', 'archive_pdf')

    def test_get_report_returns_dict(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        result = db_with_company.get_report(report_id)
        assert result is not None
        assert result['ticker'] == 'MARA'
        assert result['report_date'] == '2024-09-01'
        assert result['source_type'] == 'archive_pdf'

    def test_get_report_returns_none_for_missing_id(self, db_with_company):
        result = db_with_company.get_report(99999)
        assert result is None


class TestDataPointCRUD:
    def test_insert_data_point_and_query(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        dp = make_data_point(report_id=report_id)
        dp_id = db_with_company.insert_data_point(dp)
        assert dp_id > 0
        results = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(results) == 1
        assert results[0]['value'] == 700.0

    def test_duplicate_period_metric_upserted(self, db_with_company):
        db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(value=700.0))
        db_with_company.insert_data_point(make_data_point(value=705.0))  # upsert
        results = db_with_company.query_data_points(ticker='MARA', metric='production_btc')
        assert len(results) == 1
        assert results[0]['value'] == 705.0

    def test_query_filter_by_confidence(self, db_with_company):
        db_with_company.insert_data_point(make_data_point(confidence=0.9))
        db_with_company.insert_data_point(make_data_point(
            metric='hodl_btc', period='2024-08-01', value=10000.0,
            unit='BTC', confidence=0.6
        ))
        results = db_with_company.query_data_points(min_confidence=0.8)
        assert all(r['confidence'] >= 0.8 for r in results)


class TestReviewQueueCRUD:
    def test_insert_review_item(self, db_with_company):
        item_id = db_with_company.insert_review_item(make_review_item())
        assert item_id > 0
        items = db_with_company.get_review_items(status='PENDING')
        assert len(items) == 1

    def test_approve_review_item_promotes_to_data_points(self, db_with_company):
        item_id = db_with_company.insert_review_item(make_review_item())
        dp = db_with_company.approve_review_item(item_id)
        assert dp['value'] == 700.0
        data_points = db_with_company.query_data_points(ticker='MARA')
        assert len(data_points) == 1
        items = db_with_company.get_review_items(status='APPROVED')
        assert len(items) == 1

    def test_reject_review_item(self, db_with_company):
        item_id = db_with_company.insert_review_item(make_review_item())
        db_with_company.reject_review_item(item_id, note='Out of range')
        items = db_with_company.get_review_items(status='REJECTED')
        assert len(items) == 1
        assert items[0]['reviewer_note'] == 'Out of range'

    def test_edit_review_item_sets_corrected_value(self, db_with_company):
        item_id = db_with_company.insert_review_item(make_review_item())
        dp = db_with_company.edit_review_item(item_id, corrected_value=695.0, note='OCR error')
        assert dp['value'] == 695.0
        assert dp['confidence'] == 1.0
        results = db_with_company.query_data_points(ticker='MARA')
        assert results[0]['value'] == 695.0


class TestCompanyStatus:
    def test_returns_empty_for_no_reports(self, db):
        rows = db.get_company_status()
        assert rows == []

    def test_returns_stats_for_ticker_with_report_and_data(self, db_with_company):
        report_id = db_with_company.insert_report(make_report(
            source_type='archive_pdf', report_date='2024-01-01'
        ))
        db_with_company.insert_data_point(make_data_point(
            report_id=report_id, period='2024-01-01',
            metric='production_btc', value=100.0, confidence=0.92,
        ))
        rows = db_with_company.get_company_status()
        assert len(rows) == 1
        row = rows[0]
        assert row['ticker'] == 'MARA'
        assert row['report_count'] == 1
        assert row['data_point_count'] == 1
        assert row['prod_months'] == 1
        assert row['first_period'] == '2024-01-01'
        assert row['last_period'] == '2024-01-01'
        assert abs(row['avg_confidence'] - 0.92) < 0.01

    def test_prod_months_counts_distinct_periods(self, db_with_company):
        """Two data_points in same period (from different reports) count as 1 prod_month."""
        r1 = db_with_company.insert_report(make_report(
            source_type='archive_pdf', report_date='2024-01-01'
        ))
        r2 = db_with_company.insert_report(make_report(
            source_type='archive_html', report_date='2024-02-01'
        ))
        db_with_company.insert_data_point(make_data_point(
            report_id=r1, period='2024-01-01', metric='production_btc', value=100.0,
        ))
        # Second data_point for same period — upsert means same row, still 1 prod_month
        db_with_company.insert_data_point(make_data_point(
            report_id=r2, period='2024-01-01', metric='production_btc', value=105.0,
        ))
        rows = db_with_company.get_company_status()
        assert rows[0]['prod_months'] == 1
        assert rows[0]['report_count'] == 2

    def test_report_with_no_data_points_still_counted(self, db_with_company):
        """A report that yielded zero extractions still increments report_count."""
        db_with_company.insert_report(make_report(source_type='archive_pdf'))
        rows = db_with_company.get_company_status()
        assert len(rows) == 1
        assert rows[0]['report_count'] == 1
        assert rows[0]['data_point_count'] == 0
        assert rows[0]['prod_months'] == 0


class TestPatternApplyHelpers:
    """Tests for DB methods added to support POST /api/patterns/apply."""

    def test_get_reports_with_text_returns_reports_that_have_raw_text(self, db_with_company):
        db_with_company.insert_report(make_report(raw_text='MARA mined 750 BTC.'))
        rows = db_with_company.get_reports_with_text()
        assert len(rows) == 1
        assert rows[0]['ticker'] == 'MARA'
        assert rows[0]['report_date'] == '2024-09-01'

    def test_get_reports_with_text_excludes_null_raw_text(self, db_with_company):
        db_with_company.insert_report(make_report(raw_text=None))
        rows = db_with_company.get_reports_with_text()
        assert rows == []

    def test_get_reports_with_text_excludes_empty_raw_text(self, db_with_company):
        db_with_company.insert_report(make_report(raw_text=''))
        rows = db_with_company.get_reports_with_text()
        assert rows == []

    def test_get_reports_with_text_does_not_include_raw_text_column(self, db_with_company):
        """Only stubs returned — raw_text column absent to keep results lightweight."""
        db_with_company.insert_report(make_report(raw_text='MARA mined 750 BTC.'))
        rows = db_with_company.get_reports_with_text()
        assert 'raw_text' not in rows[0]

    def test_get_report_raw_text_returns_text(self, db_with_company):
        report_id = db_with_company.insert_report(make_report(raw_text='mined 500 bitcoin'))
        result = db_with_company.get_report_raw_text(report_id)
        assert result == 'mined 500 bitcoin'

    def test_get_report_raw_text_returns_none_for_missing_id(self, db_with_company):
        result = db_with_company.get_report_raw_text(99999)
        assert result is None

    def test_data_point_exists_true(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(report_id=report_id))
        assert db_with_company.data_point_exists('MARA', '2024-09-01', 'production_btc')

    def test_data_point_exists_false_when_none_inserted(self, db_with_company):
        assert not db_with_company.data_point_exists('MARA', '2024-09-01', 'production_btc')

    def test_data_point_exists_false_for_different_period(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(report_id=report_id))
        assert not db_with_company.data_point_exists('MARA', '2024-08-01', 'production_btc')

    def test_data_point_exists_false_for_different_metric(self, db_with_company):
        report_id = db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(report_id=report_id))
        assert not db_with_company.data_point_exists('MARA', '2024-09-01', 'hodl_btc')



# ── Phase III: new company CRUD methods ──────────────────────────────────────

class TestCompanyScraperFields:

    def test_update_company_scraper_fields(self, db_with_company):
        db_with_company.update_company_scraper_fields(
            'MARA', scraper_status='probe_ok', last_scrape_at='2026-01-01'
        )
        company = db_with_company.get_company('MARA')
        assert company['scraper_status'] == 'probe_ok'
        assert company['last_scrape_at'] == '2026-01-01'

    def test_update_company_config(self, db_with_company):
        db_with_company.update_company_config('MARA', scraper_mode='rss', sector='BTC-miners')
        company = db_with_company.get_company('MARA')
        assert company['scraper_mode'] == 'rss'
        assert company['sector'] == 'BTC-miners'

    def test_add_company_creates_new_row(self, db):
        # Use a test-only ticker not present in companies.json so sync_companies_from_config
        # does not pre-populate it, allowing add_company to create a new row.
        db.add_company(
            ticker='TESTX', name='Test Company', tier=2,
            ir_url='https://example.com/ir',
            sector='BTC-miners', scraper_mode='skip',
        )
        company = db.get_company('TESTX')
        assert company is not None
        assert company['ticker'] == 'TESTX'


# ── Phase 1: Purge and trailing data point methods ───────────────────────────

class TestPurgeDataPoints:

    def test_purge_data_points_all(self, db_with_company):
        """Insert 3 data_points, purge all, count == 0."""
        db_with_company.insert_data_point(make_data_point(period='2024-07-01', value=600.0))
        db_with_company.insert_data_point(make_data_point(period='2024-08-01', value=650.0))
        db_with_company.insert_data_point(make_data_point(period='2024-09-01', value=700.0))
        assert db_with_company.count_data_points() == 3

        deleted = db_with_company.purge_data_points()
        assert deleted == 3
        assert db_with_company.count_data_points() == 0

    def test_purge_data_points_by_ticker(self, db):
        """Insert MARA + RIOT rows, purge MARA only, RIOT survives."""
        db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_company({'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_data_point(make_data_point(ticker='MARA', period='2024-09-01'))
        db.insert_data_point(make_data_point(ticker='RIOT', period='2024-09-01'))
        assert db.count_data_points() == 2

        deleted = db.purge_data_points(ticker='MARA')
        assert deleted == 1
        remaining = db.query_data_points()
        assert len(remaining) == 1
        assert remaining[0]['ticker'] == 'RIOT'

    def test_purge_resets_extracted_at(self, db_with_company):
        """After purge, reports.extracted_at is set back to NULL."""
        report_id = db_with_company.insert_report(make_report())
        db_with_company.mark_report_extracted(report_id)
        report = db_with_company.get_report(report_id)
        assert report['extracted_at'] is not None

        db_with_company.purge_data_points(ticker='MARA')
        report = db_with_company.get_report(report_id)
        assert report['extracted_at'] is None

    def test_purge_review_queue_all(self, db_with_company):
        """Insert 2 review items, purge all, count == 0."""
        db_with_company.insert_review_item(make_review_item(period='2024-08-01'))
        db_with_company.insert_review_item(make_review_item(period='2024-09-01'))
        assert db_with_company.count_review_items(status=None) == 2

        deleted = db_with_company.purge_review_queue()
        assert deleted == 2
        assert db_with_company.count_review_items(status=None) == 0

    def test_purge_review_queue_by_ticker(self, db):
        """Purge review_queue rows for one ticker only."""
        db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_company({'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_review_item(make_review_item(ticker='MARA'))
        db.insert_review_item(make_review_item(ticker='RIOT'))
        assert db.count_review_items(status=None) == 2

        deleted = db.purge_review_queue(ticker='MARA')
        assert deleted == 1
        remaining = db.get_review_items()
        assert len(remaining) == 1
        assert remaining[0]['ticker'] == 'RIOT'


class TestPurgeAll:

    def test_purge_all_clears_reports_and_data_points(self, db_with_company):
        """purge_all removes reports and data_points, returns non-zero counts."""
        report_id = db_with_company.insert_report(make_report(raw_text='MARA mined 750 BTC.'))
        db_with_company.insert_data_point(make_data_point(period='2024-09-01'))
        db_with_company.insert_review_item(make_review_item(period='2024-09-01'))

        counts = db_with_company.purge_all()

        assert counts['reports'] == 1
        assert counts['data_points'] == 1
        assert counts['review_queue'] == 1

        # Verify tables are empty
        with db_with_company._get_connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0] == 0

    def test_purge_all_by_ticker_resets_company_scraper_fields(self, db_with_company):
        """Ticker-scoped purge keeps company row and resets its operational fields."""
        db_with_company.update_company_scraper_fields(
            'MARA', scraper_status='ok', last_scrape_at='2025-01-01T00:00:00'
        )
        company_before = db_with_company.get_company('MARA')
        assert company_before['scraper_status'] == 'ok'
        assert company_before['last_scrape_at'] == '2025-01-01T00:00:00'

        db_with_company.purge_all(ticker='MARA')

        company_after = db_with_company.get_company('MARA')
        assert company_after['scraper_status'] == 'never_run'
        assert company_after['last_scrape_at'] is None
        assert company_after['last_scrape_error'] is None

    def test_purge_all_clears_company_rows(self, db_with_company):
        """Full purge clears company catalog rows for a truly empty Ops table."""
        db_with_company.purge_all()
        company = db_with_company.get_company('MARA')
        assert company is None

    def test_purge_all_reset_mode_keeps_company_rows(self, db_with_company):
        """reset mode clears DATA but preserves companies/regime config."""
        db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(period='2024-09-01'))
        db_with_company.upsert_regime_window(
            ticker='MARA', cadence='monthly', start_date='2024-01-01',
            end_date=None, notes='baseline'
        )

        counts = db_with_company.purge_all(purge_mode='reset')

        assert counts['reports'] == 1
        assert counts['data_points'] == 1
        assert 'companies' not in counts
        assert db_with_company.get_company('MARA') is not None
        assert len(db_with_company.get_regime_windows('MARA')) == 1

    def test_purge_all_archive_mode_writes_archive_batch(self, db_with_company, tmp_path):
        """archive mode stores deleted rows in purge_archive.db."""
        db_with_company.insert_report(make_report())
        db_with_company.insert_data_point(make_data_point(period='2024-09-01'))

        counts = db_with_company.purge_all(purge_mode='archive', reason='test archive')

        assert counts['reports'] == 1
        assert counts['data_points'] == 1
        assert 'archive_batch_id' in counts

        import sqlite3
        archive_path = tmp_path / 'purge_archive.db'
        conn = sqlite3.connect(str(archive_path))
        try:
            batch = conn.execute(
                "SELECT id, mode, ticker_scope, reason FROM purge_batches WHERE id = ?",
                (counts['archive_batch_id'],)
            ).fetchone()
            assert batch is not None
            assert batch[1] == 'archive'
            rows = conn.execute(
                "SELECT COUNT(*) FROM purge_rows WHERE batch_id = ?",
                (counts['archive_batch_id'],)
            ).fetchone()[0]
            assert rows >= 2
        finally:
            conn.close()

    def test_purge_all_hard_delete_can_disable_startup_auto_sync(self, db_with_company):
        """Hard-delete can disable startup company auto-sync via config_settings."""
        db_with_company.purge_all(purge_mode='hard_delete', suppress_auto_sync=True)
        assert db_with_company.get_config('auto_sync_companies_on_startup') == '0'

    def test_startup_sync_respects_config_override(self, tmp_path):
        """If auto_sync_companies_on_startup=0, restart does not re-seed companies."""
        from infra.db import MinerDB
        db_path = str(tmp_path / 'restart_sync_test.db')
        db = MinerDB(db_path)
        db.purge_all(purge_mode='hard_delete', suppress_auto_sync=True)
        assert db.get_company('MARA') is None

        restarted = MinerDB(db_path)
        assert restarted.get_company('MARA') is None

    def test_purge_all_by_ticker_scopes_deletion(self, db):
        """purge_all(ticker=X) deletes only X rows; other tickers survive."""
        db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_company({'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_report(make_report(ticker='MARA'))
        db.insert_report(make_report(ticker='RIOT'))

        counts = db.purge_all(ticker='MARA')

        assert counts['reports'] == 1
        with db._get_connection() as conn:
            remaining = conn.execute("SELECT ticker FROM reports").fetchall()
        assert [r[0] for r in remaining] == ['RIOT']

    def test_purge_all_invalid_mode_raises(self, db_with_company):
        """purge_all() with an unrecognised purge_mode raises ValueError."""
        with pytest.raises(ValueError, match="purge_mode must be one of"):
            db_with_company.purge_all(purge_mode='obliterate')

    def test_purge_all_reset_ticker_scope_keeps_company(self, db):
        """reset mode scoped to a ticker deletes that ticker's data but keeps company row."""
        db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_company({'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_report(make_report(ticker='MARA'))
        db.insert_report(make_report(ticker='RIOT'))

        counts = db.purge_all(ticker='MARA', purge_mode='reset')

        assert counts['reports'] == 1
        assert 'companies' not in counts
        assert db.get_company('MARA') is not None
        assert db.get_company('RIOT') is not None
        with db._get_connection() as conn:
            remaining = conn.execute("SELECT ticker FROM reports").fetchall()
        assert [r[0] for r in remaining] == ['RIOT']

    def test_purge_all_archive_ticker_scope_sets_ticker_scope_in_batch(self, db):
        """archive mode scoped to a ticker records ticker_scope in the archive batch."""
        db.insert_company({'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
                           'ir_url': '', 'pr_base_url': None, 'cik': None, 'active': 1})
        db.insert_report(make_report(ticker='MARA'))

        counts = db.purge_all(ticker='MARA', purge_mode='archive', reason='ticker test')

        assert 'archive_batch_id' in counts
        import sqlite3
        archive_path = db._archive_db_path()
        conn = sqlite3.connect(archive_path)
        try:
            row = conn.execute(
                "SELECT ticker_scope, reason FROM purge_batches WHERE id = ?",
                (counts['archive_batch_id'],),
            ).fetchone()
            assert row is not None
            assert row[0] == 'MARA'
            assert row[1] == 'ticker test'
        finally:
            conn.close()

    def test_purge_all_with_chunk_id_fk(self, db_with_company):
        """purge_all succeeds when data_points.chunk_id references document_chunks.

        Regression: original code deleted document_chunks before data_points,
        causing a FK constraint error (PRAGMA foreign_keys = ON) and a silent
        full rollback when any data_point had a non-NULL chunk_id.
        """
        report_id = db_with_company.insert_report(make_report(raw_text='MARA mined 750 BTC.'))
        # Insert a document_chunk
        with db_with_company._get_connection() as conn:
            conn.execute(
                "INSERT INTO document_chunks(report_id, chunk_index, text) VALUES (?, 0, 'chunk text')",
                (report_id,)
            )
            chunk_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        # Insert a data_point that references the chunk
        with db_with_company._get_connection() as conn:
            conn.execute(
                """INSERT INTO data_points
                   (ticker, period, metric, value, unit, confidence, chunk_id)
                   VALUES ('MARA', '2024-09-01', 'production_btc', 750.0, 'BTC', 0.9, ?)""",
                (chunk_id,)
            )

        # Must not raise — FK-safe order: data_points deleted before document_chunks
        counts = db_with_company.purge_all()

        assert counts['data_points'] == 1
        assert counts['document_chunks'] == 1
        assert counts['reports'] == 1
        with db_with_company._get_connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM document_chunks").fetchone()[0] == 0


class TestGetTrailingDataPoints:

    def test_returns_desc_sorted_rows(self, db_with_company):
        """get_trailing_data_points returns rows in DESC period order."""
        for period, value in [
            ('2024-07-01', 600.0),
            ('2024-08-01', 650.0),
            ('2024-09-01', 700.0),
        ]:
            db_with_company.insert_data_point(
                make_data_point(period=period, value=value)
            )
        # before_period is 2024-10-01, so all 3 qualify
        rows = db_with_company.get_trailing_data_points('MARA', 'production_btc', '2024-10-01', limit=3)
        assert len(rows) == 3
        periods = [r['period'] for r in rows]
        assert periods == sorted(periods, reverse=True)

    def test_filters_by_before_period(self, db_with_company):
        """Only rows with period < before_period are returned."""
        for period, value in [
            ('2024-07-01', 600.0),
            ('2024-08-01', 650.0),
            ('2024-09-01', 700.0),
        ]:
            db_with_company.insert_data_point(
                make_data_point(period=period, value=value)
            )
        # before_period = 2024-09-01 should exclude 2024-09-01 and later
        rows = db_with_company.get_trailing_data_points('MARA', 'production_btc', '2024-09-01', limit=3)
        assert all(r['period'] < '2024-09-01' for r in rows)
        assert len(rows) == 2

    def test_respects_limit(self, db_with_company):
        """Limit parameter caps returned rows."""
        for i in range(5):
            db_with_company.insert_data_point(
                make_data_point(
                    period=f'2024-0{i+1}-01' if i < 9 else f'2024-{i+1}-01',
                    value=float(600 + i * 10),
                    metric='production_btc',
                )
            )
        rows = db_with_company.get_trailing_data_points('MARA', 'production_btc', '2025-01-01', limit=3)
        assert len(rows) == 3

    def test_returns_only_monthly_rows(self, db_with_company):
        """Only source_period_type='monthly' rows are returned."""
        db_with_company.insert_data_point(make_data_point(period='2024-08-01', value=650.0))
        # Insert a quarterly row
        with db_with_company._get_connection() as conn:
            conn.execute(
                """INSERT INTO data_points
                   (ticker, period, metric, value, unit, confidence, extraction_method, source_period_type)
                   VALUES ('MARA', '2024-Q3', 'production_btc', 1950.0, 'BTC', 0.9, 'llm', 'quarterly')"""
            )
        rows = db_with_company.get_trailing_data_points('MARA', 'production_btc', '2025-01-01', limit=10)
        assert all(r.get('source_period_type', 'monthly') == 'monthly' for r in rows)
        assert not any(r['period'] == '2024-Q3' for r in rows)
