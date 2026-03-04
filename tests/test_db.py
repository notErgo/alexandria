"""Tests for MinerDB schema, constraints, and CRUD operations."""
import pytest
import sqlite3
from helpers import make_report, make_data_point, make_review_item


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
        db.insert_company({
            'ticker': 'MARA', 'name': 'MARA Holdings', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': '0001437491', 'active': 1,
        })
        result = db.get_company('MARA')
        assert result is not None
        assert result['name'] == 'MARA Holdings'

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
        db.add_company(
            ticker='RIOT', name='Riot Platforms', tier=1,
            ir_url='https://www.riotplatforms.com/news',
            sector='BTC-miners', scraper_mode='skip',
        )
        company = db.get_company('RIOT')
        assert company is not None
        assert company['ticker'] == 'RIOT'
