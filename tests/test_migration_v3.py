"""
Tests for DB schema migration v3 (extracted_at column + 3 new methods).

Written test-first per TDD requirement. These tests FAIL before migration is
applied and PASS after db.py is updated with the v3 migration.
"""
import pytest
import sqlite3


@pytest.fixture
def fresh_db(tmp_path):
    """Fresh MinerDB on a new temp path — always migrates from version 0."""
    from infra.db import MinerDB
    return MinerDB(str(tmp_path / 'migration_v3_test.db'))


@pytest.fixture
def raw_conn(fresh_db):
    """Raw sqlite3 connection to the fresh DB, for PRAGMA inspection."""
    conn = sqlite3.connect(fresh_db.db_path)
    yield conn
    conn.close()


def _table_columns(conn, table_name):
    """Return set of column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _insert_company(db):
    db.insert_company({
        'ticker': 'MARA', 'name': 'MARA Holdings, Inc.',
        'tier': 1, 'ir_url': 'https://example.com',
        'pr_base_url': None, 'cik': None, 'active': 1,
    })


def _insert_report(db, report_date='2024-09-01'):
    return db.insert_report({
        'ticker': 'MARA',
        'report_date': report_date,
        'published_date': None,
        'source_type': 'archive_html',
        'source_url': None,
        'raw_text': 'MARA mined 700 BTC in September 2024.',
        'parsed_at': '2024-09-03T12:00:00',
    })


class TestSchemaVersionV3:
    def test_user_version_is_3_or_higher(self, raw_conn):
        """After init, PRAGMA user_version should be >= 3 (v4 adds config/hints tables)."""
        version = raw_conn.execute("PRAGMA user_version").fetchone()[0]
        assert version >= 3

    def test_extracted_at_column_added(self, raw_conn):
        """reports table must have an extracted_at column after migration."""
        cols = _table_columns(raw_conn, 'reports')
        assert 'extracted_at' in cols, "reports table missing extracted_at column"


class TestMarkReportExtracted:
    def test_mark_report_extracted_sets_timestamp(self, fresh_db):
        """mark_report_extracted(id) must set a non-null extracted_at timestamp."""
        _insert_company(fresh_db)
        report_id = _insert_report(fresh_db)
        # Before marking: extracted_at is NULL
        report_before = fresh_db.get_report(report_id)
        assert report_before['extracted_at'] is None

        fresh_db.mark_report_extracted(report_id)
        report_after = fresh_db.get_report(report_id)
        assert report_after['extracted_at'] is not None

    def test_mark_report_extracted_timestamp_is_iso_format(self, fresh_db):
        """extracted_at value must be parseable as ISO datetime."""
        from datetime import datetime
        _insert_company(fresh_db)
        report_id = _insert_report(fresh_db)
        fresh_db.mark_report_extracted(report_id)
        report = fresh_db.get_report(report_id)
        # Should not raise
        datetime.fromisoformat(report['extracted_at'])


class TestGetUnextractedReports:
    def test_get_unextracted_reports_filters_correctly(self, fresh_db):
        """Only reports with extracted_at IS NULL should be returned."""
        _insert_company(fresh_db)
        id1 = _insert_report(fresh_db, '2024-09-01')
        id2 = _insert_report(fresh_db, '2024-10-01')

        fresh_db.mark_report_extracted(id1)
        unextracted = fresh_db.get_unextracted_reports()

        ids = {r['id'] for r in unextracted}
        assert id1 not in ids, "extracted report should not appear in unextracted list"
        assert id2 in ids, "unextracted report should appear in unextracted list"

    def test_get_unextracted_reports_ticker_filter(self, fresh_db):
        """ticker kwarg must filter results to matching ticker only."""
        fresh_db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        _insert_company(fresh_db)  # MARA
        _insert_report(fresh_db)
        fresh_db.insert_report({
            'ticker': 'RIOT', 'report_date': '2024-09-01',
            'published_date': None, 'source_type': 'archive_html',
            'source_url': None, 'raw_text': 'RIOT mined 400 BTC.',
            'parsed_at': '2024-09-03T12:00:00',
        })

        mara_only = fresh_db.get_unextracted_reports(ticker='MARA')
        assert all(r['ticker'] == 'MARA' for r in mara_only)

    def test_get_unextracted_reports_empty_text_excluded(self, fresh_db):
        """Reports with empty raw_text must be excluded (nothing to extract)."""
        _insert_company(fresh_db)
        id_empty = fresh_db.insert_report({
            'ticker': 'MARA', 'report_date': '2024-09-01',
            'published_date': None, 'source_type': 'archive_html',
            'source_url': None, 'raw_text': '',
            'parsed_at': '2024-09-03T12:00:00',
        })
        unextracted = fresh_db.get_unextracted_reports()
        assert not any(r['id'] == id_empty for r in unextracted)


class TestGetAllReportsForExtraction:
    def test_get_all_reports_for_extraction_ignores_extracted_at(self, fresh_db):
        """get_all_reports_for_extraction returns BOTH extracted and unextracted."""
        _insert_company(fresh_db)
        id1 = _insert_report(fresh_db, '2024-09-01')
        id2 = _insert_report(fresh_db, '2024-10-01')

        fresh_db.mark_report_extracted(id1)
        all_reports = fresh_db.get_all_reports_for_extraction()

        ids = {r['id'] for r in all_reports}
        assert id1 in ids, "extracted report should still appear in all_reports_for_extraction"
        assert id2 in ids, "unextracted report should appear in all_reports_for_extraction"

    def test_get_all_reports_for_extraction_ticker_filter(self, fresh_db):
        """ticker kwarg must filter results."""
        fresh_db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://example.com', 'pr_base_url': None,
            'cik': None, 'active': 1,
        })
        _insert_company(fresh_db)  # MARA
        _insert_report(fresh_db)
        fresh_db.insert_report({
            'ticker': 'RIOT', 'report_date': '2024-09-01',
            'published_date': None, 'source_type': 'archive_html',
            'source_url': None, 'raw_text': 'RIOT mined 400 BTC.',
            'parsed_at': '2024-09-03T12:00:00',
        })

        riot_only = fresh_db.get_all_reports_for_extraction(ticker='RIOT')
        assert all(r['ticker'] == 'RIOT' for r in riot_only)
