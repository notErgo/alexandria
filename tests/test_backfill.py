"""Tests for targeted EDGAR backfill feature."""
import pytest
from datetime import date
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# db.detect_edgar_report_window
# ---------------------------------------------------------------------------

class TestDetectEdgarReportWindow:
    def test_returns_none_none_when_no_edgar_reports(self, db):
        result = db.detect_edgar_report_window('NOMATCH')
        assert result == {'min_date': None, 'max_date': None}

    def _r(self, ticker, date, source_type, url):
        return {'ticker': ticker, 'report_date': date, 'published_date': date,
                'source_type': source_type, 'source_url': url, 'raw_text': 'text',
                'parsed_at': date}

    def test_returns_correct_min_max(self, db_with_company):
        db_with_company.insert_report(self._r('MARA', '2020-06-01', 'edgar_8k', 'http://a'))
        db_with_company.insert_report(self._r('MARA', '2021-03-01', 'edgar_8k', 'http://b'))
        result = db_with_company.detect_edgar_report_window('MARA')
        assert result['min_date'] == '2020-06-01'
        assert result['max_date'] == '2021-03-01'

    def test_ignores_non_edgar_source_types(self, db_with_company):
        db_with_company.insert_report(self._r('MARA', '2019-01-01', 'archive_html', 'http://c'))
        result = db_with_company.detect_edgar_report_window('MARA')
        assert result == {'min_date': None, 'max_date': None}

    def test_only_counts_edgar_source_types(self, db_with_company):
        db_with_company.insert_report(self._r('MARA', '2019-01-01', 'archive_html', 'http://c'))
        db_with_company.insert_report(self._r('MARA', '2021-01-01', 'edgar_10q', 'http://d'))
        result = db_with_company.detect_edgar_report_window('MARA')
        assert result['min_date'] == '2021-01-01'
        assert result['max_date'] == '2021-01-01'


# ---------------------------------------------------------------------------
# EdgarConnector.fetch_8k_filings — until_date upper bound
# ---------------------------------------------------------------------------

class TestEdgarUntilDate:
    def _make_connector(self, db):
        from scrapers.edgar_connector import EdgarConnector
        import requests
        session = requests.Session()
        db.report_exists_by_accession = MagicMock(return_value=False)
        db.report_exists = MagicMock(return_value=False)
        db.insert_report = MagicMock(return_value=1)
        return EdgarConnector(db=db, session=session)

    def test_fetch_8k_until_date_filters_filings_after_cutoff(self, db_with_company):
        connector = self._make_connector(db_with_company)
        filings = [
            {'filing_date': '2022-06-01', 'accession_number': '0001-22-001',
             'primary_doc': 'doc1.htm', 'form_type': '8-K'},
            {'filing_date': '2021-03-01', 'accession_number': '0001-21-001',
             'primary_doc': 'doc2.htm', 'form_type': '8-K'},
        ]
        with patch.object(connector, '_get_submissions', return_value={}), \
             patch('scrapers.edgar_connector.parse_submissions_filings', return_value=filings), \
             patch.object(connector, '_edgar_get_text', return_value='<html></html>'), \
             patch('scrapers.edgar_connector.parse_current_report_exhibit_url', return_value='http://x'), \
             patch('scrapers.edgar_connector.parse_filing_index_for_primary_doc', return_value=None), \
             patch.object(connector, '_edgar_get_text', return_value='filing text'):
            summary = connector.fetch_8k_filings(
                cik='0001234567', ticker='MARA',
                since_date=date(2020, 1, 1),
                until_date=date(2021, 12, 31),
            )
        # 2022-06-01 > until_date(2021-12-31) → skipped; 2021-03-01 → ingested
        assert summary.reports_ingested == 1

    def test_fetch_8k_until_date_none_fetches_all(self, db_with_company):
        connector = self._make_connector(db_with_company)
        filings = [
            {'filing_date': '2022-06-01', 'accession_number': '0001-22-001',
             'primary_doc': 'doc1.htm', 'form_type': '8-K'},
            {'filing_date': '2021-03-01', 'accession_number': '0001-21-001',
             'primary_doc': 'doc2.htm', 'form_type': '8-K'},
        ]
        with patch.object(connector, '_get_submissions', return_value={}), \
             patch('scrapers.edgar_connector.parse_submissions_filings', return_value=filings), \
             patch('scrapers.edgar_connector.parse_current_report_exhibit_url', return_value='http://x'), \
             patch('scrapers.edgar_connector.parse_filing_index_for_primary_doc', return_value=None), \
             patch.object(connector, '_edgar_get_text', return_value='filing text'):
            summary = connector.fetch_8k_filings(
                cik='0001234567', ticker='MARA',
                since_date=date(2020, 1, 1),
                until_date=None,
            )
        assert summary.reports_ingested == 2

    def test_fetch_all_filings_skip_pivot_gate_bypasses_btc_anchor(self, db_with_company):
        """When skip_pivot_gate=True, btc_first_filing_date does not advance since_date."""
        from scrapers.edgar_connector import EdgarConnector
        import requests
        from miner_types import IngestSummary

        connector = EdgarConnector(db=db_with_company, session=requests.Session())
        db_with_company.get_btc_first_filing_date = MagicMock(return_value='2021-05-12')

        called_since = {}

        def fake_fetch_8k(cik, ticker, since_date, until_date=None):
            called_since['since'] = since_date
            return IngestSummary()

        with patch.object(connector, 'fetch_8k_filings', side_effect=fake_fetch_8k), \
             patch.object(connector, 'fetch_10q_filings', return_value=IngestSummary()), \
             patch.object(connector, 'fetch_10k_filings', return_value=IngestSummary()):
            connector.fetch_all_filings(
                cik='0001234567', ticker='MARA',
                since_date=date(2020, 1, 1),
                filing_regime='domestic',
                skip_pivot_gate=True,
            )

        assert called_since['since'] == date(2020, 1, 1)


# ---------------------------------------------------------------------------
# Backfill route
# ---------------------------------------------------------------------------

@pytest.fixture
def app(tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    from infra.db import MinerDB
    import importlib, run_web
    db = MinerDB(str(tmp_path / 'test.db'))
    app_globals._db = db
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def app_with_mara(app):
    with app.app_context():
        from app_globals import get_db
        db = get_db()
        # MARA is seeded from companies.json on DB init; set the pivot date
        db.update_company_scraper_fields('MARA', scraper_mode='rss')
        db.set_btc_first_filing_date('MARA', '2020-01-01')
    return app


class TestBackfillRoute:
    def test_backfill_returns_202_with_task_id(self, app_with_mara):
        client = app_with_mara.test_client()
        with patch('routes.scrape._run_backfill'):
            resp = client.post('/api/backfill/MARA', json={
                'from_date': '2020-01-01', 'to_date': '2021-04-30'
            })
        assert resp.status_code == 202
        data = resp.get_json()
        assert data['success'] is True
        assert 'task_id' in data['data']

    def test_backfill_unknown_ticker_returns_404(self, app):
        resp = app.test_client().post('/api/backfill/ZZZZ',
                                      json={'from_date': '2020-01-01', 'to_date': '2021-01-01'})
        assert resp.status_code == 404

    def test_backfill_no_cik_returns_400(self, app):
        with app.app_context():
            from app_globals import get_db
            db = get_db()
            with db._get_connection() as conn:
                conn.execute("UPDATE companies SET cik=NULL WHERE ticker='FUFU'")
        resp = app.test_client().post('/api/backfill/FUFU',
                                      json={'from_date': '2020-01-01', 'to_date': '2021-01-01'})
        assert resp.status_code == 400

    def test_backfill_duplicate_returns_409(self, app_with_mara):
        import routes.scrape as scrape_mod
        client = app_with_mara.test_client()
        scrape_mod._running_backfills.add('MARA')
        try:
            resp = client.post('/api/backfill/MARA',
                               json={'from_date': '2020-01-01', 'to_date': '2021-04-30'})
            assert resp.status_code == 409
        finally:
            scrape_mod._running_backfills.discard('MARA')

    def test_backfill_progress_not_found_returns_404(self, app):
        resp = app.test_client().get('/api/backfill/nonexistent-task-id/progress')
        assert resp.status_code == 404

    def test_backfill_progress_returns_state(self, app_with_mara):
        client = app_with_mara.test_client()
        with patch('routes.scrape._run_backfill'):
            resp = client.post('/api/backfill/MARA',
                               json={'from_date': '2020-01-01', 'to_date': '2021-04-30'})
        task_id = resp.get_json()['data']['task_id']
        prog_resp = client.get(f'/api/backfill/{task_id}/progress')
        assert prog_resp.status_code == 200
        assert prog_resp.get_json()['success'] is True

    def test_backfill_auto_detects_gap_when_no_dates_given(self, app_with_mara):
        import routes.scrape as scrape_mod
        scrape_mod._running_backfills.discard('MARA')
        client = app_with_mara.test_client()
        with app_with_mara.app_context():
            from app_globals import get_db
            get_db().insert_report({'ticker': 'MARA', 'report_date': '2021-05-01', 'published_date': '2021-05-01', 'source_type': 'edgar_8k', 'source_url': 'http://e', 'raw_text': 'text', 'parsed_at': '2021-05-01'})

        with patch('routes.scrape._run_backfill'):
            resp = client.post('/api/backfill/MARA', json={})
        assert resp.status_code == 202
        data = resp.get_json()['data']
        assert data['detected'] is True
        assert data['from_date'] == '2020-01-01'
        assert data['to_date'] == '2021-05-01'


# ---------------------------------------------------------------------------
# trigger_scrape skip-mode gate removed
# ---------------------------------------------------------------------------

class TestTriggerScrapeSkipModeGateRemoved:
    def test_skip_mode_company_with_cik_reaches_enqueue(self, db):
        """After removing the DB-level skip guard, enqueue_scrape_job should
        succeed for a skip-mode company that has a CIK."""
        db.insert_company({
            'ticker': 'CORZ', 'name': 'Core Scientific',
            'tier': 1, 'ir_url': 'https://example.com',
            'pr_base_url': 'https://example.com',
            'cik': '0001839175', 'active': 1,
        })
        db.update_company_scraper_fields('CORZ', scraper_mode='skip')
        job = db.enqueue_scrape_job('CORZ', 'historic')
        assert job['ticker'] == 'CORZ'
        assert job['status'] == 'pending'
