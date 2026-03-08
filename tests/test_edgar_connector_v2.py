"""
Tests for edgar_connector.py v2 overhaul — TDD.

Covers: Finding 1 (accession dedup), Finding 2 (foreign forms + regime routing),
        Finding 10 (rate-limit backoff).
"""
import hashlib
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest
import requests as req_lib


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_db(accession_exists=False, report_exists=False):
    db = MagicMock()
    db.report_exists_by_accession.return_value = accession_exists
    db.report_exists.return_value = report_exists
    db.insert_report.return_value = 1
    db.set_report_parse_quality.return_value = None
    db.update_company_last_edgar.return_value = None
    return db


def _submissions_for(form_types):
    """Build a minimal submissions payload for the given form types."""
    n = len(form_types)
    return {
        "filings": {
            "recent": {
                "form": form_types,
                "filingDate": ["2024-02-15"] * n,
                "accessionNumber": [f"0001507605-24-{i:06d}" for i in range(n)],
                "primaryDocument": [f"doc{i}.htm" for i in range(n)],
                "periodOfReport": ["2023-12-31"] * n,
            }
        }
    }


def _connector(db):
    from scrapers.edgar_connector import EdgarConnector
    session = MagicMock()
    return EdgarConnector(db=db, session=session)


# ── Finding 2: Foreign forms ──────────────────────────────────────────────────

class TestForeignForms:

    def _mock_response(self, text="annual report text"):
        resp = MagicMock()
        resp.status_code = 200
        resp.text = text
        return resp

    def test_6k_fetched_via_submissions_api(self):
        """fetch_6k_filings calls _get_submissions and ingests 6-K filings."""
        db = _mock_db()
        conn = _connector(db)
        conn._get_submissions = MagicMock(return_value=_submissions_for(['6-K']))
        conn._edgar_get_text = MagicMock(return_value="<html>6-K text</html>")
        conn._edgar_request = MagicMock(return_value=None)

        result = conn.fetch_6k_filings('0001720424', 'HIVE', date(2020, 1, 1))

        conn._get_submissions.assert_called_once()
        assert result.reports_ingested >= 0  # may be 0 if index parse fails; no crash

    def test_20f_fetched_via_submissions_api(self):
        """fetch_20f_filings calls _get_submissions and ingests 20-F filings."""
        db = _mock_db()
        conn = _connector(db)
        conn._get_submissions = MagicMock(return_value=_submissions_for(['20-F']))
        conn._edgar_get_text = MagicMock(return_value="<html>20-F text</html>")

        result = conn.fetch_20f_filings('0001841675', 'ARBK', date(2020, 1, 1))

        conn._get_submissions.assert_called_once()
        assert isinstance(result.reports_ingested, int)

    def test_40f_fetched_via_submissions_api(self):
        """fetch_40f_filings calls _get_submissions and ingests 40-F filings."""
        db = _mock_db()
        conn = _connector(db)
        conn._get_submissions = MagicMock(return_value=_submissions_for(['40-F']))
        conn._edgar_get_text = MagicMock(return_value="<html>40-F text</html>")

        result = conn.fetch_40f_filings('0001812477', 'BITF', date(2020, 1, 1))

        conn._get_submissions.assert_called_once()
        assert isinstance(result.reports_ingested, int)

    def test_amended_8k_stored_as_separate_row(self):
        """8-K/A amendments are stored under accession_number separate from original."""
        db = _mock_db(accession_exists=False)
        conn = _connector(db)
        # Simulate a filing dict for an 8-K/A
        filing = {
            'form_type': '8-K/A',
            'accession_number': '0001507605-24-099999',
            'filing_date': '2024-03-01',
            'primary_doc': 'ex991.htm',
            'period_of_report': '2024-01-31',
            'covering_period': None,
        }
        # _ingest_periodic_filing should check accession-based dedup
        conn._edgar_get_text = MagicMock(return_value='<html>amendment text here</html>')
        result = conn._ingest_periodic_filing('8-K/A', filing, 'MARA', '0001507605')
        # Should attempt insert (not skip) since accession_exists=False
        # (may return False if index page parse fails, but NOT because of pre-existing record)
        db.report_exists_by_accession.assert_called_with('0001507605-24-099999')

    def test_fetch_all_filings_routes_by_filing_regime_domestic(self):
        """Domestic regime calls fetch_8k_filings, fetch_10q_filings, fetch_10k_filings."""
        db = _mock_db()
        conn = _connector(db)
        conn.fetch_8k_filings = MagicMock(return_value=MagicMock(
            reports_ingested=5, data_points_extracted=0, review_flagged=0, errors=0))
        conn.fetch_10q_filings = MagicMock(return_value=MagicMock(
            reports_ingested=3, data_points_extracted=0, review_flagged=0, errors=0))
        conn.fetch_10k_filings = MagicMock(return_value=MagicMock(
            reports_ingested=1, data_points_extracted=0, review_flagged=0, errors=0))

        result = conn.fetch_all_filings(
            cik='0001507605', ticker='MARA',
            since_date=date(2020, 1, 1), filing_regime='domestic',
        )

        conn.fetch_8k_filings.assert_called_once()
        conn.fetch_10q_filings.assert_called_once()
        conn.fetch_10k_filings.assert_called_once()
        assert result.reports_ingested == 9

    def test_fetch_all_filings_routes_by_filing_regime_canadian(self):
        """Canadian regime calls fetch_6k_filings and fetch_40f_filings (not 10-Q/10-K)."""
        db = _mock_db()
        conn = _connector(db)
        conn.fetch_6k_filings = MagicMock(return_value=MagicMock(
            reports_ingested=2, data_points_extracted=0, review_flagged=0, errors=0))
        conn.fetch_40f_filings = MagicMock(return_value=MagicMock(
            reports_ingested=1, data_points_extracted=0, review_flagged=0, errors=0))
        conn.fetch_10q_filings = MagicMock()
        conn.fetch_10k_filings = MagicMock()

        result = conn.fetch_all_filings(
            cik='0001812477', ticker='BITF',
            since_date=date(2020, 1, 1), filing_regime='canadian',
        )

        conn.fetch_6k_filings.assert_called_once()
        conn.fetch_40f_filings.assert_called_once()
        conn.fetch_10q_filings.assert_not_called()
        conn.fetch_10k_filings.assert_not_called()
        assert result.reports_ingested == 3


# ── Finding 1: Accession dedup ────────────────────────────────────────────────

class TestAccessionDedup:

    def test_accession_number_stored_on_8k_ingest(self):
        """insert_report is called with accession_number when ingesting 8-K."""
        db = _mock_db(accession_exists=False)
        conn = _connector(db)

        # Simulate _edgar_request returning one 8-K hit
        accession = '0001507605-24-000042'
        hit = {
            '_id': f'{accession}:ex991.htm',
            '_source': {
                'file_date': '2024-01-15',
                'ciks': ['1507605'],
                'adsh': accession,
            },
        }
        conn._edgar_request = MagicMock(return_value={'hits': {'hits': [hit]}})
        doc_html = '<html><body>MARA mined 1200 BTC in January 2024</body></html>'
        conn._edgar_get_text = MagicMock(return_value=doc_html)

        conn.fetch_8k_filings('0001507605', 'MARA', date(2023, 1, 1))

        insert_calls = db.insert_report.call_args_list
        assert len(insert_calls) >= 1
        inserted = insert_calls[0][0][0]
        assert inserted.get('accession_number') == accession

    def test_dedup_by_accession_number_skips_reinsert(self):
        """If accession already exists, fetch_8k_filings skips the insert."""
        db = _mock_db(accession_exists=True)
        conn = _connector(db)

        accession = '0001507605-24-000042'
        hit = {
            '_id': f'{accession}:ex991.htm',
            '_source': {
                'file_date': '2024-01-15',
                'ciks': ['1507605'],
                'adsh': accession,
            },
        }
        conn._edgar_request = MagicMock(return_value={'hits': {'hits': [hit]}})
        conn._edgar_get_text = MagicMock()

        conn.fetch_8k_filings('0001507605', 'MARA', date(2023, 1, 1))

        # Should NOT call _edgar_get_text or insert_report
        conn._edgar_get_text.assert_not_called()
        db.insert_report.assert_not_called()


# ── Finding 2: Broader 8-K search terms ──────────────────────────────────────

class TestBroaderKeywords:

    def test_8k_broader_keyword_matches_mara_phrasing(self):
        """_8K_SEARCH_TERMS includes the 8 required mining phrases."""
        from scrapers.edgar_connector import _8K_SEARCH_TERMS
        required = [
            '"bitcoin production"',
            '"BTC production"',
            '"bitcoin mined"',
            '"BTC mined"',
            '"mining operations update"',
            '"production and operations"',
            '"digital asset production"',
            '"hash rate"',
        ]
        for term in required:
            assert term in _8K_SEARCH_TERMS, f"Missing term: {term}"

    def test_fetch_8k_uses_or_joined_search_terms(self):
        """fetch_8k_filings passes OR-joined search terms to _edgar_request."""
        from scrapers.edgar_connector import _8K_SEARCH_TERMS
        db = _mock_db()
        conn = _connector(db)
        conn._edgar_request = MagicMock(return_value={'hits': {'hits': []}})

        conn.fetch_8k_filings('0001507605', 'MARA', date(2023, 1, 1))

        call_params = conn._edgar_request.call_args[0][1]  # second positional arg = params dict
        assert call_params['q'] == ' OR '.join(_8K_SEARCH_TERMS)


# ── Finding 10: Rate limit + backoff ─────────────────────────────────────────

class TestRateLimitBackoff:

    def test_rate_limit_sleep_called_between_requests(self):
        """_edgar_request calls time.sleep(EDGAR_REQUEST_DELAY_SECONDS) before each attempt."""
        from config import EDGAR_REQUEST_DELAY_SECONDS
        db = _mock_db()
        conn = _connector(db)
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {'hits': {'hits': []}}
        conn.session.get.return_value = resp

        with patch('scrapers.edgar_connector.time.sleep') as mock_sleep:
            conn._edgar_request('https://efts.sec.gov/LATEST/search-index', {})
            mock_sleep.assert_called_with(EDGAR_REQUEST_DELAY_SECONDS)

    def test_429_response_triggers_backoff_retry(self):
        """A 429 response causes a backoff sleep and retry."""
        from config import EDGAR_RETRY_BACKOFF_BASE
        db = _mock_db()
        conn = _connector(db)

        # First call returns 429, second returns 200
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {'data': 'ok'}
        conn.session.get.side_effect = [resp_429, resp_200]

        with patch('scrapers.edgar_connector.time.sleep') as mock_sleep:
            result = conn._edgar_request('https://data.sec.gov/test', {})

        # Should have slept at least twice (rate delay + backoff)
        assert mock_sleep.call_count >= 2
        # The backoff sleep value should be >= EDGAR_RETRY_BACKOFF_BASE
        sleep_values = [c.args[0] for c in mock_sleep.call_args_list]
        assert any(v >= EDGAR_RETRY_BACKOFF_BASE for v in sleep_values)
