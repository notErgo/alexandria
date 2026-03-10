"""Tests for parallel EDGAR and IR ingest worker pools."""
import threading
from unittest.mock import MagicMock, patch, call
import pytest


def _make_company(ticker, cik):
    return {'ticker': ticker, 'cik': cik, 'filing_regime': 'domestic',
            'scraper_mode': 'skip', 'active': True}


def _make_ingest_summary(ingested=1, errors=0):
    from miner_types import IngestSummary
    s = IngestSummary()
    s.reports_ingested = ingested
    s.errors = errors
    return s


class TestEdgarIngestPool:

    def test_each_company_gets_own_connector(self, tmp_path):
        """Each worker creates its own EdgarConnector (not sharing a session)."""
        from cli import _run_edgar_ingest_pool
        from datetime import date

        connector_instances = []

        class _TrackingConnector:
            def __init__(self, db, session):
                connector_instances.append(self)
                self._db = db

            def fetch_all_filings(self, cik, ticker, since_date, filing_regime='domestic'):
                return _make_ingest_summary()

        companies = [
            _make_company('MARA', '0001507605'),
            _make_company('RIOT', '0001167419'),
            _make_company('CLSK', '0000827876'),
        ]

        db_path = str(tmp_path / 'test.db')
        with patch('cli.EdgarConnector', _TrackingConnector), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_edgar_ingest_pool(db_path, companies, date(2022, 1, 1), num_workers=3)

        assert len(connector_instances) == 3, \
            "Each company must get its own EdgarConnector instance"

    def test_results_aggregated_across_workers(self, tmp_path):
        """Total reports_ingested sums across all company workers."""
        from cli import _run_edgar_ingest_pool
        from datetime import date

        def _fake_fetch(cik, ticker, since_date, filing_regime='domestic'):
            return _make_ingest_summary(ingested=5)

        companies = [_make_company('MARA', '0001507605'),
                     _make_company('RIOT', '0001167419')]
        db_path = str(tmp_path / 'test.db')

        mock_connector = MagicMock()
        mock_connector.return_value.fetch_all_filings.side_effect = _fake_fetch

        with patch('cli.EdgarConnector', mock_connector), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_edgar_ingest_pool(db_path, companies, date(2022, 1, 1), num_workers=2)

        assert result.reports_ingested == 10

    def test_company_without_cik_is_skipped(self, tmp_path):
        """Companies with cik=None are skipped without error."""
        from cli import _run_edgar_ingest_pool
        from datetime import date

        fetch_calls = []

        def _fake_fetch(cik, ticker, since_date, filing_regime='domestic'):
            fetch_calls.append(ticker)
            return _make_ingest_summary()

        companies = [
            _make_company('MARA', '0001507605'),
            {'ticker': 'ABTC', 'cik': None, 'filing_regime': 'domestic',
             'scraper_mode': 'skip', 'active': True},
        ]
        db_path = str(tmp_path / 'test.db')

        mock_connector = MagicMock()
        mock_connector.return_value.fetch_all_filings.side_effect = _fake_fetch

        with patch('cli.EdgarConnector', mock_connector), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_edgar_ingest_pool(db_path, companies, date(2022, 1, 1), num_workers=2)

        assert 'ABTC' not in fetch_calls, "ABTC (no CIK) must not trigger a fetch"
        assert 'MARA' in fetch_calls

    def test_one_company_error_does_not_abort_others(self, tmp_path):
        """Exception in one worker increments errors but lets others complete."""
        from cli import _run_edgar_ingest_pool
        from datetime import date

        def _fake_fetch(cik, ticker, since_date, filing_regime='domestic'):
            if ticker == 'RIOT':
                raise RuntimeError("network failure")
            return _make_ingest_summary(ingested=3)

        companies = [_make_company('MARA', '0001507605'),
                     _make_company('RIOT', '0001167419'),
                     _make_company('CLSK', '0000827876')]
        db_path = str(tmp_path / 'test.db')

        mock_connector = MagicMock()
        mock_connector.return_value.fetch_all_filings.side_effect = _fake_fetch

        with patch('cli.EdgarConnector', mock_connector), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_edgar_ingest_pool(db_path, companies, date(2022, 1, 1), num_workers=3)

        assert result.errors == 1
        assert result.reports_ingested == 6  # MARA + CLSK succeeded

    def test_filing_regime_forwarded_to_fetch(self, tmp_path):
        """filing_regime from company config is forwarded to fetch_all_filings."""
        from cli import _run_edgar_ingest_pool
        from datetime import date

        regime_seen = []

        def _fake_fetch(cik, ticker, since_date, filing_regime='domestic'):
            regime_seen.append(filing_regime)
            return _make_ingest_summary()

        companies = [{'ticker': 'IREN', 'cik': '0001878848',
                      'filing_regime': 'foreign', 'scraper_mode': 'skip', 'active': True}]
        db_path = str(tmp_path / 'test.db')

        mock_connector = MagicMock()
        mock_connector.return_value.fetch_all_filings.side_effect = _fake_fetch

        with patch('cli.EdgarConnector', mock_connector), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            _run_edgar_ingest_pool(db_path, companies, date(2022, 1, 1), num_workers=1)

        assert regime_seen == ['foreign']


class TestIRIngestPool:

    def test_each_company_gets_own_scraper(self, tmp_path):
        """Each IR worker creates its own IRScraper (isolated session)."""
        from cli import _run_ir_ingest_pool

        scraper_instances = []

        class _TrackingScraper:
            def __init__(self, db, session):
                scraper_instances.append(self)

            def scrape_company(self, company):
                return _make_ingest_summary()

        companies = [_make_company('MARA', '0001507605'),
                     _make_company('WULF', '0001083301')]
        db_path = str(tmp_path / 'test.db')

        with patch('cli.IRScraper', _TrackingScraper), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_ir_ingest_pool(db_path, companies, num_workers=2)

        assert len(scraper_instances) == 2

    def test_ir_results_aggregated(self, tmp_path):
        """Total ingested sums across all IR workers."""
        from cli import _run_ir_ingest_pool

        mock_scraper = MagicMock()
        mock_scraper.return_value.scrape_company.return_value = _make_ingest_summary(ingested=2)

        companies = [_make_company('MARA', '0001507605'),
                     _make_company('RIOT', '0001167419'),
                     _make_company('CLSK', '0000827876')]
        db_path = str(tmp_path / 'test.db')

        with patch('cli.IRScraper', mock_scraper), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_ir_ingest_pool(db_path, companies, num_workers=3)

        assert result.reports_ingested == 6

    def test_ir_error_counted_not_raised(self, tmp_path):
        """Exception scraping one IR company increments errors; others complete."""
        from cli import _run_ir_ingest_pool

        def _fake_scrape(company):
            if company['ticker'] == 'RIOT':
                raise RuntimeError("timeout")
            return _make_ingest_summary(ingested=1)

        mock_scraper = MagicMock()
        mock_scraper.return_value.scrape_company.side_effect = _fake_scrape

        companies = [_make_company('MARA', '0001507605'),
                     _make_company('RIOT', '0001167419')]
        db_path = str(tmp_path / 'test.db')

        with patch('cli.IRScraper', mock_scraper), \
             patch('cli.MinerDB'), \
             patch('cli.requests.Session'):
            result = _run_ir_ingest_pool(db_path, companies, num_workers=2)

        assert result.errors == 1
        assert result.reports_ingested == 1
