"""Tests for EDGAR filing regime routing fixes (schema v16, issue #1 and #2)."""
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch, call

from scrapers.edgar_connector import period_of_report_to_covering_period


class TestPeriodOfReportAnnualForms(unittest.TestCase):
    """Issue #2: 20-F and 40-F must map to FY, not Q4."""

    def test_period_of_report_annual_20f(self):
        result = period_of_report_to_covering_period('2024-12-31', '20-F')
        self.assertEqual(result, '2024-FY')

    def test_period_of_report_annual_40f(self):
        result = period_of_report_to_covering_period('2024-03-31', '40-F')
        self.assertEqual(result, '2024-FY')

    def test_period_of_report_amended_20fa(self):
        result = period_of_report_to_covering_period('2023-12-31', '20-F/A')
        self.assertEqual(result, '2023-FY')

    def test_period_of_report_amended_40fa(self):
        result = period_of_report_to_covering_period('2023-03-31', '40-F/A')
        self.assertEqual(result, '2023-FY')

    def test_period_of_report_10k_unchanged(self):
        result = period_of_report_to_covering_period('2024-12-31', '10-K')
        self.assertEqual(result, '2024-FY')

    def test_period_of_report_10q_unchanged(self):
        result = period_of_report_to_covering_period('2024-03-31', '10-Q')
        self.assertEqual(result, '2024-Q1')

    def test_period_of_report_20f_march_fy(self):
        # HIVE fiscal year ends March 31
        result = period_of_report_to_covering_period('2025-03-31', '40-F')
        self.assertEqual(result, '2025-FY')

    def test_period_of_report_6k_quarterly(self):
        # 6-K is a current report (periodic), not annual — maps to quarter
        result = period_of_report_to_covering_period('2024-09-30', '6-K')
        self.assertEqual(result, '2024-Q3')


class TestFilingRegimePassedToConnector(unittest.TestCase):
    """Issue #1: filing_regime from company row must be passed to fetch_all_filings()."""

    def test_filing_regime_passed_for_foreign_company(self):
        from routes.reports import _run_edgar_ingest

        mock_db = MagicMock()
        mock_db.get_companies.return_value = [
            {
                'ticker': 'HIVE',
                'cik': '0001382101',
                'filing_regime': 'canadian',
                'active': True,
            }
        ]
        mock_connector = MagicMock()
        mock_connector.fetch_all_filings.return_value = MagicMock(
            reports_ingested=0, errors=0
        )

        # Both get_db and EdgarConnector are local imports inside _run_edgar_ingest.
        # Patch at their source module namespaces so the local from-imports pick them up.
        with patch('app_globals.get_db', return_value=mock_db), \
             patch('scrapers.edgar_connector.EdgarConnector', return_value=mock_connector), \
             patch('routes.reports._update_progress'):
            _run_edgar_ingest('task-1', auto_extract=False, warm_model=False)

        mock_connector.fetch_all_filings.assert_called_once()
        call_kwargs = mock_connector.fetch_all_filings.call_args
        regime = call_kwargs.kwargs.get('filing_regime')
        self.assertEqual(regime, 'canadian')

    def test_filing_regime_defaults_to_domestic(self):
        """Company without explicit filing_regime defaults to domestic."""
        from routes.reports import _run_edgar_ingest

        mock_db = MagicMock()
        mock_db.get_companies.return_value = [
            {
                'ticker': 'MARA',
                'cik': '0001507605',
                # no filing_regime key — must default to 'domestic'
                'active': True,
            }
        ]
        mock_connector = MagicMock()
        mock_connector.fetch_all_filings.return_value = MagicMock(
            reports_ingested=0, errors=0
        )

        with patch('app_globals.get_db', return_value=mock_db), \
             patch('scrapers.edgar_connector.EdgarConnector', return_value=mock_connector), \
             patch('routes.reports._update_progress'):
            _run_edgar_ingest('task-2', auto_extract=False, warm_model=False)

        call_kwargs = mock_connector.fetch_all_filings.call_args
        regime = call_kwargs.kwargs.get('filing_regime', 'domestic')
        self.assertEqual(regime, 'domestic')


class TestSyncCompaniesPersistsFilingRegime(unittest.TestCase):
    """Issue #1: sync_companies_from_config() must persist filing_regime and fiscal_year_end_month."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name

    def tearDown(self):
        os.unlink(self.db_path)

    def test_filing_regime_persisted_on_insert(self):
        from infra.db import MinerDB
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump([{
                'ticker': 'BITF',
                'name': 'Bitfarms Ltd.',
                'tier': 1,
                'ir_url': 'https://investor.bitfarms.com',
                'cik': '0001741231',
                'active': True,
                'scraper_mode': 'template',
                'filing_regime': 'canadian',
                'fiscal_year_end_month': 12,
            }], f)
            cfg_path = f.name
        try:
            db = MinerDB(self.db_path)
            db.sync_companies_from_config(cfg_path)
            row = db.get_company('BITF')
            self.assertIsNotNone(row)
            self.assertEqual(row['filing_regime'], 'canadian')
            self.assertEqual(row['fiscal_year_end_month'], 12)
        finally:
            os.unlink(cfg_path)

    def test_fiscal_year_end_month_persisted_on_update(self):
        from infra.db import MinerDB
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump([{
                'ticker': 'HIVE',
                'name': 'HIVE Digital Technologies Ltd.',
                'tier': 2,
                'ir_url': 'https://www.hivedigitaltechnologies.com/news/',
                'cik': '0001382101',
                'active': True,
                'scraper_mode': 'skip',
                'filing_regime': 'canadian',
                'fiscal_year_end_month': 3,
            }], f)
            cfg_path = f.name
        try:
            db = MinerDB(self.db_path)
            # Insert first pass
            db.sync_companies_from_config(cfg_path)
            # Update second pass (should keep values)
            db.sync_companies_from_config(cfg_path)
            row = db.get_company('HIVE')
            self.assertEqual(row['filing_regime'], 'canadian')
            self.assertEqual(row['fiscal_year_end_month'], 3)
        finally:
            os.unlink(cfg_path)

    def test_filing_regime_defaults_to_domestic_when_missing(self):
        from infra.db import MinerDB
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump([{
                'ticker': 'MARA',
                'name': 'MARA Holdings, Inc.',
                'tier': 1,
                'ir_url': 'https://ir.mara.com',
                'cik': '0001507605',
                'active': True,
                'scraper_mode': 'rss',
                # No filing_regime key
            }], f)
            cfg_path = f.name
        try:
            db = MinerDB(self.db_path)
            db.sync_companies_from_config(cfg_path)
            row = db.get_company('MARA')
            self.assertEqual(row['filing_regime'], 'domestic')
            self.assertEqual(row['fiscal_year_end_month'], 12)
        finally:
            os.unlink(cfg_path)


class TestForeignFormExtractionRouting(unittest.TestCase):
    """Fix 2: edgar_20f/40f must be in _ANNUAL_SOURCES; edgar_6k in _QUARTERLY_SOURCES."""

    def test_edgar_20f_in_annual_sources(self):
        from extractors.extraction_pipeline import _ANNUAL_SOURCES
        self.assertIn('edgar_20f', _ANNUAL_SOURCES)

    def test_edgar_40f_in_annual_sources(self):
        from extractors.extraction_pipeline import _ANNUAL_SOURCES
        self.assertIn('edgar_40f', _ANNUAL_SOURCES)

    def test_edgar_6k_in_quarterly_sources(self):
        from extractors.extraction_pipeline import _QUARTERLY_SOURCES
        self.assertIn('edgar_6k', _QUARTERLY_SOURCES)

    def test_edgar_20f_in_context_window_quarterly_sources(self):
        from extractors.context_window import _QUARTERLY_SOURCES
        self.assertIn('edgar_20f', _QUARTERLY_SOURCES)

    def test_edgar_40f_in_context_window_quarterly_sources(self):
        from extractors.context_window import _QUARTERLY_SOURCES
        self.assertIn('edgar_40f', _QUARTERLY_SOURCES)

    def test_edgar_6k_in_context_window_quarterly_sources(self):
        from extractors.context_window import _QUARTERLY_SOURCES
        self.assertIn('edgar_6k', _QUARTERLY_SOURCES)

    def test_context_window_selector_edgar_20f_uses_large_budget(self):
        from extractors.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET_QUARTERLY
        selector = ContextWindowSelector(doc_type='edgar_20f')
        self.assertEqual(selector.char_budget, CONTEXT_CHAR_BUDGET_QUARTERLY)

    def test_context_window_selector_edgar_40f_uses_large_budget(self):
        from extractors.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET_QUARTERLY
        selector = ContextWindowSelector(doc_type='edgar_40f')
        self.assertEqual(selector.char_budget, CONTEXT_CHAR_BUDGET_QUARTERLY)

    def test_extract_report_routes_20f_to_quarterly_path(self):
        """extract_report with edgar_20f source_type must invoke _extract_quarterly_report."""
        from unittest.mock import patch, MagicMock
        from extractors.extraction_pipeline import extract_report

        report = {
            'id': 1, 'ticker': 'HIVE', 'report_date': '2024-03-31',
            'source_type': 'edgar_20f',
            'raw_text': 'Annual report text for HIVE fiscal year 2024.',
            'covering_period': '2024-FY',
        }
        db = MagicMock()
        db.mark_report_extraction_running.return_value = None
        registry = MagicMock()
        registry.metrics = {'production_btc': []}

        with patch('extractors.extraction_pipeline._extract_quarterly_report') as mock_qr:
            mock_qr.return_value = MagicMock(reports_processed=1, errors=0)
            extract_report(report, db, registry)

        mock_qr.assert_called_once()

    def test_extract_report_routes_40f_to_quarterly_path(self):
        """extract_report with edgar_40f source_type must invoke _extract_quarterly_report."""
        from unittest.mock import patch, MagicMock
        from extractors.extraction_pipeline import extract_report

        report = {
            'id': 2, 'ticker': 'BITF', 'report_date': '2024-12-31',
            'source_type': 'edgar_40f',
            'raw_text': 'Annual report text for Bitfarms fiscal year 2024.',
            'covering_period': '2024-FY',
        }
        db = MagicMock()
        db.mark_report_extraction_running.return_value = None
        registry = MagicMock()
        registry.metrics = {'production_btc': []}

        with patch('extractors.extraction_pipeline._extract_quarterly_report') as mock_qr:
            mock_qr.return_value = MagicMock(reports_processed=1, errors=0)
            extract_report(report, db, registry)

        mock_qr.assert_called_once()


if __name__ == '__main__':
    unittest.main()
