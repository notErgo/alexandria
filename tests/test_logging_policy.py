"""
Phase 1b: Structured event= logging policy tests.
Tests that highest-value pipeline stages emit machine-parseable event= log lines.
These tests FAIL before Phase 3 fixes and PASS after.
"""
import logging
import pytest
from unittest.mock import MagicMock, patch


def _make_mock_db(source_type='ir_press_release'):
    """Build a minimal mock db for extract_report tests."""
    db = MagicMock()
    db.get_metric_schema.return_value = [
        {'key': 'production_btc', 'active': 1},
        {'key': 'holdings_btc', 'active': 1},
    ]
    db.get_metric_rules.return_value = []
    db.get_all_metric_keywords.return_value = [
        {'phrase': 'bitcoin production', 'active': 1, 'metric_key': 'production_btc', 'id': 1, 'exclude_terms': '', 'hit_count': 0}
    ]
    db.mark_report_extraction_running.return_value = None
    db.mark_report_extracted.return_value = None
    db.mark_report_extraction_failed.return_value = None
    db.get_config.return_value = None
    db.insert_benchmark_run.return_value = None
    db.update_report_summary.return_value = None
    return db


def _make_mock_report(source_type='ir_press_release'):
    return {
        'id': 1,
        'ticker': 'MARA',
        'report_date': '2024-01-01',
        'source_type': source_type,
        'raw_text': 'MARA bitcoin mined 1234 BTC in January 2024. Hash rate reached 20 EH/s.',
        'raw_html': None,
    }


def _make_mock_registry():
    registry = MagicMock()
    registry.metrics = {'production_btc': MagicMock()}
    return registry


class TestExtractReportStructuredLogging:
    """extract_report() must emit structured event= log lines at start and completion."""

    def test_extract_report_emits_interpret_start(self, caplog):
        """extract_report must log event=interpret_start at entry."""
        from interpreters.interpret_pipeline import extract_report

        db = _make_mock_db()
        report = _make_mock_report()
        registry = _make_mock_registry()

        with caplog.at_level(logging.INFO, logger='miners.interpreters.interpret_pipeline'):
            with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm:
                mock_llm.return_value = MagicMock()
                mock_llm.return_value.check_connectivity.return_value = False
                with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                    extract_report(report, db, registry)

        log_text = ' '.join(r.message for r in caplog.records)
        assert 'event=interpret_start' in log_text, (
            f"Expected 'event=interpret_start' in logs. Got:\n{log_text}"
        )

    def test_extract_report_emits_interpret_complete(self, caplog):
        """extract_report must log event=interpret_complete on successful exit."""
        from interpreters.interpret_pipeline import extract_report

        db = _make_mock_db()
        report = _make_mock_report()
        registry = _make_mock_registry()

        with caplog.at_level(logging.INFO, logger='miners.interpreters.interpret_pipeline'):
            with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm:
                mock_llm.return_value = MagicMock()
                with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                    extract_report(report, db, registry)

        log_text = ' '.join(r.message for r in caplog.records)
        assert 'event=interpret_complete' in log_text, (
            f"Expected 'event=interpret_complete' in logs. Got:\n{log_text}"
        )


class TestScrapeWorkerStructuredLogging:
    """ScrapeWorker must emit structured event= log lines when claiming and completing jobs."""

    def _make_job(self):
        return {'id': 42, 'ticker': 'MARA', 'job_type': 'edgar'}

    def test_scrape_worker_emits_scrape_job_claimed(self, caplog):
        """_process_one must log event=scrape_job_claimed after claiming a job."""
        from scrapers.scrape_worker import ScrapeWorker

        db = MagicMock()
        db.get_pending_scrape_jobs.return_value = [self._make_job()]
        db.claim_scrape_job.return_value = None
        db.complete_scrape_job.return_value = None
        db.fail_scrape_job.return_value = None
        db.get_company.return_value = {'ticker': 'MARA', 'cik': None, 'filing_regime': 'domestic'}
        db.update_company_scraper_fields.return_value = None

        worker = ScrapeWorker(db)

        with caplog.at_level(logging.INFO, logger='miners.scrapers.scrape_worker'):
            worker._process_one()

        log_text = ' '.join(r.message for r in caplog.records)
        assert 'event=scrape_job_claimed' in log_text, (
            f"Expected 'event=scrape_job_claimed' in logs. Got:\n{log_text}"
        )

    def test_scrape_worker_emits_scrape_job_complete(self, caplog):
        """_process_one must log event=scrape_job_complete after job finishes."""
        from scrapers.scrape_worker import ScrapeWorker

        db = MagicMock()
        db.get_pending_scrape_jobs.return_value = [self._make_job()]
        db.claim_scrape_job.return_value = None
        db.complete_scrape_job.return_value = None
        db.fail_scrape_job.return_value = None
        db.get_company.return_value = {'ticker': 'MARA', 'cik': None, 'filing_regime': 'domestic'}
        db.update_company_scraper_fields.return_value = None

        worker = ScrapeWorker(db)

        with caplog.at_level(logging.INFO, logger='miners.scrapers.scrape_worker'):
            worker._process_one()

        log_text = ' '.join(r.message for r in caplog.records)
        assert 'event=scrape_job_complete' in log_text, (
            f"Expected 'event=scrape_job_complete' in logs. Got:\n{log_text}"
        )
