"""Tests for ScrapeWorker background thread."""
import pytest
from unittest.mock import patch, MagicMock, call


class TestScrapeWorker:

    def test_worker_skips_running_jobs_on_startup(self, db_with_active_company):
        """_reset_interrupted resets 'running' jobs back to 'pending'."""
        from scrapers.scrape_worker import ScrapeWorker
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.claim_scrape_job(job['id'])

        worker = ScrapeWorker(db_with_active_company)
        worker._reset_interrupted()

        pending = db_with_active_company.get_pending_scrape_jobs()
        assert any(j['id'] == job['id'] for j in pending)

    def test_worker_processes_pending_job(self, db_with_active_company):
        """_process_one claims a job, calls _execute_scrape, marks it done."""
        from scrapers.scrape_worker import ScrapeWorker
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')

        worker = ScrapeWorker(db_with_active_company)
        with patch.object(worker, '_execute_scrape', return_value=None):
            processed = worker._process_one()

        assert processed is True
        jobs = db_with_active_company.get_scrape_queue_status()
        done = next(j for j in jobs if j['id'] == job['id'])
        assert done['status'] == 'done'

    def test_worker_sets_error_on_exception(self, db_with_active_company):
        """_process_one marks job as 'error' when _execute_scrape raises."""
        from scrapers.scrape_worker import ScrapeWorker
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')

        worker = ScrapeWorker(db_with_active_company)
        with patch.object(worker, '_execute_scrape', side_effect=RuntimeError('network error')):
            worker._process_one()

        jobs = db_with_active_company.get_scrape_queue_status()
        failed = next(j for j in jobs if j['id'] == job['id'])
        assert failed['status'] == 'error'
        assert 'network error' in failed['error_msg']

    def test_worker_returns_false_when_no_jobs(self, db_with_active_company):
        """_process_one returns False when queue is empty."""
        from scrapers.scrape_worker import ScrapeWorker
        worker = ScrapeWorker(db_with_active_company)
        result = worker._process_one()
        assert result is False

    def test_execute_scrape_raises_when_ir_summary_all_errors(self, db_with_active_company):
        """_execute_scrape raises RuntimeError when IR scrape returns only errors and 0 reports."""
        from scrapers.scrape_worker import ScrapeWorker
        from miner_types import IngestSummary

        db_with_active_company.update_company_scraper_fields('MARA', scraper_mode='rss')
        worker = ScrapeWorker(db_with_active_company)

        failing_summary = IngestSummary(reports_ingested=0, errors=1)

        with patch('scrapers.ir_scraper.IRScraper') as MockIR, \
             patch('scrapers.edgar_connector.EdgarConnector'):
            MockIR.return_value.scrape_company.return_value = failing_summary
            job = {'ticker': 'MARA'}
            with pytest.raises(RuntimeError, match="1 error"):
                worker._execute_scrape(job)

        company = db_with_active_company.get_company('MARA')
        assert company['scraper_status'] == 'error'

    def test_execute_scrape_ok_when_partial_errors(self, db_with_active_company):
        """_execute_scrape completes normally when some reports were ingested despite errors."""
        from scrapers.scrape_worker import ScrapeWorker
        from miner_types import IngestSummary

        db_with_active_company.update_company_scraper_fields('MARA', scraper_mode='rss')
        worker = ScrapeWorker(db_with_active_company)

        partial_summary = IngestSummary(reports_ingested=3, errors=1)

        with patch('scrapers.ir_scraper.IRScraper') as MockIR, \
             patch('scrapers.edgar_connector.EdgarConnector'):
            MockIR.return_value.scrape_company.return_value = partial_summary
            job = {'ticker': 'MARA'}
            worker._execute_scrape(job)

        company = db_with_active_company.get_company('MARA')
        assert company['scraper_status'] == 'ok'
