"""Tests for MinerDB scrape_queue CRUD methods."""
import pytest


class TestScrapeQueueDB:

    def test_enqueue_scrape_job_creates_pending_row(self, db_with_active_company):
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        assert job['status'] == 'pending'
        assert job['ticker'] == 'MARA'
        assert job['mode'] == 'historic'

    def test_get_pending_scrape_jobs_returns_fifo_order(self, db_with_active_company):
        db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        jobs = db_with_active_company.get_pending_scrape_jobs()
        assert len(jobs) == 2
        assert jobs[0]['id'] < jobs[1]['id']

    def test_claim_scrape_job_sets_running(self, db_with_active_company):
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.claim_scrape_job(job['id'])
        jobs = db_with_active_company.get_scrape_queue_status()
        claimed = next(j for j in jobs if j['id'] == job['id'])
        assert claimed['status'] == 'running'
        assert claimed['started_at'] is not None

    def test_complete_scrape_job_sets_done(self, db_with_active_company):
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.claim_scrape_job(job['id'])
        db_with_active_company.complete_scrape_job(job['id'])
        jobs = db_with_active_company.get_scrape_queue_status()
        done = next(j for j in jobs if j['id'] == job['id'])
        assert done['status'] == 'done'
        assert done['completed_at'] is not None

    def test_fail_scrape_job_sets_error(self, db_with_active_company):
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.claim_scrape_job(job['id'])
        db_with_active_company.fail_scrape_job(job['id'], 'network error')
        jobs = db_with_active_company.get_scrape_queue_status()
        failed = next(j for j in jobs if j['id'] == job['id'])
        assert failed['status'] == 'error'
        assert 'network error' in failed['error_msg']

    def test_reset_running_jobs_on_restart(self, db_with_active_company):
        job = db_with_active_company.enqueue_scrape_job('MARA', 'historic')
        db_with_active_company.claim_scrape_job(job['id'])
        count = db_with_active_company.reset_interrupted_scrape_jobs()
        assert count == 1
        jobs = db_with_active_company.get_pending_scrape_jobs()
        assert any(j['id'] == job['id'] for j in jobs)

    def test_enqueue_scrape_job_is_accepted(self, db):
        # All companies can be enqueued regardless of mode; EDGAR is always attempted.
        job = db.enqueue_scrape_job('MARA', 'historic')
        assert job['ticker'] == 'MARA'
        assert job['status'] == 'pending'
