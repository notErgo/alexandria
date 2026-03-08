"""Background thread that processes the scrape_queue FIFO, one job at a time."""
import threading
import time
import logging
from datetime import datetime, timezone

from infra.db import MinerDB

log = logging.getLogger('miners.scrapers.scrape_worker')

_POLL_INTERVAL = 5  # seconds between queue checks when idle


class ScrapeWorker(threading.Thread):
    """FIFO scrape queue worker. Processes one job at a time."""

    def __init__(self, db: MinerDB):
        super().__init__(daemon=True, name='ScrapeWorker')
        self._db = db
        self._stop = threading.Event()
        self._current_job_id = None
        self._lock = threading.Lock()

    def stop(self) -> None:
        """Signal the worker to stop after the current job finishes."""
        self._stop.set()

    def _reset_interrupted(self) -> None:
        """Reset any jobs left in 'running' state from a previous process crash."""
        count = self._db.reset_interrupted_scrape_jobs()
        if count:
            log.info("Reset %d interrupted scrape jobs to pending", count)

    def _process_one(self) -> bool:
        """Claim and execute one pending job.

        Returns True if a job was found and processed (success or error).
        Returns False if queue was empty.
        """
        jobs = self._db.get_pending_scrape_jobs()
        if not jobs:
            return False

        job = jobs[0]
        job_id = job['id']
        self._db.claim_scrape_job(job_id)
        with self._lock:
            self._current_job_id = job_id

        try:
            self._execute_scrape(job)
            self._db.complete_scrape_job(job_id)
            log.info("Scrape job %d (%s) completed successfully", job_id, job['ticker'])
        except Exception as e:
            log.error("Scrape job %d (%s) failed: %s", job_id, job['ticker'], e, exc_info=True)
            self._db.fail_scrape_job(job_id, str(e))
        finally:
            with self._lock:
                self._current_job_id = None

        return True

    def _execute_scrape(self, job: dict) -> None:
        """Run the actual scrape for a job. Raises on failure.

        Updates company scraper_status to 'running' before scraping, then
        'ok' or 'error' on completion.
        """
        ticker = job['ticker']
        company = self._db.get_company(ticker)
        if company is None:
            raise ValueError(f"Company not found: {ticker}")

        self._db.update_company_scraper_fields(ticker, scraper_status='running')
        try:
            import requests as _req
            from scrapers.ir_scraper import IRScraper
            scraper = IRScraper(db=self._db, session=_req.Session())
            scraper.scrape_company(company)
            self._db.update_company_scraper_fields(
                ticker,
                scraper_status='ok',
                last_scrape_at=datetime.now(timezone.utc).isoformat(),
                last_scrape_error=None,
            )
            log.info("Scrape completed for %s", ticker)

            # EDGAR fetch: 8-K + 10-Q + 10-K for companies with CIK (ingest only, no extraction)
            cik = company.get('cik')
            if cik:
                try:
                    import requests as _req
                    from scrapers.edgar_connector import EdgarConnector
                    from datetime import date
                    edgar = EdgarConnector(db=self._db, session=_req.Session())
                    since = date(2019, 1, 1)
                    edgar.fetch_all_filings(
                        cik=cik,
                        ticker=ticker,
                        since_date=since,
                        filing_regime=company.get('filing_regime', 'domestic'),
                    )
                    log.info("EDGAR fetch completed for %s", ticker)
                except Exception as ex:
                    log.error("EDGAR fetch failed for %s: %s", ticker, ex, exc_info=True)

        except Exception as e:
            self._db.update_company_scraper_fields(
                ticker,
                scraper_status='error',
                last_scrape_error=str(e),
            )
            raise

    def run(self) -> None:
        """Main loop: poll for pending jobs, sleep when idle."""
        log.info("ScrapeWorker started")
        self._reset_interrupted()
        while not self._stop.is_set():
            try:
                found = self._process_one()
            except Exception as e:
                log.error("Unexpected error in ScrapeWorker loop: %s", e, exc_info=True)
                found = False
            if not found:
                self._stop.wait(timeout=_POLL_INTERVAL)
        log.info("ScrapeWorker stopped")
