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
        ticker = job['ticker']
        self._db.claim_scrape_job(job_id)
        with self._lock:
            self._current_job_id = job_id
        log.info("event=scrape_job_claimed job_id=%s ticker=%s", job_id, ticker)

        import time as _time
        _t0 = _time.monotonic()
        try:
            self._execute_scrape(job)
            self._db.complete_scrape_job(job_id)
            elapsed_ms = int((_time.monotonic() - _t0) * 1000)
            log.info(
                "event=scrape_job_complete job_id=%s ticker=%s status=ok elapsed_ms=%s",
                job_id, ticker, elapsed_ms,
            )
        except Exception as e:
            elapsed_ms = int((_time.monotonic() - _t0) * 1000)
            log.error(
                "event=scrape_job_error job_id=%s ticker=%s status=error elapsed_ms=%s error=%s",
                job_id, ticker, elapsed_ms, e, exc_info=True,
            )
            self._db.fail_scrape_job(job_id, str(e))
        finally:
            with self._lock:
                self._current_job_id = None

        return True

    def _execute_scrape(self, job: dict) -> None:
        """Full ingest (IR + EDGAR) for one queued scrape job.

        Stage 1: IR press releases via IRScraper.scrape_company() — skipped if
                 scraper_mode is 'skip' or missing.
        Stage 2: EDGAR filings via EdgarConnector.fetch_all_filings() — skipped
                 if the company has no CIK.

        Updates company scraper_status to 'running' before scraping, then
        'ok' or 'error' on completion.
        """
        import requests as _req
        from scrapers.ir_scraper import IRScraper
        from scrapers.edgar_connector import EdgarConnector
        from datetime import date

        ticker = job['ticker']
        company = self._db.get_company(ticker)
        if company is None:
            raise ValueError(f"Company not found: {ticker}")

        self._db.update_company_scraper_fields(ticker, scraper_status='running')
        try:
            session = _req.Session()

            # Stage 1 — IR press releases
            scraper_mode = company.get('scraper_mode') or company.get('scrape_mode', 'skip')
            if scraper_mode and scraper_mode != 'skip':
                log.info(
                    "event=ir_scrape_start ticker=%s mode=%s",
                    ticker, scraper_mode,
                )
                ir = IRScraper(db=self._db, session=session)
                ir.scrape_company(company)
                log.info("event=ir_scrape_end ticker=%s", ticker)
            else:
                log.info("event=ir_scrape_skip ticker=%s reason=mode=%s", ticker, scraper_mode)

            # Stage 2 — EDGAR filings
            cik = company.get('cik')
            if cik:
                log.info("event=edgar_fetch_start ticker=%s cik=%s", ticker, cik)
                edgar = EdgarConnector(db=self._db, session=session)
                since = company.get('btc_first_filing_date')
                since_date = (
                    date.fromisoformat(since) if since
                    else date(2019, 1, 1)
                )
                edgar.fetch_all_filings(
                    cik=cik,
                    ticker=ticker,
                    since_date=since_date,
                    filing_regime=company.get('filing_regime', 'domestic'),
                )
                log.info("event=edgar_fetch_end ticker=%s", ticker)
            else:
                log.info("event=edgar_fetch_skip ticker=%s reason=no_cik", ticker)

            self._db.update_company_scraper_fields(
                ticker,
                scraper_status='ok',
                last_scrape_at=datetime.now(timezone.utc).isoformat(),
                last_scrape_error=None,
            )
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
