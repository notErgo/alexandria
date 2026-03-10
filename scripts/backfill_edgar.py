"""Reset btc_first_filing_date and backfill EDGAR filings from the true pivot date.

Problem being solved
--------------------
All companies were stamped with btc_first_filing_date='2023-05-19' via an earlier
detection pass that returned the same date for every ticker.  That date is wrong
for companies whose Bitcoin mining activity predates 2023 (MARA, RIOT, CLSK, HIVE …).

Because both the pipeline extraction gate and the EDGAR ingestion window use
btc_first_filing_date as a floor, two things broke:
  1. IR/archive press releases from 2021-2022 were silently skipped during extraction.
  2. EDGAR filings from before 2023-06 were never ingested.

This script fixes (2) — it clears the cached date and re-runs EDGAR ingestion from a
safe early floor, allowing detect_btc_first_filing_date() to re-detect the correct
pivot from EDGAR EFTS for each company.

Fix (1) (the extraction gate) is handled by the pipeline code change in
routes/pipeline.py (_build_extraction_batch).

Usage
-----
    cd OffChain/miners
    # backfill all companies (default since=2018-01-01)
    python3 scripts/backfill_edgar.py

    # backfill specific tickers
    python3 scripts/backfill_edgar.py --tickers MARA RIOT CLSK

    # override early floor date
    python3 scripts/backfill_edgar.py --since 2017-01-01

    # dry-run: show what would be done without hitting EDGAR
    python3 scripts/backfill_edgar.py --dry-run
"""
import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from infra.logging_config import setup_logging
setup_logging()

from infra.db import MinerDB
from config import DATA_DIR
from scrapers.edgar_connector import EdgarConnector

import requests as req_lib

log = logging.getLogger('miners.backfill_edgar')


def _reset_and_fetch(
    company: dict,
    db: MinerDB,
    connector: EdgarConnector,
    since: date,
    dry_run: bool,
) -> dict:
    ticker = company['ticker']
    cik = company.get('cik')
    if not cik:
        log.info("Skipping %s: no CIK", ticker)
        return {'ticker': ticker, 'status': 'skipped', 'reason': 'no_cik'}

    old_date = db.get_btc_first_filing_date(ticker)
    log.info(
        "event=backfill_start ticker=%s cik=%s old_btc_first_filing_date=%s since=%s",
        ticker, cik, old_date, since,
    )

    if dry_run:
        log.info("DRY RUN: would set btc_first_filing_date=%s and re-ingest EDGAR for %s", since, ticker)
        return {'ticker': ticker, 'status': 'dry_run', 'old_date': old_date}

    # Pre-seed btc_first_filing_date with the backfill since_date.
    # detect_btc_first_filing_date() uses EDGAR EFTS entity search which does NOT
    # filter by CIK — it treats the numeric CIK as an entity name text query and
    # returns the same globally-earliest hit (2023-05-19) for every company.
    # By pre-seeding with since_date, detect sees the cached value and does not
    # override the since_date floor in fetch_all_filings.
    db.set_btc_first_filing_date(ticker, since.isoformat())
    log.info("Set btc_first_filing_date=%s for %s (bypasses broken EFTS entity filter)", since, ticker)

    for attempt in range(3):
        try:
            summary = connector.fetch_all_filings(
                cik=cik,
                ticker=ticker,
                since_date=since,
                filing_regime=company.get('filing_regime', 'domestic'),
            )
            new_date = db.get_btc_first_filing_date(ticker)
            result = {
                'ticker': ticker,
                'status': 'done',
                'old_btc_first_filing_date': old_date,
                'new_btc_first_filing_date': new_date,
                'reports_ingested': summary.reports_ingested,
                'errors': summary.errors,
            }
            log.info(
                "event=backfill_done ticker=%s new_btc_first=%s ingested=%d errors=%d",
                ticker, new_date, summary.reports_ingested, summary.errors,
            )
            return result

        except req_lib.exceptions.HTTPError as exc:
            code = exc.response.status_code if exc.response else 0
            if code in (429, 502, 503, 504) and attempt < 2:
                wait = 30 * (attempt + 1)
                log.warning("HTTP %d for %s, retry %d in %ds", code, ticker, attempt + 1, wait)
                time.sleep(wait)
                continue
            log.error("event=backfill_error ticker=%s error=%s", ticker, exc)
            return {'ticker': ticker, 'status': 'error', 'error': str(exc)}

        except Exception as exc:
            if attempt < 2:
                wait = 15 * (attempt + 1)
                log.warning("Error for %s attempt %d: %s — retrying in %ds", ticker, attempt + 1, exc, wait)
                time.sleep(wait)
                continue
            log.error("event=backfill_error ticker=%s error=%s", ticker, exc, exc_info=True)
            return {'ticker': ticker, 'status': 'error', 'error': str(exc)}

    return {'ticker': ticker, 'status': 'error', 'error': 'max retries exceeded'}


def main() -> None:
    parser = argparse.ArgumentParser(description='Reset btc_first_filing_date and backfill EDGAR')
    parser.add_argument(
        '--tickers', nargs='+', metavar='TICKER',
        help='Limit to specific tickers (default: all companies with CIKs)',
    )
    parser.add_argument(
        '--since', metavar='YYYY-MM-DD', default='2018-01-01',
        help='Earliest floor date for EDGAR ingest (default: 2018-01-01). '
             'The actual floor used will be max(since, detect_btc_first_filing_date).',
    )
    parser.add_argument('--dry-run', action='store_true', help='Show plan without executing')
    args = parser.parse_args()

    since_parts = args.since.split('-')
    since = date(int(since_parts[0]), int(since_parts[1]), int(since_parts[2]))

    db = MinerDB(str(Path(DATA_DIR) / 'minerdata.db'))
    session = req_lib.Session()
    connector = EdgarConnector(db=db, session=session)

    companies = db.get_companies(active_only=False)
    if args.tickers:
        target_tickers = {t.upper() for t in args.tickers}
        companies = [c for c in companies if c['ticker'] in target_tickers]

    # Only process companies that have a CIK and are not already set to skip.
    companies = [c for c in companies if c.get('cik')]

    log.info(
        "event=backfill_plan companies=%d since=%s dry_run=%s",
        len(companies), since, args.dry_run,
    )

    results = []
    for company in companies:
        result = _reset_and_fetch(company, db, connector, since, args.dry_run)
        results.append(result)

    print("\n--- Backfill Summary ---")
    for r in results:
        status = r.get('status', '?')
        ticker = r.get('ticker', '?')
        if status == 'done':
            print(
                f"  {ticker}: {r.get('old_btc_first_filing_date')} -> {r.get('new_btc_first_filing_date')}"
                f"  ({r.get('reports_ingested', 0)} new reports, {r.get('errors', 0)} errors)"
            )
        elif status == 'dry_run':
            print(f"  {ticker}: DRY RUN (current: {r.get('old_date')})")
        elif status == 'skipped':
            print(f"  {ticker}: skipped ({r.get('reason')})")
        else:
            print(f"  {ticker}: ERROR — {r.get('error')}")

    done = sum(1 for r in results if r.get('status') == 'done')
    errors = sum(1 for r in results if r.get('status') == 'error')
    print(f"\nTotal: {done} done, {errors} errors, {len(results) - done - errors} skipped/dry-run")


if __name__ == '__main__':
    main()
