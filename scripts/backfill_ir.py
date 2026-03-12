"""Full-history IR press release backfill for paginated company IR sites.

Problem being solved
--------------------
Incremental IR scrapes stop early when they encounter a listing page where all
press releases are already in the DB. For RIOT, MARA, BTDR and BITF this means
a routine scrape never reaches history older than what was first ingested — the
early-exit fires on page 1 (already-covered 2023-2025 content) before the
scraper reaches 2020-2022.

This script disables the early-exit for index-mode companies (stop_on_all_seen=False)
so pagination continues through all listing pages regardless of prior coverage.
drupal_year companies (BTDR, BITF) already iterate every year from pr_start_year
and are run normally — the backfill just ensures they execute a full sweep.

Usage
-----
    cd OffChain/miners
    # backfill all four default tickers
    python3 scripts/backfill_ir.py

    # specific tickers
    python3 scripts/backfill_ir.py --tickers RIOT MARA

    # backfill then run LLM+regex extraction on all newly ingested documents
    python3 scripts/backfill_ir.py --auto-extract

    # dry-run: print what would run without making HTTP requests or DB writes
    python3 scripts/backfill_ir.py --dry-run
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from infra.logging_config import setup_logging
setup_logging()

from infra.db import MinerDB
from config import DATA_DIR
from scrapers.ir_scraper import IRScraper

import requests as req_lib

log = logging.getLogger('miners.backfill_ir')

_DEFAULT_TICKERS = ['RIOT', 'MARA', 'BTDR', 'BITF']

# index mode: disable early-exit so pagination continues through already-seen pages.
_FULL_PAGINATE_MODES = {'index'}
# template mode: disable fast-forward so all months from pr_start_year are attempted.
_TEMPLATE_BACKFILL_MODES = {'template'}


def _run_extraction(db: MinerDB, tickers: list, dry_run: bool) -> None:
    """Run LLM+regex extraction on all unextracted reports for the given tickers."""
    from interpreters.pattern_registry import PatternRegistry
    from interpreters.interpret_pipeline import extract_report
    from config import CONFIG_DIR

    registry = PatternRegistry.load(CONFIG_DIR)

    for ticker in tickers:
        reports = db.get_unextracted_reports(ticker=ticker)
        if not reports:
            log.info("extraction_skip ticker=%s reason=no_unextracted_reports", ticker)
            continue

        log.info("extraction_start ticker=%s reports=%d", ticker, len(reports))
        if dry_run:
            log.info("dry_run=True ticker=%s — would extract %d reports", ticker, len(reports))
            continue

        extracted = 0
        queued = 0
        errors = 0
        for i, report in enumerate(reports, 1):
            try:
                if not db.claim_report_for_extraction(report['id']):
                    continue
                s = extract_report(report, db, registry)
                extracted += s.data_points_extracted
                queued += s.review_flagged
                log.info(
                    "extraction_progress ticker=%s report=%d/%d id=%s "
                    "data_points=%d review_flagged=%d",
                    ticker, i, len(reports), report['id'],
                    s.data_points_extracted, s.review_flagged,
                )
            except Exception as e:
                errors += 1
                log.error("extraction_error ticker=%s report_id=%s error=%s",
                          ticker, report['id'], e, exc_info=True)

        log.info(
            "extraction_end ticker=%s data_points=%d review_queued=%d errors=%d",
            ticker, extracted, queued, errors,
        )


def _run_backfill(company: dict, db: MinerDB, session: req_lib.Session, dry_run: bool) -> None:
    ticker = company['ticker']
    mode = (company.get('scraper_mode') or '').strip().lower()
    start_date = company.get('pr_start_date')
    start_year = int(str(start_date)[:4]) if start_date else None
    ir_url = company.get('ir_url') or ''

    log.info(
        "backfill_start ticker=%s mode=%s pr_start_date=%s ir_url=%s dry_run=%s",
        ticker, mode, start_date, ir_url, dry_run,
    )

    if mode == 'skip':
        log.info("backfill_skip ticker=%s reason=scraper_mode_is_skip", ticker)
        return

    if not ir_url and mode not in ('rss',):
        log.warning("backfill_skip ticker=%s reason=no_ir_url", ticker)
        return

    if dry_run:
        log.info("dry_run=True ticker=%s — would run mode=%s from %s", ticker, mode, start_year)
        return

    override = dict(company)
    if mode in _FULL_PAGINATE_MODES:
        override['stop_on_all_seen'] = False
        log.info("ticker=%s index mode: stop_on_all_seen=False enabled for full pagination", ticker)
    elif mode in _TEMPLATE_BACKFILL_MODES:
        override['backfill_mode'] = True
        log.info("ticker=%s template mode: backfill_mode=True — fast-forward disabled", ticker)

    scraper = IRScraper(db=db, session=session)
    summary = scraper.scrape_company(override)

    log.info(
        "backfill_end ticker=%s ingested=%d errors=%d",
        ticker, summary.reports_ingested, summary.errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill IR press releases for paginated company sites.")
    parser.add_argument(
        '--tickers', nargs='+', default=_DEFAULT_TICKERS,
        metavar='TICKER',
        help=f"Tickers to backfill (default: {' '.join(_DEFAULT_TICKERS)})",
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help="Print what would run without making HTTP requests or DB writes",
    )
    parser.add_argument(
        '--auto-extract', action='store_true',
        help="After ingestion, run LLM+regex extraction on all newly unextracted reports",
    )
    args = parser.parse_args()

    db_path = Path(DATA_DIR) / 'minerdata.db'
    db = MinerDB(str(db_path))
    session = req_lib.Session()

    all_companies = {c['ticker']: c for c in db.get_companies(active_only=False)}
    tickers = [t.upper() for t in args.tickers]

    missing = [t for t in tickers if t not in all_companies]
    if missing:
        log.error("Unknown tickers (not in DB): %s", ', '.join(missing))
        sys.exit(1)

    log.info("backfill_ir tickers=%s dry_run=%s auto_extract=%s",
             tickers, args.dry_run, args.auto_extract)

    for ticker in tickers:
        company = all_companies[ticker]
        try:
            _run_backfill(company, db, session, dry_run=args.dry_run)
        except Exception as e:
            log.error("backfill_error ticker=%s error=%s", ticker, e, exc_info=True)

    if args.auto_extract:
        log.info("auto_extract_start tickers=%s", tickers)
        _run_extraction(db, tickers, dry_run=args.dry_run)

    log.info("backfill_ir complete")


if __name__ == '__main__':
    main()
