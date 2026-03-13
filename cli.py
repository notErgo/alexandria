"""
Bitcoin Miner Data Platform — CLI entry point.

Commands:
  ingest --source [ir|edgar] [--ticker TICKER] [--since YYYY-MM]
  query  --ticker TICKER --metric METRIC [--from YYYY-MM] [--to YYYY-MM]
         [--min-confidence 0.0-1.0] [--format table|json|csv]
  export --out FILE.csv [same filters as query]

Usage:
  python3 cli.py ingest --source ir --ticker MARA
  python3 cli.py query --ticker MARA --metric production_btc --from 2024-01
  python3 cli.py export --out results.csv --ticker MARA
"""
import argparse
import csv
import json
import logging
import os
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from infra.logging_config import setup_logging
setup_logging()

from infra.db import MinerDB
from interpreters.pattern_registry import PatternRegistry
from config import DATA_DIR, CONFIG_DIR
from pathlib import Path


def get_db() -> MinerDB:
    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)
    companies_path = str(Path(CONFIG_DIR) / 'companies.json')
    if db.get_companies(active_only=False) == []:
        db.seed_companies(companies_path)
    return db


def get_registry() -> PatternRegistry:
    return PatternRegistry.load(CONFIG_DIR)


def _period_to_full(period_str: str) -> str:
    """Convert YYYY-MM to YYYY-MM-01 for DB comparison."""
    return period_str + '-01' if period_str else None


def cmd_ingest(args):
    db = get_db()
    registry = get_registry()

    if args.source == 'ir':
        import warnings
        warnings.warn(
            "cli --source ir is deprecated. EDGAR is the canonical ingest source. "
            "Use --source edgar instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        with open(str(Path(CONFIG_DIR) / 'companies.json')) as f:
            companies = json.load(f)
        companies = [c for c in companies if c.get('active', True)]
        if args.ticker:
            companies = [c for c in companies if c['ticker'] == args.ticker.upper()]
        num_workers = getattr(args, 'workers', int(os.environ.get('MINERS_INGEST_WORKERS', '6')))
        print(f"Scraping IR for {len(companies)} companies ({num_workers} workers)...")
        db_path = str(Path(DATA_DIR) / 'minerdata.db')
        s = _run_ir_ingest_pool(db_path, companies, num_workers=num_workers)
        print(f"IR ingest complete: {s.reports_ingested} reports, {s.errors} errors")

    elif args.source == 'edgar':
        if args.ticker:
            companies = db.get_companies(active_only=False)
            companies = [c for c in companies if c['ticker'] == args.ticker.upper()]
        else:
            companies = db.get_companies(active_only=True)
        since = date(2020, 1, 1)
        if args.since:
            parts = args.since.split('-')
            since = date(int(parts[0]), int(parts[1]), 1)
        num_workers = getattr(args, 'workers', int(os.environ.get('MINERS_INGEST_WORKERS', '2')))
        print(f"Fetching EDGAR for {len(companies)} companies since {since} ({num_workers} workers)...")
        db_path = str(Path(DATA_DIR) / 'minerdata.db')
        s = _run_edgar_ingest_pool(db_path, companies, since, num_workers=num_workers)
        print(f"EDGAR ingest complete: {s.reports_ingested} reports, {s.errors} errors")


def cmd_query(args):
    db = get_db()
    rows = db.query_data_points(
        ticker=args.ticker.upper() if args.ticker else None,
        metric=args.metric or None,
        from_period=_period_to_full(args.from_period),
        to_period=_period_to_full(args.to_period),
        min_confidence=args.min_confidence,
    )

    fmt = args.format or 'table'
    if fmt == 'json':
        print(json.dumps(rows, indent=2, default=str))
    elif fmt == 'csv':
        if rows:
            writer = csv.DictWriter(sys.stdout, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        if not rows:
            print("No data found.")
            return
        header = f"{'Ticker':<6} {'Period':<8} {'Metric':<18} {'Value':>12} {'Unit':<8} {'Conf':>6}"
        print(header)
        print('-' * len(header))
        for r in rows:
            period = (r['period'] or '')[:7]
            print(f"{r['ticker']:<6} {period:<8} {r['metric']:<18} {r['value']:>12.4f} {r['unit']:<8} {r['confidence']:>6.3f}")
        print(f"\n{len(rows)} rows.")


def cmd_export(args):
    db = get_db()
    rows = db.query_data_points(
        ticker=args.ticker.upper() if args.ticker else None,
        metric=args.metric or None,
        from_period=_period_to_full(args.from_period),
        to_period=_period_to_full(args.to_period),
        min_confidence=args.min_confidence,
    )
    with open(args.out, 'w', newline='') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    print(f"Exported {len(rows)} rows to {args.out}")


log = logging.getLogger('miners.cli')

import requests  # noqa: E402 — used by ingest pool functions
from scrapers.edgar_connector import EdgarConnector
from scrapers.ir_scraper import IRScraper


def _run_edgar_ingest_pool(
    db_path: str,
    companies: list,
    since_date,
    num_workers: int = 2,
) -> 'IngestSummary':
    """Fetch EDGAR filings for multiple companies in parallel.

    Each worker owns its own MinerDB connection and EdgarConnector/Session so
    there is no shared state between threads.  Companies with no CIK are
    skipped.  Exceptions in one worker increment errors but do not abort others.
    """
    from miner_types import IngestSummary

    total = IngestSummary()
    total_lock = threading.Lock()

    def worker(company):
        ticker = company['ticker']
        cik = company.get('cik')
        if not cik:
            log.info("EDGAR ingest: skipping %s — no CIK", ticker)
            return IngestSummary()
        local_db = MinerDB(db_path)
        session = requests.Session()
        connector = EdgarConnector(db=local_db, session=session)
        regime = company.get('filing_regime', 'domestic')
        log.info("event=edgar_ingest_start ticker=%s since=%s regime=%s", ticker, since_date, regime)
        s = connector.fetch_all_filings(
            cik=cik, ticker=ticker, since_date=since_date, filing_regime=regime,
        )
        log.info("event=edgar_ingest_complete ticker=%s ingested=%d errors=%d",
                 ticker, s.reports_ingested, s.errors)
        return s

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {pool.submit(worker, c): c['ticker'] for c in companies}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                s = future.result()
                with total_lock:
                    total.reports_ingested += s.reports_ingested
                    total.errors += s.errors
            except Exception as exc:
                log.error("event=edgar_ingest_error ticker=%s error=%s", ticker, exc, exc_info=True)
                with total_lock:
                    total.errors += 1

    return total


def _run_ir_ingest_pool(
    db_path: str,
    companies: list,
    num_workers: int = 6,
) -> 'IngestSummary':
    """Scrape IR press releases for multiple companies in parallel.

    Each worker owns its own MinerDB connection and IRScraper/Session.
    Exceptions in one worker increment errors but do not abort others.
    """
    from miner_types import IngestSummary

    total = IngestSummary()
    total_lock = threading.Lock()

    def worker(company):
        ticker = company['ticker']
        local_db = MinerDB(db_path)
        session = requests.Session()
        scraper = IRScraper(db=local_db, session=session)
        log.info("event=ir_ingest_start ticker=%s mode=%s", ticker, company.get('scraper_mode', 'skip'))
        s = scraper.scrape_company(company)
        log.info("event=ir_ingest_complete ticker=%s ingested=%d errors=%d",
                 ticker, s.reports_ingested, s.errors)
        return s

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {pool.submit(worker, c): c['ticker'] for c in companies}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                s = future.result()
                with total_lock:
                    total.reports_ingested += s.reports_ingested
                    total.errors += s.errors
            except Exception as exc:
                log.error("event=ir_ingest_error ticker=%s error=%s", ticker, exc, exc_info=True)
                with total_lock:
                    total.errors += 1

    return total


def _run_worker_pool(db_path: str, report_ids: list, registry, num_workers: int = 1, attribution: str = None) -> 'ExtractionSummary':
    """Process report_ids with a pool of num_workers threads.

    Each worker owns its own MinerDB connection. Reports are claimed atomically
    via claim_report_for_extraction so no report is processed twice even if
    multiple workers see the same ID from the shared queue.

    Returns a merged ExtractionSummary across all workers.
    """
    from miner_types import ExtractionSummary
    from interpreters.interpret_pipeline import extract_report
    from infra.db import MinerDB

    work_queue: queue.Queue = queue.Queue()
    for rid in report_ids:
        work_queue.put(rid)

    total = ExtractionSummary()
    total_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        local_db = MinerDB(db_path)
        while True:
            try:
                report_id = work_queue.get_nowait()
            except queue.Empty:
                break
            if not local_db.claim_report_for_extraction(report_id):
                log.debug("worker=%d skipping report %d (already claimed)", worker_id, report_id)
                continue
            report = local_db.get_report(report_id)
            if not report:
                continue
            s = extract_report(report, local_db, registry, attribution=attribution)
            with total_lock:
                total.reports_processed += s.reports_processed
                total.data_points_extracted += s.data_points_extracted
                total.review_flagged += s.review_flagged
                total.errors += s.errors

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [pool.submit(worker, i) for i in range(num_workers)]
        for f in as_completed(futures):
            f.result()  # re-raises any uncaught exception from worker

    return total


def cmd_extract(args):
    """Run the extraction pipeline on stored reports (no re-scraping)."""
    from miner_types import ExtractionSummary
    from interpreters.interpret_pipeline import extract_report

    db = get_db()
    registry = get_registry()
    ticker_filter = args.ticker.upper() if args.ticker else None
    attribution = getattr(args, 'attribution', None) or None
    num_workers = getattr(args, 'workers', 1) or 1

    if args.force:
        reports = db.get_all_reports_for_extraction(ticker=ticker_filter)
    else:
        reports = db.get_unextracted_reports(ticker=ticker_filter)

    if not reports:
        print("No reports to extract.")
        return

    if attribution:
        print(f"Attribution override: extraction_method will be stored as '{attribution}'")

    report_ids = [r['id'] for r in reports]
    print(f"Extracting {len(report_ids)} reports with {num_workers} worker(s)...")

    if num_workers > 1:
        total = _run_worker_pool(
            db_path=db.db_path,
            report_ids=report_ids,
            registry=registry,
            num_workers=num_workers,
            attribution=attribution,
        )
    else:
        total = ExtractionSummary()
        for i, report in enumerate(reports, 1):
            s = extract_report(report, db, registry, attribution=attribution)
            total.reports_processed += s.reports_processed
            total.data_points_extracted += s.data_points_extracted
            total.review_flagged += s.review_flagged
            total.errors += s.errors
            if i % 10 == 0 or i == len(reports):
                print(f"  [{i}/{len(reports)}] {total.data_points_extracted} data points so far")

    print(
        f"Extracted {total.reports_processed} reports: "
        f"{total.data_points_extracted} data points, "
        f"{total.review_flagged} flagged for review, "
        f"{total.errors} errors"
    )


def cmd_broad_extract(args):
    """Run broad LLM extraction on stored reports to capture ALL numeric values."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
    from interpreters.broad_interpreter import BroadInterpreter

    db = get_db()
    ticker_filter = args.ticker.upper() if args.ticker else None
    extractor = BroadInterpreter(db)
    result = extractor.extract_all(ticker=ticker_filter, force=getattr(args, 'force', False))
    print(f"Broad extraction complete: {result['reports_processed']} reports, "
          f"{result['metrics_stored']} metrics stored, "
          f"{result['errors']} errors")


def cmd_gap_fill(args):
    """Infer missing monthly data_points from quarterly totals."""
    from interpreters.gap_fill import fill_quarterly_gaps

    db = get_db()
    ticker = args.ticker.upper() if args.ticker else None
    if not ticker:
        print("ERROR: --ticker is required for gap-fill")
        sys.exit(1)

    dry_run = getattr(args, 'dry_run', False)
    fill_mode = getattr(args, 'fill_mode', 'endpoint')
    result = fill_quarterly_gaps(ticker=ticker, db=db, dry_run=dry_run, fill_mode=fill_mode)

    if dry_run:
        print(f"DRY RUN — no rows written")

    if not result['rows']:
        print("No inferences computed.")
        return

    header = f"{'Period':<8}  {'Metric':<24}  {'Value':>10}  {'Method':<20}  {'Status'}"
    print(header)
    print('-' * len(header))
    for r in result['rows']:
        period = r.get('period', r.get('covering_period', ''))[:7]
        metric = r.get('metric', '')
        value = r.get('inferred_value')
        method = r.get('extraction_method', r.get('reason', ''))
        status = r.get('status', '')
        val_str = f"{value:.2f}" if value is not None else '—'
        print(f"{period:<8}  {metric:<24}  {val_str:>10}  {method:<20}  {status}")

    print(
        f"\nSummary: {result['filled']} filled, "
        f"{result['skipped']} skipped, "
        f"{result['errors']} errors"
    )


def cmd_derive_balance_change(args):
    """Compute net_btc_balance_change as MoM delta of holdings_btc from final_data_points."""
    from interpreters.gap_fill import derive_net_balance_change

    db = get_db()
    ticker = args.ticker.upper() if args.ticker else None
    if not ticker:
        print("ERROR: --ticker is required for derive-balance-change")
        sys.exit(1)

    dry_run = getattr(args, 'dry_run', False)
    overwrite = not getattr(args, 'no_overwrite', False)
    result = derive_net_balance_change(ticker=ticker, db=db, dry_run=dry_run, overwrite=overwrite)

    if dry_run:
        print("DRY RUN — no rows written")

    if not result['rows']:
        print("No deltas computed (need >= 2 consecutive finalized holdings_btc values).")
        return

    header = f"{'Period':<12}  {'Delta (BTC)':>12}  {'Status'}"
    print(header)
    print('-' * len(header))
    for r in result['rows']:
        period = r.get('period', '')[:10]
        value = r.get('value')
        status = r.get('status', '')
        reason = r.get('reason', '')
        val_str = f"{value:+.4f}" if value is not None else '—'
        suffix = f" ({reason})" if reason else ''
        print(f"{period:<12}  {val_str:>12}  {status}{suffix}")

    print(
        f"\nSummary: {result['derived']} derived, "
        f"{result['skipped']} skipped"
    )


def cmd_purge(args):
    """Clear extracted results and reset reports for a fresh extraction pass.

    Deletes all rows from data_points and review_queue, then sets
    reports.extracted_at = NULL so every report is re-queued for extraction.
    Raw report text, companies, pattern configs, llm_prompts, btc_loans,
    facilities, and source_audit are NOT touched.

    Requires --confirm to prevent accidental execution.
    """
    if not args.confirm:
        print(
            "ERROR: this command permanently deletes all data_points and review_queue rows.\n"
            "Re-run with --confirm to proceed."
        )
        sys.exit(1)

    db = get_db()
    with db._get_connection() as conn:
        rq_count = conn.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]
        dp_count = conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]
        rpt_count = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE extracted_at IS NOT NULL"
        ).fetchone()[0]

        conn.execute("DELETE FROM review_queue")
        conn.execute("DELETE FROM data_points")
        conn.execute("UPDATE reports SET extracted_at = NULL")

    print(
        f"Purge complete:\n"
        f"  {rq_count} review_queue rows deleted\n"
        f"  {dp_count} data_points rows deleted\n"
        f"  {rpt_count} reports reset (extracted_at → NULL)\n"
        f"Run 'python3 cli.py extract' to re-extract all reports."
    )


def cmd_diagnose(args):
    """Print a coverage matrix: for each company+period, show what metrics exist and why gaps exist."""
    import sqlite3
    from analysis.coverage import build_coverage_row, GapReason

    db = get_db()
    companies = db.get_companies(active_only=False)
    if args.ticker:
        companies = [c for c in companies if c['ticker'] == args.ticker.upper()]

    for company in companies:
        ticker = company['ticker']
        # Get all distinct periods for this ticker from data_points and reports
        db_path = str(Path(DATA_DIR) / 'minerdata.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        period_rows = conn.execute(
            """SELECT DISTINCT period FROM (
                SELECT period FROM data_points WHERE ticker=?
                UNION
                SELECT report_date AS period FROM reports WHERE ticker=?
            ) ORDER BY period""",
            (ticker, ticker),
        ).fetchall()
        periods = [r[0] for r in period_rows]
        conn.close()

        if not periods:
            print(f"\n{ticker}: no data")
            continue

        # Header
        metrics = ['production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh']
        header = f"\n{ticker} Coverage Report"
        print(header)
        print('=' * 60)
        col_w = 10
        print(f"{'Period':<10}", end='')
        for m in metrics:
            short = m.replace('_btc', '').replace('hashrate_', 'hash_')
            print(f"  {short:>{col_w}}", end='')
        print(f"  {'gap_reason'}")
        print('-' * 60)

        for period in periods:
            # Fetch data for this period
            db_path = str(Path(DATA_DIR) / 'minerdata.db')
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row

            dp_rows = conn.execute(
                "SELECT metric, value FROM data_points WHERE ticker=? AND period=?",
                (ticker, period),
            ).fetchall()
            report_rows = conn.execute(
                "SELECT report_date FROM reports WHERE ticker=? AND report_date=?",
                (ticker, period),
            ).fetchall()
            rq_rows = conn.execute(
                "SELECT confidence FROM review_queue WHERE ticker=? AND period=?",
                (ticker, period),
            ).fetchall()
            conn.close()

            data_points = [dict(r) for r in dp_rows]
            reports = [dict(r) for r in report_rows]
            review_queue = [dict(r) for r in rq_rows]

            row = build_coverage_row(ticker, period, data_points, reports, review_queue)

            period_short = period[:7]
            print(f"{period_short:<10}", end='')
            for m in metrics:
                val = row.values.get(m)
                cell = f"{val:>10.1f}" if val is not None else f"{'—':>10}"
                print(f"  {cell}", end='')

            if row.reason == GapReason.OK:
                gap_str = "ok"
            elif row.reason == GapReason.LOW_CONFIDENCE:
                gap_str = f"low_conf({row.max_confidence:.2f})"
            elif row.reason == GapReason.NO_EXTRACTION:
                gap_str = "no_extraction"
            else:
                gap_str = "no_file"
            print(f"  {gap_str}")


def main():
    parser = argparse.ArgumentParser(
        description="Bitcoin Miner Data Platform CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # ingest
    p_ingest = sub.add_parser('ingest', help='Ingest data from a source')
    p_ingest.add_argument('--source', choices=['ir', 'edgar'], required=True)
    p_ingest.add_argument('--ticker', help='Limit to one ticker (IR/EDGAR only)')
    p_ingest.add_argument('--since', metavar='YYYY-MM', help='Start date (EDGAR only)')
    p_ingest.add_argument(
        '--force', action='store_true', default=False,
        help='Re-ingest already-processed files (use after logic/pattern changes)',
    )
    p_ingest.add_argument(
        '--workers',
        type=int,
        default=int(os.environ.get('MINERS_INGEST_WORKERS', '2')),
        metavar='N',
        help='Parallel company workers for IR/EDGAR ingest '
             '(default: $MINERS_INGEST_WORKERS or 2; keep <=2 for EDGAR to stay under SEC rate limit)',
    )

    # query
    p_query = sub.add_parser('query', help='Query extracted data points')
    p_query.add_argument('--ticker')
    p_query.add_argument('--metric', choices=[
        'production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh', 'realization_rate'
    ])
    p_query.add_argument('--from', dest='from_period', metavar='YYYY-MM')
    p_query.add_argument('--to', dest='to_period', metavar='YYYY-MM')
    p_query.add_argument('--min-confidence', type=float, metavar='0.0-1.0')
    p_query.add_argument('--format', choices=['table', 'json', 'csv'], default='table')

    # export
    p_export = sub.add_parser('export', help='Export data to CSV file')
    p_export.add_argument('--out', required=True, metavar='FILE.csv')
    p_export.add_argument('--ticker')
    p_export.add_argument('--metric', choices=[
        'production_btc', 'hodl_btc', 'sold_btc', 'hashrate_eh', 'realization_rate'
    ])
    p_export.add_argument('--from', dest='from_period', metavar='YYYY-MM')
    p_export.add_argument('--to', dest='to_period', metavar='YYYY-MM')
    p_export.add_argument('--min-confidence', type=float, metavar='0.0-1.0')

    # extract
    p_extract = sub.add_parser(
        'extract',
        help='Run extraction pipeline on stored reports (no re-scraping)',
    )
    p_extract.add_argument('--ticker', help='Limit to one ticker')
    p_extract.add_argument(
        '--force', action='store_true', default=False,
        help='Re-extract already-extracted reports (use after pattern changes)',
    )
    p_extract.add_argument(
        '--attribution',
        metavar='METHOD',
        help='Override extraction_method stored in data_points (e.g. "codex")',
    )
    p_extract.add_argument(
        '--workers',
        type=int,
        default=int(os.environ.get('MINERS_EXTRACT_WORKERS', '3')),
        metavar='N',
        help='Number of parallel extraction workers '
             '(default: $MINERS_EXTRACT_WORKERS or 3). '
             'Set OLLAMA_NUM_PARALLEL to match for best throughput.',
    )

    # diagnose
    p_diagnose = sub.add_parser('diagnose', help='Show coverage matrix: metrics per period + gap reasons')
    p_diagnose.add_argument('--ticker', help='Limit to one ticker')

    # broad_extract
    p_broad = sub.add_parser(
        'broad_extract',
        help='Run broad LLM extraction on all stored reports to capture ALL numeric values',
    )
    p_broad.add_argument('--ticker', help='Limit to one ticker')
    p_broad.add_argument('--force', action='store_true', default=False,
                         help='Re-extract even reports already in raw_extractions')

    # gap-fill
    p_gap = sub.add_parser(
        'gap-fill',
        help='Infer missing monthly data_points from quarterly totals',
    )
    p_gap.add_argument('--ticker', required=True, help='Company ticker (required)')
    p_gap.add_argument(
        '--dry-run', action='store_true', default=False,
        help='Preview inferences without writing to DB',
    )
    p_gap.add_argument(
        '--fill-mode', default='endpoint',
        choices=['endpoint', 'stepwise', 'linear'],
        help='How to fill snapshot metric gaps: endpoint (last month only, default), '
             'stepwise (all months = quarter value), linear (interpolate from prev quarter)',
    )

    # derive-balance-change
    p_derive = sub.add_parser(
        'derive-balance-change',
        help='Compute net_btc_balance_change as MoM delta of holdings_btc in final_data_points',
    )
    p_derive.add_argument('--ticker', required=True, help='Company ticker (required)')
    p_derive.add_argument(
        '--dry-run', action='store_true', default=False,
        help='Preview computed deltas without writing to DB',
    )
    p_derive.add_argument(
        '--no-overwrite', action='store_true', default=False,
        help='Skip periods that already have a net_btc_balance_change value',
    )

    # purge
    p_purge = sub.add_parser(
        'purge',
        help='Delete all extracted results and reset reports for a fresh extraction pass',
    )
    p_purge.add_argument(
        '--confirm', action='store_true', default=False,
        help='Required: confirms you intend to delete data_points and review_queue',
    )

    args = parser.parse_args()
    if args.command == 'ingest':
        cmd_ingest(args)
    elif args.command == 'query':
        cmd_query(args)
    elif args.command == 'export':
        cmd_export(args)
    elif args.command == 'extract':
        cmd_extract(args)
    elif args.command == 'diagnose':
        cmd_diagnose(args)
    elif args.command == 'broad_extract':
        cmd_broad_extract(args)
    elif args.command == 'gap-fill':
        cmd_gap_fill(args)
    elif args.command == 'derive-balance-change':
        cmd_derive_balance_change(args)
    elif args.command == 'purge':
        cmd_purge(args)


if __name__ == '__main__':
    main()
