"""
Bitcoin Miner Data Platform — CLI entry point.

Commands:
  ingest --source [archive|ir|edgar] [--ticker TICKER] [--since YYYY-MM]
  query  --ticker TICKER --metric METRIC [--from YYYY-MM] [--to YYYY-MM]
         [--min-confidence 0.0-1.0] [--format table|json|csv]
  export --out FILE.csv [same filters as query]

Usage:
  python3 cli.py ingest --source archive
  python3 cli.py query --ticker MARA --metric production_btc --from 2024-01
  python3 cli.py export --out results.csv --ticker MARA
"""
import argparse
import csv
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from infra.logging_config import setup_logging
setup_logging()

from infra.db import MinerDB
from extractors.pattern_registry import PatternRegistry
from config import DATA_DIR, CONFIG_DIR, ARCHIVE_DIR
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

    if args.source == 'archive':
        from scrapers.archive_ingestor import ArchiveIngestor
        ingestor = ArchiveIngestor(archive_dir=ARCHIVE_DIR, db=db, registry=registry)
        summary = ingestor.ingest_all(force=getattr(args, 'force', False))
        print(f"Archive ingest complete: {summary.reports_ingested} reports, "
              f"{summary.data_points_extracted} data points, "
              f"{summary.review_flagged} flagged for review, "
              f"{summary.errors} errors")

    elif args.source == 'ir':
        import json
        import requests as req_lib
        from scrapers.ir_scraper import IRScraper
        # Always load scraping config from companies.json — the DB companies table
        # does not store scrape_mode, url_template, rss_url, pr_start_year, etc.
        with open(str(Path(CONFIG_DIR) / 'companies.json')) as f:
            companies = json.load(f)
        companies = [c for c in companies if c.get('active', True)]
        if args.ticker:
            companies = [c for c in companies if c['ticker'] == args.ticker.upper()]
        session = req_lib.Session()
        scraper = IRScraper(db=db, session=session)
        for company in companies:
            print(f"Scraping IR for {company['ticker']} (mode={company.get('scrape_mode','index')})...")
            s = scraper.scrape_company(company)
            print(f"  → {s.reports_ingested} reports, {s.data_points_extracted} data points, {s.errors} errors")

    elif args.source == 'edgar':
        import requests as req_lib
        from scrapers.edgar_connector import EdgarConnector
        session = req_lib.Session()
        connector = EdgarConnector(db=db, registry=registry, session=session)
        companies = db.get_companies(active_only=True)
        if args.ticker:
            companies = [c for c in companies if c['ticker'] == args.ticker.upper()]
        since = date(2020, 1, 1)
        if args.since:
            parts = args.since.split('-')
            since = date(int(parts[0]), int(parts[1]), 1)
        for company in companies:
            if not company.get('cik'):
                print(f"  Skipping {company['ticker']}: no CIK")
                continue
            print(f"Fetching EDGAR for {company['ticker']} since {since}...")
            s = connector.fetch_production_filings(
                cik=company['cik'], ticker=company['ticker'], since_date=since
            )
            print(f"  → {s.reports_ingested} reports, {s.data_points_extracted} data points")


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


def cmd_extract(args):
    """Run the extraction pipeline on stored reports (no re-scraping)."""
    from miner_types import ExtractionSummary
    from extractors.extraction_pipeline import extract_report

    db = get_db()
    registry = get_registry()
    ticker_filter = args.ticker.upper() if args.ticker else None

    if args.force:
        reports = db.get_all_reports_for_extraction(ticker=ticker_filter)
    else:
        reports = db.get_unextracted_reports(ticker=ticker_filter)

    if not reports:
        print("No reports to extract.")
        return

    total = ExtractionSummary()
    for report in reports:
        s = extract_report(report, db, registry)
        total.reports_processed += s.reports_processed
        total.data_points_extracted += s.data_points_extracted
        total.review_flagged += s.review_flagged
        total.errors += s.errors

    print(
        f"Extracted {total.reports_processed} reports: "
        f"{total.data_points_extracted} data points, "
        f"{total.review_flagged} flagged for review, "
        f"{total.errors} errors"
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
    p_ingest.add_argument('--source', choices=['archive', 'ir', 'edgar'], required=True)
    p_ingest.add_argument('--ticker', help='Limit to one ticker (IR/EDGAR only)')
    p_ingest.add_argument('--since', metavar='YYYY-MM', help='Start date (EDGAR only)')
    p_ingest.add_argument(
        '--force', action='store_true', default=False,
        help='Re-ingest already-processed files (use after logic/pattern changes)',
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

    # diagnose
    p_diagnose = sub.add_parser('diagnose', help='Show coverage matrix: metrics per period + gap reasons')
    p_diagnose.add_argument('--ticker', help='Limit to one ticker')

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
    elif args.command == 'purge':
        cmd_purge(args)


if __name__ == '__main__':
    main()
