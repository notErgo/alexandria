"""
EDGAR ingestion for ALL companies with CIKs — including inactive/quarterly-only.

Runs 10-Q and 10-K fetching for every company in companies.json that has a CIK,
regardless of active status. Designed to fill in quarterly data for companies
that have stopped monthly reporting (CORZ, HUT8, WULF, IREN, ABTC, SDIG, etc.)  # canonical-sources: noqa — docstring example, not a ticker list

Usage:
    cd OffChain/miners
    python3 scripts/run_edgar_all.py [--ticker TICKER] [--since YYYY-MM] [--dry-run]

OTEL: Set OTEL_RESOURCE_ATTRIBUTES=plan=full_ingest_v1,phase=edgar_all before running.
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from infra.logging_config import setup_logging
setup_logging()

from infra.db import MinerDB
from config import DATA_DIR, CONFIG_DIR
from scrapers.edgar_connector import EdgarConnector
from interpreters.extraction_pipeline import extract_report
from interpreters.pattern_registry import PatternRegistry

import requests as req_lib

log = logging.getLogger('miners.run_edgar_all')

PROGRESS_DIR = Path('/private/tmp/claude-501/miners_progress')
PROGRESS_DIR.mkdir(parents=True, exist_ok=True)


def write_progress(ticker: str, state: dict) -> None:
    path = PROGRESS_DIR / f'edgar_{ticker}.json'
    state['updated_at'] = datetime.now(timezone.utc).isoformat()
    tmp = str(path) + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, str(path))
    # Also write OTEL-style structured log line
    otel_attrs = os.environ.get('OTEL_RESOURCE_ATTRIBUTES', '')
    log.info("[OTEL] %s edgar_agent ticker=%s %s", otel_attrs, ticker,
             ' '.join(f'{k}={v}' for k, v in state.items() if k != 'updated_at'))


def run_edgar_for_company(
    company: dict,
    db: MinerDB,
    session: req_lib.Session,
    since: date,
    dry_run: bool = False,
) -> dict:
    ticker = company['ticker']
    cik = company.get('cik')
    if not cik:
        return {'ticker': ticker, 'status': 'skipped', 'reason': 'no CIK'}

    write_progress(ticker, {'status': 'running', 'phase': 'edgar_fetch', 'cik': cik})
    log.info("EDGAR fetch: %s (CIK=%s) since=%s", ticker, cik, since)

    if dry_run:
        log.info("DRY RUN: would fetch EDGAR for %s", ticker)
        return {'ticker': ticker, 'status': 'dry_run'}

    connector = EdgarConnector(db=db, session=session)

    # Retry up to 3 times on transient errors
    for attempt in range(3):
        try:
            summary = connector.fetch_all_filings(
                cik=cik, ticker=ticker, since_date=since,
                filing_regime=company.get('filing_regime', 'domestic'),
            )
            result = {
                'ticker': ticker,
                'status': 'done',
                'reports_ingested': summary.reports_ingested,
                'data_points': summary.data_points_extracted,
                'errors': summary.errors,
            }
            write_progress(ticker, result)
            log.info("EDGAR done: %s -> %d reports, %d data points",
                     ticker, summary.reports_ingested, summary.data_points_extracted)
            return result

        except req_lib.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else 0
            if status_code in (429, 502, 503, 504) and attempt < 2:
                wait = 30 * (attempt + 1)
                log.warning("HTTP %d for %s, retry %d in %ds", status_code, ticker, attempt+1, wait)
                write_progress(ticker, {'status': 'retrying', 'attempt': attempt+1, 'http_status': status_code})
                time.sleep(wait)
                continue
            write_progress(ticker, {'status': 'error', 'error': str(exc)})
            log.error("EDGAR failed for %s: %s", ticker, exc)
            return {'ticker': ticker, 'status': 'error', 'error': str(exc)}

        except Exception as exc:
            if attempt < 2:
                wait = 15 * (attempt + 1)
                log.warning("Error for %s (attempt %d): %s — retrying in %ds", ticker, attempt+1, exc, wait)
                time.sleep(wait)
                continue
            write_progress(ticker, {'status': 'error', 'error': str(exc)})
            log.error("EDGAR failed for %s: %s", ticker, exc, exc_info=True)
            return {'ticker': ticker, 'status': 'error', 'error': str(exc)}

    return {'ticker': ticker, 'status': 'error', 'error': 'max retries exceeded'}


import requests.exceptions


def main():
    parser = argparse.ArgumentParser(description='EDGAR ingestion for all CIK companies')
    parser.add_argument('--ticker', help='Limit to one ticker')
    parser.add_argument('--since', metavar='YYYY-MM', default='2020-01',
                        help='Start date for EDGAR search (default: 2020-01)')
    parser.add_argument('--dry-run', action='store_true', help='Print plan without fetching')
    parser.add_argument('--extract', action='store_true',
                        help='Run extraction pipeline on new reports after ingesting')
    args = parser.parse_args()

    # Parse since date
    since_parts = args.since.split('-')
    since = date(int(since_parts[0]), int(since_parts[1]), 1)

    # OTEL tagging
    os.environ.setdefault('OTEL_RESOURCE_ATTRIBUTES',
                          'plan=full_ingest_v1,phase=edgar_all,agent=run_edgar_all')

    db_path = str(Path(DATA_DIR) / 'minerdata.db')
    db = MinerDB(db_path)
    registry = PatternRegistry.load(CONFIG_DIR)

    # Load ALL companies from JSON (including inactive — that's the point of this script)
    companies_path = str(Path(CONFIG_DIR) / 'companies.json')
    with open(companies_path) as f:
        all_companies = json.load(f)

    # Filter to companies with CIKs
    cik_companies = [c for c in all_companies if c.get('cik')]

    if args.ticker:
        cik_companies = [c for c in cik_companies if c['ticker'].upper() == args.ticker.upper()]

    log.info("EDGAR plan: %d companies to process (since=%s)", len(cik_companies), since)
    for c in cik_companies:
        log.info("  %s (CIK=%s, active=%s, mode=%s)",
                 c['ticker'], c.get('cik'), c.get('active'), c.get('scrape_mode'))

    session = req_lib.Session()
    session.headers['User-Agent'] = 'Hermeneutic Research Platform contact@example.com'

    results = []
    for company in cik_companies:
        # Rate limit between companies — EDGAR asks for no more than 10 req/s
        time.sleep(2.0)
        result = run_edgar_for_company(
            company=company,
            db=db,
            session=session,
            since=since,
            dry_run=args.dry_run,
        )
        results.append(result)

        if args.extract and result.get('status') == 'done' and result.get('reports_ingested', 0) > 0:
            log.info("Running extraction pipeline for %s...", company['ticker'])
            reports = db.get_unextracted_reports(ticker=company['ticker'])
            for report in reports:
                try:
                    extract_report(report, db, registry)
                except Exception as exc:
                    log.error("Extraction failed for report %s: %s", report.get('id'), exc)

    # Final summary
    print("\n=== EDGAR ALL RESULTS ===")
    total_reports = sum(r.get('reports_ingested', 0) for r in results)
    total_dp = sum(r.get('data_points', 0) for r in results)
    total_errors = sum(1 for r in results if r.get('status') == 'error')
    for r in results:
        status = r.get('status', '?')
        reports = r.get('reports_ingested', 0)
        dp = r.get('data_points', 0)
        err = r.get('error', '')
        print(f"  {r['ticker']:<6} {status:<10} reports={reports} dp={dp} {err}")
    print(f"\nTOTAL: {total_reports} reports, {total_dp} data points, {total_errors} errors")

    # Write coordinator-compatible summary
    summary_path = PROGRESS_DIR / 'edgar_all_summary.json'
    with open(summary_path, 'w') as f:
        json.dump({
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'since': str(since),
            'total_reports': total_reports,
            'total_data_points': total_dp,
            'total_errors': total_errors,
            'results': results,
        }, f, indent=2)
    log.info("Summary written to %s", summary_path)


if __name__ == '__main__':
    main()
