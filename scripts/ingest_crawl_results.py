"""Ingest LLM crawl results by POSTing to POST /api/ingest/raw.

Usage:
    python3 scripts/ingest_crawl_results.py --ticker BTBT
    python3 scripts/ingest_crawl_results.py --all
    python3 scripts/ingest_crawl_results.py --ticker BTBT --dry-run

Reads: .data/crawl_results/{TICKER}/results.json
Posts: POST http://localhost:5004/api/ingest/raw
"""
import argparse
import json
import sys
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).parent.parent
_RESULTS_DIR = _REPO_ROOT / '.data' / 'crawl_results'
_API_BASE = 'http://localhost:5004'
_BATCH_SIZE = 50  # documents per POST request


def _ingest_ticker(ticker: str, dry_run: bool, api_base: str = _API_BASE) -> dict:
    results_path = _RESULTS_DIR / ticker / 'results.json'
    if not results_path.exists():
        print(f"[{ticker}] No results file at {results_path}", file=sys.stderr)
        return {'ticker': ticker, 'ingested': 0, 'skipped': 0, 'errors': 0, 'status': 'missing'}

    with open(results_path) as f:
        docs = json.load(f)

    if not isinstance(docs, list):
        print(f"[{ticker}] results.json is not a list", file=sys.stderr)
        return {'ticker': ticker, 'ingested': 0, 'skipped': 0, 'errors': 1, 'status': 'bad_format'}

    # Normalize agent output schema → ingest route schema
    for doc in docs:
        # Agent writes "url"; route expects "source_url"
        if not doc.get('source_url') and doc.get('url'):
            doc['source_url'] = doc['url']
        # Attach ticker if missing
        if not doc.get('ticker'):
            doc['ticker'] = ticker

    total = {'ingested': 0, 'skipped': 0, 'errors': 0}

    if dry_run:
        print(f"[{ticker}] dry-run: would POST {len(docs)} documents")
        return {'ticker': ticker, **total, 'status': 'dry_run'}

    # Send in batches
    for i in range(0, len(docs), _BATCH_SIZE):
        batch = docs[i: i + _BATCH_SIZE]
        try:
            resp = requests.post(
                f'{api_base}/api/ingest/raw',
                json={'documents': batch},
                timeout=60,
            )
            if resp.status_code in (200, 207):
                data = resp.json().get('data', {})
                total['ingested'] += data.get('ingested', 0)
                total['skipped'] += data.get('skipped', 0)
                total['errors'] += len(data.get('errors', []))
            else:
                print(
                    f"[{ticker}] batch {i}-{i+len(batch)}: HTTP {resp.status_code} — {resp.text[:200]}",
                    file=sys.stderr,
                )
                total['errors'] += len(batch)
        except requests.RequestException as exc:
            print(f"[{ticker}] batch {i}: request failed: {exc}", file=sys.stderr)
            total['errors'] += len(batch)

    print(
        f"[{ticker}] ingested={total['ingested']} skipped={total['skipped']} errors={total['errors']}"
    )
    return {'ticker': ticker, **total, 'status': 'ok'}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Ingest LLM crawl results into minerdata.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true', help='Ingest all tickers in crawl_results/')
    group.add_argument('--ticker', metavar='TICKER', help='Ingest one ticker')
    parser.add_argument('--dry-run', action='store_true', help='Print counts without POSTing')
    parser.add_argument('--api-base', default=_API_BASE, help=f'Base URL (default: {_API_BASE})')
    args = parser.parse_args(argv)

    api_base = args.api_base.rstrip('/')

    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = sorted(p.parent.name for p in _RESULTS_DIR.glob('*/results.json'))

    if not tickers:
        print("No results found in", _RESULTS_DIR, file=sys.stderr)
        return 1

    summaries = [_ingest_ticker(t, dry_run=args.dry_run, api_base=api_base) for t in tickers]

    total_errors = sum(s['errors'] for s in summaries)
    total_ingested = sum(s['ingested'] for s in summaries)
    total_skipped = sum(s['skipped'] for s in summaries)
    print(f"\nTotal: ingested={total_ingested} skipped={total_skipped} errors={total_errors}")
    return 1 if total_errors else 0


if __name__ == '__main__':
    sys.exit(main())
