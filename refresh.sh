#!/usr/bin/env bash
# Full pipeline: scrape live IRs + EDGAR → ingest archive → extract → coverage report
#
# Usage:
#   ./refresh.sh                   # all companies, 3 extraction workers
#   ./refresh.sh MARA              # one ticker only
#   ./refresh.sh MARA --workers 4  # one ticker, 4 workers
set -e

TICKER=${1:-""}
shift 2>/dev/null || true
TICKER_FLAG=${TICKER:+--ticker $TICKER}
EXTRACT_WORKERS=${MINERS_EXTRACT_WORKERS:-3}
# EDGAR limit: 10 req/s per IP. With 2 workers × 0.5s delay = 4 req/s total — safe.
# Do not raise above 2 without also setting EDGAR_REQUEST_DELAY to compensate.
INGEST_WORKERS=${MINERS_INGEST_WORKERS:-2}

# Allow --workers N override from remaining args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers) EXTRACT_WORKERS="$2"; INGEST_WORKERS="$2"; shift 2 ;;
        --extract-workers) EXTRACT_WORKERS="$2"; shift 2 ;;
        --ingest-workers) INGEST_WORKERS="$2"; shift 2 ;;
        *) shift ;;
    esac
done

cd "$(dirname "$0")"
source venv/bin/activate

echo "=== 1/4  Scrape live IR pages (${INGEST_WORKERS} workers) ==="
python3 cli.py ingest --source ir $TICKER_FLAG --workers "$INGEST_WORKERS"

echo ""
echo "=== 2/4  Ingest EDGAR filings (${INGEST_WORKERS} workers) ==="
python3 cli.py ingest --source edgar $TICKER_FLAG --workers "$INGEST_WORKERS"

echo ""
echo "=== 3/4  Extract (${EXTRACT_WORKERS} workers) ==="
python3 cli.py extract $TICKER_FLAG --workers "$EXTRACT_WORKERS"

echo ""
echo "=== 4/4  Coverage report ==="
python3 cli.py diagnose $TICKER_FLAG
