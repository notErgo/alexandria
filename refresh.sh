#!/usr/bin/env bash
# Full pipeline: scrape live IRs → ingest archive → show coverage report
#
# Usage:
#   ./refresh.sh              # run for all active companies
#   ./refresh.sh MARA         # run for one ticker only
set -e

TICKER=${1:-""}
FLAG=${TICKER:+--ticker $TICKER}
cd "$(dirname "$0")"
source venv/bin/activate

echo "=== 1/3  Scrape live IR pages ==="
python3 cli.py ingest --source ir $FLAG

echo ""
echo "=== 2/3  Ingest archive ==="
python3 cli.py ingest --source archive $FLAG

echo ""
echo "=== 3/3  Coverage report ==="
python3 cli.py diagnose $FLAG
