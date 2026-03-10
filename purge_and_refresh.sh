#!/usr/bin/env bash
# Safe full reset: purge all data then run the complete ingest+extract pipeline.
#
# Strategy:
#   1. If the web server is reachable, use POST /api/data/purge (keeps DB file
#      intact, no WAL orphan risk, web server stays live throughout).
#   2. If the web server is not running, delete the DB + WAL files cleanly and
#      let the CLI pipeline recreate them fresh.
#   3. Kill any in-progress CLI pipeline before purging.
#   4. Run refresh.sh to ingest + extract + report.
#
# Usage:
#   ./purge_and_refresh.sh              # all companies
#   ./purge_and_refresh.sh MARA         # one ticker only
#   ./purge_and_refresh.sh --hard       # hard_delete mode (also wipes companies table)
#   ./purge_and_refresh.sh MARA --hard

set -e
cd "$(dirname "$0")"

# ── Parse args ────────────────────────────────────────────────────────────────
TICKER=""
PURGE_MODE="reset"
REFRESH_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --hard) PURGE_MODE="hard_delete" ;;
        --*)    REFRESH_ARGS+=("$arg") ;;
        *)      if [[ -z "$TICKER" ]]; then TICKER="$arg"; fi ;;
    esac
done

TICKER_UPPER="$(echo "$TICKER" | tr '[:lower:]' '[:upper:]')"
WEB_URL="http://localhost:5004"
DB_PATH="$HOME/Documents/Hermeneutic/data/miners/minerdata.db"

echo "=== Purge + Refresh ==="
echo "  Mode:   $PURGE_MODE"
echo "  Ticker: ${TICKER_UPPER:-ALL}"
echo ""

# ── Step 1: Kill any running CLI pipeline ────────────────────────────────────
echo "--- Stopping any running pipeline processes..."
pkill -f "refresh.sh" 2>/dev/null || true
pkill -f "cli.py ingest" 2>/dev/null || true
pkill -f "cli.py extract" 2>/dev/null || true
sleep 1
echo "    Done."
echo ""

# ── Step 2: Purge ─────────────────────────────────────────────────────────────
# Build JSON payload
if [[ -n "$TICKER_UPPER" ]]; then
    PAYLOAD="{\"purge_mode\": \"$PURGE_MODE\", \"ticker\": \"$TICKER_UPPER\", \"reason\": \"purge_and_refresh.sh\"}"
else
    PAYLOAD="{\"purge_mode\": \"$PURGE_MODE\", \"reason\": \"purge_and_refresh.sh\"}"
fi

# Check if web server is up
SERVER_UP=0
if curl -sf "$WEB_URL/api/status" -o /dev/null 2>/dev/null; then
    SERVER_UP=1
fi

if [[ $SERVER_UP -eq 1 ]]; then
    echo "--- Web server is running — purging via API ($PURGE_MODE)..."
    RESULT=$(curl -sf -X POST "$WEB_URL/api/data/purge" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD" 2>&1) || {
        echo "    ERROR: API purge failed: $RESULT"
        exit 1
    }
    echo "    $RESULT"
    echo "    Done."
else
    echo "--- Web server not running — deleting DB files directly..."
    if [[ -f "$DB_PATH" ]]; then
        rm -f "$DB_PATH" "${DB_PATH}-shm" "${DB_PATH}-wal"
        echo "    Deleted: $DB_PATH (+ -shm, -wal if present)"
    else
        echo "    No DB file found at $DB_PATH — nothing to delete."
    fi
    echo "    Done."
fi
echo ""

# ── Step 3: Restart web server if it was running ─────────────────────────────
if [[ $SERVER_UP -eq 1 && "$PURGE_MODE" == "hard_delete" && -z "$TICKER_UPPER" ]]; then
    # hard_delete on full scope wipes companies table; restart so MinerDB
    # re-syncs companies from companies.json before the pipeline runs.
    echo "--- Restarting web server (hard_delete requires fresh MinerDB init)..."
    pkill -f "run_web.py" 2>/dev/null || true
    sleep 1
    source venv/bin/activate
    nohup python3 run_web.py > /tmp/miners_web.log 2>&1 &
    WEB_PID=$!
    echo "    Web server restarted (PID $WEB_PID), waiting for ready..."
    for i in $(seq 1 15); do
        if curl -sf "$WEB_URL/api/status" -o /dev/null 2>/dev/null; then
            echo "    Ready."
            break
        fi
        sleep 1
    done
    echo ""
fi

# ── Step 4: Run refresh pipeline ─────────────────────────────────────────────
echo "--- Starting refresh pipeline..."
echo ""
if [[ -n "$TICKER_UPPER" ]]; then
    ./refresh.sh "$TICKER_UPPER" "${REFRESH_ARGS[@]}"
else
    ./refresh.sh "${REFRESH_ARGS[@]}"
fi
