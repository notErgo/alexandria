#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

# Create venv if needed
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    venv/bin/pip install -q \
        --trusted-host pypi.org \
        --trusted-host files.pythonhosted.org \
        -r requirements.txt
fi

source venv/bin/activate

# Resolve writable data directory (override with MINERS_DATA_DIR).
DEFAULT_DATA_DIR="$HOME/Documents/Hermeneutic/data/miners"
DATA_DIR="${MINERS_DATA_DIR:-$DEFAULT_DATA_DIR}"
mkdir -p "$DATA_DIR" 2>/dev/null || true
if ! ( touch "$DATA_DIR/.write_probe" 2>/dev/null && rm -f "$DATA_DIR/.write_probe" ); then
    FALLBACK_DATA_DIR="$(pwd)/.data/miners"
    mkdir -p "$FALLBACK_DATA_DIR"
    export MINERS_DATA_DIR="$FALLBACK_DATA_DIR"
    echo "WARNING: Data dir not writable: $DATA_DIR"
    echo "         Falling back to: $MINERS_DATA_DIR"
else
    export MINERS_DATA_DIR="$DATA_DIR"
fi

# Check Ollama is reachable (managed externally by Ollama.app — not started here)
if curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "Ollama ready."
else
    echo "WARNING: Ollama not reachable at localhost:11434 — LLM extraction will fail."
    echo "         Start Ollama.app before running ingest."
fi

echo ""
echo "Starting Miner Data Platform on http://localhost:5004"
echo "Data directory: $MINERS_DATA_DIR"
echo ""

python3 run_web.py "$@"
