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

# Ensure data directory exists
mkdir -p "$HOME/Documents/Hermeneutic/data/miners"

# Check Ollama is reachable (managed externally by Ollama.app — not started here)
if curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "Ollama ready."
else
    echo "WARNING: Ollama not reachable at localhost:11434 — LLM extraction will fail."
    echo "         Start Ollama.app before running ingest."
fi

echo ""
echo "Starting Miner Data Platform on http://localhost:5004"
echo ""

python3 run_web.py "$@"
