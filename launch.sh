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

# LLM backend — set LLM_BACKEND=ollama to revert to Ollama
export LLM_BACKEND="${LLM_BACKEND:-llamacpp}"
export OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:8080}"

# Check / auto-start LLM backend
if [ "$LLM_BACKEND" = "llamacpp" ]; then
    if curl -sf "${OLLAMA_BASE_URL}/health" > /dev/null 2>&1; then
        echo "llama-server already running at ${OLLAMA_BASE_URL}."
    else
        SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
        if [ -x "$SCRIPT_DIR/llm.sh" ]; then
            echo "Starting llama-server in background..."
            "$SCRIPT_DIR/llm.sh" >> "$SCRIPT_DIR/llm.log" 2>&1 &
            LLM_PID=$!
            echo "  llama-server PID $LLM_PID — logs: $SCRIPT_DIR/llm.log"
            # Wait up to 30s for server to become healthy
            for i in $(seq 1 30); do
                sleep 1
                if curl -sf "${OLLAMA_BASE_URL}/health" > /dev/null 2>&1; then
                    echo "  llama-server ready (${i}s)."
                    break
                fi
                if [ $i -eq 30 ]; then
                    echo "WARNING: llama-server did not become healthy after 30s — LLM extraction may fail."
                fi
            done
        else
            echo "WARNING: llm.sh not found — llama-server not started. LLM extraction will fail."
        fi
    fi
else
    if curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; then
        echo "Ollama ready."
    else
        echo "WARNING: Ollama not reachable at localhost:11434 — LLM extraction will fail."
        echo "         Start Ollama.app before running ingest."
    fi
fi

echo ""
echo "Starting Miner Data Platform on http://localhost:5004"
echo "Data directory: $MINERS_DATA_DIR"
echo ""

python3 run_web.py "$@"
