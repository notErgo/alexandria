#!/usr/bin/env bash
# llm.sh — start llama-server with parameters from config_settings DB.
# Edit values via the UI (Settings > llama-server Config) or directly:
#   sqlite3 ~/Documents/Hermeneutic/data/miners/minerdata.db \
#     "INSERT OR REPLACE INTO config_settings (key,value) VALUES ('llama_parallel','8');"
set -e

DB="${MINERS_DATA_DIR:-$HOME/Documents/Hermeneutic/data/miners}/minerdata.db"
DEFAULT_MODEL="$HOME/.ollama/models/blobs/sha256-2bada8a7450677000f678be90653b85d364de7db25eb5ea54136ada5f3933730"

_cfg() {
    local key="$1" default="$2"
    if [ -f "$DB" ] && command -v sqlite3 &>/dev/null; then
        local val
        val=$(sqlite3 "$DB" "SELECT value FROM config_settings WHERE key='$key' LIMIT 1;" 2>/dev/null || true)
        echo "${val:-$default}"
    else
        echo "$default"
    fi
}

MODEL=$(_cfg llama_model_path "$DEFAULT_MODEL")
PARALLEL=$(_cfg llama_parallel "8")
CTX_SIZE=$(_cfg llama_ctx_size "65536")
N_PREDICT=$(_cfg llama_n_predict "768")
BATCH_SIZE=$(_cfg llama_batch_size "4096")
CACHE_TYPE_K=$(_cfg llama_cache_type_k "q8_0")
CACHE_TYPE_V=$(_cfg llama_cache_type_v "q8_0")
FLASH_ATTN=$(_cfg llama_flash_attn "1")
THREADS=$(_cfg llama_threads "4")
PORT=$(_cfg llama_port "8080")

# Expand ~ in MODEL path if present
MODEL="${MODEL/#\~/$HOME}"

ARGS=(
    --model   "$MODEL"
    --ctx-size  "$CTX_SIZE"
    --n-predict "$N_PREDICT"
    --parallel  "$PARALLEL"
    --batch-size "$BATCH_SIZE"
    --cache-type-k "$CACHE_TYPE_K"
    --cache-type-v "$CACHE_TYPE_V"
    --threads "$THREADS"
    --port    "$PORT"
    -ngl 99
)
[ "$FLASH_ATTN" = "1" ] && ARGS+=(--flash-attn on)

echo "Starting llama-server (parallel=$PARALLEL ctx=$CTX_SIZE flash_attn=$FLASH_ATTN)"
echo "  llama-server ${ARGS[*]}"
echo ""

exec llama-server "${ARGS[@]}"
