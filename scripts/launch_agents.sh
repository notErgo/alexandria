#!/usr/bin/env bash
# launch_agents.sh — spawn all 5 Codex agents in parallel for full miner data ingestion
#
# Usage:
#   ./scripts/launch_agents.sh           # launch all 5 agents
#   ./scripts/launch_agents.sh A         # launch only agent A
#   ./scripts/launch_agents.sh status    # check progress of all agents
#
# Each agent runs as: codex exec --dangerously-bypass-approvals-and-sandbox
# Output logged to: /private/tmp/claude-501/miners_progress/agent_{X}.log
# Progress JSON:    /private/tmp/claude-501/miners_progress/agent_{X}.json

set -euo pipefail

PROJ="/Users/workstation/Documents/Hermeneutic/OffChain/miners"
PROMPTS="$PROJ/scripts/prompts"
PROGRESS="/private/tmp/claude-501/miners_progress"
LOG_DIR="$PROGRESS/logs"

mkdir -p "$PROGRESS" "$LOG_DIR"

# ── Status check ──────────────────────────────────────────────────────────────
if [[ "${1:-}" == "status" ]]; then
    echo ""
    echo "=== AGENT STATUS ==="
    for agent in A B C D E; do
        json="$PROGRESS/agent_${agent}.json"
        log="$LOG_DIR/agent_${agent}.log"
        if [[ -f "$json" ]]; then
            status=$(python3 -c "import json; d=json.load(open('$json')); print(d.get('status','?'))" 2>/dev/null || echo "?")
            updated=$(python3 -c "import json; d=json.load(open('$json')); print(d.get('updated_at', d.get('status','?'))[:19])" 2>/dev/null || echo "?")
            echo "  Agent $agent: $status  (updated: $updated)"
        else
            echo "  Agent $agent: not started"
        fi
        if [[ -f "$log" ]]; then
            lines=$(wc -l < "$log" | tr -d ' ')
            echo "    Log: $log ($lines lines)"
        fi
    done
    echo ""
    echo "=== COORDINATOR SUMMARY ==="
    coord="$PROGRESS/coordinator_state.json"
    if [[ -f "$coord" ]]; then
        python3 -c "
import json
d = json.load(open('$coord'))
agents = d.get('agents', {})
print(f'  Agents: {len(agents)} total')
print(f'  Errors logged: {len(d.get(\"errors\", []))}')
print(f'  Gaps filled: {len(d.get(\"gaps_filled\", []))}')
" 2>/dev/null || echo "  (parse error)"
    else
        echo "  No coordinator state yet"
    fi
    echo ""
    exit 0
fi

# ── Preflight checks ──────────────────────────────────────────────────────────
echo "=== PREFLIGHT CHECKS ==="

# Check codex is available
if ! command -v codex &>/dev/null; then
    echo "ERROR: codex CLI not found in PATH"
    exit 1
fi
echo "  codex: $(codex --version 2>&1 | head -1)"

# Check Python venv
if [[ ! -f "$PROJ/venv/bin/python3" ]]; then
    echo "ERROR: venv not found at $PROJ/venv — run: python3 -m venv venv && pip install -r requirements.txt"
    exit 1
fi
echo "  venv: OK"

# Check DB accessible
DB="$HOME/Documents/Hermeneutic/data/miners/minerdata.db"
if [[ ! -f "$DB" ]]; then
    echo "WARNING: DB not found at $DB — will be created on first run"
else
    echo "  DB: OK ($DB)"
fi

# Check EDGAR all script
if [[ ! -f "$PROJ/scripts/run_edgar_all.py" ]]; then
    echo "ERROR: run_edgar_all.py not found"
    exit 1
fi
echo "  Scripts: OK"
echo ""

# ── Agent launcher function ───────────────────────────────────────────────────
launch_agent() {
    local id="$1"
    local prompt_file="$2"
    local log_file="$LOG_DIR/agent_${id}.log"
    local output_file="$PROGRESS/agent_${id}_last_message.txt"
    local combined_prompt="$PROGRESS/agent_${id}_prompt.txt"

    if [[ ! -f "$prompt_file" ]]; then
        echo "ERROR: prompt file not found: $prompt_file"
        return 1
    fi

    # Prepend shared wire service guide to every agent prompt
    cat "$PROMPTS/00_wire_services.md" > "$combined_prompt"
    echo "" >> "$combined_prompt"
    echo "---" >> "$combined_prompt"
    echo "" >> "$combined_prompt"
    cat "$prompt_file" >> "$combined_prompt"

    echo "Launching Agent $id..."
    echo "  Prompt: $combined_prompt (wire guide + agent-specific)"
    echo "  Log:    $log_file"

    # Write initial status
    python3 -c "
import json, datetime
d = {
    'agent': '$id',
    'status': 'launched',
    'launched_at': datetime.datetime.utcnow().isoformat(),
    'prompt_file': '$prompt_file',
    'combined_prompt': '$combined_prompt',
}
with open('$PROGRESS/agent_${id}.json', 'w') as f:
    json.dump(d, f, indent=2)
" 2>/dev/null || true

    # Launch codex agent in background
    nohup codex exec \
        --dangerously-bypass-approvals-and-sandbox \
        -C "$PROJ" \
        -o "$output_file" \
        "$(cat "$combined_prompt")" \
        > "$log_file" 2>&1 &

    local pid=$!
    echo "  PID: $pid"
    echo "$pid" > "$PROGRESS/agent_${id}.pid"
    echo ""
}

# ── Launch requested agents ───────────────────────────────────────────────────
TARGET="${1:-all}"

if [[ "$TARGET" == "all" || "$TARGET" == "A" ]]; then
    launch_agent "A" "$PROMPTS/agent_A_mara_riot.md"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "B" ]]; then
    launch_agent "B" "$PROMPTS/agent_B_clsk_bitf_btbt.md"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "C" ]]; then
    launch_agent "C" "$PROMPTS/agent_C_arbk_cifr_hive_btdr.md"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "D" ]]; then
    launch_agent "D" "$PROMPTS/agent_D_quarterly_edgar.md"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "E" ]]; then
    launch_agent "E" "$PROMPTS/agent_E_prompts_docs.md"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== LAUNCHED ==="
echo "All agents running in background."
echo ""
echo "Monitor with:"
echo "  ./scripts/launch_agents.sh status"
echo ""
echo "Tail a specific agent log:"
echo "  tail -f $LOG_DIR/agent_A.log"
echo ""
echo "Run final report after agents complete:"
echo "  source venv/bin/activate && python3 scripts/generate_report.py --out-dir reports/"
echo ""
echo "OTEL attributes: plan=full_ingest_v1"
echo "Tracking DB: ~/Documents/Hermeneutic/data/workflow/tracking.db"
