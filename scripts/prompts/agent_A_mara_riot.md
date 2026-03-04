# Agent A — MARA + RIOT Full History Ingestion

## Environment
- Working dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners
- Python venv: ./venv/bin/python3
- DB: ~/Documents/Hermeneutic/data/miners/minerdata.db
- Progress file: /private/tmp/claude-501/miners_progress/agent_A.json
- OTEL: export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_A,tickers=MARA_RIOT"

## Current State
- MARA: 83 reports, 46 production_btc months (2021-04 to 2025-09). Archive has 93 files. Start year 2020. GAP: 2020-01 to 2021-03 (15 months missing). Possible gaps within 2021-2025.
- RIOT: 64 reports, 47 production_btc months (2018-04 to 2025-09). Archive has 107 files. Start year 2020. Check for gaps in 2019-2023.

## Coordinator — Enforcement (mandatory)

Import once at the top of your session:
```python
import sys; sys.path.insert(0, '/Users/workstation/Documents/Hermeneutic/OffChain/miners/scripts')
from coordinator import CoordinatorState, BlockedError
coord = CoordinatorState('/private/tmp/claude-501/miners_progress')
```

**Before every major step for each ticker, call `require_clean()` — it raises if a block is active:**
```python
coord.require_clean('MARA')  # raises BlockedError if blocked — read the reason+fix before continuing
coord.require_clean('RIOT')
```

**Before fetching from any domain, call `require_domain_ok()`:**
```python
coord.require_domain_ok('ir.mara.com')
coord.require_domain_ok('www.riotplatforms.com')
```

**If you hit a hard failure (bad CIK, Cloudflare wall, persistent 502 with no fallback), block it:**
```python
# Ticker block — stops this and other agents from retrying the same broken path
coord.block_ticker('MARA', reason='<what failed>', fix='<what must be done to fix it>')

# Domain block — warns all agents off a broken domain
coord.block_domain('ir.mara.com', reason='502 on all requests, no fallback', fix='Use GlobeNewswire RSS instead')
```

**Clear a block after you fix the underlying issue:**
```python
coord.clear_block('MARA', resolution='Found correct URL pattern, re-ingested successfully')
coord.clear_domain_block('ir.mara.com', resolution='GlobeNewswire RSS used instead')
```

**Progress updates after every phase or every 5 gaps filled:**
```python
coord.update_agent('MARA', status='running', reports_ingested=N, metrics_found=N, gaps_found=N)
coord.mark_gap_filled('MARA', '2020-03', source='globenewswire', value=143.2)
coord.log_error('MARA', '2020-01', 'HTTP 404', url='https://...')
```

**On completion:**
```python
coord.update_agent('MARA', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('RIOT', status='done', reports_ingested=N, metrics_found=N, gaps_found=N)
```

## Step 1 — Set OTEL and activate environment
```bash
export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_A,tickers=MARA_RIOT"
cd /Users/workstation/Documents/Hermeneutic/OffChain/miners
source venv/bin/activate
```

## Step 2 — Force re-ingest all archive files
```bash
python3 cli.py ingest --source archive --force
python3 cli.py extract --force --ticker MARA
python3 cli.py extract --force --ticker RIOT
```

## Step 3 — Check what months are now covered
```bash
python3 cli.py query --ticker MARA --metric production_btc --format json | python3 -c "
import json,sys
rows = json.load(sys.stdin)
periods = sorted(set(r['period'][:7] for r in rows))
print('MARA covered:', periods)
"

python3 cli.py query --ticker RIOT --metric production_btc --format json | python3 -c "
import json,sys
rows = json.load(sys.stdin)
periods = sorted(set(r['period'][:7] for r in rows))
print('RIOT covered:', periods)
"
```

## Step 4 — Identify gaps and web-search for missing months

For MARA, expected coverage: 2020-01 through 2025-09 (69 months).
For RIOT, expected coverage: 2018-04 through 2025-09 (89 months).

For each missing month, search for press releases using these strategies:

### MARA search strategies (try in order):
1. Web search: `"Marathon Digital" OR "MARA Holdings" bitcoin production [MONTH] [YEAR] press release`
2. Web search: `site:ir.mara.com [MONTH] [YEAR] production`
3. Direct URL: `https://ir.mara.com/news-events/press-releases` (scan index for the month)
4. Archived: `https://web.archive.org/web/[YEAR][MM]*/ir.mara.com/news-events/press-releases/*`
5. GlobeNewswire: web search `site:globenewswire.com marathon digital [MONTH] [YEAR] production`

### RIOT search strategies (try in order):
1. Web search: `"Riot Platforms" OR "Riot Blockchain" bitcoin production [MONTH] [YEAR]`
2. Template URL: `https://www.riotplatforms.com/riot-announces-[month]-[year]-production-and-operations-updates/`
   - Replace [month] with lowercase month name (e.g., january, february)
   - Replace [year] with 4-digit year
3. For pre-2021 (when company was "Riot Blockchain"): search `"Riot Blockchain" bitcoin mining [MONTH] [YEAR]`
4. GlobeNewswire/BusinessWire fallback

## Step 5 — Extract ALL numeric values from each found press release

For each press release URL found, fetch the content and extract EVERY numeric metric:

**Standard metrics to always capture:**
- BTC produced/mined this month (NOT year-to-date)
- BTC held in treasury/hodl at period end
- BTC sold this month
- Hashrate operational (EH/s) — deployed/energized, NOT installed capacity
- Hashrate installed total (if different from operational)
- Mining capacity (MW) — energized
- Total capacity (MW) — including non-energized
- AI/HPC capacity (MW) if mentioned
- Miners deployed/energized (units count)
- Miners purchased/installed (units count)
- Revenue from mining (USD)
- Electricity cost (USD or USD/kWh)
- Encumbered/collateralized BTC
- Realization rate (% of spot price received for BTC sold)

**Store using direct sqlite insert for any metric not in standard pipeline:**
```python
import sqlite3, sys
sys.path.insert(0, 'src')
from config import DATA_DIR
from pathlib import Path
conn = sqlite3.connect(str(Path(DATA_DIR) / 'minerdata.db'))
# Insert to raw_extractions
conn.execute("""
    INSERT OR REPLACE INTO raw_extractions
    (report_id, ticker, period, metric_key, category, value, value_text, unit, description, confidence, source_snippet, extraction_method)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'codex_agent')
""", (report_id, ticker, period, metric_key, category, value, value_text, unit, description, confidence, snippet))
conn.commit()
conn.close()
```

**CRITICAL extraction rules:**
- NEVER confuse year-to-date with monthly. Monthly value always appears first in sentences like "produced X BTC in September and Y BTC YTD"
- RIOT tables: some show current month vs prior month side by side — always use the first data column
- MARA 2020-2021: press releases may use "Marathon Patent Group" as the company name
- RIOT 2018-2020: company was "Riot Blockchain" — same extraction rules apply

## Step 6 — Ingest newly found reports via CLI

If you found a press release URL, ingest it:
```bash
# Option A: if it's a direct HTML URL, the IR scraper can handle it
python3 cli.py ingest --source ir --ticker MARA

# Option B: store the raw text directly and extract
python3 -c "
import sys; sys.path.insert(0, 'src')
from infra.db import MinerDB
from config import DATA_DIR
from pathlib import Path
db = MinerDB(str(Path(DATA_DIR) / 'minerdata.db'))
report_id = db.insert_report({
    'ticker': 'MARA',
    'report_date': '2020-03-01',
    'published_date': None,
    'source_type': 'ir_press_release',
    'source_url': 'https://...',
    'raw_text': '... full text ...',
    'parsed_at': None,
    'covering_period': None,
})
print('Inserted report', report_id)
"
python3 cli.py extract --ticker MARA
```

## Step 7 — Write final progress JSON
Write to /private/tmp/claude-501/miners_progress/agent_A.json:
```json
{
  "agent": "A",
  "tickers": ["MARA", "RIOT"],
  "status": "done",
  "mara_reports_before": 83,
  "mara_reports_after": <N>,
  "mara_prod_months_before": 46,
  "mara_prod_months_after": <N>,
  "mara_gaps_remaining": [<list of YYYY-MM still missing>],
  "riot_reports_before": 64,
  "riot_reports_after": <N>,
  "riot_prod_months_before": 47,
  "riot_prod_months_after": <N>,
  "riot_gaps_remaining": [<list>],
  "patterns": {
    "MARA": {
      "cadence": "monthly",
      "url_format": "<discovered pattern>",
      "reporting_name_history": ["Marathon Patent Group (pre-2021)", "Marathon Digital Holdings", "MARA Holdings"],
      "known_quirks": ["<any quirks found>"]
    },
    "RIOT": {
      "cadence": "monthly",
      "url_format": "https://www.riotplatforms.com/riot-announces-{month}-{year}-production-and-operations-updates/",
      "reporting_name_history": ["Riot Blockchain (pre-2021)", "Riot Platforms"],
      "known_quirks": ["<any quirks found>"]
    }
  },
  "errors": [{"ticker":"..","period":"..","url":"..","error":".."}],
  "total_raw_extractions_added": <N>
}
```

## Error handling
- HTTP 502/503: retry after 30s, max 3 attempts, then log and move on
- 404: try archive.org fallback, log if also 404
- Parsing errors: store the raw text anyway, log the error, continue
- Never crash — catch all exceptions per URL and continue to next
