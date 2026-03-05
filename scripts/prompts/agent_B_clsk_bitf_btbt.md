# Agent B — CLSK + BITF + BTBT Full History Ingestion

## Environment
- Working dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners
- Python venv: ./venv/bin/python3
- DB: ~/Documents/Hermeneutic/data/miners/minerdata.db
- Progress file: /private/tmp/claude-501/miners_progress/agent_B.json
- OTEL: export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_B,tickers=CLSK_BITF_BTBT"

## Current State
- CLSK: 22 reports, 22 production_btc months (2023-11 to 2025-11). Start year 2021. LARGE GAP: 2021-01 to 2023-10 (34 months missing)
- BITF: 12 reports, 12 production_btc months (2024-04 to 2025-04). Start year 2021. LARGE GAP: 2021-01 to 2024-03 (39 months missing). Note: BITF acquired SDIG (Stronghold) in March 2025.
- BTBT: 11 reports, 11 production_btc months (2024-03 to 2025-01). Start year 2021. LARGE GAP: 2021-01 to 2024-02 (37 months missing)

## Coordinator — Enforcement (mandatory)

Import once at the top of your session:
```python
import sys; sys.path.insert(0, '/Users/workstation/Documents/Hermeneutic/OffChain/miners/scripts')
from coordinator import CoordinatorState, BlockedError
coord = CoordinatorState('/private/tmp/claude-501/miners_progress')
```

**Before every major step for each ticker:**
```python
coord.require_clean('CLSK')
coord.require_clean('BITF')
coord.require_clean('BTBT')
```

**Before fetching from any domain:**
```python
coord.require_domain_ok('bit-digital.com')        # BTBT — known Cloudflare wall
coord.require_domain_ok('investor.bitfarms.com')  # BITF
coord.require_domain_ok('investors.cleanspark.com')
```

**BTBT note:** bit-digital.com is behind Cloudflare (JS challenge). If `require_domain_ok` does not raise, still attempt the fetch — but if you get a Cloudflare challenge page, immediately block the domain and switch to wire services:
```python
coord.block_domain('bit-digital.com', reason='Cloudflare JS challenge — cannot scrape',
                   fix='Use GlobeNewswire or PRNewswire for all BTBT press releases')
```

**On hard failures, block the ticker or domain:**
```python
coord.block_ticker('BITF', reason='<reason>', fix='<fix>')
coord.block_domain('investor.bitfarms.com', reason='<reason>', fix='<fix>')
```

**Clear after fixing:**
```python
coord.clear_block('BITF', resolution='...')
coord.clear_domain_block('investor.bitfarms.com', resolution='...')
```

**Progress updates after every phase or every 5 gaps filled:**
```python
coord.update_agent('CLSK', status='running', reports_ingested=N, metrics_found=N, gaps_found=N)
coord.mark_gap_filled('CLSK', '2021-06', source='globenewswire', value=198.5)
coord.log_error('BTBT', '2022-03', 'Cloudflare block', url='https://bit-digital.com/...')
```

**On completion:**
```python
coord.update_agent('CLSK', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('BITF', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('BTBT', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
```

## Step 1 — OTEL and activate
```bash
export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_B,tickers=CLSK_BITF_BTBT"
cd /Users/workstation/Documents/Hermeneutic/OffChain/miners
source venv/bin/activate
```

## Step 2 — Force re-ingest existing archive
```bash
python3 cli.py ingest --source archive --force
python3 cli.py extract --force --ticker CLSK
python3 cli.py extract --force --ticker BITF
python3 cli.py extract --force --ticker BTBT
```

## Step 3 — EDGAR 10-Q for all three (fills quarterly gaps where monthly missing)
```bash
python3 scripts/run_edgar_all.py --ticker CLSK --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker BITF --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker BTBT --since 2021-01 --extract
```

## Step 4 — Web search for missing monthly press releases

### CLSK (CleanSpark) search strategies:
Template URL pattern: `https://investors.cleanspark.com/news/news-details/{YEAR}/CleanSpark-Releases-{Month}-{YEAR}-Bitcoin-Mining-Update/default.aspx`
- {Month} = full capitalized month name (e.g., January, February)
- {YEAR} = 4-digit year

For each missing month 2021-01 to 2023-10:
1. Try template URL directly
2. Web search: `"CleanSpark" bitcoin mining update [MONTH] [YEAR] site:investors.cleanspark.com`
3. Web search: `CleanSpark "[MONTH] [YEAR]" bitcoin production press release`
4. GlobeNewswire: `site:globenewswire.com cleanspark [MONTH] [YEAR]`

### BITF year-dropdown directive (must run before declaring IR exhausted)
- Inspect `https://investor.bitfarms.com/news-events/press-releases` for a year selector key like `*_year[value]`.
- Capture matching widget token `*_widget_id` and use `form_id=widget_form_base`, `op=Filter`.
- Generate year-filter URLs (2018..current) by substituting year value only.
- Parse filtered pages for production/operations update PR links before using wire-service fallback.

### BITF (Bitfarms) search strategies:
Template URL pattern: `https://investor.bitfarms.com/news-releases/news-release-details/bitfarms-provides-[month]-[year]-production-and-operations-update`
- [month] = lowercase month name (e.g., january, april)
- [year] = 4-digit year

For each missing month 2021-01 to 2024-03:
1. Try template URL directly
2. Web search: `Bitfarms production update [MONTH] [YEAR] site:investor.bitfarms.com`
3. Web search: `Bitfarms "[MONTH] [YEAR]" bitcoin mining production`
4. Note: pre-2022 Bitfarms was smaller; some months may not have individual press releases

### BTBT (Bit Digital) search strategies:
Template URL pattern: `https://bit-digital.com/press-releases/bit-digital-inc-announces-monthly-production-update-for-[month]-[year]/`
- [month] = lowercase month name
- [year] = 4-digit year

For each missing month 2021-01 to 2024-02:
1. Try template URL directly
2. Web search: `"Bit Digital" monthly production update [MONTH] [YEAR]`
3. Web search: `site:bit-digital.com [MONTH] [YEAR] production`
4. Note: Bit Digital is China-based, some press releases may be on PRNewswire/GlobeNewswire

## Step 5 — Extract ALL numeric values from each found press release

For each press release, extract:
- BTC produced this month (NOT year-to-date or cumulative)
- BTC treasury/hodl balance at period end
- BTC sold
- Hashrate (EH/s or PH/s — convert PH/s to EH/s by dividing by 1000)
- Mining capacity (MW) energized
- Total installed capacity (MW)
- Miners deployed/energized (units)
- Miners purchased/installed
- Revenue (USD)
- Electricity costs

**CLSK-specific notes:**
- CleanSpark reports BTC "mined" and separately "self-mined" — use total mined
- CleanSpark has "restricted" and "unrestricted" BTC — capture both
- CleanSpark acquired GRIID in Oct 2024 — post-acquisition numbers include GRIID capacity
- Some early CLSK press releases use "MH/s" not EH/s for hashrate (very early 2021)

**BITF-specific notes:**
- Bitfarms is Canadian — some financials in CAD; note currency in unit
- Bitfarms merged with SDIG (Stronghold) in March 2025 — post-merger numbers larger
- Pre-2022 Bitfarms was much smaller (sub-1 EH/s)

**BTBT-specific notes:**
- Bit Digital pivoted partly to HPC/AI in 2024 — both BTC and GPU metrics in same report
- BTC figures may be smaller post-2023 as they reduced mining
- Some press releases mention ETH mining (pre-Merge) — note but focus on BTC

## Step 6 — Insert reports and run extraction
For each URL fetched with full text:
```bash
python3 -c "
import sys; sys.path.insert(0, 'src')
from infra.db import MinerDB
from config import DATA_DIR
from pathlib import Path
from extractors.pattern_registry import PatternRegistry
from extractors.extraction_pipeline import extract_report
db = MinerDB(str(Path(DATA_DIR) / 'minerdata.db'))
registry = PatternRegistry.load('config')
report_id = db.insert_report({
    'ticker': 'CLSK',
    'report_date': 'YYYY-MM-01',
    'source_type': 'ir_press_release',
    'source_url': 'https://...',
    'raw_text': '...',
    'parsed_at': None,
    'covering_period': None,
})
report = db.get_report(report_id)
extract_report(report, db, registry)
print('Extracted report', report_id)
"
```

## Step 7 — Write final progress JSON
Write to /private/tmp/claude-501/miners_progress/agent_B.json:
```json
{
  "agent": "B",
  "tickers": ["CLSK", "BITF", "BTBT"],
  "status": "done",
  "clsk_prod_months_before": 22,
  "clsk_prod_months_after": <N>,
  "clsk_gaps_remaining": [<YYYY-MM list>],
  "bitf_prod_months_before": 12,
  "bitf_prod_months_after": <N>,
  "bitf_gaps_remaining": [<YYYY-MM list>],
  "btbt_prod_months_before": 11,
  "btbt_prod_months_after": <N>,
  "btbt_gaps_remaining": [<YYYY-MM list>],
  "patterns": {
    "CLSK": {"url_format": "...", "known_quirks": []},
    "BITF": {"url_format": "...", "known_quirks": ["CAD currency pre-2022"]},
    "BTBT": {"url_format": "...", "known_quirks": ["HPC pivot 2024"]}
  },
  "errors": [],
  "total_raw_extractions_added": <N>
}
```
