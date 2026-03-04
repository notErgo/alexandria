# Agent C — ARBK + CIFR + HIVE + BTDR Full Ingestion (Zero Data)

## Environment
- Working dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners
- Python venv: ./venv/bin/python3
- DB: ~/Documents/Hermeneutic/data/miners/minerdata.db
- Progress file: /private/tmp/claude-501/miners_progress/agent_C.json
- OTEL: export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_C,tickers=ARBK_CIFR_HIVE_BTDR"

## Current State
ALL FOUR companies have ZERO reports and ZERO data points in the DB. Full history ingest required.

- ARBK (Argo Blockchain): start 2021, mode=index, CIK=0001708187. IR: accessnewswire.com. Financial distress — LSE delisted Dec 2025. ~40-130 BTC/month. KNOWN ISSUE: accessnewswire.com index may be slow or return 502.
- CIFR (Cipher Mining/Cipher Digital): start 2021, mode=rss, CIK=0001838247. RSS: investors.cipherdigital.com/rss/news-releases.xml. Monthly through Sep 2025, then quarterly.
- HIVE (HIVE Digital Technologies): start 2021, mode=rss, CIK=0001537808. RSS: feeds.newsfilecorp.com/company/5335. ~290 BTC/month. Fiscal year ends March 31. TSX-V + NASDAQ dual listed.
- BTDR (Bitdeer): start 2023, mode=rss, CIK=0001899123. RSS: ir.bitdeer.com/rss/news-releases.xml. ~526 BTC/month. Singapore-based, files 6-K not 8-K.

## Coordinator — Enforcement (mandatory)

Import once at the top of your session:
```python
import sys; sys.path.insert(0, '/Users/workstation/Documents/Hermeneutic/OffChain/miners/scripts')
from coordinator import CoordinatorState, BlockedError
coord = CoordinatorState('/private/tmp/claude-501/miners_progress')
```

**Before every major step for each ticker:**
```python
coord.require_clean('ARBK')
coord.require_clean('CIFR')
coord.require_clean('HIVE')
coord.require_clean('BTDR')
```

**Before fetching from any domain:**
```python
coord.require_domain_ok('www.accessnewswire.com')   # ARBK — known 502 issues
coord.require_domain_ok('investors.cipherdigital.com')  # CIFR — RSS sometimes times out
coord.require_domain_ok('feeds.newsfilecorp.com')
coord.require_domain_ok('ir.bitdeer.com')
```

**On hard failures (3 retries exhausted, no fallback), block immediately:**
```python
# Example: accessnewswire index returns 0 after 3 attempts
coord.block_domain('www.accessnewswire.com', reason='Index scrape returns 0 after 3 retries',
                   fix='Use web search: site:accessnewswire.com "Argo Blockchain" [month] [year]')

# Example: CIFR RSS times out persistently
coord.block_domain('investors.cipherdigital.com', reason='RSS timeout after 3 retries',
                   fix='Use GlobeNewswire: site:globenewswire.com "cipher mining" [month] [year]')
```

**Clear after switching to a working alternative:**
```python
coord.clear_domain_block('www.accessnewswire.com', resolution='Switched to web search fallback')
```

**Progress updates after every phase or every 5 gaps filled:**
```python
coord.update_agent('ARBK', status='running', reports_ingested=N, metrics_found=N, gaps_found=N)
coord.mark_gap_filled('HIVE', '2022-04', source='newsfilecorp', value=290.0)
coord.log_error('ARBK', '2023-05', 'accessnewswire 502', url='https://...')
```

**On completion:**
```python
coord.update_agent('ARBK', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('CIFR', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('HIVE', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
coord.update_agent('BTDR', status='done', reports_ingested=N, metrics_found=N, gaps_found=0)
```

## Step 1 — OTEL and activate
```bash
export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_C,tickers=ARBK_CIFR_HIVE_BTDR"
cd /Users/workstation/Documents/Hermeneutic/OffChain/miners
source venv/bin/activate
```

## Step 2 — RSS Ingestion (CIFR, HIVE, BTDR)
These use RSS feeds — run IR scraper:
```bash
python3 cli.py ingest --source ir --ticker CIFR
python3 cli.py ingest --source ir --ticker HIVE
python3 cli.py ingest --source ir --ticker BTDR
```
If any returns 0 reports due to 502/timeout, retry up to 3 times with 30s delay.

## Step 3 — ARBK Index Scrape
ARBK uses index mode at accessnewswire.com. The scraper may struggle with it.
Try CLI first:
```bash
python3 cli.py ingest --source ir --ticker ARBK
```
If that fails or returns 0 reports, fall back to manual web search approach:
1. Web search: `"Argo Blockchain" operational update [MONTH] [YEAR] site:accessnewswire.com`
2. Web search: `"Argo Blockchain" monthly bitcoin production [MONTH] [YEAR]`
3. Also check: `site:globenewswire.com "Argo Blockchain" [MONTH] [YEAR]`

## Step 4 — EDGAR for all four (supplement gaps)
```bash
python3 scripts/run_edgar_all.py --ticker ARBK --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker CIFR --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker HIVE --since 2021-01 --extract
python3 scripts/run_edgar_all.py --ticker BTDR --since 2023-01 --extract
```

## Step 5 — Web search for full historical archive (2021–present)

### ARBK search strategy (all months 2021-01 to 2025-07):
Web search for each missing month:
- `"Argo Blockchain" "[MONTH] [YEAR]" operational update bitcoin`
- Check accessnewswire.com index: `https://www.accessnewswire.com/newsroom/en/blockchain-and-cryptocurrency` (filter by "Argo")
- ARBK known monthly BTC range: 40-130 BTC (smaller operator)
- Note: company in severe distress post-2024; some months may have no report

### CIFR search strategy (all months 2021-01 to 2025-09, then quarterly):
- RSS feed should cover most recent. For older months:
- Web search: `"Cipher Mining" OR "Cipher Digital" production update [MONTH] [YEAR]`
- GlobeNewswire: `site:globenewswire.com cipher mining [MONTH] [YEAR]`
- Note: Company rebranded from Cipher Mining to Cipher Digital. investors.ciphermining.com redirects to investors.cipherdigital.com
- Monthly ~210-251 BTC through Sep 2025, then quarterly format

### HIVE search strategy (all months 2021-01 to 2025-11):
- Newsfile Corp RSS is the primary source: feeds.newsfilecorp.com/company/5335
- Filter RSS items by title containing "Production Report" or "Bitcoin Production"
- Skip RETRANSMISSION items (duplicates in Newsfile RSS)
- Web search fallback: `"HIVE Digital" OR "HIVE Blockchain" bitcoin production [MONTH] [YEAR]`
- Note: HIVE fiscal year ends March 31 — some reports say "FY2022 Q3" meaning Oct-Dec 2021
- HIVE also mines ETH (pre-Merge) — ignore ETH, focus on BTC metrics

### BTDR search strategy (all months 2023-01 to 2025-12):
- RSS feed at ir.bitdeer.com/rss/news-releases.xml should cover all
- Title pattern: "Bitdeer Announces [Month] [Year] Production and Operations Update"
- For gaps: web search `Bitdeer production update [MONTH] [YEAR]`
- Note: Bitdeer is developing SEALMINER ASICs — reports may mention upcoming/prototype hashrate
- Files 6-K with SEC (not 8-K) — EDGAR approach differs

## Step 6 — Extract ALL numeric values from each report

For each company, extract and store:
- BTC produced/mined (monthly, NOT cumulative or YTD)
- BTC treasury/holdings at period end
- Hashrate operational (EH/s)
- Hashrate total fleet (EH/s) — may differ from operational
- Mining capacity (MW) energized
- Miners deployed/energized count
- Revenue or mining revenue (USD)
- Power cost per BTC or cost of revenue
- Any AI/HPC capacity or revenue (BTDR expanding into AI)
- For HIVE: ETH mined (separate metric, store as metric_key='eth_produced')
- For BTDR: SEALMINER units manufactured/shipped (operational KPI)

**HIVE-specific quirks:**
- Reports monthly BTC and sometimes GPU cloud revenue separately
- "Digital currency mining" section vs "high-performance computing" section
- Hashrate stated in EH/s AND sometimes separately for BTC vs other coins

**ARBK-specific quirks:**
- Very small company, some months may only have a brief operations update, not full press release
- Argo mines on behalf of Pluto Digital in some periods — clarify if total or self-mined
- Late 2023: Chapter 11 bankruptcy proceedings in US — some months may show zero production

**CIFR-specific quirks:**
- Pure-play BTC miner, straightforward reports
- Post-Sep 2025: switched to quarterly "Business Update" format

**BTDR-specific quirks:**
- Singapore HQ, some metrics in USD and some KPIs use non-standard terminology
- "Proprietary mining" vs "cloud hash" — capture both separately

## Step 7 — Run extraction pipeline on all new reports
```bash
python3 cli.py extract --ticker ARBK
python3 cli.py extract --ticker CIFR
python3 cli.py extract --ticker HIVE
python3 cli.py extract --ticker BTDR
```

## Step 8 — Write final progress JSON
Write to /private/tmp/claude-501/miners_progress/agent_C.json:
```json
{
  "agent": "C",
  "tickers": ["ARBK", "CIFR", "HIVE", "BTDR"],
  "status": "done",
  "arbk_reports_ingested": <N>,
  "arbk_prod_months": <N>,
  "arbk_gaps_remaining": [],
  "cifr_reports_ingested": <N>,
  "cifr_prod_months": <N>,
  "cifr_gaps_remaining": [],
  "hive_reports_ingested": <N>,
  "hive_prod_months": <N>,
  "hive_gaps_remaining": [],
  "btdr_reports_ingested": <N>,
  "btdr_prod_months": <N>,
  "btdr_gaps_remaining": [],
  "patterns": {
    "ARBK": {"ir_url": "accessnewswire.com", "btc_range": "40-130/month", "quirks": []},
    "CIFR": {"ir_url": "investors.cipherdigital.com", "rss": "confirmed", "quirks": []},
    "HIVE": {"ir_url": "newsfilecorp.com/company/5335", "fiscal_year_end": "March 31", "quirks": []},
    "BTDR": {"ir_url": "ir.bitdeer.com", "filing_type": "6-K", "quirks": []}
  },
  "errors": [],
  "total_raw_extractions_added": <N>
}
```

## Error handling
- 502 on accessnewswire.com: retry 3x with 30s delay, then use web search fallback
- RSS parse errors: try fetching raw feed XML and parsing manually
- HIVE retransmission duplicates: skip items where title contains "RETRANSMISSION"
- Never crash — catch all exceptions per URL/month and continue
