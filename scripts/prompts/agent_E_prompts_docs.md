# Agent E — Prompts, Anomaly Rules, Lessons Learned, Sector Replication Guide

## Environment
- Working dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners
- Output dir: /Users/workstation/Documents/Hermeneutic/OffChain/miners/docs/
- Progress file: /private/tmp/claude-501/miners_progress/agent_E.json
- OTEL: export OTEL_RESOURCE_ATTRIBUTES="plan=full_ingest_v1,phase=agent_E,task=documentation"

This agent does NOT write to the DB. It produces documentation artifacts only.

## Step 1 — Create docs directory
```bash
mkdir -p /Users/workstation/Documents/Hermeneutic/OffChain/miners/docs/per_company
```

## Step 2 — Generate: global_extraction_prompt.md

Write docs/global_extraction_prompt.md — a model-agnostic structured prompt usable with any LLM (Claude, GPT-4o, Gemini, Qwen) to extract all numeric metrics from any Bitcoin mining or digital asset company press release.

The prompt must:
- Be copy-pasteable as a system prompt
- Define the full set of known metric buckets with descriptions
- Include rules for disambiguation (YTD vs monthly, quarterly vs monthly, installed vs operational)
- Handle table extraction (column ambiguity, spacer columns)
- Handle inline HTML elements, concatenated tokens
- Define the exact JSON output schema with all fields
- Include few-shot examples for the 5 most common disambiguation cases:
  1. "Produced 1,242 BTC in September and 8,610 BTC year-to-date" → extract 1,242
  2. "Hashrate: installed 25 EH/s, operational 22 EH/s" → capture both separately
  3. Table with [Label | Current Month | Prior Month | %Change] → use column 1 (Current Month)
  4. "Holdings: 3,865 BTC (2,451 unrestricted, 1,414 restricted)" → capture total AND sub-components
  5. "Q3 2024: produced 1,850 BTC" inside a monthly press release → flag as quarterly scope, low confidence

## Step 3 — Generate: anomaly_detection_rules.md

Write docs/anomaly_detection_rules.md with flagging rules for suspicious extracted values.

Include rules for:
- Month-over-month swing exceeding 50% without stated reason (acquisition, curtailment, hash upgrade)
- Production_btc value that exceeds network-plausible maximum for company hashrate (e.g., >500 BTC/EH/s/month)
- Hashrate value that is implausibly large (>200 EH/s for any single company pre-2025)
- Treasury BTC that decreases by >20% without sold_btc data to explain it
- Value that is exactly the YTD total for the year (likely YTD confusion)
- Value that is a perfect multiple of 3 of prior quarter data (likely quarterly imputation)
- Zero production with no stated reason (curtailment, maintenance, outage should be noted)
- Confidence < 0.6 with no LLM/regex agreement
- Same value appearing for 3+ consecutive months (likely copy-paste error in data)
- Hashrate in wrong unit (PH/s reported as EH/s would show as 0.001 EH/s)

## Step 4 — Generate per-company docs

Write one file per company in docs/per_company/TICKER.md covering:
- Reporting cadence history (monthly from X, quarterly from Y)
- URL patterns and IR infrastructure
- Company name history / rebranding events
- Known reporting quirks and disambiguation challenges
- Key metrics beyond standard set
- Data quality notes

Companies to cover:
MARA, RIOT, CLSK, CORZ, BITF, HUT8, IREN, WULF, BTBT, BTDR, CIFR, HIVE, ARBK, ABTC, SDIG

For each, web search for any recent updates to their reporting format or IR changes:
- Check if company is still operating
- Check if reporting format changed
- Note any M&A activity that affects data interpretation

## Step 5 — Generate: sector_replication_guide.md

Write docs/sector_replication_guide.md — how to clone this entire pipeline for digital asset treasury companies.

Target sector: **Digital Asset Treasuries** — public companies that hold BTC/ETH/SOL as balance sheet assets and make announcements when they buy or sell. Examples:
- MicroStrategy / Strategy (MSTR) — the template. BTC purchases via ATM offerings, 8-K filings on each buy
- Metaplanet (Japan, TYO:3350) — aggressive BTC accumulation, reports in JPY
- Semler Scientific (SMLR) — medical device company with BTC treasury
- NEXON (NEXON CO., Tokyo) — gaming company with BTC holdings
- Tesla (TSLA) — sold most BTC; track for any future purchases
- Block (SQ) — Jack Dorsey, BTC treasury + Lightning
- Exodus Movement (EXOD) — BTC ETF proxy via treasury

The guide must cover:
1. Schema differences from miners:
   - New primary metrics: treasury_btc_total, treasury_btc_acquired, treasury_btc_sold, acquisition_price_usd, nav_per_share
   - Replace production_btc with treasury_btc_acquired (BTC bought on open market)
   - Add: shares_outstanding, premium_to_nav, cost_basis_per_btc
   - Keep: hodl_btc, sold_btc, encumbered_btc (same concepts)

2. Data sources for treasury companies:
   - SEC 8-K filings: "Item 8.01 - Other Events" for Bitcoin purchases (MSTR files these within days of each buy)
   - Press releases: announce each major purchase with price, amount, cumulative holdings
   - Quarterly 10-Q: balance sheet shows BTC fair value and cost basis
   - Annual 10-K: full BTC holdings table with acquisition dates and cost basis

3. Scraping approach differences:
   - 8-K scraping via EDGAR full-text search (keyword: "bitcoin" in 8-K filings)
   - No monthly press release cadence — event-driven announcements
   - RSS feeds on IR sites still work for most
   - Japanese companies: filings on TDnet (Tokyo Stock Exchange); use web search

4. Extraction differences:
   - Treasury companies often report "average acquisition price" — capture this
   - "Total holdings" can appear in different parts of the same document with different values (beginning vs end of period) — always use end-of-period
   - MSTR reports "BTC yield" (a custom KPI) — define and capture
   - Impairment charges vs unrealized gains/losses — note but separate from BTC count

5. EDGAR queries for treasury companies:
   - 8-K with keyword "bitcoin" or "BTC": `https://efts.sec.gov/LATEST/search-index?q=%22bitcoin%22&dateRange=custom&startdt=2020-01-01&forms=8-K&entity=microstrategy`
   - Same approach works for any ticker

6. Step-by-step bootstrap checklist for a new sector (10 steps)

## Step 6 — Generate: lessons_learned.md

Write docs/lessons_learned.md covering all pitfalls encountered building the Bitcoin miner data platform.

Organize into sections:
1. **HTML Extraction Pitfalls** — table column ambiguity, get_text separator, raw bytes vs parsed text
2. **LLM Extraction Pitfalls** — static prompts drift from company format, YTD/quarterly confusion, context window truncation, Ollama timeout tuning
3. **IR Scraping Pitfalls** — 502-prone hosts, RSS retransmission duplicates, template URL drift, JavaScript-rendered pages
4. **EDGAR Pitfalls** — rate limits (10 req/s), foreign issuers (6-K not 8-K), XBRL vs HTML, fiscal year vs calendar year
5. **Data Modeling Pitfalls** — INSERT OR REPLACE without UNIQUE constraint, quarterly stored as monthly, YTD confusion in DB
6. **Schema Design Lessons** — raw_extractions as safety net, source_period_type enum, covering_period for quarterly aggregation
7. **Agent Architecture Lessons** — Codex agents for brute-force extraction vs static LLM prompts, coordinator state file pattern, OTEL tagging for reconstruction
8. **What to do first for any new sector** — probe IR sites before writing any scraper, run EDGAR first (most reliable), identify reporting cadence early

## Step 7 — Write final progress JSON
Write to /private/tmp/claude-501/miners_progress/agent_E.json:
```json
{
  "agent": "E",
  "status": "done",
  "files_created": [
    "docs/global_extraction_prompt.md",
    "docs/anomaly_detection_rules.md",
    "docs/sector_replication_guide.md",
    "docs/lessons_learned.md",
    "docs/per_company/MARA.md",
    "docs/per_company/RIOT.md",
    "...etc"
  ],
  "per_company_docs_count": <N>,
  "notes": "<any observations about data patterns seen>"
}
```
