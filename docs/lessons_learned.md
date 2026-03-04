# Lessons Learned

**Status:** Complete
**Last Updated:** 2026-03-04
**Project:** Bitcoin miner data platform

## 1. HTML Extraction Pitfalls
- Table column ambiguity caused repeated `prior month` captures when headers were short.
- `BeautifulSoup.get_text()` without separators merged numbers and units into broken tokens.
- Raw response bytes occasionally preserved hidden delimiters that disappeared in parsed HTML.

Mitigation:
- Build table-header scoring with explicit current-period matching.
- Use `separator=' '` and normalize whitespace before regex extraction.
- Keep raw HTML snapshots for replay and parser regression tests.

## 2. LLM Extraction Pitfalls
- Static prompts drifted when companies shifted release format from mining-only to mixed mining/HPC narratives.
- YTD values were selected in place of monthly values when both appeared in one sentence.
- Context windows truncated long tables, leading to dropped treasury subcomponents.
- Local model timeouts required tuning for reliable batch extraction.

Mitigation:
- Add disambiguation rules and few-shot patterns for YTD/month conflicts.
- Add table-first extraction stage before LLM summarization.
- Tune timeout and max-token settings per model class.

## 3. IR Scraping Pitfalls
- Host instability on wire services generated intermittent 502 responses.
- Newsfile retransmissions introduced duplicates that looked valid.
- IR template URLs changed after site redesigns.
- JavaScript-rendered pages hid release body from basic requests.

Mitigation:
- Retry with bounded backoff and continue on single URL failure.
- Deduplicate on ticker + report period + normalized title hash.
- Keep fallback path: wire archive -> IR index -> EDGAR -> Archive.org.
- Add extraction adapters for static HTML and dynamic-render fallback.

## 4. EDGAR Pitfalls
- SEC throughput limits triggered temporary blocking when polling too fast.
- Foreign issuers reported through 6-K, not 8-K.
- XBRL fact naming differed by filer and period.
- Fiscal period labels did not align with calendar-month mining reporting.

Mitigation:
- Throttle requests and cache query windows.
- Include 6-K path for foreign issuers.
- Parse both HTML text and XBRL facts.
- Keep `covering_period` fields separate from publication date.

## 5. Data Modeling Pitfalls
- `INSERT OR REPLACE` without strict unique keys silently replaced valid rows.
- Quarterly data was inserted into monthly buckets when period type was not explicit.
- YTD values contaminated month-level series.

Mitigation:
- Enforce composite uniqueness on ticker, report_date, source_type, and period label.
- Require `source_period_type` for all extractions.
- Add anomaly rule for exact YTD matches on non-January months.

## 6. Schema Design Lessons
- `raw_extractions` table acted as recovery layer when parsed metrics were disputed.
- `source_period_type` enum reduced accidental aggregation errors.
- `covering_period` fields enabled quarterly-to-month reconciliation logic.

Recommendation:
- Keep immutable raw text and parsed output side by side.
- Version extraction schema so backfills can be replayed safely.

## 7. Agent Architecture Lessons
- Codex-style execution agents were efficient for broad scraping and deterministic transforms.
- Static prompt-only workflows degraded under format drift.
- Coordinator state files prevented duplicated effort across parallel agents.
- OTEL tagging (`plan`, `phase`, `task`) enabled post-run reconstruction and latency analysis.

Recommendation:
- Pair deterministic rules with model extraction and maintain reconciliation checks.

## 8. What To Do First For Any New Sector
- Probe IR and wire sources before writing custom scrapers.
- Run EDGAR discovery first because filing archives are stable.
- Determine reporting cadence early (monthly, quarterly, event-driven).
- Draft anomaly rules before bulk ingest to catch systematic extraction errors early.
