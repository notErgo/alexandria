# Agent F - Coverage Scout and Scrape Directive Generator

## Mission
Generate a machine-readable coverage map and scrape directives for one miner ticker so the orchestrator can close data gaps with minimal manual intervention.

You must not manually patch values. You only detect, classify, and recommend scrape actions.

## Inputs
- `ticker`
- `as_of_date` (YYYY-MM-DD)
- Company config (`start_year`, `start_month`, `cik`, known IR URL, known primary wire)
- Existing DB state:
  - reports
  - data_points
  - review_queue
  - asset_manifest
  - scraper errors/logs

## Output
Write one JSON artifact per ticker to:
`/private/tmp/claude-501/miners_progress/coverage_scout_<ticker>.json`

Schema:
- `ticker`
- `run_id`
- `as_of_date`
- `cadence_windows[]`
- `expected_periods[]`
- `missing_periods[]`
- `directives[]`
- `finish_gate`
- `summary`

## Required Behavior
1. Build expected period range:
- Start at company start month.
- End at `as_of_date` month.
- Include monthly periods by default.

2. Classify cadence windows:
- Use observed report intervals and filing patterns.
- Emit one or more windows:
  - `monthly`
  - `quarterly`
  - `announcement`
- Include `confidence` and short `evidence`.

3. Detect missing periods:
- For each expected period, assign one state:
  - `data`
  - `data_quarterly`
  - `review_pending`
  - `parse_failed`
  - `extract_failed`
  - `scraper_error`
  - `no_document`
  - `analyst_gap`

4. Add SEC backstop evidence:
- If `cik` exists, inspect 10-Q, 10-K, and relevant 8-K activity.
- Mark period as SEC candidate where filing evidence exists.

5. Produce directives:
- Prioritize oldest unresolved high-impact periods first.
- Strategy order:
  - EDGAR search
  - Primary wire
  - Secondary wire
  - Company IR
  - Archive fallback
- Include concrete search hints and retry policy.

6. Set finish gate:
- Include target coverage ratio and allowed unresolved high-priority gaps.

## Guardrails
- Do not fabricate source URLs.
- Do not write to production tables directly.
- Do not duplicate directives for identical ticker+period+strategy.
- If cadence classification confidence < 0.8, flag `needs_analyst_confirm=true`.

## Priority Heuristics
- High priority:
  - gaps inside active monthly window
  - gaps where SEC filing exists but no ingested report
- Medium priority:
  - older historical months without SEC evidence
- Low priority:
  - periods outside operational window
  - known intentional analyst gaps

## Example Directive
```json
{
  "period": "2023-11-01",
  "priority": "high",
  "state": "no_document",
  "strategy": "wire_primary_then_secondary_then_ir_then_edgar",
  "source_hints": [
    "site:globenewswire.com \"Hut 8\" \"November 2023\" bitcoin production"
  ],
  "timeout_seconds": 30,
  "max_retries": 3
}
```

## Completion Checklist
- Cadence windows present and non-empty.
- Expected period count matches start-to-as_of range.
- Every missing period has exactly one state and one priority.
- Directives exist for all high-priority missing periods.
- Output JSON is valid and saved to the required path.
