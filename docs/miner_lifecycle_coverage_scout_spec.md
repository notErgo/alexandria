# Miner Lifecycle Coverage Scout Specification

## Overview
This document defines a low-manual workflow for taking a miner from scrape and ingest through processing, review cleanup, and finalization. The core addition is a Coverage Scout agent that maps missing ranges, classifies reporting cadence, and returns machine-readable scrape directives to the orchestrator.

Primary goal: reduce manual refinement by converting repeatable discovery patterns into explicit rules.

## Scope
In scope:
- Coverage definition for expected periods per miner
- Missing-range detection across monthly and quarterly cadences
- SEC-driven backstop discovery for filings that mention Bitcoin production context
- Agent output contract used by scraping orchestration

Out of scope:
- New extractor model architecture
- UI redesign
- Changes to analyst approval policy

## Lifecycle Stages
1. Baseline ingest:
- Run existing ingestion stack (`manifest_scanner`, IR scrapers, EDGAR connector).

2. Coverage Scout pass:
- Build expected period map from miner start date through current month.
- Detect cadence regime windows (monthly vs quarterly).
- Identify missing periods and assign reason category.
- Produce scrape directives.

3. Directed scrape:
- Orchestrator executes directives in priority order.
- New reports are ingested and extracted.

4. Processing and bridge:
- Run extraction pipeline.
- Run quarterly bridge logic where policy allows.

5. Review cleanup:
- Route low-confidence or ambiguous values to review queue.
- Preserve analyst-protected overrides.

6. Finish gate:
- Coverage and quality thresholds are checked.
- Miner state marked as finished or returned to Coverage Scout pass.

## Coverage Definition
Coverage Scout computes expected periods with these rules:
- Monthly window:
  - Include every month from `companies.start_year/start_month` to `as_of_month`.
- Quarterly window:
  - Include quarter boundaries from cadence switch date to `as_of_month`.
- Mixed regime:
  - Use explicit windows when known.
  - If unknown, infer from observed report periodicity:
    - Monthly if median report interval <= 45 days.
    - Quarterly if median report interval >= 70 days.
    - Otherwise classify as mixed and emit a confidence warning.

Expected-period output is the canonical list used for gap detection.

## Gap Taxonomy
Each expected period is assigned one reason:
- `no_document`: no source found yet
- `scraper_error`: source attempted but fetch failed
- `parse_failed`: source ingested but parse failed
- `extract_failed`: extraction ran and yielded no usable metric
- `review_pending`: value exists but blocked in review queue
- `data`: accepted monthly value
- `data_quarterly`: accepted quarterly/annual-derived fill
- `analyst_gap`: intentional, approved absence

## SEC Backstop Range Logic
For each miner with CIK:
- Query SEC submissions for `10-Q`, `10-K`, and `8-K`.
- Build filing coverage index by filing date and period of report.
- For 8-K, prioritize exhibits and body text containing production keywords:
  - `bitcoin production`, `btc mined`, `hashrate`, `operational update`.
- Mark expected periods with any SEC filing evidence as `sec_candidate`.
- Emit directives that prioritize SEC candidate periods when primary IR/wire scrape is missing.

This provides bounded, auditable missing-range discovery instead of open-ended manual searching.

## Cadence Classification
Coverage Scout returns cadence windows with confidence:
- `monthly` window: regular monthly updates or explicit monthly production releases.
- `quarterly` window: shift to earnings-only cadence or SEC quarterly pattern.
- `announcement` window: irregular treasury-style disclosures (for cases like ABTC).

Required output fields per window:
- `ticker`
- `cadence`
- `start_date`
- `end_date` (nullable for current window)
- `confidence` (0-1)
- `evidence` (short strings with source links or filing IDs)

## Scrape Directive Contract
Coverage Scout emits directives for orchestrator execution:

```json
{
  "ticker": "HUT8",
  "run_id": "coverage_scout_2026-03-05",
  "as_of_date": "2026-03-05",
  "cadence_windows": [
    {
      "cadence": "monthly",
      "start_date": "2021-01-01",
      "end_date": "2025-03-01",
      "confidence": 0.93,
      "evidence": ["wire_release_frequency", "historical_ops_update_titles"]
    },
    {
      "cadence": "quarterly",
      "start_date": "2025-04-01",
      "end_date": null,
      "confidence": 0.87,
      "evidence": ["edgar_10q_sequence"]
    }
  ],
  "missing_periods": [
    {
      "period": "2023-11-01",
      "state": "no_document",
      "priority": "high",
      "reason": "expected_monthly_no_source"
    }
  ],
  "directives": [
    {
      "period": "2023-11-01",
      "strategy": "wire_primary_then_secondary_then_ir_then_edgar",
      "source_hints": [
        "site:globenewswire.com \"Hut 8\" \"November 2023\" bitcoin production"
      ],
      "timeout_seconds": 30,
      "max_retries": 3
    }
  ],
  "finish_gate": {
    "target_coverage_ratio": 0.9,
    "max_high_priority_gaps": 2
  }
}
```

## Orchestrator Integration
Orchestrator loop:
1. Read scout output JSON.
2. Execute `directives` in priority order.
3. Ingest and extract new reports.
4. Recompute coverage states.
5. Stop when finish gate is satisfied or directive budget is exhausted.

Required idempotency:
- Do not reinsert duplicate reports for the same ticker-period-source URL.
- Persist run-level state to allow resume after interruption.

Scout run policy:
- `never`: do not run scout during this pipeline execution.
- `auto` (default): run only when artifacts are missing/stale, or when keyword-driven deep discovery is requested.
- `always`: always run scout before scrape enqueue.

## Minimal Manual Workflow
Manual operator only does:
1. Select pilot tickers.
2. Start Coverage Scout run.
3. Review unresolved high-priority gaps.
4. Approve or override cadence windows if confidence < threshold.

Everything else is agent/orchestrator-driven.

## Pilot Selection Criteria
Select pilots using current DB evidence, not ticker age alone:
- At least one ticker with strong existing history (`reports` + `data_points`) to validate no-regression behavior.
- At least one ticker with cadence transition risk (monthly to quarterly).
- At least one ticker with sparse/zero extracted data to test gap-closing behavior.
- At least one announcement-style ticker to validate non-monthly cadence handling.

## Pilot Set (Current DB-Aligned)
Recommended initial pilot tickers:
- `RIOT` (high-history baseline)
- `CLSK` (medium-history baseline)
- `WULF` (sparse data + cadence transition)
- `ABTC` (announcement/treasury style)

Rationale: this set exercises all core lifecycle paths while matching observed in-DB state.

## Finish Criteria
A ticker is finished when all are true:
- Coverage ratio for target metric set >= 90%.
- No unresolved `high` priority gaps older than 60 days.
- Cadence windows confidence >= 0.8 or explicitly analyst-approved.
- Review queue backlog for ticker <= configured limit.

## Decisions Log
- Coverage Scout is read-first and emits directives; it does not mutate production data directly.
- SEC is treated as coverage backstop, not sole source.
- Cadence classification is explicit and versioned per run.
- Finish gate is quantitative and enforced by orchestrator.
