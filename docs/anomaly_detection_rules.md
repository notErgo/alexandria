# Anomaly Detection Rules

**Status:** Complete
**Last Updated:** 2026-03-04
**Scope:** Post-extraction validation for miner monthly and quarterly metrics

## Purpose
Flag extracted values that are numerically possible yet likely wrong due to scope confusion, unit conversion mistakes, table misalignment, or extraction drift.

## Input Contract
- Input row granularity: `ticker + report_date + metric`.
- Input fields required: `value`, `unit`, `confidence`, `source_period_type`, `evidence`, prior 12-month history for the ticker.
- Output: `anomaly_code`, `severity`, `reason`, `recommended_action`.

## Rule Set

### 1) MoM swing greater than 50% without explicit reason
- Rule ID: `mom_swing_unexplained`
- Condition:
  - `metric in {production_btc, hashrate_eh_operational, treasury_btc_total}`
  - `abs(current - prior_month) / max(prior_month, 1) > 0.50`
  - Evidence text does not contain any trigger keyword.
- Trigger keywords:
  - `acquisition`, `merger`, `curtailment`, `outage`, `maintenance`, `energized`, `expansion`, `upgrade`, `weather`, `grid event`, `hosting transition`.
- Severity: `medium`
- Action: Require manual review.

### 2) Production exceeds network-plausible bound for hashrate
- Rule ID: `production_hashrate_impossible`
- Condition:
  - Both `production_btc` and `hashrate_eh_operational` present.
  - `production_btc / max(hashrate_eh_operational, 0.001) > 500` BTC per EH/s per month.
- Severity: `high`
- Action: Re-check period scope and hashrate units.

### 3) Implausible single-company hashrate pre-2025
- Rule ID: `hashrate_implausible_pre2025`
- Condition:
  - `report_date < 2025-01-01`
  - `hashrate_eh_operational > 200` or `hashrate_eh_installed > 200`.
- Severity: `high`
- Action: Verify PH/s to EH/s conversion and decimal placement.

### 4) Treasury drop greater than 20% without sold BTC evidence
- Rule ID: `treasury_drop_unexplained`
- Condition:
  - `treasury_btc_total` exists for current and prior month.
  - Drop ratio > 0.20.
  - `sold_btc` is null or zero.
  - Evidence text lacks `sold`, `disposed`, `liquidated`, `repayment`, or `collateral release`.
- Severity: `medium`
- Action: Check if sale is disclosed in a separate filing.

### 5) Extracted monthly value equals annual YTD total
- Rule ID: `ytd_confusion_exact_match`
- Condition:
  - `production_btc == ytd_production_btc` and month is not January.
- Severity: `high`
- Action: Replace monthly with null pending review.

### 6) Perfect multiple-of-3 pattern against prior quarter
- Rule ID: `quarterly_imputation_pattern`
- Condition:
  - `current_month_value * 3 == last_quarter_value` or `current_month_value == last_quarter_value / 3` within 1% tolerance.
  - Source text contains quarter labels (`Q1`, `Q2`, `Q3`, `Q4`) and lacks explicit monthly label.
- Severity: `medium`
- Action: Mark as quarterly scope conflict.

### 7) Zero production without documented reason
- Rule ID: `zero_production_unexplained`
- Condition:
  - `production_btc == 0`
  - Evidence lacks `curtailment`, `maintenance`, `outage`, `weather`, `shutdown`, `energization delay`.
- Severity: `high`
- Action: Manual validation against source release.

### 8) Low confidence and no model agreement
- Rule ID: `low_confidence_no_agreement`
- Condition:
  - `confidence < 0.60`
  - No agreement marker from independent extractors (LLM vs regex/table).
- Severity: `medium`
- Action: Route to human queue.

### 9) Flatline value repeated for 3+ months
- Rule ID: `stale_value_repetition`
- Condition:
  - Same exact numeric value for same ticker+metric across 3 or more consecutive months.
  - Exclude constants such as contract MW ceilings that can stay fixed.
- Severity: `medium`
- Action: Verify report text changed each month.

### 10) Hashrate unit mismatch (PH/s read as EH/s)
- Rule ID: `hashrate_unit_mismatch`
- Condition:
  - Raw text uses `PH/s` and normalized EH field is not divided by 1000.
  - Or normalized EH value is `< 0.01` while nearby text shows whole-number PH/s context.
- Severity: `high`
- Action: Re-run unit normalization pass.

## Severity Policy
- `high`: block auto-publish; require human approval.
- `medium`: publish with warning and queue review.
- `low`: log only.

## SQL-Oriented Implementation Sketch
```sql
-- Example: unexplained >50% monthly swing in production
WITH monthly AS (
  SELECT
    ticker,
    report_date,
    production_btc,
    LAG(production_btc) OVER (PARTITION BY ticker ORDER BY report_date) AS prior_prod,
    evidence
  FROM extracted_metrics_monthly
)
SELECT ticker, report_date, production_btc, prior_prod
FROM monthly
WHERE prior_prod IS NOT NULL
  AND ABS(production_btc - prior_prod) / CASE WHEN prior_prod = 0 THEN 1 ELSE prior_prod END > 0.50
  AND lower(evidence) NOT LIKE '%acquisition%'
  AND lower(evidence) NOT LIKE '%curtailment%'
  AND lower(evidence) NOT LIKE '%outage%';
```

## Verification Checklist
- Confirm each anomaly rule stores: code, severity, reason, action.
- Confirm high severity anomalies block downstream aggregation.
- Confirm anomaly logs include source URL and evidence snippet.
