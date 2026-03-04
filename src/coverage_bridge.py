"""
Coverage bridge: fills monthly cells from quarterly/annual filing data.

Reads already-extracted quarterly data_points (source_period_type in ('quarterly','annual'))
and distributes values to monthly cells that are missing data.

Algorithm per (ticker, covering_period, metric):
  Flow metric + quarterly regime:  Q/3 -> each of 3 months -> quarterly_carry, conf=0.80
  Flow metric + monthly regime:
    2 known: infer missing = Q - m1 - m2 -> quarterly_inferred, conf=0.65
    0 or 1 known: Q/3 -> each missing month -> quarterly_carry, conf=0.70
  Snapshot metric + quarterly regime: Q value -> last month of quarter -> quarterly_carry, conf=0.80
  Snapshot metric + monthly regime + missing: route to review_queue with needs_disaggregation
  Annual: same logic as quarterly but for 12 months (flow: FY/12 -> each month)
"""
import logging
from typing import Optional

from config import FLOW_METRICS, SNAPSHOT_METRICS
from miner_types import (
    BridgeSummary,
    EXTRACTION_METHOD_QUARTERLY_CARRY,
    EXTRACTION_METHOD_QUARTERLY_INFERRED,
    EXTRACTION_METHOD_ANNUAL_CARRY,
)

log = logging.getLogger('miners.coverage_bridge')

# Analyst-protected extraction methods that must not be overwritten by bridge logic
_PROTECTED_METHODS = frozenset({
    'analyst', 'analyst_approved', 'review_approved', 'review_edited',
})


# ── Public helper functions (importable in tests) ─────────────────────────────

def month_to_quarter(period: str) -> str:
    """Convert a YYYY-MM-01 period string to its covering quarter (e.g. '2025-Q2').

    Raises ValueError if the period string cannot be parsed.
    """
    year = int(period[:4])
    month = int(period[5:7])
    if month <= 3:
        return f"{year}-Q1"
    elif month <= 6:
        return f"{year}-Q2"
    elif month <= 9:
        return f"{year}-Q3"
    else:
        return f"{year}-Q4"


def quarter_months(covering_period: str) -> list:
    """Return the three YYYY-MM-01 month strings for a given quarter (e.g. '2025-Q1').

    Returns months in ascending order (Jan, Feb, Mar for Q1).
    """
    year = int(covering_period[:4])
    q = covering_period[5:]  # 'Q1', 'Q2', 'Q3', or 'Q4'
    first_month = {'Q1': 1, 'Q2': 4, 'Q3': 7, 'Q4': 10}[q]
    return [
        f"{year}-{first_month:02d}-01",
        f"{year}-{first_month + 1:02d}-01",
        f"{year}-{first_month + 2:02d}-01",
    ]


def annual_months(covering_period: str) -> list:
    """Return all 12 YYYY-MM-01 month strings for a fiscal year (e.g. '2024-FY').

    Returns months Jan through Dec in ascending order.
    """
    year = int(covering_period[:4])
    return [f"{year}-{m:02d}-01" for m in range(1, 13)]


def _get_constituent_months(covering_period: str) -> list:
    """Return the constituent months for a covering_period ('YYYY-Qn' or 'YYYY-FY')."""
    if covering_period.endswith('-FY'):
        return annual_months(covering_period)
    return quarter_months(covering_period)


def _is_annual(covering_period: str) -> bool:
    return covering_period.endswith('-FY')


# ── Main bridge function ──────────────────────────────────────────────────────

def bridge_gaps(
    db,
    ticker: str,
    covering_period: str,
    metric: str,
) -> BridgeSummary:
    """Fill monthly cells for a single (ticker, covering_period, metric) combination.

    Reads the quarterly/annual data_point for covering_period and distributes
    values to monthly cells that are missing data. Analyst-protected cells
    are never overwritten.

    Returns BridgeSummary with counts.
    """
    summary = BridgeSummary()

    # Retrieve the quarterly/annual data_point
    quarterly_dp = db.get_quarterly_data_point(ticker, covering_period, metric)
    if quarterly_dp is None:
        summary.cells_skipped_no_quarterly += 1
        return summary

    quarterly_value = float(quarterly_dp['value'])
    report_id = quarterly_dp.get('covering_report_id')

    # Determine constituent months
    months = _get_constituent_months(covering_period)
    if not months:
        return summary

    # Check cadence for the first month of the period
    cadence = db.get_regime_cadence_for_period(ticker, months[0])

    # Classify metric
    is_flow = metric in FLOW_METRICS

    summary.cells_evaluated = len(months)

    if is_flow:
        _bridge_flow_metric(
            db=db,
            ticker=ticker,
            covering_period=covering_period,
            metric=metric,
            months=months,
            quarterly_value=quarterly_value,
            report_id=report_id,
            cadence=cadence,
            is_annual=_is_annual(covering_period),
            summary=summary,
        )
    else:
        _bridge_snapshot_metric(
            db=db,
            ticker=ticker,
            covering_period=covering_period,
            metric=metric,
            months=months,
            quarterly_value=quarterly_value,
            report_id=report_id,
            cadence=cadence,
            summary=summary,
        )

    return summary


def _bridge_flow_metric(
    db,
    ticker: str,
    covering_period: str,
    metric: str,
    months: list,
    quarterly_value: float,
    report_id: Optional[int],
    cadence: str,
    is_annual: bool,
    summary: BridgeSummary,
) -> None:
    """Bridge logic for flow metrics (production_btc, sold_btc, net_btc_balance_change).

    Flow metrics are summable — the quarterly total equals the sum of monthly values.
    """
    n = len(months)
    carry_value = quarterly_value / n

    # Determine which months are already filled and not analyst-protected
    missing_months = []
    known_total = 0.0
    all_analyst_protected = True

    for month in months:
        if db.data_point_exists(ticker, month, metric):
            dp = db.get_data_point_by_key(ticker, month, metric)
            if dp and dp.get('extraction_method') in _PROTECTED_METHODS:
                # Analyst-protected: count as known but don't overwrite
                known_total += float(dp.get('value', 0.0))
            else:
                # Existing non-protected dp: count as known
                known_total += float(db.get_data_point_value(ticker, month, metric) or 0.0)
                all_analyst_protected = False
        else:
            missing_months.append(month)
            all_analyst_protected = False

    if not missing_months:
        return

    n_missing = len(missing_months)
    n_known = n - n_missing

    if cadence == 'quarterly':
        # Quarterly regime: distribute Q/n to each missing month
        method = EXTRACTION_METHOD_ANNUAL_CARRY if is_annual else EXTRACTION_METHOD_QUARTERLY_CARRY
        confidence = 0.80
        fill_value = carry_value
        for month in missing_months:
            if _is_protected(db, ticker, month, metric):
                continue
            _insert_monthly_dp(
                db, ticker, month, metric, fill_value,
                method, confidence, covering_period, report_id,
            )
            summary.cells_filled_carry += 1

    elif cadence == 'monthly':
        if n_missing == 1 and n_known == n - 1:
            # Exactly 1 month missing and all others known — infer the remainder
            known_values = []
            for month in months:
                if month not in missing_months:
                    val = db.get_data_point_value(ticker, month, metric)
                    if val is not None:
                        known_values.append(float(val))
            known_sum = sum(known_values)
            inferred_value = quarterly_value - known_sum

            month = missing_months[0]
            if not _is_protected(db, ticker, month, metric):
                _insert_monthly_dp(
                    db, ticker, month, metric, inferred_value,
                    EXTRACTION_METHOD_QUARTERLY_INFERRED, 0.65,
                    covering_period, report_id,
                )
                summary.cells_filled_inferred += 1
        else:
            # 0 or 2+ known — cannot reliably infer; use carry (Q/n)
            method = EXTRACTION_METHOD_ANNUAL_CARRY if is_annual else EXTRACTION_METHOD_QUARTERLY_CARRY
            confidence = 0.70
            for month in missing_months:
                if _is_protected(db, ticker, month, metric):
                    continue
                _insert_monthly_dp(
                    db, ticker, month, metric, carry_value,
                    method, confidence, covering_period, report_id,
                )
                summary.cells_filled_carry += 1


def _bridge_snapshot_metric(
    db,
    ticker: str,
    covering_period: str,
    metric: str,
    months: list,
    quarterly_value: float,
    report_id: Optional[int],
    cadence: str,
    summary: BridgeSummary,
) -> None:
    """Bridge logic for snapshot metrics (hodl_btc, hashrate_eh, etc.).

    Snapshot metrics are point-in-time — they cannot be disaggregated across months.
    """
    if cadence == 'quarterly':
        # Quarterly regime: assign the snapshot value to the LAST month of the quarter/year
        last_month = months[-1]
        if not db.data_point_exists(ticker, last_month, metric):
            _insert_monthly_dp(
                db, ticker, last_month, metric, quarterly_value,
                EXTRACTION_METHOD_QUARTERLY_CARRY, 0.80,
                covering_period, report_id,
            )
            summary.cells_filled_carry += 1

    elif cadence == 'monthly':
        # Monthly regime: route missing months to review_queue with needs_disaggregation
        for month in months:
            if not db.data_point_exists(ticker, month, metric):
                try:
                    db.insert_review_item({
                        'data_point_id':   None,
                        'ticker':          ticker,
                        'period':          month,
                        'metric':          metric,
                        'raw_value':       str(quarterly_value),
                        'confidence':      0.50,
                        'source_snippet':  (
                            f"Quarterly value {quarterly_value} for {covering_period}; "
                            f"cannot disaggregate snapshot metric across months"
                        ),
                        'status':          'PENDING',
                        'llm_value':       quarterly_value,
                        'regex_value':     None,
                        'agreement_status': 'needs_disaggregation',
                    })
                    summary.cells_routed_review += 1
                except Exception as e:
                    log.error(
                        "Failed to insert review item for %s %s %s: %s",
                        ticker, month, metric, e, exc_info=True,
                    )


def _is_protected(db, ticker: str, period: str, metric: str) -> bool:
    """Return True if the existing data_point for ticker/period/metric is analyst-protected."""
    if not db.data_point_exists(ticker, period, metric):
        return False
    dp = db.get_data_point_by_key(ticker, period, metric)
    return dp is not None and dp.get('extraction_method') in _PROTECTED_METHODS


def _insert_monthly_dp(
    db,
    ticker: str,
    period: str,
    metric: str,
    value: float,
    extraction_method: str,
    confidence: float,
    covering_period: str,
    report_id: Optional[int],
) -> None:
    """Insert a monthly data_point derived from quarterly/annual data."""
    from config import FLOW_METRICS

    # Determine unit from metric name (reuse existing helper logic)
    from infra.db import _metric_unit
    unit = _metric_unit(metric)

    period_type = 'annual' if covering_period.endswith('-FY') else 'quarterly'

    dp = {
        'report_id':          report_id,
        'ticker':             ticker,
        'period':             period,
        'metric':             metric,
        'value':              value,
        'unit':               unit,
        'confidence':         confidence,
        'extraction_method':  extraction_method,
        'source_snippet':     f"Derived from {covering_period} {period_type} filing value {value:.4f}",
        'source_period_type': 'monthly',  # stored as monthly row even though derived from quarterly
        'covering_report_id': report_id,
        'covering_period':    covering_period,
    }
    try:
        db.insert_data_point(dp)
        log.debug(
            "Bridge: stored %s %s %s %s = %.4f (%s conf=%.2f)",
            period_type, ticker, period, metric, value, extraction_method, confidence,
        )
    except Exception as e:
        log.error(
            "Bridge insert failed for %s %s %s: %s", ticker, period, metric, e, exc_info=True
        )


# ── High-level batch bridge ───────────────────────────────────────────────────

def bridge_all_gaps(db, ticker: Optional[str] = None) -> BridgeSummary:
    """Run bridge_gaps for all quarterly/annual data_points in the DB.

    Iterates through all data_points where source_period_type != 'monthly',
    grouped by (ticker, covering_period, metric), and calls bridge_gaps for each.

    Returns a combined BridgeSummary with aggregated counts.
    """
    combined = BridgeSummary()

    quarterly_dps = db.get_all_quarterly_data_points(ticker=ticker)
    log.info(
        "bridge_all_gaps: found %d quarterly/annual data_points to process",
        len(quarterly_dps),
    )

    # Deduplicate by (ticker, covering_period, metric)
    seen = set()
    for dp in quarterly_dps:
        t = dp.get('ticker')
        cp = dp.get('covering_period') or dp.get('period')
        m = dp.get('metric')
        if not (t and cp and m):
            continue
        key = (t, cp, m)
        if key in seen:
            continue
        seen.add(key)

        try:
            result = bridge_gaps(db=db, ticker=t, covering_period=cp, metric=m)
            combined.cells_evaluated += result.cells_evaluated
            combined.cells_filled_carry += result.cells_filled_carry
            combined.cells_filled_inferred += result.cells_filled_inferred
            combined.cells_routed_review += result.cells_routed_review
            combined.cells_skipped_no_quarterly += result.cells_skipped_no_quarterly
        except Exception as e:
            log.error(
                "bridge_gaps failed for %s %s %s: %s", t, cp, m, e, exc_info=True
            )

    log.info(
        "bridge_all_gaps complete: %d evaluated, %d carry fills, %d inferred fills, "
        "%d review items, %d skipped",
        combined.cells_evaluated, combined.cells_filled_carry,
        combined.cells_filled_inferred, combined.cells_routed_review,
        combined.cells_skipped_no_quarterly,
    )
    return combined
