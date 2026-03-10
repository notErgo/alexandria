"""
Gap-fill engine: infer missing monthly data_points from quarterly totals.

For flow metrics (production, sales) with exactly one missing month in a quarter,
computes the missing month as quarterly_total - sum(known_months).

For snapshot metrics (holdings, hashrate) propagates the quarter-end value to
the last month of the quarter when it is missing.

All inferred rows are written with a structured inference_notes JSON blob and
an extraction_method that starts with 'inferred_' so they are never mistaken
for real extractions.
"""
import json
import logging
from typing import Optional

log = logging.getLogger('miners.interpreters.gap_fill')

# Import canonical metric classification from config (single source of truth).
# Do NOT redefine these sets here — update config.py instead.
from config import FLOW_METRICS, SNAPSHOT_METRICS

# Analyst-protected extraction methods — never overwrite these.
_PROTECTED_METHODS = frozenset({
    'analyst', 'analyst_approved', 'review_approved', 'review_edited',
})


def fill_quarterly_gaps(
    ticker: str,
    db,
    metrics: Optional[list] = None,
    dry_run: bool = False,
) -> dict:
    """Infer missing monthly data_points from quarterly data for a ticker.

    Args:
        ticker: Company ticker symbol.
        db: MinerDB instance.
        metrics: Optional list of metric keys to process. Defaults to all
            FLOW_METRICS | SNAPSHOT_METRICS.
        dry_run: If True, compute inferences but do not write to DB.

    Returns:
        dict with keys 'filled', 'skipped', 'errors', 'rows' (list of
        detail dicts for each inferred row).
    """
    from period_utils import quarter_to_month_range

    target_metrics = set(metrics) if metrics else (FLOW_METRICS | SNAPSHOT_METRICS)
    ticker = ticker.upper()

    filled = 0
    skipped = 0
    errors = 0
    rows = []

    # Fetch all quarterly data_points for this ticker.
    quarterly_rows = db.get_all_quarterly_data_points(ticker=ticker)
    if not quarterly_rows:
        log.info("gap_fill_start ticker=%s quarterly_rows=0 — nothing to fill", ticker)
        return {'filled': 0, 'skipped': 0, 'errors': 0, 'rows': []}

    log.info(
        "gap_fill_start ticker=%s quarterly_rows=%d dry_run=%s",
        ticker, len(quarterly_rows), dry_run,
    )

    for q_row in quarterly_rows:
        covering_period = q_row.get('covering_period') or q_row.get('period')
        if not covering_period or 'Q' not in covering_period.upper():
            continue

        metric = q_row.get('metric')
        if metric not in target_metrics:
            continue

        q_value = q_row.get('value')
        if q_value is None:
            skipped += 1
            continue

        months = quarter_to_month_range(covering_period)
        if not months:
            log.warning(
                "gap_fill_bad_period ticker=%s covering_period=%s metric=%s",
                ticker, covering_period, metric,
            )
            skipped += 1
            continue

        # Build map of known monthly values for this metric.
        known: dict[str, float] = {}
        for month in months:
            val = db.get_data_point_value(ticker, month + '-01', metric)
            if val is None:
                # Also try without day suffix (YYYY-MM format).
                val = db.get_data_point_value(ticker, month, metric)
            if val is not None:
                known[month] = val

        # Also check for analyst-protected rows so we never overwrite them.
        protected: set[str] = set()
        for month in months:
            for period_fmt in (month + '-01', month):
                row = db.get_data_point_by_key(ticker, period_fmt, metric)
                if row and row.get('extraction_method') in _PROTECTED_METHODS:
                    protected.add(month)

        if metric in FLOW_METRICS:
            _fill_flow_metric(
                ticker=ticker, covering_period=covering_period, metric=metric,
                q_value=q_value, q_row=q_row, months=months, known=known,
                protected=protected, db=db, dry_run=dry_run,
                filled_out=rows,
            )
        elif metric in SNAPSHOT_METRICS:
            _fill_snapshot_metric(
                ticker=ticker, covering_period=covering_period, metric=metric,
                q_value=q_value, q_row=q_row, months=months, known=known,
                protected=protected, db=db, dry_run=dry_run,
                filled_out=rows,
            )

    for r in rows:
        if r.get('status') == 'filled':
            filled += 1
        elif r.get('status') == 'skipped':
            skipped += 1
        else:
            errors += 1

    log.info(
        "gap_fill_end ticker=%s filled=%d skipped=%d errors=%d dry_run=%s",
        ticker, filled, skipped, errors, dry_run,
    )
    return {'filled': filled, 'skipped': skipped, 'errors': errors, 'rows': rows}


def _fill_flow_metric(
    *, ticker, covering_period, metric, q_value, q_row, months, known,
    protected, db, dry_run, filled_out,
):
    """Infer missing month(s) for a flow metric."""
    missing = [m for m in months if m not in known]

    if len(missing) == 0:
        filled_out.append({
            'status': 'skipped',
            'reason': 'all_months_present',
            'ticker': ticker,
            'covering_period': covering_period,
            'metric': metric,
        })
        return

    if len(missing) == 1:
        # Delta inference: inferred = quarterly - sum(known)
        known_sum = sum(known.values())
        inferred = q_value - known_sum
        if inferred < 0:
            log.warning(
                "gap_fill_negative_delta ticker=%s period=%s metric=%s "
                "q_value=%s known_sum=%s — skip",
                ticker, covering_period, metric, q_value, known_sum,
            )
            filled_out.append({
                'status': 'skipped',
                'reason': 'negative_delta',
                'ticker': ticker,
                'covering_period': covering_period,
                'metric': metric,
                'quarterly_value': q_value,
                'known_sum': known_sum,
            })
            return

        month = missing[0]
        if month in protected:
            filled_out.append({
                'status': 'skipped',
                'reason': 'protected',
                'ticker': ticker,
                'period': month,
                'metric': metric,
            })
            return

        notes = json.dumps({
            'method': 'quarterly_delta',
            'quarterly_period': covering_period,
            'quarterly_value': q_value,
            'known_months': known,
            'computed_value': inferred,
        })
        _write_inferred(
            ticker=ticker, period=month, metric=metric, value=inferred,
            extraction_method='inferred_delta', q_row=q_row,
            covering_period=covering_period, inference_notes=notes,
            db=db, dry_run=dry_run, filled_out=filled_out,
        )

    elif len(missing) == 3:
        # All months missing: prorate equally.
        prorated = q_value / 3.0
        for month in missing:
            if month in protected:
                filled_out.append({
                    'status': 'skipped',
                    'reason': 'protected',
                    'ticker': ticker,
                    'period': month,
                    'metric': metric,
                })
                continue
            notes = json.dumps({
                'method': 'quarterly_prorate',
                'quarterly_period': covering_period,
                'quarterly_value': q_value,
                'known_months': {},
                'computed_value': prorated,
            })
            _write_inferred(
                ticker=ticker, period=month, metric=metric, value=prorated,
                extraction_method='inferred_prorated', q_row=q_row,
                covering_period=covering_period, inference_notes=notes,
                db=db, dry_run=dry_run, filled_out=filled_out,
            )
    else:
        # 2 of 3 missing: not enough information for delta inference.
        filled_out.append({
            'status': 'skipped',
            'reason': 'insufficient_known_months',
            'ticker': ticker,
            'covering_period': covering_period,
            'metric': metric,
            'missing_count': len(missing),
        })


def _fill_snapshot_metric(
    *, ticker, covering_period, metric, q_value, q_row, months, known,
    protected, db, dry_run, filled_out,
):
    """Propagate quarter-end value to the last month of the quarter."""
    last_month = months[-1]
    if last_month in known:
        filled_out.append({
            'status': 'skipped',
            'reason': 'last_month_present',
            'ticker': ticker,
            'period': last_month,
            'metric': metric,
        })
        return

    if last_month in protected:
        filled_out.append({
            'status': 'skipped',
            'reason': 'protected',
            'ticker': ticker,
            'period': last_month,
            'metric': metric,
        })
        return

    notes = json.dumps({
        'method': 'quarterly_endpoint',
        'quarterly_period': covering_period,
        'quarterly_value': q_value,
        'known_months': known,
        'computed_value': q_value,
    })
    _write_inferred(
        ticker=ticker, period=last_month, metric=metric, value=q_value,
        extraction_method='inferred_snapshot', q_row=q_row,
        covering_period=covering_period, inference_notes=notes,
        db=db, dry_run=dry_run, filled_out=filled_out,
    )


def _write_inferred(
    *, ticker, period, metric, value, extraction_method, q_row,
    covering_period, inference_notes, db, dry_run, filled_out,
):
    """Write one inferred data_point row and append a result entry."""
    dp = {
        'report_id':            q_row.get('report_id'),
        'ticker':               ticker,
        'period':               period + '-01' if len(period) == 7 else period,
        'metric':               metric,
        'value':                value,
        'unit':                 q_row.get('unit', 'BTC'),
        'confidence':           0.5,
        'extraction_method':    extraction_method,
        'source_snippet':       f'inferred from {covering_period}',
        'source_period_type':   'inferred',
        'covering_report_id':   q_row.get('report_id'),
        'covering_period':      covering_period,
        'inference_notes':      inference_notes,
        'expected_granularity': 'monthly',
        'time_grain':           'monthly',
    }

    detail = {
        'status':             'would_fill' if dry_run else 'filled',
        'ticker':             ticker,
        'period':             period,
        'metric':             metric,
        'inferred_value':     value,
        'extraction_method':  extraction_method,
        'covering_period':    covering_period,
        'dry_run':            dry_run,
    }

    if not dry_run:
        try:
            db.insert_data_point(dp)
            log.info(
                "gap_fill_wrote ticker=%s period=%s metric=%s value=%s method=%s",
                ticker, period, metric, value, extraction_method,
            )
        except Exception:
            log.exception(
                "gap_fill_write_error ticker=%s period=%s metric=%s",
                ticker, period, metric,
            )
            detail['status'] = 'error'

    filled_out.append(detail)
