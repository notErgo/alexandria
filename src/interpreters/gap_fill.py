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
    fill_mode: str = 'endpoint',
) -> dict:
    """Infer missing monthly data_points from quarterly data for a ticker.

    Args:
        ticker: Company ticker symbol.
        db: MinerDB instance.
        metrics: Optional list of metric keys to process. Defaults to all
            FLOW_METRICS | SNAPSHOT_METRICS.
        dry_run: If True, compute inferences but do not write to DB.
        fill_mode: How to fill snapshot metric gaps. One of:
            'endpoint' (default) — only fill the last month of the quarter
                (quarter-end propagation, original behavior).
            'stepwise' — fill all missing months in the quarter with the
                quarter-end value.
            'linear' — interpolate from the previous quarter's end value
                to the current quarter's end value across all 3 months.
                Falls back to stepwise for the first quarter in the series.

    Returns:
        dict with keys 'filled', 'skipped', 'errors', 'rows' (list of
        detail dicts for each inferred row).
    """
    from period_utils import quarter_to_month_range

    _VALID_FILL_MODES = frozenset({'endpoint', 'stepwise', 'linear'})
    if fill_mode not in _VALID_FILL_MODES:
        raise ValueError(f"fill_mode must be one of {sorted(_VALID_FILL_MODES)}, got {fill_mode!r}")

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
        "gap_fill_start ticker=%s quarterly_rows=%d dry_run=%s fill_mode=%s",
        ticker, len(quarterly_rows), dry_run, fill_mode,
    )

    # For linear mode, pre-build per-metric sorted series to look up prev quarter value.
    prev_q_value_map: dict = {}  # {(metric, covering_period): prev_q_value | None}
    if fill_mode == 'linear':
        _by_metric: dict = {}
        for qr in quarterly_rows:
            m = qr.get('metric')
            covering = qr.get('covering_period') or qr.get('period')
            if m and covering and 'Q' in covering.upper():
                _by_metric.setdefault(m, []).append((covering, qr.get('value')))
        for m, series in _by_metric.items():
            series.sort(key=lambda x: _quarter_sort_key(x[0]))
            for idx, (qp, _) in enumerate(series):
                prev_q_value_map[(m, qp)] = series[idx - 1][1] if idx > 0 else None

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
            prev_q_value = prev_q_value_map.get((metric, covering_period))
            _fill_snapshot_metric(
                ticker=ticker, covering_period=covering_period, metric=metric,
                q_value=q_value, q_row=q_row, months=months, known=known,
                protected=protected, db=db, dry_run=dry_run,
                filled_out=rows, fill_mode=fill_mode,
                prev_q_value=prev_q_value,
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
    protected, db, dry_run, filled_out, fill_mode='endpoint', prev_q_value=None,
):
    """Fill missing months for a snapshot metric according to fill_mode.

    fill_mode='endpoint': original behaviour — only fill the last month of the
        quarter if it is missing (quarter-end propagation).
    fill_mode='stepwise': fill all missing months in the quarter with q_value.
    fill_mode='linear': interpolate from prev_q_value to q_value across all
        three months (1/3, 2/3, 3/3 fractions). Falls back to stepwise when
        prev_q_value is None.
    """
    if fill_mode == 'endpoint':
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
        return

    # stepwise or linear: fill all missing months
    n = len(months)  # typically 3 for a quarter
    for idx, month in enumerate(months):
        if month in known:
            continue
        if month in protected:
            filled_out.append({
                'status': 'skipped',
                'reason': 'protected',
                'ticker': ticker,
                'period': month,
                'metric': metric,
            })
            continue

        if fill_mode == 'stepwise' or prev_q_value is None:
            inferred_value = q_value
            method_tag = 'quarterly_stepwise'
            extra_notes: dict = {}
        else:
            # linear: fraction = (idx+1) / n  (1-based position in quarter)
            fraction = (idx + 1) / n
            inferred_value = prev_q_value + (q_value - prev_q_value) * fraction
            method_tag = 'quarterly_linear'
            extra_notes = {
                'prev_quarter_value': prev_q_value,
                'fraction': round(fraction, 6),
            }

        notes = json.dumps({
            'method': method_tag,
            'quarterly_period': covering_period,
            'quarterly_value': q_value,
            'known_months': known,
            'computed_value': round(inferred_value, 8),
            **extra_notes,
        })
        extraction_method = 'inferred_stepwise' if fill_mode == 'stepwise' or prev_q_value is None else 'inferred_linear'
        _write_inferred(
            ticker=ticker, period=month, metric=metric, value=inferred_value,
            extraction_method=extraction_method, q_row=q_row,
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


def _quarter_sort_key(covering_period: str) -> tuple:
    """Convert '2023-Q2' to (2023, 2) for chronological sorting."""
    import re as _re
    m = _re.match(r'^(\d{4})-Q(\d)$', (covering_period or '').upper())
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (0, 0)


def _months_apart(period_a: str, period_b: str) -> int:
    """Return signed number of months from period_a to period_b (YYYY-MM-01 format)."""
    ya, ma = int(period_a[:4]), int(period_a[5:7])
    yb, mb = int(period_b[:4]), int(period_b[5:7])
    return (yb - ya) * 12 + (mb - ma)


def derive_net_balance_change(
    ticker: str,
    db,
    dry_run: bool = False,
    overwrite: bool = True,
) -> dict:
    """Compute net_btc_balance_change as MoM delta of holdings_btc and write to final_data_points.

    Reads holdings_btc from final_data_points (analyst-accepted values) and
    computes month-over-month differences for consecutive monthly periods.
    Writes results to final_data_points with source_ref='derived:holdings_btc'.

    Only processes consecutive monthly pairs (exactly 1 month apart).
    Non-consecutive gaps are logged and skipped.

    Args:
        ticker: Company ticker symbol.
        db: MinerDB instance.
        dry_run: If True, compute deltas but do not write.
        overwrite: If False, skip periods that already have a net_btc_balance_change
            value in final_data_points (preserves analyst edits).

    Returns:
        dict with keys 'derived', 'skipped', 'rows'.
    """
    import re as _re
    ticker = ticker.upper()

    # Collect holdings_btc from final_data_points.
    finals = db.get_final_data_points(ticker)
    monthly_re = _re.compile(r'^\d{4}-\d{2}-01$')
    holdings = sorted(
        [f for f in finals if f.get('metric') == 'holdings_btc' and monthly_re.match(f.get('period', ''))],
        key=lambda r: r['period'],
    )

    if len(holdings) < 2:
        log.info(
            "derive_nbc_start ticker=%s holdings_rows=%d — need >= 2 for deltas",
            ticker, len(holdings),
        )
        return {'derived': 0, 'skipped': 0, 'rows': []}

    log.info(
        "derive_nbc_start ticker=%s holdings_rows=%d dry_run=%s overwrite=%s",
        ticker, len(holdings), dry_run, overwrite,
    )

    # Build existing net_btc_balance_change map to support overwrite=False.
    existing_nbc = {
        f['period'] for f in finals
        if f.get('metric') == 'net_btc_balance_change'
    }

    derived = 0
    skipped = 0
    rows = []

    for i in range(1, len(holdings)):
        prev = holdings[i - 1]
        curr = holdings[i]
        gap = _months_apart(prev['period'], curr['period'])

        if gap != 1:
            log.debug(
                "derive_nbc_skip ticker=%s period=%s gap_months=%d",
                ticker, curr['period'], gap,
            )
            skipped += 1
            rows.append({
                'status': 'skipped',
                'reason': f'gap_{gap}_months',
                'ticker': ticker,
                'period': curr['period'],
            })
            continue

        delta = curr['value'] - prev['value']

        if not overwrite and curr['period'] in existing_nbc:
            skipped += 1
            rows.append({
                'status': 'skipped',
                'reason': 'already_exists',
                'ticker': ticker,
                'period': curr['period'],
                'value': delta,
            })
            continue

        rows.append({
            'status': 'would_derive' if dry_run else 'derived',
            'ticker': ticker,
            'period': curr['period'],
            'value': delta,
            'prev_period': prev['period'],
            'prev_holdings': prev['value'],
            'curr_holdings': curr['value'],
        })

        if not dry_run:
            try:
                db.upsert_final_data_point(
                    ticker=ticker,
                    period=curr['period'],
                    metric='net_btc_balance_change',
                    value=delta,
                    unit='BTC',
                    confidence=1.0,
                    source_ref='derived:holdings_btc',
                    time_grain='monthly',
                )
                derived += 1
                log.info(
                    "derive_nbc_wrote ticker=%s period=%s delta=%s",
                    ticker, curr['period'], delta,
                )
            except Exception:
                log.exception(
                    "derive_nbc_write_error ticker=%s period=%s",
                    ticker, curr['period'],
                )
                rows[-1]['status'] = 'error'
        else:
            derived_count_for_dry = sum(1 for r in rows if r.get('status') == 'would_derive')

    log.info(
        "derive_nbc_end ticker=%s derived=%d skipped=%d dry_run=%s",
        ticker, derived, skipped, dry_run,
    )
    return {'derived': derived, 'skipped': skipped, 'rows': rows}
