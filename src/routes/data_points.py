"""Data query and export API routes."""
import csv
import io
import logging
import re

from flask import Blueprint, jsonify, request, make_response, g

log = logging.getLogger('miners.routes.data_points')

bp = Blueprint('data_points', __name__)

_VALID_METRICS = {
    'production_btc', 'hodl_btc', 'hodl_btc_unrestricted', 'hodl_btc_restricted',
    'sold_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
}
_PERIOD_RE = re.compile(r'^\d{4}-\d{2}$')


def _validate_filters(args):
    """Validate query parameters. Returns (params_dict, error_response or None).

    'ticker' supports multiple values (repeated param: ?ticker=MARA&ticker=RIOT).
    """
    tickers = args.getlist('ticker') if hasattr(args, 'getlist') else (
        [args.get('ticker')] if args.get('ticker') else []
    )
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    metric = args.get('metric')
    from_period = args.get('from_period')
    to_period = args.get('to_period')
    min_confidence_str = args.get('min_confidence')

    if tickers:
        from app_globals import get_db
        db = get_db()
        for t in tickers:
            if not db.get_company(t):
                return None, (jsonify({'success': False, 'error': {
                    'code': 'INVALID_TICKER', 'message': f'Ticker {t!r} not recognized'
                }}), 400)

    if metric and metric not in _VALID_METRICS:
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_METRIC', 'message': f'Metric {metric!r} not valid'
        }}), 400)

    if from_period and not _PERIOD_RE.match(from_period):
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE', 'message': 'from_period must be YYYY-MM'
        }}), 400)

    if to_period and not _PERIOD_RE.match(to_period):
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE', 'message': 'to_period must be YYYY-MM'
        }}), 400)

    min_confidence = None
    if min_confidence_str is not None:
        try:
            min_confidence = float(min_confidence_str)
            if not 0.0 <= min_confidence <= 1.0:
                raise ValueError
        except ValueError:
            return None, (jsonify({'success': False, 'error': {
                'code': 'INVALID_CONFIDENCE', 'message': 'min_confidence must be float in [0.0, 1.0]'
            }}), 400)

    # Convert YYYY-MM to YYYY-MM-01 for DB comparison
    from_full = (from_period + '-01') if from_period else None
    to_full = (to_period + '-01') if to_period else None

    return {
        'tickers': tickers or None,   # list or None
        'metric': metric or None,
        'from_period': from_full,
        'to_period': to_full,
        'min_confidence': min_confidence,
    }, None


@bp.route('/api/data')
def get_data():
    from app_globals import get_db
    params, err = _validate_filters(request.args)
    if err:
        return err
    db = get_db()
    rows = db.query_data_points(**params)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/data/lineage')
def get_data_lineage():
    """Return full provenance for a single data point (ticker + metric + period).

    Query params: ticker (str), metric (str), period (YYYY-MM).
    Returns: confidence, extraction_method, source_snippet, source_type,
             report_date, source_url — raw_text is never exposed.
    """
    from app_globals import get_db
    ticker = request.args.get('ticker', '').strip().upper()
    metric = request.args.get('metric', '').strip()
    period = request.args.get('period', '').strip()

    if not ticker or not metric or not period:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_PARAMS',
            'message': 'ticker, metric, period are all required',
        }}), 400

    if metric not in _VALID_METRICS:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_METRIC',
            'message': f'Unknown metric: {metric!r}',
        }}), 400

    if not _PERIOD_RE.match(period):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE',
            'message': 'period must be YYYY-MM',
        }}), 400

    db = get_db()
    period_db = period + '-01'
    try:
        rows = db.query_data_points(
            ticker=ticker, metric=metric,
            from_period=period_db, to_period=period_db,
            limit=1,
        )
    except Exception:
        log.error("lineage query failed for %s/%s/%s", ticker, metric, period, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'SERVER_ERROR', 'message': 'Internal server error',
        }}), 500

    if not rows:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND',
            'message': f'No data for {ticker} {metric} {period}',
        }}), 404

    row = rows[0]
    report = {}
    if row.get('report_id'):
        try:
            report = db.get_report(row['report_id']) or {}
        except Exception:
            log.error("get_report failed for report_id=%s", row['report_id'], exc_info=True)

    return jsonify({'success': True, 'data': {
        'ticker':            row['ticker'],
        'metric':            row['metric'],
        'period':            period,
        'value':             row['value'],
        'unit':              row['unit'],
        'confidence':        row['confidence'],
        'extraction_method': row['extraction_method'],
        'source_snippet':    row['source_snippet'],
        'created_at':        row['created_at'],
        'source_type':       report.get('source_type'),
        'report_date':       report.get('report_date'),
        'source_url':        report.get('source_url'),
    }})


@bp.route('/api/export.csv')
def export_csv():
    from app_globals import get_db
    params, err = _validate_filters(request.args)
    if err:
        return err
    db = get_db()
    rows = db.query_data_points_for_export(**params)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        'ticker', 'period', 'metric', 'value', 'unit',
        'confidence', 'extraction_method', 'source_url',
        'llm_value', 'regex_value', 'agreement_status',
        'source_snippet', 'created_at',
    ], extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        # Emit empty string for None values so CSV is clean
        cleaned = {k: ('' if v is None else v) for k, v in row.items()}
        writer.writerow(cleaned)

    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=miners_export.csv'
    return response
