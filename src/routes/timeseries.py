"""
Timeseries API route: aligned monthly series per metric across tickers.
Also provides:
  GET  /api/timeseries/suggest  — interpolation + nearby reports + review items for a missing period
  POST /api/timeseries/fill     — submit a suggested value to the review queue (PENDING)

GET /api/timeseries
Query params:
  tickers  — comma-separated, e.g. "MARA,RIOT"  (default: all tickers with data)
  from     — YYYY-MM  (default: earliest period in DB)
  to       — YYYY-MM  (default: latest period in DB)

Response:
  {
    "success": true,
    "data": {
      "time_spine": ["2021-05", ..., "2025-12"],
      "metrics": [
        {
          "metric": "hashrate_eh",
          "label": "Hashrate",
          "unit": "EH/s",
          "series": [
            {
              "ticker": "MARA",
              "values": [1.29, null, 2.4, ...],
              "present": [true, false, true, ...],
              "completeness": 0.87,
              "data_points": 47
            }
          ]
        }
      ]
    }
  }
"""
import logging
import math
import re
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.timeseries')

bp = Blueprint('timeseries', __name__)

# Fallback metric order used only by suggest/fill validation when DB is unavailable.
# Main get_timeseries() derives metric list from metric_schema at request time (SSOT).
_METRIC_ORDER_FALLBACK = [
    ('hashrate_eh',             'Hashrate',      'EH/s'),
    ('production_btc',          'Production',    'BTC'),
    ('sales_btc',               'Sold',          'BTC'),
    ('holdings_btc',            'Holdings',      'BTC'),
    ('unrestricted_holdings',   'Holdings (Unres.)', 'BTC'),
    ('restricted_holdings_btc', 'Holdings (Restr.)', 'BTC'),
    ('realization_rate',        'Realization',   '%'),
    ('net_btc_balance_change',  'Net BTC Change','BTC'),
    ('encumbered_btc',          'Encumbered BTC','BTC'),
    ('mining_mw',               'Mining MW',     'MW'),
    ('ai_hpc_mw',               'AI/HPC MW',     'MW'),
    ('hpc_revenue_usd',         'HPC Revenue',   'USD'),
    ('gpu_count',               'GPU Count',     'units'),
]

_PERIOD_RE = re.compile(r'^\d{4}-\d{2}$')


def _build_time_spine(from_ym: str, to_ym: str) -> list:
    """Build list of YYYY-MM strings from from_ym to to_ym inclusive."""
    fy, fm = int(from_ym[:4]), int(from_ym[5:7])
    ty, tm = int(to_ym[:4]), int(to_ym[5:7])
    spine = []
    y, m = fy, fm
    while (y, m) <= (ty, tm):
        spine.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return spine


def _period_to_ym(period: str) -> str:
    """Convert DB period 'YYYY-MM-01' to 'YYYY-MM'."""
    return period[:7]


@bp.route('/api/timeseries')
def get_timeseries():
    try:
        from app_globals import get_db
        db = get_db()

        # ── Parse and validate query params ─────────────────────────────────
        raw_tickers = request.args.get('tickers', '').strip()
        raw_from    = request.args.get('from', '').strip()
        raw_to      = request.args.get('to', '').strip()

        # Validate ticker list (if provided)
        tickers = []
        if raw_tickers:
            for t in raw_tickers.split(','):
                t = t.strip().upper()
                if not t:
                    continue
                if not re.match(r'^[A-Z0-9]{1,6}$', t):
                    return jsonify({'success': False, 'error': {
                        'message': f'Invalid ticker: {t}'}}), 400
                tickers.append(t)

        # Validate date range
        if raw_from and not _PERIOD_RE.match(raw_from):
            return jsonify({'success': False, 'error': {
                'message': f'Invalid from date: {raw_from} (expected YYYY-MM)'}}), 400
        if raw_to and not _PERIOD_RE.match(raw_to):
            return jsonify({'success': False, 'error': {
                'message': f'Invalid to date: {raw_to} (expected YYYY-MM)'}}), 400

        # ── Fetch analyst-accepted data only (final_data_points) ─────────────
        if tickers:
            all_rows = []
            for t in tickers:
                rows = db.query_final_data_points(
                    ticker=t,
                    from_period=f"{raw_from}-01" if raw_from else None,
                    to_period=f"{raw_to}-01" if raw_to else None,
                    limit=10000,
                )
                all_rows.extend(rows)
        else:
            all_rows = db.query_final_data_points(
                from_period=f"{raw_from}-01" if raw_from else None,
                to_period=f"{raw_to}-01" if raw_to else None,
                limit=50000,
            )

        if not all_rows:
            # Return empty but valid response
            return jsonify({'success': True, 'data': {
                'time_spine': [], 'metrics': []}})

        # ── Determine time spine ─────────────────────────────────────────────
        all_periods = [_period_to_ym(r['period']) for r in all_rows]
        min_period = raw_from if raw_from else min(all_periods)
        max_period = raw_to   if raw_to   else max(all_periods)
        time_spine = _build_time_spine(min_period, max_period)
        spine_index = {ym: i for i, ym in enumerate(time_spine)}

        # ── Build index: (ticker, metric, YYYY-MM) → value ───────────────────
        data_index: dict = {}
        active_tickers: set = set()
        for row in all_rows:
            ym = _period_to_ym(row['period'])
            key = (row['ticker'], row['metric'], ym)
            data_index[key] = row['value']
            active_tickers.add(row['ticker'])

        # Use requested tickers (if supplied), else all tickers found in data
        ticker_list = tickers if tickers else sorted(active_tickers)

        # ── Build response metrics (SSOT: metric_schema, not hardcoded list) ──
        n = len(time_spine)
        metrics_out = []

        # Derive metric order from metric_schema DB (SSOT); fall back to hardcoded list
        try:
            schema_rows = db.get_metric_schema(sector=_SECTOR, active_only=True)
            metric_order = [(r['key'], r['label'], r.get('unit', '')) for r in schema_rows]
        except Exception:
            metric_order = _METRIC_ORDER_FALLBACK

        for metric_key, label, unit in metric_order:
            series_out = []

            for ticker in ticker_list:
                values  = []
                present = []
                for ym in time_spine:
                    val = data_index.get((ticker, metric_key, ym))
                    values.append(val)            # None if missing
                    present.append(val is not None)

                dp_count = sum(present)
                if dp_count == 0:
                    continue  # Skip tickers with zero points for this metric

                completeness = round(dp_count / n, 4) if n > 0 else 0.0
                series_out.append({
                    'ticker':       ticker,
                    'values':       values,
                    'present':      present,
                    'completeness': completeness,
                    'data_points':  dp_count,
                })

            if not series_out:
                continue  # Skip metrics with no data

            metrics_out.append({
                'metric': metric_key,
                'label':  label,
                'unit':   unit,
                'series': series_out,
            })

        return jsonify({
            'success': True,
            'data': {
                'time_spine': time_spine,
                'metrics':    metrics_out,
            }
        })

    except Exception as e:
        log.error("Timeseries query failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {
            'message': 'Internal server error'}}), 500


# ── Metric key → unit lookup (fallback; suggest/fill validate against DB at runtime) ──
_METRIC_UNITS = {k: unit for k, _label, unit in _METRIC_ORDER_FALLBACK}
_METRIC_KEYS  = set(_METRIC_UNITS)


_SECTOR = 'BTC-miners'


def _get_valid_metric_keys(db) -> set:
    """Return current valid metric keys from metric_schema (SSOT). Falls back to hardcoded set."""
    try:
        rows = db.get_metric_schema(sector=_SECTOR, active_only=True)
        return {r['key'] for r in rows}
    except Exception:
        return _METRIC_KEYS


def _get_metric_unit(db, metric: str) -> str:
    """Return unit for a metric key. Looks in metric_schema, falls back to _METRIC_UNITS."""
    try:
        rows = db.get_metric_schema(sector=_SECTOR, active_only=True)
        for r in rows:
            if r['key'] == metric:
                return r.get('unit', '')
    except Exception:
        pass
    return _METRIC_UNITS.get(metric, '')


def _ym_to_months(ym: str) -> int:
    """Convert YYYY-MM to total months since year 0 (for distance math)."""
    y, m = int(ym[:4]), int(ym[5:7])
    return y * 12 + m


@bp.route('/api/timeseries/suggest')
def suggest_period():
    """Return interpolation estimate + nearby reports + review queue items for a missing period."""
    try:
        from app_globals import get_db
        db = get_db()

        ticker = request.args.get('ticker', '').strip().upper()
        metric = request.args.get('metric', '').strip()
        period = request.args.get('period', '').strip()

        # ── Validation ───────────────────────────────────────────────────
        if not re.match(r'^[A-Z0-9]{1,6}$', ticker):
            return jsonify({'success': False, 'error': {'message': 'Invalid ticker'}}), 400
        valid_keys = _get_valid_metric_keys(db)
        if metric not in valid_keys:
            return jsonify({'success': False, 'error': {
                'message': f'Unknown metric: {metric}'}}), 400
        if not _PERIOD_RE.match(period):
            return jsonify({'success': False, 'error': {
                'message': 'period must be YYYY-MM'}}), 400

        unit = _get_metric_unit(db, metric)

        # ── Fetch data points for interpolation (raw extracted data) ─────
        rows = db.query_data_points(ticker=ticker, metric=metric, limit=10000)
        # Build map: YYYY-MM → value
        dp_map = {r['period'][:7]: r['value'] for r in rows}

        # ── Find nearest neighbors ────────────────────────────────────────
        target_months = _ym_to_months(period)
        prev_ym = None
        prev_val = None
        next_ym = None
        next_val = None

        for ym, val in sorted(dp_map.items()):
            m = _ym_to_months(ym)
            if m < target_months:
                if prev_ym is None or m > _ym_to_months(prev_ym):
                    prev_ym, prev_val = ym, val
            elif m > target_months:
                if next_ym is None or m < _ym_to_months(next_ym):
                    next_ym, next_val = ym, val

        # ── Compute interpolation ─────────────────────────────────────────
        interpolation = None
        if prev_ym is not None and next_ym is not None:
            d_prev = target_months - _ym_to_months(prev_ym)
            d_next = _ym_to_months(next_ym) - target_months
            total  = d_prev + d_next
            interp_val = prev_val + (next_val - prev_val) * d_prev / total
            interpolation = {
                'value':  round(interp_val, 6),
                'method': 'linear',
                'from':   {'period': prev_ym, 'value': prev_val},
                'to':     {'period': next_ym, 'value': next_val},
            }
        elif prev_ym is not None:
            interpolation = {
                'value':  prev_val,
                'method': 'extrapolation',
                'from':   {'period': prev_ym, 'value': prev_val},
                'to':     None,
            }
        elif next_ym is not None:
            interpolation = {
                'value':  next_val,
                'method': 'extrapolation',
                'from':   None,
                'to':     {'period': next_ym, 'value': next_val},
            }

        # ── Nearby reports + review items ────────────────────────────────
        nearby_reports  = db.get_nearby_reports(ticker, period, window_days=90)
        review_items    = db.get_review_items_for_period(ticker, period + '-01', metric)

        return jsonify({
            'success': True,
            'data': {
                'ticker':         ticker,
                'metric':         metric,
                'period':         period,
                'unit':           unit,
                'interpolation':  interpolation,
                'nearby_reports': nearby_reports,
                'review_items':   review_items,
            }
        })

    except Exception as e:
        log.error("Suggest query failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/timeseries/fill', methods=['POST'])
def fill_period():
    """Submit a suggested value for a missing period to the review queue."""
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}

        ticker = str(body.get('ticker', '')).strip().upper()
        metric = str(body.get('metric', '')).strip()
        period = str(body.get('period', '')).strip()
        note   = str(body.get('note', '')).strip()

        # ── Validation ───────────────────────────────────────────────────
        if not re.match(r'^[A-Z0-9]{1,6}$', ticker):
            return jsonify({'success': False, 'error': {'message': 'Invalid ticker'}}), 400
        valid_keys = _get_valid_metric_keys(db)
        if metric not in valid_keys:
            return jsonify({'success': False, 'error': {
                'message': f'Unknown metric: {metric}'}}), 400
        if not _PERIOD_RE.match(period):
            return jsonify({'success': False, 'error': {
                'message': 'period must be YYYY-MM'}}), 400

        raw_value = body.get('value')
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': {
                'message': 'value must be a number'}}), 400
        if not math.isfinite(value) or value <= 0:
            return jsonify({'success': False, 'error': {
                'message': 'value must be a finite positive number'}}), 400

        period_db = period + '-01'

        # ── Conflict check ────────────────────────────────────────────────
        existing = db.query_data_points(
            ticker=ticker, metric=metric,
            from_period=period_db, to_period=period_db, limit=1,
        )
        if existing:
            return jsonify({'success': False, 'error': {
                'message': 'Data already exists for this period'}}), 409

        # ── Insert review item ────────────────────────────────────────────
        snippet = note if note else f'Suggested value for {ticker} {metric} {period}'
        review_id = db.insert_review_item({
            'data_point_id': None,
            'ticker':         ticker,
            'period':         period_db,
            'metric':         metric,
            'raw_value':      str(value),
            'confidence':     0.5,
            'source_snippet': snippet,
            'status':         'PENDING',
        })
        log.info("fill: created review item %d for %s %s %s = %s", review_id, ticker, metric, period, value)

        return jsonify({'success': True, 'data': {'review_id': review_id}})

    except Exception as e:
        log.error("Fill endpoint failed: %s", e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
