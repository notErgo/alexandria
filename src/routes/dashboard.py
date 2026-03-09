"""
Dashboard API routes.

  GET /api/dashboard/stacked  — cross-company stacked bar data for one metric
    ?metric=production_btc
    ?months=24              — how many most-recent months to include (default 24)

Response:
  {
    "success": true,
    "data": {
      "metric": "production_btc",
      "label":  "Production",
      "unit":   "BTC",
      "time_spine": ["2023-01", "2023-02", ...],   // YYYY-MM, oldest→newest
      "series": [                                   // sorted largest current-month first
        {
          "ticker": "MARA",
          "values": [692.0, null, 730.0, ...]       // null = no data for that period
        },
        ...
      ]
    }
  }
"""
import logging
from typing import List, Optional

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.dashboard')

bp = Blueprint('dashboard', __name__)

_VALID_METRICS = {
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc', 'mining_mw', 'ai_hpc_mw',
    'hpc_revenue_usd', 'gpu_count',
}

_METRIC_META = {
    'production_btc':         ('Production',    'BTC'),
    'holdings_btc':           ('Holdings',      'BTC'),
    'unrestricted_holdings':  ('Holdings (Unres.)', 'BTC'),
    'restricted_holdings_btc': ('Holdings (Restr.)', 'BTC'),
    'sales_btc':              ('Sold',          'BTC'),
    'hashrate_eh':            ('Hashrate',      'EH/s'),
    'realization_rate':       ('Realization',   '%'),
    'net_btc_balance_change': ('Net BTC Change','BTC'),
    'encumbered_btc':         ('Encumbered BTC','BTC'),
    'mining_mw':              ('Mining MW',     'MW'),
    'ai_hpc_mw':              ('AI/HPC MW',     'MW'),
    'hpc_revenue_usd':        ('HPC Revenue',   'USD'),
    'gpu_count':              ('GPU Count',     'units'),
}


def sort_series_by_current_month(series: list) -> list:
    """Sort company series descending by the last non-null value.

    Companies with no data (all None) sort last with effective value 0.
    """
    def last_value(s):
        for v in reversed(s.get('values', [])):
            if v is not None:
                return v
        return 0.0

    return sorted(series, key=last_value, reverse=True)


@bp.route('/api/dashboard/stacked')
def stacked_bar():
    """Return stacked bar data for a single metric across all companies."""
    try:
        from app_globals import get_db

        metric = request.args.get('metric', '').strip()
        if not metric:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM',
                'message': "'metric' query parameter is required"
            }}), 400
        if metric not in _VALID_METRICS:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM',
                'message': f"Unknown metric '{metric}'. Valid: {sorted(_VALID_METRICS)}"
            }}), 400

        try:
            months = int(request.args.get('months', 24))
            if months < 1 or months > 120:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM',
                'message': "'months' must be an integer between 1 and 120"
            }}), 400

        db = get_db()
        label, unit = _METRIC_META.get(metric, (metric, ''))

        # Fetch all data points for this metric
        rows = db.query_data_points(metric=metric, limit=10000)
        if not rows:
            return jsonify({'success': True, 'data': {
                'metric': metric, 'label': label, 'unit': unit,
                'time_spine': [], 'series': [],
            }})

        # Build sorted time spine (YYYY-MM strings, most recent N months)
        all_periods = sorted({r['period'][:7] for r in rows})  # YYYY-MM
        time_spine = all_periods[-months:]

        # Group values by ticker → period
        by_ticker: dict = {}
        for row in rows:
            ym = row['period'][:7]
            if ym not in time_spine:
                continue
            by_ticker.setdefault(row['ticker'], {})[ym] = row['value']

        # Build aligned series (None for missing periods)
        series = []
        for ticker, period_map in sorted(by_ticker.items()):
            values = [period_map.get(ym) for ym in time_spine]
            series.append({'ticker': ticker, 'values': values})

        # Sort companies largest-current-month first
        series = sort_series_by_current_month(series)

        return jsonify({'success': True, 'data': {
            'metric': metric,
            'label': label,
            'unit': unit,
            'time_spine': time_spine,
            'series': series,
        }})
    except Exception:
        log.error('Error building stacked bar data', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
