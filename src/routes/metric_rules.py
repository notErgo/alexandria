"""API routes for per-metric agreement and outlier threshold configuration."""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.metric_rules')

bp = Blueprint('metric_rules', __name__)

_METRIC_NAMES = {
    'production_btc', 'hodl_btc', 'sold_btc', 'hodl_btc_unrestricted',
    'hodl_btc_restricted', 'net_btc_balance_change', 'encumbered_btc',
    'hashrate_eh', 'realization_rate', 'mining_mw', 'ai_hpc_mw',
    'hpc_revenue_usd', 'gpu_count',
}


@bp.route('/api/metric_rules')
def list_metric_rules():
    """Return all metric_rules rows."""
    from app_globals import get_db
    db = get_db()
    rules = db.get_metric_rules()
    return jsonify({'success': True, 'data': rules})


@bp.route('/api/metric_rules/<metric>')
def get_metric_rule(metric: str):
    """Return metric_rules for a single metric."""
    from app_globals import get_db
    db = get_db()
    rows = db.get_metric_rules(metric=metric)
    if not rows:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': f'No rule found for metric {metric!r}',
        }}), 404
    return jsonify({'success': True, 'data': rows[0]})


@bp.route('/api/metric_rules/<metric>', methods=['PUT'])
def update_metric_rule(metric: str):
    """Update agreement_threshold, outlier_threshold, outlier_min_history, notes for a metric.

    Body (JSON):
        agreement_threshold  (float, 0.0-1.0, required)
        outlier_threshold    (float >= 0.0, required)
        outlier_min_history  (int >= 1, required)
        enabled              (int 0|1, optional, default 1)
        notes                (str, optional)
    """
    from app_globals import get_db
    body = request.get_json(silent=True) or {}

    # Validate agreement_threshold
    try:
        ag_thresh = float(body['agreement_threshold'])
        if not 0.0 <= ag_thresh <= 1.0:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM',
            'message': 'agreement_threshold must be a float in [0.0, 1.0]',
        }}), 400

    # Validate outlier_threshold
    try:
        out_thresh = float(body['outlier_threshold'])
        if out_thresh < 0.0:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM',
            'message': 'outlier_threshold must be a non-negative float',
        }}), 400

    # Validate outlier_min_history
    try:
        min_hist = int(body['outlier_min_history'])
        if min_hist < 1:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM',
            'message': 'outlier_min_history must be an integer >= 1',
        }}), 400

    enabled = int(bool(body.get('enabled', 1)))
    notes = body.get('notes')
    if notes is not None:
        notes = str(notes)[:500]

    db = get_db()
    try:
        row = db.upsert_metric_rule(
            metric=metric,
            agreement_threshold=ag_thresh,
            outlier_threshold=out_thresh,
            outlier_min_history=min_hist,
            enabled=enabled,
            notes=notes,
        )
    except Exception as e:
        log.error("upsert_metric_rule failed for %s: %s", metric, e, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'DB_ERROR', 'message': 'Failed to update metric rule',
        }}), 500

    return jsonify({'success': True, 'data': row})
