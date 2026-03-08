"""API routes for per-metric agreement and outlier threshold configuration."""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.metric_rules')

bp = Blueprint('metric_rules', __name__)

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

    vrmin = body.get('valid_range_min')
    vrmax = body.get('valid_range_max')
    if vrmin is not None:
        try:
            vrmin = float(vrmin)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': 'valid_range_min must be a number',
            }}), 400
    if vrmax is not None:
        try:
            vrmax = float(vrmax)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': 'valid_range_max must be a number',
            }}), 400
    if vrmin is not None and vrmax is not None and vrmin >= vrmax:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM', 'message': 'valid_range_min must be less than valid_range_max',
        }}), 400

    db = get_db()
    try:
        row = db.upsert_metric_rule(
            metric=metric,
            agreement_threshold=ag_thresh,
            outlier_threshold=out_thresh,
            outlier_min_history=min_hist,
            enabled=enabled,
            notes=notes,
            valid_range_min=vrmin,
            valid_range_max=vrmax,
        )
    except Exception as e:
        log.error("upsert_metric_rule failed for %s: %s", metric, e, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'DB_ERROR', 'message': 'Failed to update metric rule',
        }}), 500

    return jsonify({'success': True, 'data': row})


@bp.route('/api/metric_rules', methods=['POST'])
def create_metric_rule():
    """Create a new metric_rules row for any metric key.

    Body (JSON):
        metric               (str, required)
        agreement_threshold  (float, 0.0-1.0, required)
        outlier_threshold    (float >= 0.0, required)
        outlier_min_history  (int >= 1, required)
        enabled              (int 0|1, optional, default 1)
        notes                (str, optional)
    """
    from app_globals import get_db
    body = request.get_json(silent=True) or {}

    metric = (body.get('metric') or '').strip()
    if not metric:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM', 'message': "'metric' is required",
        }}), 400

    try:
        ag_thresh = float(body['agreement_threshold'])
        if not 0.0 <= ag_thresh <= 1.0:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM',
            'message': 'agreement_threshold must be a float in [0.0, 1.0]',
        }}), 400

    try:
        out_thresh = float(body['outlier_threshold'])
        if out_thresh < 0.0:
            raise ValueError
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM',
            'message': 'outlier_threshold must be a non-negative float',
        }}), 400

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

    vrmin = body.get('valid_range_min')
    vrmax = body.get('valid_range_max')
    if vrmin is not None:
        try:
            vrmin = float(vrmin)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': 'valid_range_min must be a number',
            }}), 400
    if vrmax is not None:
        try:
            vrmax = float(vrmax)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': 'valid_range_max must be a number',
            }}), 400
    if vrmin is not None and vrmax is not None and vrmin >= vrmax:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PARAM', 'message': 'valid_range_min must be less than valid_range_max',
        }}), 400

    db = get_db()
    try:
        row = db.upsert_metric_rule(
            metric=metric,
            agreement_threshold=ag_thresh,
            outlier_threshold=out_thresh,
            outlier_min_history=min_hist,
            enabled=enabled,
            notes=notes,
            valid_range_min=vrmin,
            valid_range_max=vrmax,
        )
    except Exception as e:
        log.error("create metric_rule failed for %s: %s", metric, e, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'DB_ERROR', 'message': 'Failed to create metric rule',
        }}), 500

    return jsonify({'success': True, 'data': row}), 201


@bp.route('/api/metric_rules/<metric>', methods=['DELETE'])
def delete_metric_rule(metric: str):
    """Delete the metric_rules row for the given metric."""
    from app_globals import get_db
    db = get_db()
    try:
        db.delete_metric_rule(metric)
    except Exception as e:
        log.error("delete_metric_rule failed for %s: %s", metric, e, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'DB_ERROR', 'message': 'Failed to delete metric rule',
        }}), 500
    return jsonify({'success': True})
