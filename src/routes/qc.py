"""QC snapshot routes.

POST /api/qc/snapshot  — record a precision/recall snapshot
GET  /api/qc/summary   — return stored snapshots
"""
import logging
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.qc')
bp = Blueprint('qc', __name__)


def get_db():
    from app_globals import get_db as _get_db
    return _get_db()


@bp.route('/api/qc/snapshot', methods=['POST'])
def qc_snapshot():
    """Record a QC precision snapshot.

    Body:
      { run_date, auto_accepted, review_accepted, review_rejected }

    precision_est = auto_accepted / (auto_accepted + review_rejected)
    """
    body = request.get_json(silent=True) or {}
    run_date = body.get('run_date')
    if not run_date:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'run_date' is required",
        }}), 400

    auto_accepted = body.get('auto_accepted', 0)
    review_accepted = body.get('review_accepted', 0)
    review_rejected = body.get('review_rejected', 0)

    denominator = auto_accepted + review_rejected
    precision_est = (auto_accepted / denominator) if denominator > 0 else None

    snapshot = {
        'run_date': run_date,
        'auto_accepted': auto_accepted,
        'review_accepted': review_accepted,
        'review_rejected': review_rejected,
        'precision_est': precision_est,
    }
    try:
        get_db().upsert_qc_snapshot(snapshot)
    except Exception:
        log.error("Failed to store QC snapshot", exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'INTERNAL_ERROR', 'message': 'Failed to store snapshot',
        }}), 500

    return jsonify({'success': True, 'data': snapshot})


@bp.route('/api/qc/summary', methods=['GET'])
def qc_summary():
    """Return all QC snapshots ordered by run_date descending."""
    try:
        snapshots = get_db().get_qc_snapshots()
    except Exception:
        log.error("Failed to fetch QC snapshots", exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'INTERNAL_ERROR', 'message': 'Failed to fetch snapshots',
        }}), 500

    return jsonify({'success': True, 'data': snapshots})
