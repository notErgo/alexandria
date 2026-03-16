"""QC snapshot and ticker health check routes.

POST /api/qc/snapshot          — record a precision/recall snapshot
GET  /api/qc/summary           — return stored snapshots
GET  /api/qc/ticker_report     — run and return a per-ticker health card
POST /api/qc/reset_orphaned    — reset running reports to pending for a ticker
GET  /api/qc/ticker_history    — return stored health check history for a ticker
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


@bp.route('/api/qc/ticker_report', methods=['GET'])
def ticker_report():
    """Run a health check for a single ticker and return the health card.

    Query params:
        ticker  (required)
        months  (optional, default=24)
    """
    from interpreters.qc_check import run_ticker_health_check

    ticker = request.args.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'ticker' query param is required",
        }}), 400

    try:
        months = int(request.args.get('months', 24))
    except (ValueError, TypeError):
        months = 24

    db = get_db()
    company = db.get_company(ticker)
    if company is None:
        return jsonify({'success': False, 'error': {
            'code': 'TICKER_NOT_FOUND', 'message': f"Unknown ticker: {ticker}",
        }}), 404

    try:
        health_card = run_ticker_health_check(db, ticker, months=months)
    except Exception:
        log.error("ticker_report failed for ticker=%s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'INTERNAL_ERROR', 'message': 'Health check failed',
        }}), 500

    try:
        db.save_health_check(ticker, health_card, trigger='api', months=months)
    except Exception:
        log.warning("Failed to persist health check for ticker=%s", ticker, exc_info=True)

    return jsonify({'success': True, 'data': health_card})


@bp.route('/api/qc/reset_orphaned', methods=['POST'])
def reset_orphaned():
    """Reset extraction_status='running' reports to 'pending' for a ticker.

    Query params:
        ticker  (required)
    """
    ticker = request.args.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'ticker' query param is required",
        }}), 400

    try:
        count = get_db().reset_orphaned_reports(ticker)
    except Exception:
        log.error("reset_orphaned failed for ticker=%s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'INTERNAL_ERROR', 'message': 'Reset failed',
        }}), 500

    return jsonify({'success': True, 'data': {'reset_count': count}})


@bp.route('/api/qc/ticker_history', methods=['GET'])
def ticker_history():
    """Return stored health check history for a ticker.

    Query params:
        ticker  (required)
        limit   (optional, default=20)
    """
    ticker = request.args.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'ticker' query param is required",
        }}), 400

    try:
        limit = int(request.args.get('limit', 20))
    except (ValueError, TypeError):
        limit = 20

    try:
        history = get_db().get_health_check_history(ticker, limit=limit)
    except Exception:
        log.error("ticker_history failed for ticker=%s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'INTERNAL_ERROR', 'message': 'Failed to fetch history',
        }}), 500

    return jsonify({'success': True, 'data': history})
