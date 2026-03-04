"""Regime configuration API routes."""
import logging
from flask import Blueprint, jsonify, request
from app_globals import get_db

log = logging.getLogger('miners.routes.regime')
bp = Blueprint('regime', __name__)

_VALID_CADENCES = {'monthly', 'quarterly'}


@bp.route('/api/regime/<ticker>')
def get_regime(ticker):
    db = get_db()
    windows = db.get_regime_windows(ticker.upper())
    return jsonify({'success': True, 'data': windows})


@bp.route('/api/regime/<ticker>', methods=['POST'])
def add_regime_window(ticker):
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    body = request.get_json(silent=True) or {}
    cadence = body.get('cadence', '').strip()
    start_date = body.get('start_date', '').strip()
    end_date = body.get('end_date') or None
    notes = body.get('notes', '').strip()

    if cadence not in _VALID_CADENCES:
        return jsonify({'success': False, 'error': {'message': f'cadence must be monthly or quarterly'}}), 400
    if not start_date:
        return jsonify({'success': False, 'error': {'message': 'start_date required (YYYY-MM-DD)'}}), 400

    try:
        window = db.upsert_regime_window(ticker, cadence, start_date, end_date, notes)
    except Exception:
        log.error("Failed to add regime window for %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': window}), 201


@bp.route('/api/regime/<ticker>/<int:window_id>', methods=['DELETE'])
def delete_regime_window(ticker, window_id):
    db = get_db()
    db.delete_regime_window(window_id)
    return jsonify({'success': True})
