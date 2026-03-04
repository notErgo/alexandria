"""
Facilities, BTC loans, and source audit API routes.

  GET  /api/facilities            — list all facilities (optionally ?ticker=X)
  POST /api/facilities            — insert a facility
  GET  /api/btc_loans             — list BTC loans (?ticker=X required)
  POST /api/btc_loans             — insert a BTC loan
  GET  /api/source_audit          — list source audit rows (?ticker=X required)
  POST /api/source_audit          — upsert a source_audit row
"""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.facilities')

bp = Blueprint('facilities', __name__)

_VALID_PURPOSES = {'MINING', 'AI_HPC', 'HYBRID'}


@bp.route('/api/facilities')
def list_facilities():
    try:
        from app_globals import get_db
        db = get_db()
        ticker = request.args.get('ticker')
        if ticker:
            ticker = ticker.upper()
        facilities = db.get_facilities(ticker)
        return jsonify({'success': True, 'data': {'facilities': facilities}})
    except Exception:
        log.error('Error listing facilities', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/facilities', methods=['POST'])
def create_facility():
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        ticker = (body.get('ticker') or '').strip().upper()
        name = (body.get('name') or '').strip()
        purpose = (body.get('purpose') or 'MINING').strip().upper()

        if not ticker:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'ticker' is required"
            }}), 400
        if not name:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'name' is required"
            }}), 400
        if purpose not in _VALID_PURPOSES:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': f"'purpose' must be one of {sorted(_VALID_PURPOSES)}"
            }}), 400

        size_mw = body.get('size_mw')
        if size_mw is not None:
            try:
                size_mw = float(size_mw)
                if size_mw < 0:
                    raise ValueError
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT', 'message': "'size_mw' must be a non-negative number"
                }}), 400

        lat = body.get('lat')
        lon = body.get('lon')
        if lat is not None:
            try:
                lat = float(lat)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT', 'message': "'lat' must be a number"
                }}), 400
        if lon is not None:
            try:
                lon = float(lon)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT', 'message': "'lon' must be a number"
                }}), 400

        record = {
            'ticker': ticker,
            'name': name,
            'address': (body.get('address') or '').strip() or None,
            'city': (body.get('city') or '').strip() or None,
            'state': (body.get('state') or '').strip() or None,
            'lat': lat,
            'lon': lon,
            'purpose': purpose,
            'size_mw': size_mw,
            'operational_since': body.get('operational_since'),
        }
        fid = db.insert_facility(record)
        return jsonify({'success': True, 'data': {'id': fid}}), 201
    except Exception:
        log.error('Error creating facility', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/btc_loans')
def list_btc_loans():
    try:
        from app_globals import get_db
        db = get_db()

        ticker = request.args.get('ticker', '').strip().upper()
        if not ticker:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': "'ticker' query parameter is required"
            }}), 400

        loans = db.get_btc_loans(ticker)
        return jsonify({'success': True, 'data': {'loans': loans}})
    except Exception:
        log.error('Error listing BTC loans', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/btc_loans', methods=['POST'])
def create_btc_loan():
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        ticker = (body.get('ticker') or '').strip().upper()
        if not ticker:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'ticker' is required"
            }}), 400

        total_btc = body.get('total_btc_encumbered')
        if total_btc is None:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'total_btc_encumbered' is required"
            }}), 400
        try:
            total_btc = float(total_btc)
            if total_btc < 0:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'total_btc_encumbered' must be a non-negative number"
            }}), 400

        record = {
            'ticker': ticker,
            'counterparty': (body.get('counterparty') or '').strip() or None,
            'total_btc_encumbered': total_btc,
            'as_of_date': body.get('as_of_date'),
        }
        lid = db.insert_btc_loan(record)
        return jsonify({'success': True, 'data': {'id': lid}}), 201
    except Exception:
        log.error('Error creating BTC loan', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/source_audit')
def list_source_audit():
    try:
        from app_globals import get_db
        db = get_db()

        ticker = request.args.get('ticker', '').strip().upper()
        if not ticker:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_PARAM', 'message': "'ticker' query parameter is required"
            }}), 400

        rows = db.get_source_audit(ticker)
        return jsonify({'success': True, 'data': {'rows': rows}})
    except Exception:
        log.error('Error listing source audit', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/source_audit', methods=['POST'])
def upsert_source_audit():
    try:
        from app_globals import get_db
        db = get_db()

        body = request.get_json(silent=True) or {}
        ticker = (body.get('ticker') or '').strip().upper()
        source_type = (body.get('source_type') or '').strip().upper()

        if not ticker:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'ticker' is required"
            }}), 400
        if not source_type:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'source_type' is required"
            }}), 400

        valid_source_types = {'IR_PRIMARY', 'GLOBENEWSWIRE', 'PRNEWSWIRE', 'EDGAR', 'WAYBACK'}
        if source_type not in valid_source_types:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': f"'source_type' must be one of {sorted(valid_source_types)}"
            }}), 400

        http_status = body.get('http_status')
        if http_status is not None:
            try:
                http_status = int(http_status)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': {
                    'code': 'INVALID_INPUT', 'message': "'http_status' must be an integer"
                }}), 400

        record = {
            'ticker': ticker,
            'source_type': source_type,
            'url': body.get('url'),
            'last_checked': body.get('last_checked'),
            'http_status': http_status,
            'status': (body.get('status') or 'NOT_TRIED').strip().upper(),
            'notes': body.get('notes'),
        }
        db.upsert_source_audit(record)
        return jsonify({'success': True})
    except Exception:
        log.error('Error upserting source audit', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
