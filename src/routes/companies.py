"""Company and metric schema API routes."""
import logging
import sqlite3
from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.companies')

bp = Blueprint('companies', __name__)

_VALID_SCRAPER_MODES = {'rss', 'index', 'template', 'skip'}


@bp.route('/api/companies')
def list_companies():
    from app_globals import get_db
    db = get_db()
    companies = db.get_companies(active_only=False)
    return jsonify({'success': True, 'data': companies})


@bp.route('/api/companies', methods=['POST'])
def create_company():
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    ticker = body.get('ticker', '').strip().upper()
    name = body.get('name', '').strip()
    sector = body.get('sector', 'BTC-miners').strip()
    scraper_mode = body.get('scraper_mode', 'skip').strip()

    if not ticker or len(ticker) > 10:
        return jsonify({'success': False, 'error': {'message': 'ticker required (max 10 chars)'}}), 400
    if not name or len(name) > 100:
        return jsonify({'success': False, 'error': {'message': 'name required (max 100 chars)'}}), 400
    if scraper_mode not in _VALID_SCRAPER_MODES:
        return jsonify({'success': False, 'error': {'message': f'scraper_mode must be one of {sorted(_VALID_SCRAPER_MODES)}'}}), 400

    try:
        company = db.add_company(
            ticker=ticker, name=name, sector=sector,
            scraper_mode=scraper_mode,
            pr_base_url=body.get('pr_base_url'),
            cik=body.get('cik'),
            scraper_issues_log=body.get('scraper_issues_log', ''),
        )
    except Exception:
        log.error("Failed to create company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True, 'data': company}), 201


@bp.route('/api/companies/<ticker>', methods=['GET'])
def get_company(ticker):
    from app_globals import get_db
    db = get_db()
    company = db.get_company(ticker.upper())
    if company is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': f'Company {ticker!r} not found'
        }}), 404
    return jsonify({'success': True, 'data': company})


@bp.route('/api/companies/<ticker>', methods=['PUT'])
def update_company(ticker):
    from app_globals import get_db
    db = get_db()
    ticker = ticker.upper()
    if db.get_company(ticker) is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    body = request.get_json(silent=True) or {}
    allowed = {'name', 'pr_base_url', 'scraper_mode', 'scraper_issues_log', 'cik', 'sector'}
    kwargs = {k: v for k, v in body.items() if k in allowed}
    if 'scraper_mode' in kwargs and kwargs['scraper_mode'] not in _VALID_SCRAPER_MODES:
        return jsonify({'success': False, 'error': {'message': f'scraper_mode must be one of {sorted(_VALID_SCRAPER_MODES)}'}}), 400
    try:
        company = db.update_company_config(ticker, **kwargs)
    except Exception:
        log.error("Failed to update company %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': company})


# ── Metric schema routes ──────────────────────────────────────────────────────

@bp.route('/api/metric_schema')
def list_metric_schema():
    from app_globals import get_db
    db = get_db()
    sector = request.args.get('sector', 'BTC-miners')
    rows = db.get_metric_schema(sector)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/metric_schema', methods=['POST'])
def add_metric_schema():
    from app_globals import get_db
    db = get_db()
    body = request.get_json(silent=True) or {}
    key = body.get('key', '').strip()
    label = body.get('label', '').strip()
    unit = body.get('unit', '').strip()
    sector = body.get('sector', 'BTC-miners').strip()

    if not key or len(key) > 50 or ' ' in key:
        return jsonify({'success': False, 'error': {'message': 'key required (max 50 chars, no spaces)'}}), 400
    if not label or len(label) > 100:
        return jsonify({'success': False, 'error': {'message': 'label required (max 100 chars)'}}), 400

    try:
        row = db.add_analyst_metric(key, label, unit, sector)
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': {
            'message': f'A column named {key!r} already exists in this sector\'s schema.'
        }}), 409
    except Exception:
        log.error("Failed to add metric schema %s", key, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True, 'data': row}), 201
