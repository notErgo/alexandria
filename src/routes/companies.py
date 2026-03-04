"""Company API routes."""
import logging
from flask import Blueprint, jsonify

log = logging.getLogger('miners.routes.companies')

bp = Blueprint('companies', __name__)


@bp.route('/api/companies')
def list_companies():
    from app_globals import get_db
    db = get_db()
    companies = db.get_companies(active_only=True)
    return jsonify({'success': True, 'data': companies})


@bp.route('/api/companies/<ticker>')
def get_company(ticker):
    from app_globals import get_db
    db = get_db()
    company = db.get_company(ticker.upper())
    if company is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': f'Company {ticker!r} not found'
        }}), 404
    return jsonify({'success': True, 'data': company})
