"""Scrape queue management API routes."""
import logging
from flask import Blueprint, jsonify, request
from app_globals import get_db, get_scrape_worker

log = logging.getLogger('miners.routes.scrape')
bp = Blueprint('scrape', __name__)


@bp.route('/api/scrape/trigger/<ticker>', methods=['POST'])
def trigger_scrape(ticker):
    db = get_db()
    ticker = ticker.upper()
    company = db.get_company(ticker)
    if company is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    if company.get('scraper_mode') == 'skip':
        return jsonify({'success': False, 'error': {
            'message': f'Scrape skipped — scraper_mode is skip for {ticker}'
        }}), 400
    try:
        job = db.enqueue_scrape_job(ticker, 'historic')
    except ValueError as e:
        return jsonify({'success': False, 'error': {'message': str(e)}}), 400
    except Exception:
        log.error("Failed to enqueue scrape job for %s", ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True, 'data': job}), 202


@bp.route('/api/scrape/queue')
def scrape_queue():
    db = get_db()
    jobs = db.get_scrape_queue_status()
    return jsonify({'success': True, 'data': jobs})


@bp.route('/api/scrape/queue/<int:job_id>')
def scrape_job(job_id):
    db = get_db()
    jobs = db.get_scrape_queue_status()
    job = next((j for j in jobs if j['id'] == job_id), None)
    if job is None:
        return jsonify({'success': False, 'error': {'message': 'Job not found'}}), 404
    return jsonify({'success': True, 'data': job})
