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


@bp.route('/api/scrape/trigger-all', methods=['POST'])
def trigger_scrape_all():
    """POST /api/scrape/trigger-all — enqueue scrape jobs for all non-skip companies.

    Skips companies whose scraper_mode is 'skip' or that already have a pending/running job.
    Returns counts of enqueued, skipped (mode=skip), and already-queued companies.
    """
    db = get_db()
    companies = db.get_companies(active_only=False)
    enqueued = []
    skipped_mode = []
    already_queued = []
    errors = []
    for company in companies:
        ticker = company['ticker']
        if company.get('scraper_mode') == 'skip':
            skipped_mode.append(ticker)
            continue
        try:
            job = db.enqueue_scrape_job(ticker, 'historic')
            enqueued.append(ticker)
            log.info("Enqueued overnight scrape job for %s (job_id=%s)", ticker, job['id'])
        except ValueError:
            # Already pending or running
            already_queued.append(ticker)
        except Exception:
            log.error("Failed to enqueue scrape job for %s", ticker, exc_info=True)
            errors.append(ticker)
    log.info(
        "event=trigger_all_scrape enqueued=%d skipped_mode=%d already_queued=%d errors=%d",
        len(enqueued), len(skipped_mode), len(already_queued), len(errors),
    )
    return jsonify({
        'success': True,
        'data': {
            'enqueued': enqueued,
            'skipped_mode': skipped_mode,
            'already_queued': already_queued,
            'errors': errors,
        },
    }), 202


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
