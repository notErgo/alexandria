"""Scrape queue management API routes."""
import logging
import threading
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from flask import Blueprint, jsonify, request
from app_globals import get_db

log = logging.getLogger('miners.routes.scrape')
bp = Blueprint('scrape', __name__)

# Thread-safe progress tracking for backfill tasks
_backfill_progress: dict = {}
_backfill_lock = threading.RLock()
_running_backfills: set = set()
_backfills_lock = threading.Lock()


def _update_backfill_progress(task_id: str, state: dict) -> None:
    with _backfill_lock:
        _backfill_progress[task_id] = state


def _run_backfill(
    task_id: str,
    ticker: str,
    from_date_str: str,
    to_date_str: str,
    auto_extract: bool,
) -> None:
    """Background thread: bounded EDGAR fetch then optional extraction."""
    from app_globals import get_db as _get_db
    import requests as _req
    from scrapers.edgar_connector import EdgarConnector

    db = _get_db()
    _update_backfill_progress(task_id, {
        'status': 'running', 'ticker': ticker,
        'phase': 'edgar_fetch',
        'from_date': from_date_str, 'to_date': to_date_str,
    })
    try:
        company = db.get_company(ticker)
        if company is None:
            raise ValueError(f"Company not found: {ticker}")
        cik = company.get('cik')
        if not cik:
            raise ValueError(f"No CIK configured for {ticker}")

        from_date = date.fromisoformat(from_date_str)
        to_date = date.fromisoformat(to_date_str)

        session = _req.Session()
        edgar = EdgarConnector(db=db, session=session)
        summary = edgar.fetch_all_filings(
            cik=cik,
            ticker=ticker,
            since_date=from_date,
            filing_regime=company.get('filing_regime', 'domestic'),
            until_date=to_date,
            skip_pivot_gate=True,
        )
        log.info(
            "event=backfill_fetch_complete ticker=%s from=%s to=%s ingested=%d errors=%d",
            ticker, from_date_str, to_date_str,
            summary.reports_ingested, summary.errors,
        )
        _update_backfill_progress(task_id, {
            'status': 'running', 'ticker': ticker,
            'phase': 'fetch_complete',
            'reports_ingested': summary.reports_ingested,
            'errors': summary.errors,
        })

        extract_result: Optional[dict] = None
        if auto_extract:
            _update_backfill_progress(task_id, {
                'status': 'running', 'ticker': ticker,
                'phase': 'extracting',
                'reports_ingested': summary.reports_ingested,
            })
            from routes.reports import _run_auto_extract
            edgar_source_types = [
                'edgar_8k', 'edgar_10q', 'edgar_10k',
                'edgar_6k', 'edgar_20f', 'edgar_40f',
            ]
            extract_result = _run_auto_extract(
                db=db,
                tickers=[ticker],
                source_types=edgar_source_types,
                triggered_by=f'backfill:{ticker}:{from_date_str}:{to_date_str}',
            )
            log.info(
                "event=backfill_extract_complete ticker=%s processed=%d data_points=%d",
                ticker,
                extract_result.get('reports_processed', 0),
                extract_result.get('data_points_extracted', 0),
            )

        final: dict = {
            'status': 'complete', 'ticker': ticker,
            'from_date': from_date_str, 'to_date': to_date_str,
            'reports_ingested': summary.reports_ingested,
            'errors': summary.errors,
        }
        if extract_result:
            final['reports_processed'] = extract_result.get('reports_processed', 0)
            final['data_points_extracted'] = extract_result.get('data_points_extracted', 0)
            final['review_flagged'] = extract_result.get('review_flagged', 0)
        _update_backfill_progress(task_id, final)

    except Exception as exc:
        log.error(
            "event=backfill_error ticker=%s task_id=%s error=%s",
            ticker, task_id, exc, exc_info=True,
        )
        _update_backfill_progress(task_id, {
            'status': 'error', 'ticker': ticker,
            'message': 'Internal server error',
        })
    finally:
        with _backfills_lock:
            _running_backfills.discard(ticker)


@bp.route('/api/scrape/trigger/<ticker>', methods=['POST'])
def trigger_scrape(ticker):
    db = get_db()
    ticker = ticker.upper()
    company = db.get_company(ticker)
    if company is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
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


@bp.route('/api/backfill/<ticker>', methods=['POST'])
def backfill_edgar(ticker):
    """POST /api/backfill/<ticker> — targeted EDGAR fetch for a bounded date window.

    Body (all optional):
      from_date   YYYY-MM-DD  start of fetch window (auto-detected from gap if omitted)
      to_date     YYYY-MM-DD  end of fetch window   (auto-detected from gap if omitted)
      auto_extract bool       run extraction on newly ingested reports (default false)

    Gap auto-detection: queries reports for the earliest EDGAR report date, uses
    btc_first_filing_date (or 2019-01-01) as from_date, and the earliest existing
    EDGAR report date as to_date.

    Returns 202 with task_id. Poll GET /api/backfill/<task_id>/progress for status.
    """
    db = get_db()
    ticker = ticker.upper()
    company = db.get_company(ticker)
    if company is None:
        return jsonify({'success': False, 'error': {'message': f'Company {ticker!r} not found'}}), 404
    cik = company.get('cik')
    if not cik:
        return jsonify({'success': False, 'error': {'message': f'No CIK configured for {ticker}'}}), 400

    body = request.get_json(silent=True) or {}
    from_date_str: Optional[str] = body.get('from_date')
    to_date_str: Optional[str] = body.get('to_date')
    auto_extract = bool(body.get('auto_extract', False))
    detected = False

    if not from_date_str or not to_date_str:
        window = db.detect_edgar_report_window(ticker)
        btc_floor = company.get('btc_first_filing_date') or '2019-01-01'
        min_edgar = window.get('min_date')
        if min_edgar and min_edgar <= btc_floor:
            return jsonify({'success': False, 'error': {
                'message': 'No detectable gap — earliest EDGAR report is at or before the BTC pivot date'
            }}), 400
        from_date_str = from_date_str or btc_floor
        to_date_str = to_date_str or (min_edgar or datetime.now(timezone.utc).strftime('%Y-%m-%d'))
        detected = True

    try:
        date.fromisoformat(from_date_str)
        date.fromisoformat(to_date_str)
    except ValueError as exc:
        return jsonify({'success': False, 'error': {'message': f'Invalid date: {exc}'}}), 400

    with _backfills_lock:
        if ticker in _running_backfills:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING',
                'message': f'Backfill already running for {ticker}',
            }}), 409
        _running_backfills.add(ticker)

    task_id = str(uuid.uuid4())
    _update_backfill_progress(task_id, {
        'status': 'queued', 'ticker': ticker,
        'from_date': from_date_str, 'to_date': to_date_str,
        'auto_extract': auto_extract,
    })

    t = threading.Thread(
        target=_run_backfill,
        args=(task_id, ticker, from_date_str, to_date_str, auto_extract),
        daemon=True,
        name=f'Backfill-{ticker}',
    )
    t.start()

    log.info(
        "event=backfill_start ticker=%s from=%s to=%s auto_extract=%s task_id=%s",
        ticker, from_date_str, to_date_str, auto_extract, task_id,
    )
    return jsonify({'success': True, 'data': {
        'task_id': task_id,
        'from_date': from_date_str,
        'to_date': to_date_str,
        'detected': detected,
    }}), 202


@bp.route('/api/backfill/<task_id>/progress')
def backfill_progress(task_id):
    """GET /api/backfill/<task_id>/progress — poll a running backfill task."""
    with _backfill_lock:
        state = _backfill_progress.get(task_id)
    if state is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': 'Task ID not found',
        }}), 404
    return jsonify({'success': True, 'data': state})
