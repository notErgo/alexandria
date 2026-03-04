"""Ingest trigger routes with progress tracking."""
import logging
import threading
import uuid
from typing import Optional

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.reports')

bp = Blueprint('reports', __name__)

# Thread-safe progress tracking
_ingest_progress: dict = {}
_progress_lock = threading.RLock()
_running_tasks: set = set()
_tasks_lock = threading.Lock()


def _update_progress(task_id: str, state: dict) -> None:
    with _progress_lock:
        _ingest_progress[task_id] = state


def _run_archive_ingest(task_id: str) -> None:
    from app_globals import get_db, get_registry
    from scrapers.archive_ingestor import ArchiveIngestor
    from config import ARCHIVE_DIR

    _update_progress(task_id, {'status': 'running', 'source': 'archive'})
    try:
        ingestor = ArchiveIngestor(
            archive_dir=ARCHIVE_DIR, db=get_db(), registry=get_registry()
        )
        summary = ingestor.ingest_all()
        _update_progress(task_id, {
            'status': 'complete',
            'source': 'archive',
            'reports_ingested': summary.reports_ingested,
            'data_points_extracted': summary.data_points_extracted,
            'review_flagged': summary.review_flagged,
            'errors': summary.errors,
        })
    except Exception as e:
        log.error("Archive ingest failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('archive')


def _run_ir_ingest(task_id: str) -> None:
    import requests as req_lib
    from app_globals import get_db
    from scrapers.ir_scraper import IRScraper

    _update_progress(task_id, {'status': 'running', 'source': 'ir'})
    try:
        db = get_db()
        session = req_lib.Session()
        scraper = IRScraper(db=db, session=session)
        companies = db.get_companies(active_only=True)
        totals = {'reports_ingested': 0, 'data_points_extracted': 0,
                  'review_flagged': 0, 'errors': 0}
        for company in companies:
            s = scraper.scrape_company(company)
            for k in totals:
                totals[k] += getattr(s, k)
        _update_progress(task_id, {'status': 'complete', 'source': 'ir', **totals})
    except Exception as e:
        log.error("IR ingest failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('ir')


def _run_edgar_ingest(task_id: str) -> None:
    import requests as req_lib
    from datetime import date
    from app_globals import get_db
    from scrapers.edgar_connector import EdgarConnector

    _update_progress(task_id, {'status': 'running', 'source': 'edgar'})
    try:
        db = get_db()
        session = req_lib.Session()
        connector = EdgarConnector(db=db, session=session)
        companies = db.get_companies(active_only=True)
        since = date(2019, 1, 1)
        totals = {'reports_ingested': 0, 'errors': 0}
        for company in companies:
            if company.get('cik'):
                s = connector.fetch_all_filings(
                    cik=company['cik'], ticker=company['ticker'], since_date=since
                )
                totals['reports_ingested'] += s.reports_ingested
                totals['errors'] += s.errors
        _update_progress(task_id, {'status': 'complete', 'source': 'edgar', **totals})
    except Exception as e:
        log.error("EDGAR ingest failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('edgar')


def _run_edgar_bridge(task_id: str, ticker: Optional[str]) -> None:
    from app_globals import get_db
    from coverage_bridge import bridge_all_gaps

    _update_progress(task_id, {'status': 'running', 'source': 'edgar_bridge'})
    try:
        db = get_db()
        summary = bridge_all_gaps(db, ticker=ticker)
        _update_progress(task_id, {
            'status': 'complete',
            'source': 'edgar_bridge',
            'cells_evaluated': summary.cells_evaluated,
            'cells_filled_carry': summary.cells_filled_carry,
            'cells_filled_inferred': summary.cells_filled_inferred,
            'cells_routed_review': summary.cells_routed_review,
            'cells_skipped_no_quarterly': summary.cells_skipped_no_quarterly,
        })
    except Exception as e:
        log.error("EDGAR bridge failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('edgar_bridge')


@bp.route('/api/ingest/archive', methods=['POST'])
def ingest_archive():
    with _tasks_lock:
        if 'archive' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'Archive ingest already in progress'
            }}), 409
        _running_tasks.add('archive')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'archive'})
    t = threading.Thread(target=_run_archive_ingest, args=(task_id,), daemon=True)
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


@bp.route('/api/ingest/ir', methods=['POST'])
def ingest_ir():
    with _tasks_lock:
        if 'ir' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'IR ingest already in progress'
            }}), 409
        _running_tasks.add('ir')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'ir'})
    t = threading.Thread(target=_run_ir_ingest, args=(task_id,), daemon=True)
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


@bp.route('/api/ingest/edgar', methods=['POST'])
def ingest_edgar():
    with _tasks_lock:
        if 'edgar' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'EDGAR ingest already in progress'
            }}), 409
        _running_tasks.add('edgar')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'edgar'})
    t = threading.Thread(target=_run_edgar_ingest, args=(task_id,), daemon=True)
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


@bp.route('/api/ingest/edgar/refetch_8k', methods=['POST'])
def ingest_edgar_refetch_8k():
    """Re-fetch exhibit text for stale 8-K records that stored the EDGAR index page.

    Optional body: {"ticker": "MARA"} to limit to one ticker.
    """
    body = request.get_json(silent=True) or {}
    ticker = body.get('ticker')
    if ticker is not None and not isinstance(ticker, str):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'ticker' must be a string"
        }}), 400

    with _tasks_lock:
        if 'edgar_refetch_8k' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': '8-K refetch already in progress'
            }}), 409
        _running_tasks.add('edgar_refetch_8k')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'edgar_refetch_8k'})
    t = threading.Thread(
        target=_run_edgar_refetch_8k, args=(task_id, ticker), daemon=True
    )
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


def _run_edgar_refetch_8k(task_id: str, ticker: Optional[str]) -> None:
    import requests as req_lib
    from app_globals import get_db
    from scrapers.edgar_connector import EdgarConnector

    _update_progress(task_id, {'status': 'running', 'source': 'edgar_refetch_8k'})
    try:
        db = get_db()
        session = req_lib.Session()
        connector = EdgarConnector(db=db, session=session)
        summary = connector.refetch_stale_8k_exhibits(ticker=ticker)
        _update_progress(task_id, {
            'status': 'complete',
            'source': 'edgar_refetch_8k',
            'refetched': summary.reports_ingested,
            'errors': summary.errors,
        })
    except Exception as e:
        log.error("8-K refetch failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('edgar_refetch_8k')


@bp.route('/api/ingest/edgar/bridge', methods=['POST'])
def ingest_edgar_bridge():
    """Trigger coverage bridge pass to fill monthly gaps from quarterly/annual filings."""
    body = request.get_json(silent=True) or {}
    ticker = body.get('ticker')
    if ticker is not None and not isinstance(ticker, str):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'ticker' must be a string"
        }}), 400

    with _tasks_lock:
        if 'edgar_bridge' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'EDGAR bridge already in progress'
            }}), 409
        _running_tasks.add('edgar_bridge')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'edgar_bridge'})
    t = threading.Thread(
        target=_run_edgar_bridge, args=(task_id, ticker), daemon=True
    )
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


def _run_html_download(task_id: str, tickers: list, since_year: Optional[int]) -> None:
    import json
    from pathlib import Path
    from app_globals import get_db
    from scrapers.html_downloader import HTMLDownloader
    from config import ARCHIVE_DIR, CONFIG_DIR

    _update_progress(task_id, {'status': 'running', 'source': 'html_download'})
    try:
        companies_path = Path(CONFIG_DIR) / 'companies.json'
        with open(companies_path) as f:
            companies = json.load(f)

        downloader = HTMLDownloader(archive_dir=ARCHIVE_DIR)
        summary = downloader.download_all(
            companies=companies,
            since_year=since_year,
            tickers=tickers if tickers else None,
        )
        _update_progress(task_id, {
            'status': 'complete',
            'source': 'html_download',
            'downloaded': summary.downloaded,
            'skipped_existing': summary.skipped_existing,
            'skipped_not_found': summary.skipped_not_found,
            'errors': summary.errors,
            'companies': summary.companies_processed,
        })
    except Exception as e:
        log.error("HTML download failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('html_download')


@bp.route('/api/ingest/html-download', methods=['POST'])
def ingest_html_download():
    """
    Trigger HTML press release download for one or all companies.

    Optional JSON body:
      { "tickers": ["RIOT", "CLSK"],   // omit for all companies
        "since_year": 2023 }           // omit to use each company's pr_start_year
    """
    body = request.get_json(silent=True) or {}

    tickers = body.get('tickers', [])
    if not isinstance(tickers, list):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT', 'message': "'tickers' must be a list of ticker strings"
        }}), 400

    since_year = body.get('since_year')
    if since_year is not None:
        if not isinstance(since_year, int) or since_year < 2015 or since_year > 2030:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT', 'message': "'since_year' must be an integer between 2015 and 2030"
            }}), 400

    with _tasks_lock:
        if 'html_download' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'HTML download already in progress'
            }}), 409
        _running_tasks.add('html_download')

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {'status': 'queued', 'source': 'html_download'})
    t = threading.Thread(
        target=_run_html_download,
        args=(task_id, tickers, since_year),
        daemon=True,
    )
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id}}), 202


def _prewarm_llm() -> bool:
    """
    Send a minimal request to Ollama to force the model into VRAM before the
    first real extraction call. Returns True if the model responded, False if
    Ollama is unreachable or times out.

    Without pre-warming, the first /api/generate call in a re-audit bears the
    full model-load latency (~60-120s for qwen3.5:35b on Apple Silicon), which
    can push it over LLM_TIMEOUT_SECONDS and cause the first report to fall
    back to regex-only silently.
    """
    import requests as _req
    from config import LLM_BASE_URL, LLM_MODEL_ID, LLM_TIMEOUT_SECONDS
    try:
        resp = _req.post(
            f"{LLM_BASE_URL}/api/generate",
            json={"model": LLM_MODEL_ID, "prompt": "hi", "stream": False, "keep_alive": "2h"},
            timeout=LLM_TIMEOUT_SECONDS,
        )
        return resp.status_code == 200
    except Exception as e:
        log.warning("LLM pre-warm failed: %s", e)
        return False


def _run_reaudit(task_id: str, ticker: Optional[str] = None) -> None:
    """Re-run ingest_all(force=True) on the archive to re-audit all data points."""
    from app_globals import get_db, get_registry
    from scrapers.archive_ingestor import ArchiveIngestor
    from config import ARCHIVE_DIR

    scope = f'ticker={ticker}' if ticker else 'all'
    _update_progress(task_id, {'status': 'running', 'source': 'reaudit', 'scope': scope,
                               'phase': 'warming up LLM…'})
    warmed = _prewarm_llm()
    if warmed:
        log.info("LLM pre-warm complete — model loaded into VRAM")
    else:
        log.warning("LLM pre-warm failed — first extraction call will bear cold-start latency")
    _update_progress(task_id, {'status': 'running', 'source': 'reaudit', 'scope': scope,
                               'phase': 'extracting…', 'reports_processed': 0, 'reports_total': None})

    def _progress_cb(processed: int, total: int) -> None:
        _update_progress(task_id, {
            'status': 'running', 'source': 'reaudit', 'scope': scope,
            'phase': 'extracting…',
            'reports_processed': processed,
            'reports_total': total,
        })

    try:
        ingestor = ArchiveIngestor(
            archive_dir=ARCHIVE_DIR, db=get_db(), registry=get_registry()
        )
        summary = ingestor.ingest_all(force=True, progress_callback=_progress_cb)
        _update_progress(task_id, {
            'status': 'complete',
            'source': 'reaudit',
            'scope': scope,
            'reports_ingested': summary.reports_ingested,
            'data_points_extracted': summary.data_points_extracted,
            'review_flagged': summary.review_flagged,
            'errors': summary.errors,
        })
    except Exception as e:
        log.error("Re-audit failed: %s", e, exc_info=True)
        _update_progress(task_id, {'status': 'error', 'message': 'Internal server error'})
    finally:
        with _tasks_lock:
            _running_tasks.discard('reaudit')


@bp.route('/api/ingest/reaudit', methods=['POST'])
def ingest_reaudit():
    """Trigger a full re-audit of all archive data points (force=True ingest).

    Optional body: {"ticker": "MARA"} — scope to a single company.
    Note: ticker scoping is stored in progress state; the underlying
    ingest_all currently processes all companies regardless.
    Returns 409 if a re-audit is already running.
    """
    with _tasks_lock:
        if 'reaudit' in _running_tasks:
            return jsonify({'success': False, 'error': {
                'code': 'ALREADY_RUNNING', 'message': 'Re-audit already in progress'
            }}), 409
        _running_tasks.add('reaudit')

    body = request.get_json(silent=True) or {}
    ticker = (body.get('ticker') or '').strip().upper() or None

    task_id = str(uuid.uuid4())
    _update_progress(task_id, {
        'status': 'queued',
        'source': 'reaudit',
        'scope': f'ticker={ticker}' if ticker else 'all',
    })
    t = threading.Thread(
        target=_run_reaudit, args=(task_id, ticker), daemon=True, name='reaudit'
    )
    t.start()
    return jsonify({'success': True, 'data': {'task_id': task_id, 'status': 'queued'}}), 202


@bp.route('/api/ingest/<task_id>/progress')
def ingest_progress(task_id):
    with _progress_lock:
        state = _ingest_progress.get(task_id)
    if state is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND', 'message': 'Task ID not found'
        }}), 404
    return jsonify({'success': True, 'data': state})
