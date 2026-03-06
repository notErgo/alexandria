"""Crawl routes — trigger and monitor LLM-driven IR crawls.

Endpoints:
  POST /api/crawl/start              { "tickers": [...], "provider": "ollama"|"anthropic", "prompt": "..." }
                                     → 202 { task_id, tickers }
  GET  /api/crawl/status             → per-ticker snapshots for the latest session
  GET  /api/crawl/<task_id>/progress → per-ticker snapshots for a specific task
  GET  /api/crawl/prompt/<ticker>    → { ticker, prompt } — per-ticker prompt file or master template
"""
import logging
import uuid
from pathlib import Path

from flask import Blueprint, jsonify, request

bp = Blueprint('crawl', __name__)
log = logging.getLogger('miners.routes.crawl')

_REPO_ROOT = Path(__file__).parent.parent.parent
_CRAWL_PROMPTS_DIR = _REPO_ROOT / 'scripts' / 'crawl_prompts'
_MASTER_TEMPLATE_PATH = _CRAWL_PROMPTS_DIR / '_master_template.md'


@bp.route('/api/crawl/start', methods=['POST'])
def crawl_start():
    body = request.get_json(silent=True) or {}
    tickers = body.get('tickers')
    if not isinstance(tickers, list) or not tickers:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT',
            'message': "'tickers' must be a non-empty list",
        }}), 400

    provider = body.get('provider') or None
    prompt = body.get('prompt') or None

    import scrapers.llm_crawler as _crawler
    from app_globals import get_db
    task_id = str(uuid.uuid4())
    per_ticker = _crawler.start_crawl(
        tickers,
        task_id=task_id,
        provider=provider,
        prompt=prompt,
        db=get_db(),
    )

    log.info(
        'event=crawl_start_request task_id=%s tickers=%s provider=%s',
        task_id, tickers, provider or 'default',
    )
    return jsonify({'success': True, 'data': {
        'task_id': task_id,
        'tickers': list(per_ticker.keys()),
    }}), 202


@bp.route('/api/crawl/prompt/<ticker>', methods=['GET'])
def crawl_prompt(ticker: str):
    """Return the crawl prompt for a ticker.

    Checks scripts/crawl_prompts/{TICKER}_crawl.md first.
    Falls back to the master template if available.
    Returns 404 with a fallback stub when neither exists.
    """
    ticker = ticker.upper()
    ticker_path = _CRAWL_PROMPTS_DIR / f'{ticker}_crawl.md'
    if ticker_path.exists():
        return jsonify({'success': True, 'data': {
            'ticker': ticker,
            'prompt': ticker_path.read_text(),
            'source': 'ticker_file',
        }})
    if _MASTER_TEMPLATE_PATH.exists():
        return jsonify({'success': True, 'data': {
            'ticker': ticker,
            'prompt': _MASTER_TEMPLATE_PATH.read_text(),
            'source': 'master_template',
        }})
    return jsonify({'success': False, 'error': {
        'code': 'NOT_FOUND',
        'message': f'No crawl prompt file found for {ticker}. '
                   f'Create scripts/crawl_prompts/{ticker}_crawl.md to enable crawling.',
    }}), 404


@bp.route('/api/crawl/status', methods=['GET'])
def crawl_status():
    import scrapers.llm_crawler as _crawler
    snapshots = _crawler.get_crawl_status()
    return jsonify({'success': True, 'data': snapshots})


@bp.route('/api/crawl/<task_id>/progress', methods=['GET'])
def crawl_progress(task_id: str):
    import scrapers.llm_crawler as _crawler
    task = _crawler.get_crawl_task(task_id)
    if task is None:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND',
            'message': f'No crawl task with id {task_id!r}',
        }}), 404
    return jsonify({'success': True, 'data': list(task.values())})
