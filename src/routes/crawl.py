"""Crawl routes — trigger and monitor LLM-driven IR crawls.

Endpoints:
  POST /api/crawl/start          { "tickers": ["MARA", "RIOT"] }
                                 → 202 { task_id, tickers }
  GET  /api/crawl/status         → per-ticker snapshots for the latest session
  GET  /api/crawl/<task_id>/progress → per-ticker snapshots for a specific task
"""
import logging
import uuid

from flask import Blueprint, jsonify, request

bp = Blueprint('crawl', __name__)
log = logging.getLogger('miners.routes.crawl')


@bp.route('/api/crawl/start', methods=['POST'])
def crawl_start():
    body = request.get_json(silent=True) or {}
    tickers = body.get('tickers')
    if not isinstance(tickers, list) or not tickers:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_INPUT',
            'message': "'tickers' must be a non-empty list",
        }}), 400

    import scrapers.llm_crawler as _crawler
    task_id = str(uuid.uuid4())
    per_ticker = _crawler.start_crawl(tickers, task_id=task_id)

    log.info('event=crawl_start_request task_id=%s tickers=%s', task_id, tickers)
    return jsonify({'success': True, 'data': {
        'task_id': task_id,
        'tickers': list(per_ticker.keys()),
    }}), 202


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
