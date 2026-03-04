"""
LLM benchmark API routes.

  GET  /api/benchmark/runs?ticker=&model=&limit=  — recent benchmark rows
  GET  /api/benchmark/summary                     — per-model aggregate stats
"""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.benchmark')

bp = Blueprint('benchmark', __name__)


@bp.route('/api/benchmark/runs')
def benchmark_runs():
    """Return recent llm_benchmark_runs rows, optionally filtered."""
    try:
        from app_globals import get_db
        db = get_db()

        ticker = request.args.get('ticker') or None
        model = request.args.get('model') or None
        try:
            limit = int(request.args.get('limit', 100))
            if not (1 <= limit <= 1000):
                return jsonify({'success': False, 'error': {'message': 'limit must be 1–1000'}}), 400
        except ValueError:
            return jsonify({'success': False, 'error': {'message': 'limit must be an integer'}}), 400

        rows = db.get_benchmark_runs(model=model, ticker=ticker, limit=limit)
        return jsonify({'success': True, 'data': rows})
    except Exception:
        log.error("GET /api/benchmark/runs failed", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/benchmark/summary')
def benchmark_summary():
    """Return per-model aggregate stats from llm_benchmark_runs."""
    try:
        from app_globals import get_db
        db = get_db()
        rows = db.get_benchmark_summary()
        return jsonify({'success': True, 'data': rows})
    except Exception:
        log.error("GET /api/benchmark/summary failed", exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
