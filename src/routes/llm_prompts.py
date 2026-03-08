"""
LLM prompt DB store API routes.

  GET  /api/llm_prompts           — list all active prompts
  GET  /api/llm_prompts/preview   — render the full assembled batch prompt (no document body)
  GET  /api/llm_prompts/<metric>  — get active prompt for metric (404 if not set)
  POST /api/llm_prompts/<metric>  — upsert prompt for metric
"""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.llm_prompts')

bp = Blueprint('llm_prompts', __name__)

# All valid metric names — must match the constants used elsewhere in the app
_VALID_METRICS = {
    'production_btc', 'hodl_btc', 'hodl_btc_unrestricted', 'hodl_btc_restricted',
    'sold_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc', 'mining_mw', 'ai_hpc_mw',
    'hpc_revenue_usd', 'gpu_count',
}


@bp.route('/api/llm_prompts')
def list_llm_prompts():
    try:
        from app_globals import get_db
        db = get_db()
        prompts = db.list_llm_prompts()
        return jsonify({'success': True, 'data': {'prompts': prompts}})
    except Exception:
        log.error('Error listing LLM prompts', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/llm_prompts/preview')
def preview_llm_prompt():
    """Render the full assembled batch prompt exactly as the LLM receives it.

    Uses a stub document body so the user can see the complete prompt structure —
    preamble, company context hint, search term injection, per-metric instruction
    blocks, and output format — without running a live extraction.

    Query params:
        ticker (optional): inject the per-ticker context hint if one is set.
        metrics (optional): comma-separated metric keys to include; defaults to
                            the three active core metrics.
    """
    try:
        from app_globals import get_db
        from interpreters.llm_interpreter import LLMInterpreter

        db = get_db()
        ticker = request.args.get('ticker') or None
        metrics_param = request.args.get('metrics')
        if metrics_param:
            metrics = [m.strip() for m in metrics_param.split(',') if m.strip()]
        else:
            try:
                rows = db.get_metric_schema('BTC-miners', active_only=True)
                metrics = [r['key'] for r in rows] if rows else ['production_btc', 'hodl_btc', 'sold_btc']
            except Exception:
                log.warning('Could not load active metrics for preview', exc_info=True)
                metrics = ['production_btc', 'hodl_btc', 'sold_btc']

        import requests as req_lib
        interpreter = LLMInterpreter(session=req_lib.Session(), db=db)
        stub_doc = '[document text will appear here during extraction]'
        prompt = interpreter._build_batch_prompt(stub_doc, metrics, ticker=ticker)

        return jsonify({'success': True, 'data': {
            'prompt': prompt,
            'ticker': ticker,
            'metrics': metrics,
        }})
    except Exception:
        log.error('Error rendering prompt preview', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/llm_prompts/<metric>')
def get_llm_prompt(metric):
    try:
        from app_globals import get_db
        from interpreters.llm_interpreter import LLMInterpreter
        db = get_db()
        prompt = db.get_llm_prompt(metric)
        default_prompt = LLMInterpreter.get_default_prompt(metric)
        return jsonify({'success': True, 'data': {
            'prompt': prompt,
            'default_prompt': default_prompt,
        }})
    except Exception:
        log.error('Error fetching LLM prompt for %s', metric, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/llm_prompts/<metric>', methods=['POST'])
def update_llm_prompt(metric):
    try:
        from app_globals import get_db
        db = get_db()

        if metric not in _VALID_METRICS:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_METRIC',
                'message': f"Unknown metric '{metric}'. Valid metrics: {sorted(_VALID_METRICS)}"
            }}), 400

        body = request.get_json(silent=True) or {}
        prompt_text = body.get('prompt_text', '').strip()
        if not prompt_text:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'prompt_text' is required and must be non-empty"
            }}), 400
        if len(prompt_text) > 10000:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'prompt_text' must be ≤10000 characters"
            }}), 400

        model = body.get('model')
        db.upsert_llm_prompt(metric, prompt_text, model)
        return jsonify({'success': True})
    except Exception:
        log.error('Error updating LLM prompt for %s', metric, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
