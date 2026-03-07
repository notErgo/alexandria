"""
LLM prompt DB store API routes.

  GET  /api/llm_prompts           — list all active prompts
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
