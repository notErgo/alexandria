"""
LLM prompt DB store API routes.

  GET    /api/llm_prompts           — list all active prompts
  GET    /api/llm_prompts/preview   — render the full assembled batch prompt (no document body)
  GET    /api/llm_prompts/<metric>  — get active prompt for metric (404 if not set)
  POST   /api/llm_prompts/<metric>  — upsert prompt for metric
  DELETE /api/llm_prompts/<metric>  — deactivate DB override (revert to hardcoded default)
"""
import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger('miners.routes.llm_prompts')

bp = Blueprint('llm_prompts', __name__)

# SYNC: keep identical to sibling _VALID_METRICS_FALLBACK in interpret.py / data_points.py / dashboard.py
_VALID_METRICS_FALLBACK = frozenset({
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc', 'mining_mw', 'ai_hpc_mw',
    'hpc_revenue_usd', 'gpu_count',
})


def _get_valid_metrics(db) -> frozenset:
    """Return set of valid metric keys from DB SSOT (metric_schema table)."""
    try:
        rows = db.get_metric_schema(sector='BTC-miners', active_only=False)
        if rows:
            return frozenset(r['key'] for r in rows)
    except Exception:
        pass
    return _VALID_METRICS_FALLBACK


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
                metrics = [r['key'] for r in rows] if rows else ['production_btc', 'holdings_btc', 'sales_btc']
            except Exception:
                log.warning('Could not load active metrics for preview', exc_info=True)
                metrics = ['production_btc', 'holdings_btc', 'sales_btc']

        period_type = request.args.get('period_type', 'monthly')
        if period_type not in ('monthly', 'quarterly', 'annual'):
            period_type = 'monthly'

        import requests as req_lib
        interpreter = LLMInterpreter(session=req_lib.Session(), db=db)
        stub_doc = '[document text will appear here during extraction]'
        if period_type == 'monthly':
            prompt = interpreter._build_batch_prompt(stub_doc, metrics, ticker=ticker)
        else:
            prompt = interpreter._build_quarterly_batch_prompt(
                stub_doc, metrics, ticker=ticker, period_type=period_type
            )

        return jsonify({'success': True, 'data': {
            'prompt': prompt,
            'ticker': ticker,
            'metrics': metrics,
            'period_type': period_type,
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

        if metric not in _get_valid_metrics(db):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_METRIC',
                'message': f"Unknown metric '{metric}'",
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


@bp.route('/api/llm_prompts/<metric>', methods=['DELETE'])
def reset_llm_prompt(metric):
    """Deactivate the DB override for a metric, reverting to the hardcoded default.

    Sets active=0 on the llm_prompts row so _get_prompt() falls back to
    _DEFAULT_PROMPTS in llm_interpreter.py on the next extraction run.
    """
    try:
        from app_globals import get_db
        db = get_db()

        if metric not in _get_valid_metrics(db):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_METRIC',
                'message': f"Unknown metric '{metric}'",
            }}), 400

        with db._get_connection() as conn:
            conn.execute(
                "UPDATE llm_prompts SET active = 0 WHERE metric = ?",
                (metric,),
            )
        return jsonify({'success': True, 'message': f"DB override for '{metric}' deactivated; hardcoded default will be used."})
    except Exception:
        log.error('Error resetting LLM prompt for %s', metric, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
