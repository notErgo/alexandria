"""
Pattern suggestion routes.

  GET  /api/suggestions/<ticker>        — generate pattern suggestions for ticker
  POST /api/suggestions/<ticker>/apply  — analyst-approved append to ticker hint or metric prompt
"""
import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

bp = Blueprint('suggestions', __name__)
log = logging.getLogger('miners.routes.suggestions')


def get_db():
    from app_globals import get_db as _get_db
    return _get_db()

_VALID_METRICS_FALLBACK = frozenset({
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc', 'mining_mw', 'ai_hpc_mw',
    'hpc_revenue_usd', 'gpu_count',
})


def _get_valid_metrics(db) -> frozenset:
    """Return set of valid metric keys from metric_schema table."""
    try:
        rows = db.get_metric_schema(sector='BTC-miners', active_only=False)
        if rows:
            return frozenset(r['key'] for r in rows)
    except Exception:
        pass
    return _VALID_METRICS_FALLBACK


@bp.route('/api/suggestions/<ticker>')
def get_suggestions(ticker: str):
    """Generate pattern suggestions for a ticker from recent extraction results."""
    try:
        db = get_db()

        company = db.get_company(ticker.upper())
        if company is None:
            return jsonify({
                'success': False,
                'error': {'code': 'TICKER_NOT_FOUND', 'message': f'Ticker not found: {ticker}'},
            }), 404

        run_id_raw = request.args.get('run_id')
        run_id = int(run_id_raw) if run_id_raw and run_id_raw.isdigit() else None

        from interpreters.pattern_suggester import generate_suggestions
        result = generate_suggestions(db, ticker.upper(), run_id=run_id)

        return jsonify({'success': True, 'data': result})

    except Exception:
        log.error('Error generating suggestions for %s', ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/suggestions/<ticker>/apply', methods=['POST'])
def apply_suggestion(ticker: str):
    """Append analyst-reviewed text to a ticker hint or metric prompt."""
    try:
        db = get_db()

        company = db.get_company(ticker.upper())
        if company is None:
            return jsonify({
                'success': False,
                'error': {'code': 'TICKER_NOT_FOUND', 'message': f'Ticker not found: {ticker}'},
            }), 404

        body = request.get_json(silent=True) or {}
        target = body.get('target', '')
        append_text = body.get('append_text', '').strip() if body.get('append_text') else ''

        if not append_text:
            return jsonify({
                'success': False,
                'error': {'code': 'MISSING_APPEND_TEXT', 'message': 'append_text is required and must be non-empty'},
            }), 400

        if len(append_text) > 2000:
            return jsonify({
                'success': False,
                'error': {'code': 'TEXT_TOO_LONG', 'message': 'append_text must be <= 2000 chars'},
            }), 400

        if target not in ('ticker_hint', 'metric_prompt'):
            return jsonify({
                'success': False,
                'error': {'code': 'INVALID_TARGET', 'message': "target must be 'ticker_hint' or 'metric_prompt'"},
            }), 400

        applied_to = target
        metric = None

        if target == 'ticker_hint':
            current = db.get_ticker_hint(ticker.upper()) or ''
            new_text = (current + '\n\n' + append_text).strip() if current else append_text
            db.upsert_ticker_hint(ticker.upper(), new_text)
            log.info(
                'event=suggestion_apply_ticker_hint ticker=%s chars_appended=%d',
                ticker.upper(), len(append_text),
            )

        else:  # metric_prompt
            metric = body.get('metric', '').strip()
            if not metric:
                return jsonify({
                    'success': False,
                    'error': {'code': 'MISSING_METRIC', 'message': 'metric is required for target=metric_prompt'},
                }), 400

            valid_metrics = _get_valid_metrics(db)
            if metric not in valid_metrics:
                return jsonify({
                    'success': False,
                    'error': {'code': 'INVALID_METRIC', 'message': f'Unknown metric: {metric}'},
                }), 400

            existing = db.get_llm_prompt(metric)
            current = (existing.get('prompt_text') or '') if existing else ''
            new_text = (current + '\n\n' + append_text).strip() if current else append_text
            db.upsert_llm_prompt(metric, new_text)
            log.info(
                'event=suggestion_apply_metric_prompt ticker=%s metric=%s chars_appended=%d',
                ticker.upper(), metric, len(append_text),
            )

        return jsonify({
            'success': True,
            'data': {
                'applied_to': applied_to,
                'ticker': ticker.upper(),
                'metric': metric,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            },
        })

    except Exception:
        log.error('Error applying suggestion for %s', ticker, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
