"""Interpretation layer API — analyst finalization of extracted values.

Endpoints:
  POST /api/interpret/<ticker>/generate_prompt — generate a custom extraction prompt via Ollama
  POST /api/interpret/<ticker>/finalize        — write to final_data_points
  GET  /api/interpret/<ticker>/final           — read final_data_points for ticker
  DELETE /api/interpret/<ticker>/final         — clear final_data_points for ticker
  POST /api/delete/final                       — bulk purge (all tickers or scoped)

LLM extraction is handled by the single extraction endpoint:
  POST /api/operations/interpret  (source_scope='ir'|'sec'|'both', custom_prompt=...)
"""
import json
import logging
import math
import re

import requests
from flask import Blueprint, jsonify, request

from app_globals import get_db
from config import LLM_BASE_URL, LLM_TIMEOUT_SECONDS

log = logging.getLogger('miners.routes.interpret')

bp = Blueprint('interpret', __name__)

# KEEP IN SYNC with data_points.py, llm_prompts.py, dashboard.py
# This fallback is used when metric_schema DB table is unavailable.
# A truncated fallback silently rejects valid metrics — keep it complete.
_VALID_METRICS_FALLBACK = frozenset({
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
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
_PERIOD_MONTHLY_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_PERIOD_QUARTER_RE = re.compile(r'^\d{4}-Q\d$')
_PERIOD_ANNUAL_RE  = re.compile(r'^\d{4}-FY$')


def _active_model(db) -> str:
    try:
        val = db.get_config('ollama_model')
        if val:
            return val
    except Exception:
        pass
    from config import LLM_MODEL_ID
    return LLM_MODEL_ID


def _valid_period(period: str) -> bool:
    return bool(
        _PERIOD_MONTHLY_RE.match(period) or
        _PERIOD_QUARTER_RE.match(period) or
        _PERIOD_ANNUAL_RE.match(period)
    )


def _format_period_label(period: str) -> str:
    """Convert raw period string to a human-readable label."""
    q = _PERIOD_QUARTER_RE.match(period)
    if q:
        year, qn = period.split('-')
        return f"{qn} {year}"
    fy = _PERIOD_ANNUAL_RE.match(period)
    if fy:
        return f"FY {period[:4]}"
    # YYYY-MM-01 → YYYY-MM
    return period[:7]
@bp.route('/api/interpret/<ticker>/generate_prompt', methods=['POST'])
def generate_prompt(ticker: str):
    """Generate a custom extraction prompt via meta-prompt to Ollama.

    Input:  { "goal": "I want to extract monthly production for 2023" }
    Output: { "success": true, "data": { "generated_prompt": "..." } }
    """
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    goal = str(body.get('goal') or '').strip()

    valid_metrics = sorted(_get_valid_metrics(db))
    model = _active_model(db)

    meta_prompt = (
        f"You are a prompt engineering assistant for a Bitcoin mining data extraction system.\n"
        f"Available metrics: {', '.join(valid_metrics)}\n"
        f"Company ticker: {ticker_upper}\n\n"
        f"User goal: {goal or 'Extract all available Bitcoin mining operational data.'}\n\n"
        "Write an extraction prompt that instructs an LLM to extract the relevant data from a document.\n"
        "The prompt must instruct the LLM to return a JSON array only (no prose), in this format:\n"
        '[{"metric":"<key>","period":"<YYYY-MM-01 or YYYY-Qn or YYYY-FY>","value":<number>,'
        '"confidence":<0-1>,"rationale":"<brief>"}]\n'
        "Return only the extraction prompt text. Do not explain or wrap it in JSON."
    )

    try:
        from interpreters.llm_interpreter import LLMInterpreter
        _llm_check = LLMInterpreter(session=requests.Session(), db=db)
        if not _llm_check.check_connectivity():
            log.warning("event=generate_prompt_ollama_unavailable ticker=%s", ticker_upper)
            return jsonify({'success': False, 'error': {
                'code': 'LLM_UNAVAILABLE',
                'message': 'Ollama is not reachable or the model is not loaded',
            }}), 503
    except Exception:
        log.error("generate_prompt connectivity check failed ticker=%s", ticker_upper, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'LLM_UNAVAILABLE', 'message': 'LLM connectivity check failed',
        }}), 503

    try:
        resp = requests.post(
            f"{LLM_BASE_URL}/api/generate",
            json={'model': model, 'prompt': meta_prompt, 'stream': False},
            timeout=LLM_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        generated = resp.json().get('response', '').strip()
    except Exception:
        log.error("generate_prompt LLM call failed ticker=%s", ticker_upper, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'LLM_ERROR', 'message': 'LLM request failed',
        }}), 502

    log.info("event=generate_prompt_complete ticker=%s goal=%r", ticker_upper, goal[:80] if goal else '')
    return jsonify({'success': True, 'data': {'generated_prompt': generated}})


@bp.route('/api/interpret/<ticker>/finalize', methods=['POST'])
def finalize(ticker: str):
    """Write analyst-accepted values into final_data_points."""
    db = get_db()
    ticker_upper = ticker.upper()
    valid_metrics = _get_valid_metrics(db)
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    values = body.get('values')
    if not isinstance(values, list) or not values:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_VALUES', 'message': 'body must include non-empty "values" list',
        }}), 400

    errors = []
    for i, entry in enumerate(values):
        metric = entry.get('metric')
        period = entry.get('period')
        value = entry.get('value')
        if metric not in valid_metrics:
            errors.append(f"values[{i}]: unknown metric {metric!r}")
            continue
        if not period or not _valid_period(str(period)):
            errors.append(f"values[{i}]: invalid period {period!r}")
            continue
        try:
            v = float(value)
            if not math.isfinite(v) or v < 0:
                raise ValueError
        except (TypeError, ValueError):
            errors.append(f"values[{i}]: value must be a finite positive number")
            continue

    if errors:
        return jsonify({'success': False, 'error': {
            'code': 'VALIDATION_ERROR', 'message': '; '.join(errors),
        }}), 400

    count = 0
    failed = 0
    dismissed = 0
    for entry in values:
        period_str = str(entry['period'])
        metric_str = str(entry['metric'])
        try:
            db.upsert_final_data_point(
                ticker=ticker_upper,
                period=period_str,
                metric=metric_str,
                value=float(entry['value']),
                unit=str(entry.get('unit') or ''),
                confidence=float(entry.get('confidence') or 1.0),
                analyst_note=entry.get('analyst_note'),
                source_ref=entry.get('source_ref'),
            )
            count += 1
            dismissed += db.dismiss_review_items_for_cell(ticker_upper, period_str, metric_str)
        except Exception:
            log.error("upsert_final_data_point failed ticker=%s entry=%r", ticker_upper, entry, exc_info=True)
            failed += 1

    log.info("event=finalize_complete ticker=%s count=%d failed=%d dismissed=%d", ticker_upper, count, failed, dismissed)
    if failed:
        return jsonify({'success': False, 'error': {
            'code': 'DB_WRITE_FAILED',
            'message': f'{failed} of {count + failed} values could not be saved (database may be busy)',
        }}), 500
    return jsonify({'success': True, 'data': {'count': count}})


@bp.route('/api/interpret/<ticker>/final', methods=['GET'])
def get_final(ticker: str):
    """Return all final_data_points rows for a ticker."""
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404
    rows = db.get_final_data_points(ticker_upper)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/interpret/<ticker>/final', methods=['DELETE'])
def delete_final(ticker: str):
    """Purge all final_data_points rows for one ticker."""
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    result = db.purge_final_data_points(ticker=ticker_upper, mode='clear')
    log.info("event=delete_final ticker=%s deleted=%d", ticker_upper, result['deleted'])
    return jsonify({'success': True, 'data': result})


@bp.route('/api/delete/final', methods=['POST'])
@bp.route('/api/interpret/final/purge', methods=['POST'])
def purge_final():
    """Bulk purge of final_data_points (all or ticker-scoped).

    Body:
        confirm (bool, required): must be true
        mode (str): 'clear' (default) or 'archive'
        ticker (str, optional): limit to one ticker
    """
    db = get_db()
    body = request.get_json(silent=True) or {}

    if not body.get('confirm'):
        return jsonify({'success': False, 'error': {
            'code': 'CONFIRM_REQUIRED', 'message': 'Request body must include {"confirm": true}',
        }}), 400

    mode = str(body.get('mode') or 'clear').strip().lower()
    if mode not in {'clear', 'archive'}:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_MODE', 'message': "mode must be 'clear' or 'archive'",
        }}), 400

    ticker = body.get('ticker')
    if ticker:
        ticker = str(ticker).strip().upper() or None
    if ticker and not db.get_company(ticker):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    reason = str(body.get('reason') or '').strip() or None

    try:
        result = db.purge_final_data_points(ticker=ticker, mode=mode, reason=reason)
    except Exception:
        log.error("purge_final_data_points failed", exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'PURGE_ERROR', 'message': 'Internal error during purge',
        }}), 500

    log.info(
        "event=purge_final_complete mode=%s ticker=%s deleted=%d archive_batch_id=%s",
        mode, ticker or 'ALL', result['deleted'], result.get('archive_batch_id'),
    )
    return jsonify({'success': True, 'data': result})


@bp.route('/api/interpret/<ticker>/reviewed', methods=['POST'])
def mark_reviewed(ticker: str):
    """Mark one or more periods as reviewed for a ticker.

    Body: {"periods": ["2024-01-01", "2024-02-01", ...]}
    Returns 201 with {"success": true, "data": {"count": N}}.
    """
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    periods = body.get('periods')
    if not isinstance(periods, list) or not periods:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_PERIODS', 'message': 'body must include non-empty "periods" list',
        }}), 400

    count = db.set_reviewed_periods(ticker_upper, [str(p) for p in periods])
    log.info("event=mark_reviewed ticker=%s periods=%d inserted=%d", ticker_upper, len(periods), count)
    return jsonify({'success': True, 'data': {'count': count}}), 201


@bp.route('/api/interpret/<ticker>/reviewed', methods=['DELETE'])
def unmark_reviewed(ticker: str):
    """Unmark one period as reviewed.

    Body: {"period": "2024-01-01"}
    Returns 200 with {"success": true, "data": {"deleted": N}}.
    """
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    period = body.get('period')
    if not period:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_PERIOD', 'message': 'body must include "period" field',
        }}), 400

    deleted = db.unset_reviewed_period(ticker_upper, str(period))
    log.info("event=unmark_reviewed ticker=%s period=%s deleted=%d", ticker_upper, period, deleted)
    return jsonify({'success': True, 'data': {'deleted': deleted}})


@bp.route('/api/interpret/<ticker>/reviewed/all', methods=['DELETE'])
def clear_all_reviewed(ticker: str):
    """Clear all reviewed_periods for a ticker.

    Returns 200 with {"success": true, "data": {"deleted": N}}.
    """
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    deleted = db.unset_all_reviewed(ticker_upper)
    log.info("event=clear_all_reviewed ticker=%s deleted=%d", ticker_upper, deleted)
    return jsonify({'success': True, 'data': {'deleted': deleted}})


