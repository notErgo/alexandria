"""Interpretation layer API — analyst finalization of extracted values.

Endpoints:
  POST /api/interpret/<ticker>/reprompt   — LLM reconciliation suggestions
  POST /api/interpret/<ticker>/finalize   — write to final_data_points
  GET  /api/interpret/<ticker>/final      — read final_data_points for ticker
  DELETE /api/interpret/<ticker>/final    — clear final_data_points for ticker
  POST /api/interpret/final/purge         — bulk purge (all tickers or scoped)
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

_VALID_METRICS = {
    'production_btc', 'hodl_btc', 'hodl_btc_unrestricted', 'hodl_btc_restricted',
    'sold_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
}
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


def _dp_row(dp: dict) -> str:
    period = _format_period_label(dp.get('period', ''))
    return (
        f"{period} | {dp.get('metric','')} | {dp.get('value','')} | "
        f"{dp.get('unit','')} | {dp.get('confidence','')} | {dp.get('created_at','')[:10]}"
    )


def _build_reprompt(ticker: str, monthly: list, sec: list, finals: list, commentary: str, metrics: list) -> str:
    def _section(rows, label):
        if not rows:
            return f"{label}:\n(none)\n"
        header = "period | metric | value | unit | confidence | source_date"
        lines = [header] + [_dp_row(r) for r in rows if not metrics or r.get('metric') in metrics]
        return f"{label}:\n" + "\n".join(lines) + "\n"

    final_lines = ["period | metric | value | analyst_note"]
    for f in finals:
        if metrics and f.get('metric') not in metrics:
            continue
        final_lines.append(
            f"{_format_period_label(f.get('period',''))} | {f.get('metric','')} | "
            f"{f.get('value','')} | {f.get('analyst_note','') or ''}"
        )
    final_section = "CURRENT FINALIZED VALUES (analyst layer):\n" + "\n".join(final_lines) + "\n"

    return (
        f"You are a financial data analyst reconciling two independent extraction datasets for {ticker}.\n\n"
        + _section(monthly, "MONTHLY PRESS RELEASE DATA")
        + "\n"
        + _section(sec, "SEC FILING DATA (quarterly/annual)")
        + "\n"
        + final_section
        + f"\nANALYST NOTES: {commentary or '(none)'}\n\n"
        "For each (period, metric) pair: identify the most reliable value, note any discrepancy.\n"
        "Return JSON array only — no prose before or after:\n"
        '[{"metric":"...","period":"...","value":<number>,"confidence":<0-1>,"rationale":"..."}]\n'
        "Use period format YYYY-MM-01 for monthly, YYYY-Qn for quarterly, YYYY-FY for annual.\n"
        "Omit pairs where data is absent or you have no recommendation."
    )


@bp.route('/api/interpret/<ticker>/reprompt', methods=['POST'])
def reprompt(ticker: str):
    """Ask the LLM to reconcile monthly and SEC data into suggestions."""
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    commentary = str(body.get('commentary') or '').strip()
    metrics_filter = body.get('metrics') or []
    if metrics_filter:
        invalid = [m for m in metrics_filter if m not in _VALID_METRICS]
        if invalid:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_METRIC', 'message': f'Unknown metrics: {invalid}',
            }}), 400

    monthly = db.query_data_points(ticker=ticker_upper, source_period_types=['monthly'], limit=2000)
    sec = db.query_data_points(ticker=ticker_upper, source_period_types=['quarterly', 'annual'], limit=500)
    finals = db.get_final_data_points(ticker_upper)

    prompt = _build_reprompt(ticker_upper, monthly, sec, finals, commentary, metrics_filter)
    model = _active_model(db)

    try:
        resp = requests.post(
            f"{LLM_BASE_URL}/api/generate",
            json={'model': model, 'prompt': prompt, 'stream': False},
            timeout=LLM_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        raw = resp.json().get('response', '')
    except Exception:
        log.error("reprompt LLM call failed ticker=%s", ticker_upper, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'LLM_ERROR', 'message': 'LLM request failed',
        }}), 502

    # Extract JSON array from response
    suggestions = []
    try:
        # Strip any markdown fences
        text = raw.strip()
        if '```' in text:
            text = re.sub(r'```[a-z]*\n?', '', text).strip()
        # Find the first [ ... ] block
        start = text.find('[')
        end = text.rfind(']')
        if start != -1 and end != -1:
            suggestions = json.loads(text[start:end + 1])
        if not isinstance(suggestions, list):
            suggestions = []
    except Exception:
        log.warning("reprompt JSON parse failed ticker=%s raw=%r", ticker_upper, raw[:200])
        suggestions = []

    # Validate and sanitize each suggestion
    clean = []
    for s in suggestions:
        if not isinstance(s, dict):
            continue
        m = s.get('metric')
        p = s.get('period')
        v = s.get('value')
        if m not in _VALID_METRICS:
            continue
        if not _valid_period(str(p)):
            continue
        try:
            v = float(v)
            if not math.isfinite(v) or v < 0:
                continue
        except (TypeError, ValueError):
            continue
        clean.append({
            'metric': m,
            'period': str(p),
            'value': v,
            'confidence': float(s.get('confidence') or 0.8),
            'rationale': str(s.get('rationale') or ''),
        })

    return jsonify({'success': True, 'data': {'suggestions': clean}})


@bp.route('/api/interpret/<ticker>/finalize', methods=['POST'])
def finalize(ticker: str):
    """Write analyst-accepted values into final_data_points."""
    db = get_db()
    ticker_upper = ticker.upper()
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
        if metric not in _VALID_METRICS:
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
    for entry in values:
        try:
            db.upsert_final_data_point(
                ticker=ticker_upper,
                period=str(entry['period']),
                metric=str(entry['metric']),
                value=float(entry['value']),
                unit=str(entry.get('unit') or ''),
                confidence=float(entry.get('confidence') or 1.0),
                analyst_note=entry.get('analyst_note'),
                source_ref=entry.get('source_ref'),
            )
            count += 1
        except Exception:
            log.error("upsert_final_data_point failed ticker=%s entry=%r", ticker_upper, entry, exc_info=True)

    log.info("event=finalize_complete ticker=%s count=%d", ticker_upper, count)
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
