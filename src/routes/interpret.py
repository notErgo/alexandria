"""Interpretation layer API — analyst finalization of extracted values.

Endpoints:
  POST /api/interpret/<ticker>/reprompt   — LLM reconciliation suggestions
  POST /api/interpret/<ticker>/finalize   — write to final_data_points
  GET  /api/interpret/<ticker>/final      — read final_data_points for ticker
  DELETE /api/interpret/<ticker>/final    — clear final_data_points for ticker
  POST /api/delete/final                  — bulk purge (all tickers or scoped)
  POST /api/interpret/<ticker>/rerun-sec  — re-run interpret pipeline on stored EDGAR filings
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
    """Ask the LLM to reconcile monthly and SEC data into suggestions.

    Optional body field ``custom_prompt`` (str): if provided and non-empty,
    bypasses ``_build_reprompt`` and sends the custom text directly to Ollama.
    """
    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    body = request.get_json(silent=True) or {}
    custom_prompt = str(body.get('custom_prompt') or '').strip()
    commentary = str(body.get('commentary') or '').strip()
    valid_metrics = _get_valid_metrics(db)
    metrics_filter = body.get('metrics') or []
    if metrics_filter:
        invalid = [m for m in metrics_filter if m not in valid_metrics]
        if invalid:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_METRIC', 'message': f'Unknown metrics: {invalid}',
            }}), 400

    if custom_prompt:
        if 'JSON' not in custom_prompt and 'json' not in custom_prompt:
            log.warning("event=reprompt_custom_prompt_no_json ticker=%s", ticker_upper)
        prompt = custom_prompt
    else:
        monthly = db.query_data_points(ticker=ticker_upper, source_period_types=['monthly'], limit=2000)
        sec = db.query_data_points(ticker=ticker_upper, source_period_types=['quarterly', 'annual'], limit=500)
        finals = db.get_final_data_points(ticker_upper)
        prompt = _build_reprompt(ticker_upper, monthly, sec, finals, commentary, metrics_filter)

    model = _active_model(db)

    # Warm-up check: verify Ollama is reachable and the model is loaded before calling.
    try:
        from interpreters.llm_interpreter import LLMInterpreter
        _llm_check = LLMInterpreter(session=requests.Session(), db=db)
        if not _llm_check.check_connectivity():
            log.warning("event=reprompt_ollama_unavailable ticker=%s model=%s", ticker_upper, model)
            return jsonify({'success': False, 'error': {
                'code': 'LLM_UNAVAILABLE',
                'message': 'Ollama is not reachable or the model is not loaded',
            }}), 503
    except Exception:
        log.error("reprompt Ollama connectivity check failed ticker=%s", ticker_upper, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'LLM_UNAVAILABLE', 'message': 'LLM connectivity check failed',
        }}), 503

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
        if m not in valid_metrics:
            continue
        if not _valid_period(str(p)):
            continue
        try:
            v = float(v)
            if not math.isfinite(v) or v < 0:
                continue
        except (TypeError, ValueError):
            continue
        try:
            conf = float(s.get('confidence') or 0.8)
            if not math.isfinite(conf):
                conf = 0.8
        except (TypeError, ValueError):
            conf = 0.8
        clean.append({
            'metric': m,
            'period': str(p),
            'value': v,
            'confidence': conf,
            'rationale': str(s.get('rationale') or ''),
        })

    return jsonify({'success': True, 'data': {'suggestions': clean}})


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


_EDGAR_SOURCE_TYPES = frozenset({
    'edgar_10q', 'edgar_10k', 'edgar_6k', 'edgar_20f', 'edgar_40f',
})


@bp.route('/api/interpret/<ticker>/rerun-sec', methods=['POST'])
def rerun_sec(ticker: str):
    """Re-run the interpret pipeline on all stored EDGAR filings for a ticker.

    Equivalent to `cli.py extract --ticker X --force` filtered to EDGAR source types.
    Does not re-scrape; reads raw_html/raw_text already in the reports table.

    Returns:
        {"success": true, "data": {"reports_processed": N, "data_points_extracted": N,
                                   "review_flagged": N, "errors": N}}
    """
    from app_globals import get_registry
    from interpreters.interpret_pipeline import extract_report
    from miner_types import ExtractionSummary

    db = get_db()
    ticker_upper = ticker.upper()
    if not db.get_company(ticker_upper):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    registry = get_registry()
    all_reports = db.get_all_reports_for_extraction(ticker=ticker_upper)
    edgar_reports = [r for r in all_reports if r.get('source_type') in _EDGAR_SOURCE_TYPES]

    log.info(
        "event=rerun_sec_start ticker=%s total_reports=%d edgar_reports=%d",
        ticker_upper, len(all_reports), len(edgar_reports),
    )

    total = ExtractionSummary()
    for report in edgar_reports:
        try:
            s = extract_report(report, db, registry)
            total.reports_processed += s.reports_processed
            total.data_points_extracted += s.data_points_extracted
            total.review_flagged += s.review_flagged
            total.errors += s.errors
        except Exception:
            log.error(
                "rerun_sec extraction failed report_id=%s", report.get('id'), exc_info=True
            )
            total.errors += 1

    log.info(
        "event=rerun_sec_end ticker=%s reports_processed=%d data_points=%d review=%d errors=%d",
        ticker_upper, total.reports_processed, total.data_points_extracted,
        total.review_flagged, total.errors,
    )
    return jsonify({'success': True, 'data': {
        'reports_processed': total.reports_processed,
        'data_points_extracted': total.data_points_extracted,
        'review_flagged': total.review_flagged,
        'errors': total.errors,
    }})
