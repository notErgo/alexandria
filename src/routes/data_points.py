"""Data query and export API routes."""
import csv
import io
import logging
import re

from flask import Blueprint, jsonify, request, make_response, g

log = logging.getLogger('miners.routes.data_points')

bp = Blueprint('data_points', __name__)

# SYNC: keep identical to sibling _VALID_METRICS_FALLBACK in interpret.py / llm_prompts.py / dashboard.py
_VALID_METRICS_FALLBACK = frozenset({
    'production_btc', 'holdings_btc', 'unrestricted_holdings', 'restricted_holdings_btc',
    'sales_btc', 'hashrate_eh', 'realization_rate',
    'net_btc_balance_change', 'encumbered_btc',
    'mining_mw', 'ai_hpc_mw', 'hpc_revenue_usd', 'gpu_count',
})
_PERIOD_RE = re.compile(r'^\d{4}-\d{2}$')


def _get_valid_metrics(db) -> frozenset:
    """Return set of valid metric keys from DB SSOT (metric_schema table)."""
    try:
        rows = db.get_metric_schema(sector='BTC-miners', active_only=False)
        if rows:
            return frozenset(r['key'] for r in rows)
    except Exception:
        pass
    return _VALID_METRICS_FALLBACK


def _validate_filters(args):
    """Validate query parameters. Returns (params_dict, error_response or None).

    'ticker' supports multiple values (repeated param: ?ticker=MARA&ticker=RIOT).
    """
    tickers = args.getlist('ticker') if hasattr(args, 'getlist') else (
        [args.get('ticker')] if args.get('ticker') else []
    )
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    metric = args.get('metric')
    from_period = args.get('from_period')
    to_period = args.get('to_period')
    min_confidence_str = args.get('min_confidence')

    if tickers or metric:
        from app_globals import get_db
        db = get_db()
        for t in tickers:
            if not db.get_company(t):
                return None, (jsonify({'success': False, 'error': {
                    'code': 'INVALID_TICKER', 'message': f'Ticker {t!r} not recognized'
                }}), 400)
    else:
        db = None

    if metric and db is not None and metric not in _get_valid_metrics(db):
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_METRIC', 'message': f'Metric {metric!r} not valid'
        }}), 400)

    if from_period and not _PERIOD_RE.match(from_period):
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE', 'message': 'from_period must be YYYY-MM'
        }}), 400)

    if to_period and not _PERIOD_RE.match(to_period):
        return None, (jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE', 'message': 'to_period must be YYYY-MM'
        }}), 400)

    min_confidence = None
    if min_confidence_str is not None:
        try:
            min_confidence = float(min_confidence_str)
            if not 0.0 <= min_confidence <= 1.0:
                raise ValueError
        except ValueError:
            return None, (jsonify({'success': False, 'error': {
                'code': 'INVALID_CONFIDENCE', 'message': 'min_confidence must be float in [0.0, 1.0]'
            }}), 400)

    # Convert YYYY-MM to YYYY-MM-01 for DB comparison
    from_full = (from_period + '-01') if from_period else None
    to_full = (to_period + '-01') if to_period else None

    # Optional source filter: 'edgar' (priority 1), 'ir' (priority 2), 'archive' (priority 3)
    source_filter = args.get('source', '').strip().lower()
    _SOURCE_PRIORITY_MAP = {'edgar': 1, 'ir': 2, 'archive': 3}
    max_source_priority = None
    if source_filter:
        if source_filter not in _SOURCE_PRIORITY_MAP:
            return None, (jsonify({'success': False, 'error': {
                'code': 'INVALID_SOURCE',
                'message': f"source must be one of: {', '.join(_SOURCE_PRIORITY_MAP)}",
            }}), 400)
        max_source_priority = _SOURCE_PRIORITY_MAP[source_filter]

    return {
        'tickers': tickers or None,   # list or None
        'metric': metric or None,
        'from_period': from_full,
        'to_period': to_full,
        'min_confidence': min_confidence,
        'max_source_priority': max_source_priority,
    }, None


@bp.route('/api/data')
def get_data():
    from app_globals import get_db
    params, err = _validate_filters(request.args)
    if err:
        return err
    db = get_db()
    rows = db.query_data_points(**params)
    return jsonify({'success': True, 'data': rows})


@bp.route('/api/data/lineage')
def get_data_lineage():
    """Return full provenance for a single data point (ticker + metric + period).

    Query params: ticker (str), metric (str), period (YYYY-MM).
    Returns: confidence, extraction_method, source_snippet, source_type,
             report_date, source_url — raw_text is never exposed.
    """
    from app_globals import get_db
    ticker = request.args.get('ticker', '').strip().upper()
    metric = request.args.get('metric', '').strip()
    period = request.args.get('period', '').strip()

    if not ticker or not metric or not period:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_PARAMS',
            'message': 'ticker, metric, period are all required',
        }}), 400

    db = get_db()
    if metric not in _get_valid_metrics(db):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_METRIC',
            'message': f'Unknown metric: {metric!r}',
        }}), 400

    if not _PERIOD_RE.match(period):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_DATE',
            'message': 'period must be YYYY-MM',
        }}), 400

    period_db = period + '-01'
    try:
        rows = db.query_data_points(
            ticker=ticker, metric=metric,
            from_period=period_db, to_period=period_db,
            limit=1,
        )
    except Exception:
        log.error("lineage query failed for %s/%s/%s", ticker, metric, period, exc_info=True)
        return jsonify({'success': False, 'error': {
            'code': 'SERVER_ERROR', 'message': 'Internal server error',
        }}), 500

    if not rows:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND',
            'message': f'No data for {ticker} {metric} {period}',
        }}), 404

    row = rows[0]
    report = {}
    if row.get('report_id'):
        try:
            report = db.get_report(row['report_id']) or {}
        except Exception:
            log.error("get_report failed for report_id=%s", row['report_id'], exc_info=True)

    return jsonify({'success': True, 'data': {
        'ticker':            row['ticker'],
        'metric':            row['metric'],
        'period':            period,
        'value':             row['value'],
        'unit':              row['unit'],
        'confidence':        row['confidence'],
        'extraction_method': row['extraction_method'],
        'source_snippet':    row['source_snippet'],
        'created_at':        row['created_at'],
        'source_type':       report.get('source_type'),
        'report_date':       report.get('report_date'),
        'source_url':        report.get('source_url'),
    }})


_VALID_SOURCE_TYPES = frozenset({
    'archive_html', 'archive_pdf', 'ir_press_release',
    'edgar_8k', 'edgar_10q', 'edgar_10k',
})
_VALID_EXTRACTION_STATUSES = frozenset({
    'pending', 'running', 'done', 'failed', 'dead_letter',
})


@bp.route('/api/data/documents')
def get_documents():
    """Search reports (documents) for the Mode B document browser.

    Query params:
        ticker           (str, optional)  — filter by company ticker
        from_date        (YYYY-MM, opt)   — report_date lower bound
        to_date          (YYYY-MM, opt)   — report_date upper bound
        source_type      (str, optional)  — e.g. archive_html, edgar_8k
        extraction_status (str, optional) — pending|running|done|failed|dead_letter
        limit            (int, optional)  — max results, default 500

    Returns:
        list of {id, ticker, report_date, source_type, extraction_status,
                 source_url, data_point_count}
        raw_text is never included; use GET /api/data/document/<id> for that.
    """
    from app_globals import get_db

    ticker_raw = request.args.get('ticker', '').strip().upper() or None
    from_date_raw = request.args.get('from_date', '').strip() or None
    to_date_raw = request.args.get('to_date', '').strip() or None
    source_type = request.args.get('source_type', '').strip() or None
    extraction_status = request.args.get('extraction_status', '').strip() or None

    # Validate ticker
    if ticker_raw:
        db = get_db()
        if not db.get_company(ticker_raw):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_TICKER',
                'message': f'Ticker {ticker_raw!r} not recognized',
            }}), 400
    else:
        db = get_db()

    # Validate date format (YYYY-MM)
    for label, val in (('from_date', from_date_raw), ('to_date', to_date_raw)):
        if val and not _PERIOD_RE.match(val):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_DATE',
                'message': f'{label} must be YYYY-MM',
            }}), 400

    # Convert YYYY-MM to YYYY-MM-01 for DB report_date comparison
    from_full = (from_date_raw + '-01') if from_date_raw else None
    to_full = (to_date_raw + '-01') if to_date_raw else None

    if source_type and source_type not in _VALID_SOURCE_TYPES:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_SOURCE_TYPE',
            'message': f'source_type must be one of {sorted(_VALID_SOURCE_TYPES)}',
        }}), 400

    if extraction_status and extraction_status not in _VALID_EXTRACTION_STATUSES:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_EXTRACTION_STATUS',
            'message': f'extraction_status must be one of {sorted(_VALID_EXTRACTION_STATUSES)}',
        }}), 400

    try:
        limit = min(int(request.args.get('limit', 500)), 500)
    except (TypeError, ValueError):
        limit = 500

    docs = db.search_reports(
        ticker=ticker_raw,
        from_date=from_full,
        to_date=to_full,
        source_type=source_type,
        extraction_status=extraction_status,
        limit=limit,
    )
    return jsonify({'success': True, 'data': docs})


@bp.route('/api/data/document/<int:report_id>')
def get_document(report_id: int):
    """Return full document payload for the Mode B document viewer.

    Returns raw_text (required by viewer), report metadata, and all
    extracted data_point matches for snippet highlighting.

    Response shape:
        data: {
            id, ticker, report_date, source_type, source_url,
            extraction_status, raw_text,
            matches: [{metric, period, value, unit, confidence,
                       extraction_method, source_snippet}]
        }
    """
    from app_globals import get_db
    db = get_db()
    report = db.get_report(report_id)
    if not report:
        return jsonify({'success': False, 'error': {
            'code': 'NOT_FOUND',
            'message': f'Report {report_id} not found',
        }}), 404

    raw_text = db.get_report_raw_text(report_id) or ''
    matches = db.get_data_points_by_report(report_id)

    return jsonify({'success': True, 'data': {
        'id':               report['id'],
        'ticker':           report['ticker'],
        'report_date':      report['report_date'],
        'source_type':      report['source_type'],
        'source_url':       report.get('source_url'),
        'extraction_status': report.get('extraction_status'),
        'raw_text':         raw_text,
        'matches':          matches,
    }})


@bp.route('/api/export.csv')
def export_csv():
    from app_globals import get_db
    params, err = _validate_filters(request.args)
    if err:
        return err
    db = get_db()
    rows = db.query_data_points_for_export(**params)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        'ticker', 'period', 'metric', 'value', 'unit',
        'confidence', 'extraction_method', 'source_url',
        'llm_value', 'regex_value', 'agreement_status',
        'source_snippet', 'created_at',
    ], extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        # Emit empty string for None values so CSV is clean
        cleaned = {k: ('' if v is None else v) for k, v in row.items()}
        writer.writerow(cleaned)

    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=miners_export.csv'
    return response


def _period_sort_key(period: str) -> tuple:
    """Convert a period string to a (year, month) tuple for correct cross-type sorting.

    Monthly  '2025-01-01' → (2025, 1)
    Quarterly '2025-Q3'   → (2025, 9)   end-month of the quarter
    Annual    '2024-FY'   → (2024, 12)
    """
    import re as _re
    if not period:
        return (0, 0)
    m = _re.match(r'^(\d{4})-(\d{2})-\d{2}$', period)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    q = _re.match(r'^(\d{4})-Q(\d)$', period)
    if q:
        return (int(q.group(1)), int(q.group(2)) * 3)
    fy = _re.match(r'^(\d{4})-FY$', period)
    if fy:
        return (int(fy.group(1)), 12)
    return (0, 0)


def _latest_per_metric(dps: list, metrics: list) -> dict:
    """Return {metric: dp_dict} keeping the most recent period per metric."""
    best = {}
    for dp in dps:
        m = dp.get('metric')
        if m not in metrics:
            continue
        if m not in best or _period_sort_key(dp['period']) > _period_sort_key(best[m]['period']):
            best[m] = dp
    return best


def _is_sec_period(period: str) -> bool:
    """Return True for quarterly (YYYY-Qn) or annual (YYYY-FY) periods."""
    import re as _re
    return bool(_re.match(r'^\d{4}-Q\d$', period or '') or
                _re.match(r'^\d{4}-FY$', period or ''))


@bp.route('/api/scorecard')
def scorecard():
    """Return latest finalized monthly and SEC value per metric per company.

    Only surfaces values that have been explicitly accepted into final_data_points
    (review_approved, review_edited, or analyst-finalized). Unreviewed raw
    data_points are never shown on the dashboard.

    Response shape:
      data.companies[ticker] = {
        name, scraper_status,
        monthly: { metric: {value, period, confidence, is_finalized} | null },
        sec:     { metric: {value, period, confidence, is_finalized} | null },
      }
    """
    from app_globals import get_db
    db = get_db()
    try:
        _schema_rows = db.get_metric_schema('BTC-miners', active_only=False)
        SCORECARD_METRICS = [
            r['key'] for r in _schema_rows
            if r.get('show_on_scorecard', 1)
        ]
    except Exception:
        SCORECARD_METRICS = [
            'production_btc', 'sales_btc', 'hashrate_eh',
            'holdings_btc', 'unrestricted_holdings',
        ]
    companies = db.get_companies(active_only=False)
    result = {}
    for company in companies:
        ticker = company['ticker']

        finals = db.get_final_data_points(ticker)
        final_monthly = _latest_per_metric(
            [f for f in finals if not _is_sec_period(f['period'])], SCORECARD_METRICS
        )
        final_sec = _latest_per_metric(
            [f for f in finals if _is_sec_period(f['period'])], SCORECARD_METRICS
        )

        def _cell(dp):
            if dp is None:
                return None
            return {
                'value':              dp.get('value'),
                'period':             dp.get('period'),
                'confidence':         dp.get('confidence'),
                'source_period_type': dp.get('source_period_type'),
                'is_finalized':       True,
            }

        result[ticker] = {
            'name':           company.get('name'),
            'scraper_status': company.get('scraper_status'),
            'monthly': {m: _cell(final_monthly.get(m)) for m in SCORECARD_METRICS},
            'sec':     {m: _cell(final_sec.get(m))     for m in SCORECARD_METRICS},
        }

    return jsonify({'success': True, 'data': {'companies': result, 'metrics': SCORECARD_METRICS}})


@bp.route('/api/export_llm_csv')
def export_llm_csv():
    """LLM-direct CSV export — bypasses the agreement engine.

    Calls Ollama once per stored report for the ticker, parses the JSON
    extraction response, then aggregates to monthly + quarterly rows.

    Query params:
        ticker (required)
        from_period (YYYY-MM, optional)
        to_period   (YYYY-MM, optional)

    Response headers:
        X-LLM-Direct: true  (marks output as unreviewed, not agreement-validated)
    """
    import json as _json
    import requests as _requests
    from app_globals import get_db
    from config import LLM_BASE_URL, LLM_TIMEOUT_SECONDS, FLOW_METRICS

    ticker = request.args.get('ticker', '').strip().upper()
    from_period_raw = request.args.get('from_period', '').strip()
    to_period_raw = request.args.get('to_period', '').strip()

    if not ticker:
        return jsonify({'success': False, 'error': {
            'code': 'MISSING_TICKER', 'message': 'ticker is required',
        }}), 400

    db = get_db()
    if not db.get_company(ticker):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER', 'message': f'Unknown ticker: {ticker!r}',
        }}), 404

    from_full = (from_period_raw + '-01') if from_period_raw and _PERIOD_RE.match(from_period_raw) else None
    to_full = (to_period_raw + '-01') if to_period_raw and _PERIOD_RE.match(to_period_raw) else None

    reports = db.get_reports_with_text(ticker=ticker, from_period=from_full, to_period=to_full)

    try:
        schema_rows = db.get_metric_schema('BTC-miners', active_only=True)
        metrics = [r['key'] for r in schema_rows] if schema_rows else list(_VALID_METRICS_FALLBACK)
    except Exception:
        metrics = list(_VALID_METRICS_FALLBACK)

    try:
        model_val = db.get_config('ollama_model')
        from config import LLM_MODEL_ID
        model = model_val or LLM_MODEL_ID
    except Exception:
        from config import LLM_MODEL_ID
        model = LLM_MODEL_ID

    # monthly_data: (period_month, metric) -> {values, unit, confidence_sum, count}
    monthly_data: dict = {}

    for report in reports:
        raw_text = db.get_report_raw_text(report['id'])
        if not raw_text or not raw_text.strip():
            continue

        prompt = (
            f"Extract Bitcoin mining operational data for {ticker} from this document.\n"
            f"Metrics to extract: {', '.join(metrics)}\n"
            "Return JSON array only — no prose:\n"
            '[{"metric":"<key>","period":"<YYYY-MM-01 or YYYY-Qn or YYYY-FY>",'
            '"value":<number>,"unit":"<str>","confidence":<0-1>}]\n'
            "Omit metrics not found. Use period format YYYY-MM-01 for monthly.\n\n"
            f"DOCUMENT:\n{raw_text[:8000]}"
        )

        try:
            resp = _requests.post(
                f"{LLM_BASE_URL}/api/generate",
                json={'model': model, 'prompt': prompt, 'stream': False},
                timeout=LLM_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            raw_response = resp.json().get('response', '')
        except Exception:
            log.warning(
                "export_llm_csv LLM call failed report_id=%s ticker=%s",
                report['id'], ticker, exc_info=True,
            )
            continue

        try:
            text = raw_response.strip()
            if '```' in text:
                text = re.sub(r'```[a-z]*\n?', '', text).strip()
            start = text.find('[')
            end = text.rfind(']')
            if start == -1 or end == -1:
                continue
            items = _json.loads(text[start:end + 1])
            if not isinstance(items, list):
                continue
        except Exception:
            log.warning("export_llm_csv JSON parse failed report_id=%s", report['id'])
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            metric_key = item.get('metric', '')
            period = str(item.get('period', ''))
            raw_val = item.get('value')
            unit = str(item.get('unit') or '')
            try:
                confidence = float(item.get('confidence') or 0.5)
            except (TypeError, ValueError):
                confidence = 0.5

            if metric_key not in metrics:
                continue
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue

            # Normalize period to YYYY-MM
            m_monthly = re.match(r'^(\d{4}-\d{2})', period)
            if m_monthly:
                period_month = m_monthly.group(1)
            else:
                m_q = re.match(r'^(\d{4})-Q(\d)$', period)
                if m_q:
                    qnum = int(m_q.group(2))
                    period_month = f"{m_q.group(1)}-{qnum * 3:02d}"
                else:
                    continue

            key = (period_month, metric_key)
            if key not in monthly_data:
                monthly_data[key] = {'values': [], 'unit': unit, 'confidence_sum': 0.0, 'count': 0}
            monthly_data[key]['values'].append(val)
            monthly_data[key]['confidence_sum'] += confidence
            monthly_data[key]['count'] += 1

    def _quarter_from_month(period_month: str) -> str:
        month = int(period_month[5:7])
        qnum = (month - 1) // 3 + 1
        return f"{period_month[:4]}-Q{qnum}"

    # Aggregate monthly → per-cell average
    monthly_agg = {}
    for (period_month, metric_key), data in monthly_data.items():
        avg_val = sum(data['values']) / len(data['values'])
        avg_conf = data['confidence_sum'] / data['count']
        monthly_agg[(period_month, metric_key)] = {
            'value': avg_val, 'unit': data['unit'],
            'confidence': avg_conf, 'doc_count': data['count'],
        }

    # Aggregate to quarterly
    quarterly_build: dict = {}
    for (period_month, metric_key), agg in monthly_agg.items():
        qkey = (_quarter_from_month(period_month), metric_key)
        if qkey not in quarterly_build:
            quarterly_build[qkey] = {
                'values': [], 'unit': agg['unit'], 'doc_count': 0,
                'is_flow': metric_key in FLOW_METRICS,
            }
        quarterly_build[qkey]['values'].append(agg['value'])
        quarterly_build[qkey]['doc_count'] += agg['doc_count']

    quarterly_final = {}
    for (quarter, metric_key), data in quarterly_build.items():
        q_val = sum(data['values']) if data['is_flow'] else data['values'][-1]
        quarterly_final[(quarter, metric_key)] = {'value': q_val, 'unit': data['unit']}

    # Build CSV rows
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        'ticker', 'period_month', 'period_quarter', 'metric',
        'monthly_value', 'quarterly_value', 'unit', 'llm_confidence', 'source_doc_count',
    ])
    writer.writeheader()
    for (period_month, metric_key), agg in sorted(monthly_agg.items()):
        quarter = _quarter_from_month(period_month)
        q_data = quarterly_final.get((quarter, metric_key))
        writer.writerow({
            'ticker': ticker,
            'period_month': period_month,
            'period_quarter': quarter,
            'metric': metric_key,
            'monthly_value': round(agg['value'], 6),
            'quarterly_value': round(q_data['value'], 6) if q_data else '',
            'unit': agg['unit'],
            'llm_confidence': round(agg['confidence'], 3),
            'source_doc_count': agg['doc_count'],
        })

    log.info(
        "event=export_llm_csv_complete ticker=%s reports=%d rows=%d",
        ticker, len(reports), len(monthly_agg),
    )
    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={ticker}_llm_export.csv'
    response.headers['X-LLM-Direct'] = 'true'
    return response


def _run_purge_request(*, route_label: str, allowed_modes: set[str], default_mode: str, body: dict | None = None):
    """Run explicit purge/reset modes for operational data."""
    from app_globals import get_db
    body = body if body is not None else (request.get_json(silent=True) or {})

    if not body.get('confirm'):
        return jsonify({'success': False, 'error': {
            'code': 'CONFIRM_REQUIRED',
            'message': 'Request body must include {"confirm": true}',
        }}), 400

    ticker = body.get('ticker')
    if ticker:
        ticker = str(ticker).strip().upper()
        if not ticker:
            ticker = None
    purge_mode = str(body.get('purge_mode') or default_mode).strip().lower()
    reason = str(body.get('reason') or '').strip() or None
    suppress_auto_sync = bool(body.get('suppress_auto_sync', False))
    if purge_mode not in allowed_modes:
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_PURGE_MODE',
            'message': f"purge_mode must be one of {sorted(allowed_modes)}",
        }}), 400

    db = get_db()

    if ticker and not db.get_company(ticker):
        return jsonify({'success': False, 'error': {
            'code': 'INVALID_TICKER',
            'message': f'Ticker {ticker!r} not recognized',
        }}), 400

    log.info(
        "event=purge_start route=%s purge_mode=%s ticker=%s suppress_auto_sync=%s reason=%r",
        route_label, purge_mode, ticker or 'ALL', suppress_auto_sync, reason,
    )
    try:
        counts = db.purge_all(
            ticker=ticker,
            purge_mode=purge_mode,
            reason=reason,
            suppress_auto_sync=(suppress_auto_sync and purge_mode == 'hard_delete' and not ticker),
        )
    except Exception as e:
        log.error(
            "event=purge_error route=%s purge_mode=%s ticker=%s error=%r",
            route_label, purge_mode, ticker or 'ALL', str(e), exc_info=True,
        )
        return jsonify({'success': False, 'error': {
            'code': 'PURGE_ERROR', 'message': 'Internal error during purge',
        }}), 500

    log.info(
        "event=purge_complete route=%s purge_mode=%s ticker=%s counts=%s",
        route_label, purge_mode, ticker or 'ALL', counts,
    )
    return jsonify({'success': True, 'data': {
        'counts': counts,
        'ticker': ticker or 'ALL',
        'purge_mode': purge_mode,
        'auto_sync_companies_on_startup': db.get_config('auto_sync_companies_on_startup', default='1'),
    }})


@bp.route('/api/delete/scrape', methods=['POST'])
@bp.route('/api/data/purge', methods=['POST'])
def purge_data():
    """Canonical SCRAPE-stage delete endpoint.

    purge_mode:
      - reset: clear scraped sources and downstream layers; keep company config.
      - archive: same as reset, but copy deleted rows to purge_archive.db.
      - hard_delete: legacy alias support on /api/data/purge only.

    Body (JSON):
        confirm (bool, required): must be true to proceed
        ticker (str, optional): limit purge to one ticker
        purge_mode (str, optional): reset|archive (default archive)
        reason (str, optional): operator reason for audit/archive metadata
        suppress_auto_sync (bool, optional): legacy full hard_delete only

    Returns:
        {"success": true, "data": {"counts": {...}, "ticker": "ALL", "purge_mode": "archive"}}
    """
    route_label = request.path
    return _run_purge_request(
        route_label=route_label,
        allowed_modes={'reset', 'archive', 'hard_delete'} if route_label == '/api/data/purge' else {'reset', 'archive'},
        default_mode='archive',
    )


@bp.route('/api/delete/all', methods=['POST'])
def delete_all():
    """Canonical ALL-stage delete endpoint."""
    body = dict(request.get_json(silent=True) or {})
    body['purge_mode'] = 'hard_delete'
    body['suppress_auto_sync'] = True
    return _run_purge_request(
        route_label='/api/delete/all',
        allowed_modes={'hard_delete'},
        default_mode='hard_delete',
        body=body,
    )
