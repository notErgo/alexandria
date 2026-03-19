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


@bp.route('/api/data/documents/<int:report_id>/keywords')
def document_keywords(report_id: int):
    """Return keyword scan results for a report.

    Query params:
      phrases  (required, comma-separated list of phrases to scan for)

    Response data.results is a list of {phrase, found, count, offsets} per phrase.
    """
    try:
        from app_globals import get_db
        db = get_db()

        phrases_str = request.args.get('phrases', '').strip()
        if not phrases_str:
            return jsonify({'success': False, 'error': {
                'code': 'PHRASES_REQUIRED',
                'message': "'phrases' query parameter is required",
            }}), 400

        phrases = [p.strip() for p in phrases_str.split(',') if p.strip()]
        if not phrases:
            return jsonify({'success': False, 'error': {
                'code': 'PHRASES_REQUIRED',
                'message': "'phrases' must contain at least one non-empty phrase",
            }}), 400

        # Verify report exists
        with db._get_connection() as conn:
            exists = conn.execute(
                "SELECT id FROM reports WHERE id = ?", (report_id,)
            ).fetchone()
        if not exists:
            return jsonify({'success': False, 'error': {
                'code': 'NOT_FOUND',
                'message': f"Report {report_id} not found",
            }}), 404

        results = db.scan_document_keywords(report_id, phrases)
        return jsonify({'success': True, 'data': {
            'report_id': report_id,
            'results': results,
        }})
    except Exception:
        log.error("Error in document_keywords %d", report_id, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


_MONTHLY_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


@bp.route('/api/export.csv')
def export_csv():
    """Export the same data shown in the timeseries dashboard as a wide CSV.

    Reads from final_data_points (analyst-accepted values only), restricts
    to monthly periods, and pivots to: period, MARA, RIOT, ...

    Query params (all optional):
        ticker        — repeated; omit for all tickers
        metric        — metric key filter
        from_period   — YYYY-MM lower bound
        to_period     — YYYY-MM upper bound
    """
    from app_globals import get_db
    params, err = _validate_filters(request.args)
    if err:
        return err
    db = get_db()

    tickers_filter = params.get('tickers')   # list[str] or None
    metric_filter  = params.get('metric')
    from_period    = params.get('from_period')
    to_period      = params.get('to_period')

    # Fetch from final_data_points — same source as /api/timeseries
    if tickers_filter:
        rows = []
        for t in tickers_filter:
            rows.extend(db.query_final_data_points(
                ticker=t, metric=metric_filter,
                from_period=from_period, to_period=to_period,
            ))
    else:
        rows = db.query_final_data_points(
            metric=metric_filter, from_period=from_period, to_period=to_period,
        )

    # Monthly periods only — drop quarterly (YYYY-Qn) and annual (YYYY-FY) rows
    rows = [r for r in rows if _MONTHLY_RE.match(r.get('period', ''))]

    # Pivot: YYYY-MM rows × ticker columns
    # final_data_points has a UNIQUE(ticker, period, metric) constraint so no
    # conflict resolution is needed — there is at most one value per cell.
    pivot: dict = {}
    tickers_seen: set = set()
    for row in rows:
        ym = row['period'][:7]   # YYYY-MM-DD → YYYY-MM
        ticker = row['ticker']
        tickers_seen.add(ticker)
        pivot.setdefault(ym, {})[ticker] = row['value']

    sorted_tickers = sorted(tickers_seen)
    sorted_periods = sorted(pivot.keys())    # YYYY-MM strings sort correctly

    metric_slug = (metric_filter or 'all').replace('/', '_')
    fname = f"miners_{metric_slug}.csv"

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['period'] + sorted_tickers)
    for ym in sorted_periods:
        cell = pivot[ym]
        writer.writerow([ym] + [
            '' if cell.get(t) is None else cell.get(t, '')
            for t in sorted_tickers
        ])

    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={fname}'
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


@bp.route('/api/data/management-inventory', methods=['GET'])
def management_inventory():
    """Return per-ticker counts for all pipeline stages.

    Query params:
        ticker (optional) — limit to a single ticker

    Returns:
        [{"ticker": "MARA", "reports": N, "data_points": N,
          "review_pending": N, "review_all": N, "final_values": N}, ...]
    """
    try:
        from app_globals import get_db
        db = get_db()

        ticker_filter = (request.args.get('ticker') or '').strip().upper() or None

        companies = db.get_companies(active_only=False)
        if ticker_filter:
            companies = [c for c in companies if c['ticker'] == ticker_filter]

        # Build per-ticker counts in a single pass via SQL for efficiency
        with db._get_connection() as conn:
            def _count(sql, params=()):
                return conn.execute(sql, params).fetchone()[0]

            result = []
            for company in companies:
                t = company['ticker']
                result.append({
                    'ticker': t,
                    'reports': _count("SELECT COUNT(*) FROM reports WHERE ticker=?", (t,)),
                    'data_points': _count("SELECT COUNT(*) FROM data_points WHERE ticker=?", (t,)),
                    'review_pending': _count(
                        "SELECT COUNT(*) FROM review_queue WHERE ticker=? AND status='PENDING'", (t,)
                    ),
                    'review_all': _count("SELECT COUNT(*) FROM review_queue WHERE ticker=?", (t,)),
                    'final_values': _count(
                        "SELECT COUNT(*) FROM final_data_points WHERE ticker=?", (t,)
                    ),
                })

        return jsonify({'success': True, 'data': result})
    except Exception:
        log.exception("management_inventory failed")
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
