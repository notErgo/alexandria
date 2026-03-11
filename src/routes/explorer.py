"""Metrics explorer and document registry API routes."""
import logging
from datetime import date
from flask import Blueprint, jsonify, request
from app_globals import get_db, get_registry

log = logging.getLogger('miners.routes.explorer')
bp = Blueprint('explorer', __name__)


# ── Explorer grid ─────────────────────────────────────────────────────────────

@bp.route('/api/explorer/grid')
def explorer_grid():
    db = get_db()
    ticker_filter = request.args.get('ticker', '').upper() or None
    try:
        months = max(1, min(120, int(request.args.get('months', 36))))
    except (ValueError, TypeError):
        months = 36
    state_filter = request.args.get('state') or None
    try:
        min_confidence = float(request.args.get('min_confidence', 0))
    except (ValueError, TypeError):
        min_confidence = 0.0
    metric_filter = request.args.get('metric') or None

    companies = db.get_companies(active_only=False)
    if ticker_filter:
        companies = [c for c in companies if c['ticker'] == ticker_filter]

    metrics = db.get_metric_schema('BTC-miners')
    metric_keys = [m['key'] for m in metrics if not metric_filter or m['key'] == metric_filter]

    today = date.today()
    from coverage_logic import compute_expected_periods, compute_cell_state_v2

    grid = []
    for company in companies:
        ticker = company['ticker']
        regime_windows = db.get_regime_windows(ticker)
        if regime_windows:
            periods = compute_expected_periods(regime_windows, today)
        else:
            from coverage_logic import generate_month_range
            periods = generate_month_range(months)

        for period in periods[-months:]:
            for metric_key in metric_keys:
                dps = db.query_data_points(ticker=ticker, metric=metric_key,
                                           from_period=period, to_period=period)
                has_dp = bool(dps)
                has_analyst_gap = any(
                    d.get('extraction_method') == 'analyst_gap' for d in dps
                )
                rq_items = db.get_review_items_for_period(ticker, period, metric_key)
                has_rq = bool(rq_items)
                manifest = db.get_manifest_by_ticker(ticker)
                period_manifest = [m for m in manifest if m.get('period') == period]
                has_manifest = bool(period_manifest)
                has_parse_error = any(
                    r.get('parse_quality') == 'parse_failed'
                    for m in period_manifest
                    if m.get('report_id')
                    for r in [db.get_report(m['report_id'])]
                    if r
                )
                has_extract_error = (
                    has_manifest and not has_dp and not has_rq and
                    any(m.get('ingest_state') == 'ingested' for m in period_manifest)
                )
                has_scraper_error = company.get('scraper_status') == 'error'

                state = compute_cell_state_v2(
                    is_analyst_gap=has_analyst_gap,
                    has_data_point=has_dp and not has_analyst_gap,
                    has_review_pending=has_rq,
                    has_manifest=has_manifest,
                    has_parse_error=has_parse_error,
                    has_extract_error=has_extract_error,
                    has_scraper_error=has_scraper_error,
                )

                if state_filter and state != state_filter:
                    continue

                best_dp = dps[0] if dps else None
                if min_confidence and best_dp and best_dp.get('confidence', 0) < min_confidence:
                    continue

                grid.append({
                    'ticker': ticker,
                    'period': period,
                    'metric': metric_key,
                    'state': state,
                    'value': best_dp.get('value') if best_dp else None,
                    'confidence': best_dp.get('confidence') if best_dp else None,
                    'doc_id': period_manifest[0].get('id') if period_manifest else None,
                })

    return jsonify({'success': True, 'data': {'grid': grid, 'total': len(grid)}})


@bp.route('/api/explorer/cell/<ticker>/<period>/<metric>')
def explorer_cell(ticker, period, metric):
    db = get_db()
    ticker = ticker.upper()
    from coverage_logic import compute_cell_state_v2

    dps = db.query_data_points(ticker=ticker, metric=metric,
                                from_period=period, to_period=period)
    has_analyst_gap = any(d.get('extraction_method') == 'analyst_gap' for d in dps)
    rq_items = db.get_review_items_for_period(ticker, period, metric)
    manifest = db.get_manifest_by_ticker(ticker)
    period_manifest = [m for m in manifest if m.get('period') == period]
    has_manifest = bool(period_manifest)

    # Fetch raw document text and HTML from the first associated report
    raw_text = None
    raw_html = None
    report = None
    if period_manifest and period_manifest[0].get('report_id'):
        report = db.get_report(period_manifest[0]['report_id'])
        if report:
            raw_text = report.get('raw_text', '')
            raw_html = report.get('raw_html') or None

    state = compute_cell_state_v2(
        is_analyst_gap=has_analyst_gap,
        has_data_point=bool(dps) and not has_analyst_gap,
        has_review_pending=bool(rq_items),
        has_manifest=has_manifest,
        has_parse_error=False,
        has_extract_error=False,
        has_scraper_error=False,
    )

    best_dp = dps[0] if dps else None
    best_rq = rq_items[0] if rq_items else None

    # Build match highlights from source_snippet
    matches = []
    for dp in dps:
        if dp.get('source_snippet') and not dp.get('extraction_method') == 'analyst_gap':
            matches.append({
                'text': dp['source_snippet'],
                'metric': metric,
                'confidence': dp.get('confidence', 0),
                'tier': 'primary' if dp.get('confidence', 0) >= 0.75 else 'secondary',
            })

    return jsonify({'success': True, 'data': {
        'ticker': ticker,
        'period': period,
        'metric': metric,
        'state': state,
        'value': best_dp.get('value') if best_dp else None,
        'confidence': best_dp.get('confidence') if best_dp else None,
        'extraction_method': best_dp.get('extraction_method') if best_dp else None,
        'source_snippet': best_dp.get('source_snippet') if best_dp else None,
        'raw_text': raw_text,
        'raw_html': raw_html,
        'matches': matches,
        'review_item': best_rq,
        'all_data_points': dps,
    }})


@bp.route('/api/explorer/cell/<ticker>/<period>/<metric>/save', methods=['POST'])
def explorer_cell_save(ticker, period, metric):
    db = get_db()
    ticker = ticker.upper()
    body = request.get_json(silent=True) or {}
    try:
        value = float(body['value'])
    except (KeyError, ValueError, TypeError):
        return jsonify({'success': False, 'error': {'message': 'value must be numeric'}}), 400

    # Mutation hierarchy: check if analyst-protected
    existing = db.query_data_points(ticker=ticker, metric=metric,
                                     from_period=period, to_period=period)
    _ANALYST_PROTECTED = {'analyst', 'analyst_approved', 'review_approved', 'review_edited'}
    if existing and existing[0].get('extraction_method') in _ANALYST_PROTECTED:
        return jsonify({'success': False, 'error': {
            'message': 'Analyst-protected value cannot be overwritten. Use /override to acknowledge.'
        }}), 409

    note = str(body.get('note', ''))[:500]
    extraction_method = 'manual' if body.get('manual') else 'analyst_edited'
    try:
        db.insert_data_point({
            'report_id': None,
            'ticker': ticker,
            'period': period,
            'metric': metric,
            'value': value,
            'unit': '',
            'confidence': 1.0,
            'extraction_method': extraction_method,
            'source_snippet': note or None,
        })
    except Exception:
        log.error("Failed to save cell %s/%s/%s", ticker, period, metric, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500

    return jsonify({'success': True})


@bp.route('/api/explorer/cell/<ticker>/<period>/<metric>/gap', methods=['POST'])
def explorer_cell_gap(ticker, period, metric):
    db = get_db()
    ticker = ticker.upper()
    try:
        db.insert_data_point({
            'report_id': None,
            'ticker': ticker,
            'period': period,
            'metric': metric,
            'value': 0.0,
            'unit': '',
            'confidence': 1.0,
            'extraction_method': 'analyst_gap',
            'source_snippet': 'Analyst marked: no data expected for this period',
        })
    except Exception:
        log.error("Failed to mark gap %s/%s/%s", ticker, period, metric, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
    return jsonify({'success': True})


@bp.route('/api/explorer/reextract', methods=['POST'])
def explorer_reextract():
    db = get_db()
    body = request.get_json(silent=True) or {}
    selection = str(body.get('selection', ''))[:5000].strip()
    ticker = body.get('ticker', '').upper()
    period = body.get('period', '')

    if not selection:
        return jsonify({'success': False, 'error': {'message': 'selection required'}}), 400

    candidates = []
    try:
        import requests as req_lib
        from interpreters.llm_interpreter import LLMInterpreter
        metrics = db.get_metric_schema('BTC-miners')
        llm = LLMInterpreter(session=req_lib.Session(), db=db)
        if not llm.check_connectivity():
            return jsonify({'success': False, 'error': {'message': 'LLM unavailable'}}), 503
        for m in metrics:
            if not m.get('has_extraction_pattern'):
                continue
            results = llm.extract_batch(selection, [m['key']], ticker=ticker)
            r = results.get(m['key'])
            if r is not None:
                candidates.append({
                    'metric': m['key'],
                    'source': 'llm',
                    'value': r.value,
                    'unit': r.unit,
                    'confidence': r.confidence,
                    'snippet': r.source_snippet,
                })
    except Exception:
        log.error("Re-extract LLM failed", exc_info=True)

    # Sort by confidence descending
    candidates.sort(key=lambda c: c.get('confidence', 0), reverse=True)
    return jsonify({'success': True, 'data': {'candidates': candidates}})


# ── Document registry ─────────────────────────────────────────────────────────

@bp.route('/api/registry')
def registry():
    db = get_db()
    ticker = request.args.get('ticker', '').upper() or None
    period = request.args.get('period') or None
    doc_type = request.args.get('doc_type') or None
    extraction_status = request.args.get('extraction_status') or None

    # Build query with joins
    clauses = []
    params = []
    if ticker:
        clauses.append("am.ticker = ?")
        params.append(ticker)
    if period:
        clauses.append("am.period = ?")
        params.append(period)
    if doc_type:
        clauses.append("am.source_type = ?")
        params.append(doc_type)

    where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''

    with db._get_connection() as conn:
        rows = conn.execute(
            f"""SELECT am.*,
                       r.id as report_id_join,
                       r.extracted_at,
                       r.parse_quality,
                       r.published_date,
                       (SELECT COUNT(*) FROM data_points dp
                        WHERE dp.ticker=am.ticker AND dp.period=am.period) as metrics_found
                FROM asset_manifest am
                LEFT JOIN reports r ON am.report_id = r.id
                {where}
                ORDER BY am.ticker, am.period DESC
                LIMIT 500""",
            params,
        ).fetchall()
        items = [dict(r) for r in rows]

    # Apply extraction_status filter in Python (simpler than SQL)
    if extraction_status == 'not_extracted':
        items = [i for i in items if not i.get('extracted_at')]
    elif extraction_status == 'extracted':
        items = [i for i in items if i.get('extracted_at')]
    elif extraction_status == 'parse_failed':
        items = [i for i in items if i.get('parse_quality') == 'parse_failed']

    return jsonify({'success': True, 'data': {'items': items, 'total': len(items)}})


# ── Document explorer ──────────────────────────────────────────────────────────

@bp.route('/api/documents')
def documents():
    """Return ingested reports with their extracted data_points.

    Each item represents one report (document). The `metrics` field is a dict
    of metric_key → value for data_points linked to that report.

    Query params:
      ticker   — filter by ticker (optional)
      source_type — filter by source type e.g. ir_press_release (optional)
      extracted — 'yes'|'no'|'' — filter by whether the report has data_points
      limit    — max rows (default 200, max 500)
    """
    db = get_db()
    ticker = request.args.get('ticker', '').upper() or None
    source_type = request.args.get('source_type') or None
    extracted_filter = request.args.get('extracted') or None
    try:
        limit = max(1, min(500, int(request.args.get('limit', 200))))
    except (ValueError, TypeError):
        limit = 200

    clauses = []
    params = []
    if ticker:
        clauses.append("r.ticker = ?")
        params.append(ticker)
    if source_type:
        clauses.append("r.source_type = ?")
        params.append(source_type)

    where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
    params.extend([limit])

    with db._get_connection() as conn:
        report_rows = conn.execute(
            f"""SELECT r.id, r.ticker, r.source_type, r.report_date,
                       r.covering_period, r.source_url, r.extracted_at,
                       r.parse_quality
                FROM reports r
                {where}
                ORDER BY r.ticker, r.report_date DESC
                LIMIT ?""",
            params,
        ).fetchall()
        reports = [dict(r) for r in report_rows]

        # Fetch all data_points for these report_ids in one query
        if reports:
            report_ids = [r['id'] for r in reports]
            placeholders = ','.join('?' * len(report_ids))
            dp_rows = conn.execute(
                f"SELECT report_id, metric, value, extraction_method "
                f"FROM data_points WHERE report_id IN ({placeholders})",
                report_ids,
            ).fetchall()
        else:
            dp_rows = []

    # Build a map: report_id → {metric: value}
    dp_map: dict = {}
    for dp in dp_rows:
        rid = dp[0]
        if rid not in dp_map:
            dp_map[rid] = {}
        dp_map[rid][dp[1]] = {'value': dp[2], 'method': dp[3]}

    # Attach metrics to each report
    for r in reports:
        r['metrics'] = dp_map.get(r['id'], {})

    # Apply extracted filter
    if extracted_filter == 'yes':
        reports = [r for r in reports if r['metrics']]
    elif extracted_filter == 'no':
        reports = [r for r in reports if not r['metrics']]

    # Collect all metric keys seen across all documents
    all_metric_keys: list = []
    seen: set = set()
    for r in reports:
        for k in r['metrics']:
            if k not in seen:
                seen.add(k)
                all_metric_keys.append(k)

    return jsonify({'success': True, 'data': {
        'reports': reports,
        'total': len(reports),
        'metric_keys': sorted(all_metric_keys),
    }})
