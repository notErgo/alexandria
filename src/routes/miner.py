"""
Consolidated miner data API blueprint.

Endpoints:
  GET /api/miner/<ticker>/timeline      — pivoted table: all periods × all metrics
  GET /api/miner/<ticker>/<period>/analysis — pattern matches for a report's raw text
"""
import logging
import math
from flask import Blueprint, jsonify, request, Response

from app_globals import get_db, get_registry
from interpreters.regex_interpreter import extract_all

log = logging.getLogger('miners.routes.miner')

bp = Blueprint('miner', __name__)

# Core metrics — always shown in the timeline table even when no data exists.
# hashrate_eh and realization_rate are NOT core: they only appear when data exists.
CORE_METRICS = ['production_btc', 'hodl_btc', 'sold_btc']

# All known metrics in canonical display order.
# Non-core metrics appear as columns only when data exists for that ticker.
ALL_METRICS_ORDER = [
    'production_btc',
    'hodl_btc',
    'hodl_btc_unrestricted',
    'hodl_btc_restricted',
    'sold_btc',
    'hashrate_eh',
    'realization_rate',
    'net_btc_balance_change',
    'encumbered_btc',
    'mining_mw',
    'ai_hpc_mw',
    'hpc_revenue_usd',
    'gpu_count',
]

METRIC_LABELS = {
    'production_btc': 'Production BTC',
    'hodl_btc': 'Holdings BTC',
    'hodl_btc_unrestricted': 'Holdings (Unres.)',
    'hodl_btc_restricted': 'Holdings (Restr.)',
    'sold_btc': 'Sold BTC',
    'hashrate_eh': 'Hashrate EH/s',
    'realization_rate': 'Real. Rate',
    'net_btc_balance_change': 'Net BTC Change',
    'encumbered_btc': 'Encumbered BTC',
    'mining_mw': 'Mining MW',
    'ai_hpc_mw': 'AI/HPC MW',
    'hpc_revenue_usd': 'HPC Revenue USD',
    'gpu_count': 'GPU Count',
}

METRIC_UNITS = {
    'production_btc': 'BTC',
    'hodl_btc': 'BTC',
    'hodl_btc_unrestricted': 'BTC',
    'hodl_btc_restricted': 'BTC',
    'sold_btc': 'BTC',
    'hashrate_eh': 'EH/s',
    'realization_rate': '%',
    'net_btc_balance_change': 'BTC',
    'encumbered_btc': 'BTC',
    'mining_mw': 'MW',
    'ai_hpc_mw': 'MW',
    'hpc_revenue_usd': 'USD',
    'gpu_count': '',
}


def _build_monthly_spine(min_period: str, max_period: str) -> list:
    """
    Return a list of YYYY-MM-01 strings covering every month from
    min_period to max_period inclusive, in ascending order.
    """
    min_y, min_m = int(min_period[:4]), int(min_period[5:7])
    max_y, max_m = int(max_period[:4]), int(max_period[5:7])
    spine = []
    y, m = min_y, min_m
    while (y, m) <= (max_y, max_m):
        spine.append(f"{y:04d}-{m:02d}-01")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return spine


def _period_label_sec(period: str) -> str:
    """Convert YYYY-Qn / YYYY-FY to a readable label."""
    import re as _re
    q = _re.match(r'^(\d{4})-Q(\d)$', period)
    if q:
        return f"Q{q.group(2)} {q.group(1)}"
    fy = _re.match(r'^(\d{4})-FY$', period)
    if fy:
        return f"FY {fy.group(1)}"
    return period[:7]


@bp.route('/api/miner/<ticker>/timeline')
def get_miner_timeline(ticker: str):
    """
    Return a pivoted timeline of all periods for a ticker.

    Query params:
      source: 'monthly' (default) | 'sec'
        - monthly: monthly YYYY-MM-01 spine with gap rows.
        - sec: quarterly/annual periods only, no spine padding.

    Each row for monthly represents one calendar month. Gap rows
    (all metrics null) are flagged with is_gap=True.

    Metric cells include is_finalized=True when a final_data_points row
    exists for that (ticker, period, metric).

    Response shape:
      {
        "success": true,
        "data": {
          "ticker": "MARA",
          "source": "monthly",
          "company": {ticker, name, ir_url, pr_base_url, cik},
          "stats": {total_periods, gap_periods, first_period, last_period},
          "rows": [
            {
              "period": "2022-07-01",
              "period_label": "2022-07",
              "has_report": bool,
              "report_id": int|null,
              "report_date": str|null,
              "source_type": str|null,
              "source_url": str|null,
              "is_gap": bool,
              "metrics": {
                "production_btc": {value, unit, confidence, extraction_method,
                                   is_finalized} | null,
                ...
              }
            }
          ]
        }
      }
    """
    try:
        db = get_db()
        ticker_upper = ticker.upper()
        company = db.get_company(ticker_upper)
        if company is None:
            return jsonify({'success': False, 'error': {'message': f'Unknown ticker: {ticker}'}}), 404

        source = request.args.get('source', 'monthly').lower()
        if source not in ('monthly', 'sec'):
            return jsonify({'success': False, 'error': {
                'message': "source must be 'monthly' or 'sec'"}}), 400

        # Fetch data points for the requested source
        if source == 'sec':
            all_dps = db.query_data_points(
                ticker=ticker_upper,
                source_period_types=['quarterly', 'annual'],
                limit=100000,
            )
        else:
            all_dps = db.query_data_points(
                ticker=ticker_upper,
                source_period_types=['monthly'],
                limit=100000,
            )

        import re as _re
        _QUARTERLY_PERIOD_RE = _re.compile(r'^\d{4}-(Q\d|FY)$')

        # Build a set of finalized (period, metric) keys for is_finalized flag
        finals = db.get_final_data_points(ticker_upper)
        finalized_keys = {(f['period'], f['metric']) for f in finals}

        # Build a set of reviewed periods for is_reviewed flag
        reviewed_set = db.get_reviewed_periods(ticker_upper)

        # Build pivot dict: period → metric → cell dict
        # Monthly source: normalize all periods to YYYY-MM-01 so that data points
        # stored with non-standard dates (e.g. "2025-01-31" from 8-K filing dates)
        # align with the monthly spine which always uses YYYY-MM-01.
        pivot: dict = {}
        for dp in all_dps:
            period = dp['period']
            if source == 'monthly' and len(period) >= 10:
                period = period[:7] + '-01'
            metric = dp['metric']
            if period not in pivot:
                pivot[period] = {}
            pivot[period][metric] = {
                'value':             dp['value'],
                'unit':              dp.get('unit'),
                'confidence':        dp['confidence'],
                'extraction_method': dp.get('extraction_method'),
                'source_snippet':    dp.get('source_snippet'),
                'is_finalized':      (period, metric) in finalized_keys,
                'is_pending':        False,
            }

        # Merge PENDING review_queue items into pivot.
        # These are extractions awaiting analyst approval — visible in the timeline
        # with a distinct 'review_pending' extraction_method so the user can see
        # which periods have candidate data that needs review.
        # Monthly items have period = YYYY-MM-DD (from report_date).
        # Quarterly items have period = YYYY-Qn / YYYY-FY (from covering_period).
        pending_rq = db.get_review_items(ticker=ticker_upper, status='PENDING', limit=100000)
        for rq in pending_rq:
            rq_period = rq.get('period') or ''
            is_quarterly_period = bool(_QUARTERLY_PERIOD_RE.match(rq_period))
            if source == 'sec' and not is_quarterly_period:
                continue
            if source == 'monthly' and is_quarterly_period:
                continue
            if source == 'monthly' and len(rq_period) >= 10:
                rq_period = rq_period[:7] + '-01'
            metric = rq.get('metric')
            if not metric or not rq_period:
                continue
            rq_value = (
                rq.get('llm_value') if rq.get('llm_value') is not None
                else rq.get('regex_value')
            )
            if rq_period not in pivot:
                pivot[rq_period] = {}
            if metric not in pivot[rq_period]:
                pivot[rq_period][metric] = {
                    'value':             rq_value,
                    'unit':              None,
                    'confidence':        rq.get('confidence') or 0.0,
                    'extraction_method': 'review_pending',
                    'is_finalized':      False,
                    'is_pending':        True,
                }

        empty_keys = CORE_METRICS[:]
        if not pivot:
            try:
                _empty_schema = db.get_metric_schema(sector='BTC-miners', active_only=True)
            except Exception:
                _empty_schema = []
            _e_labels = {r['key']: r['label'] for r in _empty_schema} if _empty_schema else METRIC_LABELS
            _e_units  = {r['key']: r.get('unit', '') for r in _empty_schema} if _empty_schema else METRIC_UNITS
            return jsonify({
                'success': True,
                'data': {
                    'ticker': ticker_upper,
                    'source': source,
                    'company': {
                        'ticker': company['ticker'],
                        'name': company['name'],
                        'ir_url': company.get('ir_url'),
                        'pr_base_url': company.get('pr_base_url'),
                        'cik': company.get('cik'),
                    },
                    'stats': {
                        'total_periods': 0,
                        'gap_periods': 0,
                        'first_period': None,
                        'last_period': None,
                    },
                    'metric_keys': empty_keys,
                    'metric_labels': {k: _e_labels.get(k, k) for k in empty_keys},
                    'metric_units': {k: _e_units.get(k, '') for k in empty_keys},
                    'rows': [],
                },
            })

        # Build report lookup: period key → {id, report_date, source_type, source_url}
        # Indexed by YYYY-MM (from report_date) for monthly source, and also by
        # covering_period (e.g. "2025-Q1", "2025-FY") for SEC source so that
        # SEC spine rows can find their corresponding report.
        report_by_period: dict = {}
        with db._get_connection() as conn:
            report_rows = conn.execute(
                """SELECT id, report_date, source_type, source_url, covering_period
                   FROM reports
                   WHERE ticker = ?
                   ORDER BY report_date""",
                (ticker_upper,),
            ).fetchall()
        for row in report_rows:
            rd = row[1] or ''
            ym = rd[:7]  # YYYY-MM
            entry = {
                'id':          row[0],
                'report_date': row[1],
                'source_type': row[2],
                'source_url':  row[3],
            }
            report_by_period[ym] = entry
            # Also index by covering_period for SEC rows ("2025-Q1", "2025-FY", etc.)
            covering = row[4]
            if covering and covering != ym:
                report_by_period[covering] = entry

        # Fetch metric_schema as SSOT for labels and units.
        # Falls back to hardcoded dicts if DB table is empty.
        try:
            schema_rows = db.get_metric_schema(sector='BTC-miners', active_only=True)
        except Exception:
            schema_rows = []
        if schema_rows:
            _label_map = {r['key']: r['label'] for r in schema_rows}
            _unit_map  = {r['key']: r.get('unit', '') for r in schema_rows}
        else:
            _label_map = METRIC_LABELS
            _unit_map  = METRIC_UNITS

        # Determine metric_keys: always include CORE_METRICS plus any metric with data.
        metrics_with_data: set = set()
        for period_data in pivot.values():
            metrics_with_data.update(period_data.keys())
        core_set = set(CORE_METRICS)
        metric_keys = [m for m in ALL_METRICS_ORDER
                       if m in core_set or m in metrics_with_data]
        if not metric_keys:
            metric_keys = CORE_METRICS[:]

        all_periods_sorted = sorted(pivot.keys())
        min_period = all_periods_sorted[0]
        max_period = all_periods_sorted[-1]

        # Build spine
        if source == 'monthly':
            spine = _build_monthly_spine(min_period, max_period)
        else:
            # SEC: no padding — only periods with actual data
            spine = all_periods_sorted

        # Build rows
        rows = []
        for period in spine:
            # Report lookup key: monthly uses YYYY-MM prefix; SEC uses the full
            # covering_period string (e.g. "2025-Q1") since that was indexed above.
            if source == 'monthly':
                period_ym = period[:7]
            else:
                period_ym = period  # covering_period key already indexed in report_by_period

            metrics_data = pivot.get(period, {})
            metric_cells = {metric: metrics_data.get(metric) for metric in metric_keys}

            # A period is a gap only if no core metric has any value (accepted or pending)
            is_gap = all(metric_cells.get(m) is None for m in CORE_METRICS)

            report_info = report_by_period.get(period_ym)
            has_report = report_info is not None

            if source == 'sec':
                period_label = _period_label_sec(period)
            else:
                period_label = period[:7]

            rows.append({
                'period':      period,
                'period_label': period_label,
                'has_report':  has_report,
                'report_id':   report_info['id'] if report_info else None,
                'report_date': report_info['report_date'] if report_info else None,
                'source_type': report_info['source_type'] if report_info else None,
                'source_url':  report_info['source_url'] if report_info else None,
                'is_gap':      is_gap,
                'is_reviewed': period in reviewed_set,
                'metrics':     metric_cells,
            })

        # Sort descending (most recent first)
        from routes.data_points import _period_sort_key
        rows.sort(key=lambda r: _period_sort_key(r['period']), reverse=True)

        total_periods = len(spine)
        gap_periods = sum(1 for r in rows if r['is_gap'])

        return jsonify({
            'success': True,
            'data': {
                'ticker':  ticker_upper,
                'source':  source,
                'company': {
                    'ticker':      company['ticker'],
                    'name':        company['name'],
                    'ir_url':      company.get('ir_url'),
                    'pr_base_url': company.get('pr_base_url'),
                    'cik':         company.get('cik'),
                },
                'stats': {
                    'total_periods': total_periods,
                    'gap_periods':   gap_periods,
                    'first_period':  min_period[:7],
                    'last_period':   max_period[:7],
                },
                'metric_keys':    metric_keys,
                'metric_labels':  {k: _label_map.get(k, k) for k in metric_keys},
                'metric_units':   {k: _unit_map.get(k, '') for k in metric_keys},
                'rows':           rows,
            },
        })

    except Exception as e:
        log.error("get_miner_timeline failed for %s: %s", ticker, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/miner/<ticker>/<period>/analysis')
def get_miner_analysis(ticker: str, period: str):
    """
    Return stored LLM extraction results for this ticker+period.

    Reads accepted data_points and pending review_queue rows — does NOT
    re-run regex patterns live. This reflects what the LLM actually extracted
    and stored, not what current patterns would match.

    Response shape:
      {
        "success": true,
        "data": {
          "ticker": "MARA",
          "period": "2022-07-01",
          "has_source": bool,
          "matches": [
            {
              "metric": "production_btc",
              "metric_label": "Production BTC",
              "pattern_id": "llm_qwen3.5:35b",   // extraction_method
              "regex": null,
              "value": 742.0,
              "unit": "BTC",
              "confidence": 0.91,
              "source_snippet": "mined 742 bitcoin during...",
              "status": "accepted"                // or "pending_review"
            }
          ]
        }
      }
    """
    try:
        db = get_db()
        ticker_upper = ticker.upper()
        company = db.get_company(ticker_upper)
        if company is None:
            return jsonify({'success': False, 'error': {'message': f'Unknown ticker: {ticker}'}}), 404

        # Normalize period: accept YYYY-MM or YYYY-MM-DD
        if len(period) == 7:
            period_normalized = period + '-01'
        else:
            period_normalized = period[:10]

        has_source = db.find_report_for_period(ticker_upper, period_normalized) is not None

        all_matches = []

        # Accepted data points
        accepted = db.query_data_points(ticker=ticker_upper, from_period=period_normalized, to_period=period_normalized)
        for dp in accepted:
            all_matches.append({
                'metric': dp['metric'],
                'metric_label': METRIC_LABELS.get(dp['metric'], dp['metric']),
                'pattern_id': dp.get('extraction_method') or '—',
                'regex': None,
                'value': dp['value'],
                'unit': dp.get('unit', ''),
                'confidence': dp.get('confidence', 0.0),
                'source_snippet': dp.get('source_snippet', ''),
                'status': 'accepted',
            })

        # Pending review queue items (not yet accepted)
        accepted_metrics = {m['metric'] for m in all_matches}
        with db._get_connection() as conn:
            rq_rows = conn.execute(
                "SELECT metric, llm_value, regex_value, confidence, source_snippet, agreement_status "
                "FROM review_queue WHERE ticker=? AND period=? AND status='PENDING'",
                (ticker_upper, period_normalized),
            ).fetchall()
        for row in rq_rows:
            metric = row[0]
            if metric in accepted_metrics:
                continue  # already shown in accepted
            value = row[1] if row[1] is not None else row[2]
            all_matches.append({
                'metric': metric,
                'metric_label': METRIC_LABELS.get(metric, metric),
                'pattern_id': row[5] or 'pending',
                'regex': None,
                'value': value,
                'unit': '',
                'confidence': row[3] or 0.0,
                'source_snippet': row[4] or '',
                'status': 'pending_review',
            })

        # Sort: metric order first, then confidence descending
        metric_order = {m: i for i, m in enumerate(ALL_METRICS_ORDER)}
        all_matches.sort(key=lambda m: (metric_order.get(m['metric'], 99), -m['confidence']))

        return jsonify({
            'success': True,
            'data': {
                'ticker': ticker_upper,
                'period': period_normalized,
                'has_source': has_source,
                'matches': all_matches,
            },
        })

    except Exception as e:
        log.error("get_miner_analysis failed for %s/%s: %s", ticker, period, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


def _find_report_for_raw(db, ticker_upper: str, period: str):
    period_normalized = (period + '-01') if len(period) == 7 else period[:10]
    return db.find_report_for_period(ticker_upper, period_normalized)


@bp.route('/api/miner/<ticker>/<period>/raw-source')
def get_miner_raw_source(ticker: str, period: str):
    """Serve raw HTML for a report period — used by the inline iframe and the
    'Rendered' new-tab button.  Prefers raw_html; falls back to raw_text."""
    try:
        db = get_db()
        ticker_upper = ticker.upper()
        if db.get_company(ticker_upper) is None:
            return jsonify({'success': False, 'error': {'message': 'Unknown ticker'}}), 404

        report_info = _find_report_for_raw(db, ticker_upper, period)
        if report_info is None:
            return jsonify({'success': False, 'error': {'message': 'No report for this period'}}), 404

        content = db.get_report_raw_html(report_info['id']) or db.get_report_raw_text(report_info['id'])
        if not content:
            return jsonify({'success': False, 'error': {'message': 'Report has no stored content'}}), 404

        return Response(content, mimetype='text/html; charset=utf-8')

    except Exception as e:
        log.error("get_miner_raw_source failed for %s/%s: %s", ticker, period, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/miner/<ticker>/<period>/raw-text')
def get_miner_raw_text(ticker: str, period: str):
    """Serve plain text for a report period — used by the highlight panel.

    When raw_html is available, extracts clean text from it (giving better
    quality than the 50 k-truncated raw_text stored at ingest time).
    Falls back to raw_text when raw_html is absent.
    """
    try:
        db = get_db()
        ticker_upper = ticker.upper()
        if db.get_company(ticker_upper) is None:
            return jsonify({'success': False, 'error': {'message': 'Unknown ticker'}}), 404

        report_info = _find_report_for_raw(db, ticker_upper, period)
        if report_info is None:
            return jsonify({'success': False, 'error': {'message': 'No report for this period'}}), 404

        raw_html = db.get_report_raw_html(report_info['id'])
        if raw_html:
            from infra.text_utils import html_to_plain
            content = html_to_plain(raw_html)
        else:
            content = db.get_report_raw_text(report_info['id'])

        if not content:
            return jsonify({'success': False, 'error': {'message': 'Report has no stored content'}}), 404

        source_type = report_info.get('source_type', '')
        if source_type in ('ir_press_release', 'wire_press_release'):
            from infra.text_utils import strip_press_release_boilerplate
            content = strip_press_release_boilerplate(content)

        return Response(content, mimetype='text/plain; charset=utf-8')

    except Exception as e:
        log.error("get_miner_raw_text failed for %s/%s: %s", ticker, period, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/miner/<ticker>/<period>/<metric>/fill', methods=['POST'])
def fill_miner_data_point(ticker: str, period: str, metric: str):
    """
    Manually insert a value into data_points, bypassing the review queue.

    Confidence is set to 1.0 (manual entry always wins). Uses INSERT OR REPLACE
    semantics so an existing value for the same ticker+period+metric is overwritten.

    Body: {"value": <positive finite float>, "note": "<optional string>"}

    Returns: {"success": true, "data": {"ticker", "period", "metric", "value"}}
    """
    try:
        db = get_db()
        ticker_upper = ticker.upper()

        # Validate company
        if db.get_company(ticker_upper) is None:
            return jsonify({'success': False, 'error': {'message': f'Unknown ticker: {ticker}'}}), 404

        # Validate metric
        if metric not in METRIC_UNITS:
            return jsonify({'success': False, 'error': {'message': f'Unknown metric: {metric}'}}), 400

        # Normalize period
        if len(period) == 7:
            period_normalized = period + '-01'
        else:
            period_normalized = period[:10]

        # Parse and validate value
        body = request.get_json(silent=True) or {}
        raw_value = body.get('value')
        if raw_value is None:
            return jsonify({'success': False, 'error': {'message': 'Missing required field: value'}}), 400
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': {'message': 'value must be a number'}}), 400
        if not math.isfinite(value) or value <= 0:
            return jsonify({'success': False, 'error': {'message': 'value must be a positive finite number'}}), 400

        note = str(body.get('note', '') or '')

        db.insert_data_point({
            'report_id': None,
            'ticker': ticker_upper,
            'period': period_normalized,
            'metric': metric,
            'value': value,
            'unit': METRIC_UNITS.get(metric, ''),
            'confidence': 1.0,
            'extraction_method': 'manual',
            'source_snippet': note or None,
        })

        log.info("Manual fill: %s/%s/%s = %s", ticker_upper, period_normalized, metric, value)
        return jsonify({
            'success': True,
            'data': {
                'ticker': ticker_upper,
                'period': period_normalized,
                'metric': metric,
                'value': value,
            },
        })

    except Exception as e:
        log.error("fill_miner_data_point failed for %s/%s/%s: %s", ticker, period, metric, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/metrics')
def get_all_metrics():
    """Return all registered metrics with labels and units, in canonical order."""
    metrics = [
        {'key': k, 'label': METRIC_LABELS.get(k, k), 'unit': METRIC_UNITS.get(k, '')}
        for k in ALL_METRICS_ORDER
    ]
    return jsonify({'success': True, 'data': {'metrics': metrics}})


@bp.route('/api/miner/<ticker>/coverage_summary')
def get_coverage_summary(ticker: str):
    """Return report and extraction coverage summary for a ticker.

    Response shape:
      {
        "success": true,
        "data": {
          "monthly": {
            "total_reports": N,
            "extracted": N,
            "earliest": "YYYY-MM",
            "latest": "YYYY-MM",
            "by_source": {"archive_pdf": N, ...}
          },
          "sec": {
            "total_reports": N,
            "extracted": N,
            "earliest": "YYYY-Qn",
            "latest": "YYYY-Qn",
            "by_source": {"edgar_10q": N, ...}
          }
        }
      }
    """
    try:
        db = get_db()
        ticker_upper = ticker.upper()
        company = db.get_company(ticker_upper)
        if company is None:
            return jsonify({'success': False, 'error': {'message': f'Unknown ticker: {ticker}'}}), 404

        _SEC_SOURCE_TYPES = {'edgar_10q', 'edgar_10k', 'edgar_8k', 'edgar_20f', 'edgar_40f', 'edgar_6k'}

        with db._get_connection() as conn:
            report_rows = conn.execute(
                """SELECT id, report_date, source_type, extracted_at, covering_period
                   FROM reports
                   WHERE ticker = ?
                   ORDER BY report_date""",
                (ticker_upper,),
            ).fetchall()

        monthly_reports = []
        sec_reports = []
        for row in report_rows:
            st = (row[2] or '').lower()
            entry = {
                'id':              row[0],
                'report_date':     row[1] or '',
                'source_type':     row[2],
                'extracted':       row[3] is not None,
                'covering_period': row[4],
            }
            if st in _SEC_SOURCE_TYPES:
                sec_reports.append(entry)
            else:
                monthly_reports.append(entry)

        def _summarize(entries, period_key='report_date'):
            if not entries:
                return {
                    'total_reports': 0,
                    'extracted': 0,
                    'earliest': None,
                    'latest': None,
                    'by_source': {},
                }
            total = len(entries)
            extracted = sum(1 for e in entries if e['extracted'])
            dates = [e[period_key] for e in entries if e.get(period_key)]
            earliest = min(dates)[:7] if dates else None
            latest = max(dates)[:7] if dates else None
            by_source: dict = {}
            for e in entries:
                st = e.get('source_type') or 'unknown'
                by_source[st] = by_source.get(st, 0) + 1
            return {
                'total_reports': total,
                'extracted': extracted,
                'earliest': earliest,
                'latest': latest,
                'by_source': by_source,
            }

        # For SEC, prefer covering_period for earliest/latest when available
        sec_summary = _summarize(sec_reports, period_key='report_date')
        if sec_reports:
            covering_periods = [e['covering_period'] for e in sec_reports if e.get('covering_period')]
            if covering_periods:
                sec_summary['earliest'] = min(covering_periods)
                sec_summary['latest'] = max(covering_periods)

        return jsonify({
            'success': True,
            'data': {
                'monthly': _summarize(monthly_reports),
                'sec': sec_summary,
            },
        })

    except Exception as e:
        log.error("get_coverage_summary failed for %s: %s", ticker, e, exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500
