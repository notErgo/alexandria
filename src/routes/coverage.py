"""
Coverage dashboard API routes.

  GET  /api/coverage/summary        — aggregate coverage counts
  GET  /api/coverage/grid           — heatmap grid (?months=36)
  GET  /api/coverage/assets/<ticker>/<period> — cell detail
  GET  /api/coverage/period_trace   — pipeline trace for a (ticker, period) pair
  POST /api/manifest/scan           — scan archive directory
  GET  /coverage                    — render coverage.html
"""
import logging
from pathlib import Path

from flask import Blueprint, jsonify, request, render_template, redirect

log = logging.getLogger('miners.routes.coverage')

bp = Blueprint('coverage', __name__)


def _do_scan(db):
    """Scan archive directory. Extracted for monkeypatching in tests."""
    from config import ARCHIVE_DIR
    from scrapers.manifest_scanner import scan_archive_directory
    return scan_archive_directory(Path(ARCHIVE_DIR), db)


@bp.route('/api/coverage/summary')
def coverage_summary():
    """Return aggregate coverage counts."""
    try:
        from app_globals import get_db
        db = get_db()
        with db._get_connection() as conn:
            total_reports = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
            extracted = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE extracted_at IS NOT NULL"
            ).fetchone()[0]
            pending_extraction = conn.execute(
                "SELECT COUNT(*) FROM reports WHERE extracted_at IS NULL AND raw_text IS NOT NULL AND raw_text <> ''"
            ).fetchone()[0]
            accepted_dp = conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]
            pending_review = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE status='PENDING'"
            ).fetchone()[0]
            manifest_total = conn.execute("SELECT COUNT(*) FROM asset_manifest").fetchone()[0]
            manifest_pending = conn.execute(
                "SELECT COUNT(*) FROM asset_manifest WHERE ingest_state='pending'"
            ).fetchone()[0]
            # Companies with zero data_points
            total_companies = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE active=1"
            ).fetchone()[0]
            companies_with_data = conn.execute(
                "SELECT COUNT(DISTINCT ticker) FROM data_points"
            ).fetchone()[0]
            companies_with_zero = total_companies - companies_with_data

        return jsonify({'success': True, 'data': {
            'total_reports': total_reports,
            'extracted': extracted,
            'pending_extraction': pending_extraction,
            'accepted_data_points': accepted_dp,
            'pending_review': pending_review,
            'manifest_total': manifest_total,
            'manifest_pending': manifest_pending,
            'companies_with_zero_data': companies_with_zero,
        }})
    except Exception:
        log.error('Error in coverage_summary', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/coverage/grid')
def coverage_grid():
    """Return coverage heatmap grid."""
    try:
        months_str = request.args.get('months', '36')
        try:
            months = int(months_str)
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'months' must be an integer between 1 and 120",
            }}), 400
        if not (1 <= months <= 120):
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'months' must be between 1 and 120",
            }}), 400

        from app_globals import get_db
        db = get_db()
        import time
        t0 = time.time()
        grid = db.get_coverage_grid(months=months)
        elapsed = time.time() - t0
        log.info("Coverage grid computed in %.2fs for %d months", elapsed, months)
        return jsonify({'success': True, 'data': {'grid': grid, 'months': months}})
    except Exception:
        log.error('Error in coverage_grid', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/coverage/assets/<ticker>/<period>')
def coverage_assets(ticker: str, period: str):
    """Return manifest entries and reports for a (ticker, period) cell."""
    try:
        from app_globals import get_db
        db = get_db()
        ticker = ticker.upper()
        manifest = db.get_manifest_by_ticker(ticker)
        # Filter to this period
        manifest = [m for m in manifest if m.get('period') == period]
        with db._get_connection() as conn:
            rows = conn.execute(
                "SELECT id, ticker, report_date, source_type, source_url, extracted_at FROM reports WHERE ticker=? AND report_date=?",
                (ticker, period),
            ).fetchall()
        reports = [dict(r) for r in rows]
        return jsonify({'success': True, 'data': {
            'ticker': ticker,
            'period': period,
            'manifest': manifest,
            'reports': reports,
        }})
    except Exception:
        log.error('Error in coverage_assets', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/coverage/period_trace')
def period_trace():
    """Return pipeline trace for a (ticker, period) pair.

    Query params:
      ticker  (required)
      period  (required, YYYY-MM or YYYY-MM-01)
      metric  (optional, filter per_metric to this metric only)
    """
    try:
        from app_globals import get_db
        db = get_db()

        ticker = request.args.get('ticker', '').upper()
        period = request.args.get('period', '')
        metric_filter = request.args.get('metric', None)

        if not ticker or not period:
            return jsonify({'success': False, 'error': {
                'code': 'INVALID_INPUT',
                'message': "'ticker' and 'period' are required",
            }}), 400

        # Normalize period to YYYY-MM-01
        period_ym = period[:7]
        period_db = period_ym + '-01'

        # Fetch report for this ticker+period
        with db._get_connection() as conn:
            report_row = conn.execute(
                """SELECT id, extraction_status, parse_quality, raw_text
                   FROM reports
                   WHERE ticker = ? AND report_date LIKE ?
                   ORDER BY
                     CASE source_type
                       WHEN 'ir_press_release' THEN 3
                       WHEN 'archive_html'     THEN 3
                       WHEN 'archive_pdf'      THEN 3
                       WHEN 'edgar_8k'         THEN 2
                       ELSE 1
                     END DESC,
                     report_date DESC, id DESC
                   LIMIT 1""",
                (ticker, period_ym + '%'),
            ).fetchone()

            manifest_row = conn.execute(
                """SELECT id FROM asset_manifest
                   WHERE ticker = ? AND period = ?
                   LIMIT 1""",
                (ticker, period_db),
            ).fetchone()

            # Fetch active metrics from metric_schema
            metric_rows = conn.execute(
                "SELECT key FROM metric_schema WHERE active = 1 ORDER BY key"
            ).fetchall()
            active_metrics = [r[0] for r in metric_rows]
            if metric_filter:
                active_metrics = [m for m in active_metrics if m == metric_filter]

        has_manifest = manifest_row is not None
        has_report = report_row is not None

        if has_report:
            report_id = report_row['id']
            extraction_status = report_row['extraction_status']
            parse_quality = report_row['parse_quality']
            raw_text = report_row['raw_text'] or ''
            char_count = len(raw_text)
            has_raw_text = bool(raw_text)
        else:
            report_id = None
            extraction_status = None
            parse_quality = None
            char_count = 0
            has_raw_text = False

        # Per-metric breakdown
        per_metric = {}
        any_data_point = False
        any_review_pending = False
        any_llm_empty = False

        for m in active_metrics:
            dp = None
            rq = None
            if has_report:
                with db._get_connection() as conn:
                    dp_row = conn.execute(
                        """SELECT id, value, unit, confidence, extraction_method
                           FROM data_points
                           WHERE ticker = ? AND period LIKE ? AND metric = ?
                           LIMIT 1""",
                        (ticker, period_ym + '%', m),
                    ).fetchone()
                    rq_row = conn.execute(
                        """SELECT id, agreement_status, status, confidence
                           FROM review_queue
                           WHERE ticker = ? AND period LIKE ? AND metric = ?
                             AND status = 'PENDING'
                             AND coalesce(precedence_state, 'active') = 'active'
                           LIMIT 1""",
                        (ticker, period_ym + '%', m),
                    ).fetchone()
                if dp_row:
                    dp = dict(dp_row)
                    any_data_point = True
                if rq_row:
                    rq = dict(rq_row)
                    if rq_row['agreement_status'] == 'LLM_EMPTY':
                        any_llm_empty = True
                    else:
                        any_review_pending = True
            per_metric[m] = {'data_point': dp, 'review_item': rq}

        # Derive keyword_gated: extraction ran, document has text, but no data and no review items
        keyword_gated = bool(
            extraction_status == 'done'
            and char_count > 0
            and not any_data_point
            and not any_review_pending
            and not any_llm_empty
        )

        from coverage_logic import compute_cell_state_v2
        cell_state = compute_cell_state_v2(
            is_analyst_gap=False,
            has_data_point=any_data_point,
            has_review_pending=any_review_pending,
            has_manifest=has_manifest,
            has_parse_error=(parse_quality == 'parse_failed'),
            has_extract_error=(extraction_status == 'done' and not any_data_point),
            has_scraper_error=False,
            has_llm_empty_rq=any_llm_empty,
        )

        return jsonify({'success': True, 'data': {
            'ticker': ticker,
            'period': period_db,
            'has_manifest': has_manifest,
            'has_raw_text': has_raw_text,
            'char_count': char_count,
            'parse_quality': parse_quality,
            'extraction_status': extraction_status,
            'keyword_gated': keyword_gated,
            'has_data_point': any_data_point,
            'has_review_pending': any_review_pending,
            'has_llm_empty_rq': any_llm_empty,
            'cell_state': cell_state,
            'per_metric': per_metric,
        }})
    except Exception:
        log.error('Error in period_trace', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/api/manifest/scan', methods=['POST'])
def manifest_scan():
    """Scan the archive directory and upsert manifest entries."""
    try:
        from app_globals import get_db
        db = get_db()
        result = _do_scan(db)
        return jsonify({'success': True, 'data': {
            'total_found': result.total_found,
            'already_ingested': result.already_ingested,
            'newly_discovered': result.newly_discovered,
            'legacy_undated': result.legacy_undated,
            'failed': result.failed,
            'tickers_scanned': result.tickers_scanned,
            'ticker_counts': result.ticker_counts,
        }})
    except Exception:
        log.error('Error in manifest_scan', exc_info=True)
        return jsonify({'success': False, 'error': {'message': 'Internal server error'}}), 500


@bp.route('/coverage')
def coverage_page():
    """Redirect to unified ops page, registry tab."""
    return redirect('/ops?tab=registry')
