"""
Coverage dashboard API routes.

  GET  /api/coverage/summary        — aggregate coverage counts
  GET  /api/coverage/grid           — heatmap grid (?months=36)
  GET  /api/coverage/assets/<ticker>/<period> — cell detail
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
