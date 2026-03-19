"""
Pure health check functions for per-ticker QC analysis.

Exported:
    run_ticker_health_check(db, ticker, months=24) -> dict
"""
import logging
from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Optional

log = logging.getLogger('miners.interpreters.qc_check')


def _month_range_from(start_period: str, end_period: str) -> list:
    """Return list of YYYY-MM-01 strings from start_period to end_period (inclusive)."""
    try:
        start = datetime.strptime(start_period[:10], '%Y-%m-%d').date().replace(day=1)
    except ValueError:
        return []
    today = date.today().replace(day=1)
    try:
        end = datetime.strptime(end_period[:10], '%Y-%m-%d').date().replace(day=1)
    except ValueError:
        end = today
    result = []
    d = start
    while d <= end:
        result.append(d.strftime('%Y-%m-01'))
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)
    return result


def run_ticker_health_check(db, ticker: str, months: int = 24) -> dict:
    """Run four health checks for a ticker and return a health card dict.

    Checks:
        outliers          — values deviating >2x from trailing 3-point average
        coverage_gaps     — missing monthly periods (monthly-cadence companies only)
        stuck_queue       — review_queue flooded with LLM_EMPTY entries
        extraction_backlog — pending/failed/orphaned-running reports
    """
    from interpreters.outlier import detect_outlier

    generated_at = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')

    # ── Check 1: Outlier scan ────────────────────────────────────────────────
    outliers = []
    try:
        data_points = db.query_data_points(ticker=ticker, limit=500)
        by_metric: dict = defaultdict(list)
        for dp in data_points:
            period = dp.get('period', '') or ''
            # Skip quarterly periods
            if 'Q' in period:
                continue
            try:
                val = float(dp.get('value') or 0)
            except (TypeError, ValueError):
                continue
            by_metric[dp.get('metric')].append((period, val))

        for metric, entries in by_metric.items():
            entries.sort(key=lambda x: x[0])
            values = [v for _, v in entries]
            for i, (period, val) in enumerate(entries):
                trailing = values[max(0, i - 3):i]
                is_outlier, trailing_avg = detect_outlier(
                    val, trailing, threshold_pct=2.0, min_history=2
                )
                if is_outlier and trailing_avg is not None:
                    deviation_pct = abs(val - trailing_avg) / max(abs(trailing_avg), 1e-9)
                    outliers.append({
                        'period': period,
                        'metric': metric,
                        'value': val,
                        'trailing_avg': round(trailing_avg, 4),
                        'deviation_pct': round(deviation_pct, 4),
                    })
    except Exception:
        log.warning("qc_check outlier scan failed for ticker=%s", ticker, exc_info=True)

    # ── Check 2: Coverage gap scan ───────────────────────────────────────────
    coverage_gaps: dict = {
        'expected_periods': 0,
        'actual_periods': 0,
        'gap_ratio': 0.0,
        'missing_periods': [],
    }
    try:
        company = db.get_company(ticker)
        cadence = (company or {}).get('reporting_cadence', 'monthly')
        if cadence == 'monthly':
            all_dps = db.query_data_points(ticker=ticker, limit=2000)
            monthly_periods: set = set()
            for dp in all_dps:
                period = dp.get('period', '') or ''
                if period and 'Q' not in period:
                    monthly_periods.add(period[:10])

            if monthly_periods:
                earliest = min(monthly_periods)
                today_str = date.today().replace(day=1).strftime('%Y-%m-01')
                expected = _month_range_from(earliest, today_str)
                actual_normalized: set = set()
                for p in monthly_periods:
                    try:
                        d = datetime.strptime(p[:10], '%Y-%m-%d').date().replace(day=1)
                        actual_normalized.add(d.strftime('%Y-%m-01'))
                    except ValueError:
                        pass
                missing = sorted(set(expected) - actual_normalized)
                coverage_gaps = {
                    'expected_periods': len(expected),
                    'actual_periods': len(actual_normalized),
                    'gap_ratio': round(len(missing) / len(expected), 4) if expected else 0.0,
                    'missing_periods': missing,
                }
    except Exception:
        log.warning("qc_check coverage scan failed for ticker=%s", ticker, exc_info=True)

    # ── Check 3: Stuck queue ─────────────────────────────────────────────────
    stuck_queue: dict = {
        'total_pending': 0,
        'llm_empty_count': 0,
        'flagged': False,
    }
    try:
        stats = db.get_review_queue_stats(ticker)
        llm_empty_count = stats.get('llm_empty_count', 0)
        stuck_queue = {
            'total_pending': stats.get('total_pending', 0),
            'llm_empty_count': llm_empty_count,
            'flagged': llm_empty_count > 50,
        }
    except Exception:
        log.warning("qc_check stuck queue scan failed for ticker=%s", ticker, exc_info=True)

    # ── Check 4: Extraction backlog ──────────────────────────────────────────
    extraction_backlog: dict = {
        'pending': 0,
        'failed': 0,
        'orphaned_running': 0,
    }
    try:
        reports = db.search_reports(ticker=ticker)
        extraction_backlog = {
            'pending': sum(1 for r in reports if r.get('extraction_status') == 'pending'),
            'failed': sum(1 for r in reports if r.get('extraction_status') == 'failed'),
            'orphaned_running': sum(1 for r in reports if r.get('extraction_status') == 'running'),
        }
    except Exception:
        log.warning("qc_check backlog scan failed for ticker=%s", ticker, exc_info=True)

    return {
        'ticker': ticker,
        'generated_at': generated_at,
        'checks': {
            'outliers': outliers,
            'coverage_gaps': coverage_gaps,
            'stuck_queue': stuck_queue,
            'extraction_backlog': extraction_backlog,
        },
    }
