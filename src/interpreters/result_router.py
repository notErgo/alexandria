"""
LLM result routing for the extraction pipeline.

Routes a single metric's LLM result to either data_points (auto-accept)
or review_queue (low-confidence or outlier-flagged).

Public API:
  _apply_llm_result(metric, llm_result, db, report, ...) -> None
  validate_period_granularity(result_granularity, expected_granularity) -> bool
"""
import logging
from typing import Optional

from interpreters.report_text import _clean_for_llm

log = logging.getLogger('miners.interpreters.result_router')

# Protected extraction methods — data points with these methods are never overwritten
_PROTECTED_METHODS = frozenset({
    'analyst', 'analyst_approved', 'review_approved', 'review_edited'
})

# Maximum characters of document text sent to the LLM for outlier self-correction pass.
_LLM_TEXT_MAX_CHARS = 8000


def validate_period_granularity(result_granularity: Optional[str], expected_granularity: str) -> bool:
    """Return True if result_granularity is acceptable for expected_granularity.

    None and 'unknown' always pass — the LLM did not label the period.
    Monthly expectation rejects quarterly and annual results.
    Quarterly expectation rejects annual results.
    Annual expectation accepts anything.
    """
    if result_granularity is None or result_granularity == 'unknown':
        return True
    if expected_granularity == 'monthly':
        return result_granularity not in ('quarterly', 'annual')
    if expected_granularity == 'quarterly':
        return result_granularity != 'annual'
    return True


def _build_outlier_concern(metric: str, llm_value: float, trailing_avg) -> str:
    """Build a human-readable concern string for outlier correction prompts."""
    if trailing_avg is not None:
        return (
            f"The extracted value {llm_value} for {metric} deviates significantly "
            f"from this company's trailing average of {trailing_avg:.4f}. "
            f"Re-read the document and verify that you captured the company's current-period "
            f"value rather than a network-wide, quarterly, or year-to-date figure."
        )
    return (
        f"The extracted value {llm_value} for {metric} was flagged as suspicious. "
        f"Re-read the document and verify the exact current-period value."
    )


def _apply_llm_result(
    metric: str,
    llm_result,
    db,
    report: dict,
    confidence_threshold: float,
    summary,
    attribution: Optional[str] = None,
    llm_interpreter=None,
    metric_rule: Optional[dict] = None,
    run_config=None,
) -> None:
    """Apply LLM-only extraction routing for one metric in one report."""
    ticker = report['ticker']
    period_str = report['report_date']
    report_id = report['id']

    existing = db.data_point_exists(ticker, period_str, metric)
    if existing:
        with db._get_connection() as conn:
            row = conn.execute(
                "SELECT extraction_method FROM data_points WHERE ticker=? AND period=? AND metric=?",
                (ticker, period_str, metric),
            ).fetchone()
            if row and row[0] in _PROTECTED_METHODS:
                log.debug("Skipping analyst-protected %s %s %s", ticker, period_str, metric)
                return

    _expected_grain = run_config.expected_granularity if run_config is not None else 'monthly'
    if llm_result is not None:
        _result_grain = getattr(llm_result, 'period_granularity', None)
        if not validate_period_granularity(_result_grain, _expected_grain):
            log.warning(
                "event=temporal_reject ticker=%s period=%s metric=%s "
                "result_grain=%r expected=%r snippet=%r",
                ticker, period_str, metric,
                _result_grain, _expected_grain,
                (llm_result.source_snippet or '')[:60],
            )
            summary.temporal_rejects += 1
            llm_result = None

    if llm_result is None:
        return

    _time_grain = db._derive_time_grain(period_str)
    active_rule = metric_rule if (metric_rule and metric_rule.get('enabled', 1)) else None

    from config import OUTLIER_THRESHOLDS, OUTLIER_MIN_HISTORY
    from interpreters.outlier import detect_outlier

    outlier_threshold = (
        active_rule['outlier_threshold']
        if active_rule and 'outlier_threshold' in active_rule
        else OUTLIER_THRESHOLDS.get(metric, 1.0)
    )
    history_limit = (
        active_rule.get('outlier_min_history', OUTLIER_MIN_HISTORY)
        if active_rule else OUTLIER_MIN_HISTORY
    )
    trailing_vals: list = []
    is_outlier = False
    trailing_avg = None
    try:
        trailing_rows = db.get_trailing_data_points(
            ticker, period_str, metric, limit=history_limit
        )
        trailing_vals = [r['value'] for r in trailing_rows]
        is_outlier, trailing_avg = detect_outlier(
            llm_result.value, trailing_vals, outlier_threshold
        )
    except Exception as _oe:
        log.debug("Outlier check failed for %s %s %s (non-fatal): %s",
                  ticker, period_str, metric, _oe)

    if is_outlier:
        log.info(
            "Outlier flagged: %s %s %s = %.4f (trailing_avg=%.4f, threshold=%.0f%%)",
            ticker, period_str, metric, llm_result.value,
            trailing_avg, outlier_threshold * 100,
        )
        raw_text = report.get('raw_text') or ''
        if raw_text and llm_interpreter is not None:
            try:
                concern = _build_outlier_concern(metric, llm_result.value, trailing_avg)
                llm_text = _clean_for_llm(raw_text)[:_LLM_TEXT_MAX_CHARS]
                corrected = llm_interpreter.extract_with_correction(
                    llm_text, metric, llm_result.value, concern, ticker=ticker
                )
                if corrected is not None and corrected.value is not None:
                    _outlier_ok = True
                    if trailing_vals:
                        _is_out, _ = detect_outlier(
                            corrected.value, trailing_vals, outlier_threshold
                        )
                        _outlier_ok = not _is_out
                    llm_result = corrected
                    is_outlier = not _outlier_ok
            except Exception as _corr_err:
                log.debug("Self-correction pass failed (non-fatal): %s", _corr_err)

    if llm_result.confidence >= confidence_threshold and not is_outlier:
        db.insert_data_point({
            "report_id": report_id,
            "ticker": ticker,
            "period": period_str,
            "metric": metric,
            "value": llm_result.value,
            "unit": llm_result.unit,
            "confidence": llm_result.confidence,
            "extraction_method": attribution or llm_result.extraction_method,
            "source_snippet": llm_result.source_snippet,
            "expected_granularity": _expected_grain,
            "time_grain": _time_grain,
        })
        summary.data_points_extracted += 1
        return

    db.insert_review_item({
        "report_id": report_id,
        "data_point_id": None,
        "ticker": ticker,
        "period": period_str,
        "metric": metric,
        "raw_value": str(llm_result.value),
        "confidence": llm_result.confidence,
        "source_snippet": llm_result.source_snippet,
        "status": "PENDING",
        "llm_value": llm_result.value,
        "regex_value": None,
        "agreement_status": "OUTLIER_FLAGGED" if is_outlier else "LLM_ONLY",
        "expected_granularity": _expected_grain,
        "time_grain": _time_grain,
    })
    summary.review_flagged += 1
