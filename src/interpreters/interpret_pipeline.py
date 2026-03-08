"""
Extraction pipeline: LLM+regex+agreement on a single stored report.

Public API:
  extract_report(report, db, registry) -> ExtractionSummary

This module owns all LLM+regex+agreement logic. Every ingestion path
(archive, IR, EDGAR) stores raw text in the reports table, then calls
extract_report to do the actual extraction. This decouples fetch+store
from extraction, enabling re-extraction without re-scraping.

Analyst protection: rows with extraction_method IN
  ('analyst', 'analyst_approved', 'review_approved', 'review_edited')
are never overwritten by the pipeline.
"""
import logging
import re
import threading
from typing import Optional

from interpreters.context_window import ContextWindowSelector

log = logging.getLogger('miners.interpreters.interpret_pipeline')

# Protected extraction methods — data points with these methods are never overwritten
_PROTECTED_METHODS = frozenset({
    'analyst', 'analyst_approved', 'review_approved', 'review_edited'
})

# Module-level LLM extractor singleton (lazy init — avoids import at module load)
_llm_interpreter_instance = None
_llm_interpreter_lock = threading.Lock()

# Cached connectivity result — checked once at batch start, reused for 5 min.
# Avoids 2 HTTP round-trips to Ollama on every report in a batch run.
# The cache is keyed by extractor identity (id()) so that test mocks always
# bypass the cached result for the real singleton (and vice versa).
import time as _time
_llm_available_cache: bool = False
_llm_available_cache_time: float = 0.0
_llm_available_cache_extractor_id: int = -1   # id() of the cached extractor
_llm_cache_lock = threading.Lock()            # separate lock from the singleton lock
_LLM_AVAILABLE_CACHE_TTL: float = 300.0      # seconds

# Maximum characters of document text sent to the LLM per metric call.
# Monthly production press releases have all key figures in the first ~5 paragraphs.
# Truncating to 8 000 chars (~2 000 tokens) keeps prefill time on Apple Silicon
# to ~20–30 s — well within the 180 s timeout. Regex extraction still runs on the
# full raw_text, so no data is lost; the LLM just sees a shorter window.
_LLM_TEXT_MAX_CHARS = 8000

# Quarterly/annual filings are larger; send up to 40K chars to cover MD&A section.
_LLM_QUARTERLY_TEXT_MAX_CHARS = 40_000

# Source types that follow the quarterly/annual extraction path
_QUARTERLY_SOURCES = frozenset({'edgar_10q', 'edgar_6k'})
_ANNUAL_SOURCES    = frozenset({'edgar_10k', 'edgar_20f', 'edgar_40f'})


def _active_metric_keys(db, registry) -> list:
    """Return the list of metric keys to send to the LLM.

    Queries metric_schema for active=1 rows (sector='BTC-miners').
    Falls back to all registry keys if the schema table is empty or unavailable,
    so extraction always runs even on a fresh DB without a seeded schema.
    """
    try:
        rows = db.get_metric_schema('BTC-miners', active_only=True)
        keys = [r['key'] for r in rows] if rows else []
        if keys:
            return keys
    except Exception as e:
        log.warning("Could not load active metric schema — falling back to registry: %s", e)
    return list(registry.metrics.keys())

# Sentinel phrases that mark the start of boilerplate sections.
# Only stripped when the match falls at or after the 40% mark of the document,
# preventing false positives from titles like "Forward-Looking Statements Disclosure".
_BOILERPLATE_SENTINELS = [
    re.compile(r'\bFORWARD.LOOKING\s+STATEMENTS?\b', re.IGNORECASE),
    re.compile(r'\bSAFE\s+HARBOR\s+STATEMENTS?\b', re.IGNORECASE),
    re.compile(r'\bCAUTIONARY\s+STATEMENTS?\b', re.IGNORECASE),
    re.compile(r'\bNON.GAAP\s+FINANCIAL\s+MEASURE', re.IGNORECASE),
    re.compile(  # canonical-sources: noqa — regex pattern matching company names, not a ticker list
        r'\bABOUT\s+(?:MARATHON|MARA|RIOT|CLEANSPARK|CIPHER|CORE\s+SCIENTIFIC|'
        r'BIT\s+DIGITAL|HIVE|HUT\s+8|ARGO|STRONGHOLD|TERAWULF|IRIS)\b',
        re.IGNORECASE,
    ),
]


def _build_concern_context(decision, metric: str, trailing_avg) -> str:
    """Build a human-readable concern string for the self-correction prompt.

    Called when the agreement engine routes to REVIEW_QUEUE (disagreement or outlier).
    Provides the LLM with specific context about why the first extraction was flagged.
    """
    if getattr(decision, 'outlier_flag', False) and trailing_avg is not None:
        return (
            f"The agreed value {decision.llm_value} for {metric} deviates significantly "
            f"from this company's 3-month trailing average of {trailing_avg:.4f}. "
            f"Did you capture a network-wide, year-to-date, or quarterly figure instead of "
            f"the company's single-month operational value?"
        )
    rv = decision.regex_value
    lv = decision.llm_value
    if rv is not None and lv is not None:
        pct = abs(rv - lv) / max(abs(rv), abs(lv), 1e-9) * 100
        return (
            f"Regex pattern found {metric} = {rv:.4f}, but you returned {lv:.4f} "
            f"({pct:.1f}% difference). "
            f"Re-read the document to determine which value is correct for THIS company's "
            f"current reporting month."
        )
    if rv is None and lv is not None:
        return (
            f"No regex pattern matched {metric} in the document, but you returned {lv:.4f}. "
            f"Verify that this value is explicitly stated in the document for the current month "
            f"and not inferred or approximated."
        )
    return f"First extraction result for {metric} was flagged for review. Please re-read carefully."


def _clean_for_llm(text: str) -> str:
    """Strip boilerplate sections from the back 60%+ of the document.

    Finds the earliest sentinel that appears at or after the 40% mark and
    truncates there. Prevents false positives from titles/headings in the
    document preamble that mention boilerplate topics.
    """
    cutoff = len(text)
    threshold = int(len(text) * 0.4)  # only strip if match is past 40% mark
    for pattern in _BOILERPLATE_SENTINELS:
        m = pattern.search(text)
        if m and m.start() >= threshold:
            cutoff = min(cutoff, m.start())
    return text[:cutoff].rstrip()


def _prior_period(period_str: str) -> Optional[str]:
    """Return YYYY-MM-01 for the month before period_str; None if parse fails."""
    try:
        parts = period_str.split('-')
        year, month = int(parts[0]), int(parts[1])
        if month == 1:
            return f"{year - 1}-12-01"
        return f"{year}-{month - 1:02d}-01"
    except Exception:
        return None


def _is_quarterly_doc(report: dict) -> bool:
    """Return True if report.source_type is a quarterly filing (10-Q)."""
    return report.get('source_type') in _QUARTERLY_SOURCES


def _is_annual_doc(report: dict) -> bool:
    """Return True if report.source_type is an annual filing (10-K)."""
    return report.get('source_type') in _ANNUAL_SOURCES


def _get_missing_metrics(db, ticker: str, period: str, all_metrics: list) -> list:
    """Return metrics with no entry in data_points for the given (ticker, period)."""
    return [m for m in all_metrics if not db.data_point_exists(ticker, period, m)]


def _try_gap_fill(
    report: dict,
    db,
    llm_interpreter,
    llm_text: str,
    all_metrics: list,
    confidence_threshold: float,
    summary,
) -> None:
    """Try to fill prior-period gaps via a targeted second-pass LLM call.

    Only fires when:
    (a) prior period exists (report_date is parseable)
    (b) prior period has at least one missing metric in data_points
    (c) LLM returns a result with confidence >= threshold

    Never overwrites existing data.
    """
    period_str = report.get('report_date')
    prior = _prior_period(period_str)
    if prior is None:
        return

    ticker = report.get('ticker')
    missing = _get_missing_metrics(db, ticker, prior, all_metrics)
    if not missing:
        log.debug("Gap fill: all metrics filled for %s %s, skipping", ticker, prior)
        return

    log.info(
        "Gap fill: checking prior period %s for %s (missing: %s)", prior, ticker, missing
    )
    try:
        results = llm_interpreter.extract_for_period(llm_text, missing, period_str, prior)
    except Exception as e:
        log.error("Gap fill failed for %s %s: %s", ticker, prior, e, exc_info=True)
        return

    try:
        _gf_meta = dict(llm_interpreter._last_call_meta)
        from interpreters.llm_interpreter import _active_model
        gf_hits = lambda t: sum(1 for r in results.values() if r is not None and r.confidence >= t)
        db.insert_benchmark_run({
            'model': _active_model(db),
            'call_type': 'gap_fill',
            'ticker': ticker,
            'period': prior,
            'report_id': report.get('id'),
            'prompt_chars': len(llm_text),
            'response_chars': _gf_meta.get('response_chars', 0),
            'prompt_tokens': _gf_meta.get('prompt_tokens', 0),
            'response_tokens': _gf_meta.get('response_tokens', 0),
            'total_duration_ms': _gf_meta.get('total_duration_ms', 0),
            'eval_duration_ms': _gf_meta.get('eval_duration_ms', 0),
            'metrics_requested': len(missing),
            'metrics_extracted': len(results),
            'hits_90': gf_hits(0.90),
            'hits_80': gf_hits(0.80),
            'hits_75': gf_hits(0.75),
        })
    except Exception as _bench_err:
        log.debug("Gap fill benchmark write failed (non-fatal): %s", _bench_err)

    for metric, result in results.items():
        if result is None or result.confidence < confidence_threshold:
            continue
        # Only store if slot is still empty — another extraction may have filled it
        if db.data_point_exists(ticker, prior, metric):
            continue
        db.insert_data_point({
            'report_id': report.get('id'),
            'ticker': ticker,
            'period': prior,
            'metric': metric,
            'value': result.value,
            'unit': result.unit,
            'confidence': result.confidence,
            'extraction_method': result.extraction_method,
            'source_snippet': result.source_snippet,
        })
        summary.data_points_extracted += 1
        log.info(
            "Gap fill: stored %s %s %s = %.4f", ticker, prior, metric, result.value
        )


def _get_llm_interpreter(db):
    """Return a module-level LLMInterpreter singleton, or None if unavailable."""
    global _llm_interpreter_instance
    with _llm_interpreter_lock:
        if _llm_interpreter_instance is None:
            try:
                import requests
                from interpreters.llm_interpreter import LLMInterpreter
                session = requests.Session()
                _llm_interpreter_instance = LLMInterpreter(session=session, db=db)
                log.debug("LLM extractor initialized")
            except Exception as e:
                log.warning("Could not initialize LLM extractor: %s", e)
                return None
        return _llm_interpreter_instance


def _check_llm_available(llm_interpreter) -> bool:
    """Return cached LLM availability, refreshing every 5 min.

    Calling check_connectivity() on every report wastes 2 HTTP round-trips each
    time. This function caches the result for _LLM_AVAILABLE_CACHE_TTL seconds
    so a batch of 50 reports pays the connectivity cost once, not 50 times.

    The cache is keyed by extractor identity (id()) so that test mocks always
    see a fresh check — they are different Python objects from the real singleton.
    """
    global _llm_available_cache, _llm_available_cache_time, _llm_available_cache_extractor_id
    if llm_interpreter is None:
        return False
    now = _time.monotonic()
    extractor_id = id(llm_interpreter)
    with _llm_cache_lock:
        if (extractor_id == _llm_available_cache_extractor_id
                and now - _llm_available_cache_time < _LLM_AVAILABLE_CACHE_TTL):
            return _llm_available_cache
        result = llm_interpreter.check_connectivity()
        _llm_available_cache = result
        _llm_available_cache_time = now
        _llm_available_cache_extractor_id = extractor_id
    return result


def _build_regex_by_metric(text: str, registry, report_date: str = None,
                            metric_rules_by_name: dict = None) -> dict:
    """Run regex extraction for all metrics. Returns best result per metric.

    Passes report_date to extract_all so temporally-scoped patterns are
    filtered to those valid for this report's period. Passes valid_range from
    metric_rules_by_name so DB-configured ceilings override hardcoded defaults.
    """
    from interpreters.regex_interpreter import extract_all
    rules = metric_rules_by_name or {}
    regex_by_metric = {}
    for metric, patterns in registry.metrics.items():
        rule = rules.get(metric)
        valid_range = None
        if rule:
            mn = rule.get('valid_range_min')
            mx = rule.get('valid_range_max')
            if mn is not None and mx is not None:
                valid_range = (float(mn), float(mx))
        results = extract_all(text, patterns, metric, report_date=report_date,
                              valid_range=valid_range)
        if results:
            regex_by_metric[metric] = results[0]  # sorted confidence-desc; first = best
    return regex_by_metric


def _run_llm_batch(llm_interpreter, text: str, metrics: list, ticker: str = None) -> tuple:
    """
    Try batch extraction first (1 Ollama call for all metrics).
    Falls back to per-metric extract() if batch returns empty.

    Returns (results: dict, meta: dict) where meta contains timing info from
    llm_interpreter._last_call_meta (populated by _call_ollama).
    """
    result = llm_interpreter.extract_batch(text, metrics, ticker=ticker)
    meta = dict(llm_interpreter._last_call_meta)
    if result:
        log.info("  LLM batch returned %d/%d metrics", len(result), len(metrics))
        return result, meta
    log.warning("  LLM batch empty — falling back to per-metric (%d calls)", len(metrics))
    fallback = {}
    for metric in metrics:
        r = llm_interpreter.extract(text, metric)
        if r is not None:
            fallback[metric] = r
        # Update meta with the last per-metric call (best-effort; final call wins)
        meta = dict(llm_interpreter._last_call_meta)
    return fallback, meta


def _apply_agreement(
    metric: str,
    regex_best,
    llm_result,
    db,
    report: dict,
    llm_available: bool,
    confidence_threshold: float,
    summary,
    attribution: Optional[str] = None,
    llm_interpreter=None,
    metric_rule: Optional[dict] = None,
) -> None:
    """Apply the agreement engine for one metric in one report. Writes to DB.

    When attribution is set (e.g. 'codex'), it overrides the extraction_method
    stored in data_points so the source agent is recorded. Analyst-protected
    methods are never overwritten regardless.

    llm_interpreter is passed so the self-correction pass can make a targeted
    retry call when disagreement or outlier is detected.
    """
    from interpreters.agreement import evaluate_agreement

    ticker = report['ticker']
    period_str = report['report_date']
    report_id = report['id']

    # Check analyst protection before any write
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

    if llm_available:
        # Use metric_rule overrides only when the rule is enabled
        active_rule = metric_rule if (metric_rule and metric_rule.get('enabled', 1)) else None
        agreement_threshold = active_rule['agreement_threshold'] if active_rule else None
        decision = evaluate_agreement(regex_best, llm_result, metric=metric, threshold=agreement_threshold)

        # Outlier detection: if AUTO_ACCEPT, check against trailing data points
        # (also used by self-correction pass below — initialize here for both uses)
        is_outlier = False
        trailing_vals: list = []
        outlier_threshold = 1.0
        if decision.decision == 'AUTO_ACCEPT' and decision.accepted_value is not None:
            from config import OUTLIER_THRESHOLDS, OUTLIER_MIN_HISTORY
            from interpreters.agreement import detect_outlier
            if active_rule and 'outlier_threshold' in active_rule:
                outlier_threshold = active_rule['outlier_threshold']
            else:
                outlier_threshold = OUTLIER_THRESHOLDS.get(metric, 1.0)
            history_limit = (
                active_rule.get('outlier_min_history', OUTLIER_MIN_HISTORY)
                if active_rule else OUTLIER_MIN_HISTORY
            )
            try:
                trailing_rows = db.get_trailing_data_points(
                    ticker, period_str, metric, limit=history_limit
                )
                trailing_vals = [r['value'] for r in trailing_rows]
                is_outlier, trailing_avg = detect_outlier(
                    decision.accepted_value, trailing_vals, outlier_threshold
                )
            except Exception as _oe:
                log.debug("Outlier check failed for %s %s %s (non-fatal): %s",
                          ticker, period_str, metric, _oe)
                is_outlier, trailing_avg = False, None

            if is_outlier:
                log.info(
                    "Outlier flagged: %s %s %s = %.4f (trailing_avg=%.4f, threshold=%.0f%%)",
                    ticker, period_str, metric, decision.accepted_value,
                    trailing_avg, outlier_threshold * 100,
                )
                decision.decision = 'REVIEW_QUEUE'
                decision.accepted_value = None
                decision.outlier_flag = True
                decision.outlier_trailing_avg = trailing_avg

        # Conditional self-correction pass: if routed to review (disagree or outlier),
        # give the LLM one targeted retry with explicit concern context.
        # Only runs when a raw_text is available on the report dict (always true for
        # monthly reports; quarterly/annual use a different path).
        if decision.decision == 'REVIEW_QUEUE':
            raw_text = report.get('raw_text') or ''
            if raw_text and llm_result is not None and llm_interpreter is not None:
                try:
                    concern = _build_concern_context(decision, metric, trailing_avg if is_outlier else None)
                    llm_text = _clean_for_llm(raw_text)[:_LLM_TEXT_MAX_CHARS]
                    corrected = llm_interpreter.extract_with_correction(
                        llm_text, metric, llm_result.value, concern, ticker=ticker
                    )
                    if corrected is not None and corrected.value is not None:
                        from interpreters.agreement import detect_outlier
                        re_decision = evaluate_agreement(regex_best, corrected, metric=metric)
                        if re_decision.decision == 'AUTO_ACCEPT':
                            # Check outlier again on corrected value
                            _outlier_ok = True
                            if trailing_vals:
                                _is_out, _ = detect_outlier(
                                    re_decision.accepted_value, trailing_vals, outlier_threshold
                                )
                                _outlier_ok = not _is_out
                            if _outlier_ok:
                                log.info(
                                    "Self-correction resolved: %s %s %s = %.4f",
                                    ticker, period_str, metric, re_decision.accepted_value,
                                )
                                db.insert_data_point({
                                    "report_id": report_id,
                                    "ticker": ticker,
                                    "period": period_str,
                                    "metric": metric,
                                    "value": re_decision.accepted_value,
                                    "unit": corrected.unit,
                                    "confidence": corrected.confidence,
                                    "extraction_method": attribution or 'llm_corrected',
                                    "source_snippet": corrected.source_snippet,
                                })
                                summary.data_points_extracted += 1
                                return  # Skip review_queue write below
                        # Correction failed or still outlier — fall through to review_queue
                        decision.llm_value = corrected.value  # store corrected value for analyst
                        log.debug(
                            "Self-correction did not resolve %s %s %s — routing to review",
                            ticker, period_str, metric,
                        )
                except Exception as _corr_err:
                    log.debug("Self-correction pass failed (non-fatal): %s", _corr_err)

        if decision.decision == 'AUTO_ACCEPT':
            db.insert_data_point({
                "report_id": report_id,
                "ticker": ticker,
                "period": period_str,
                "metric": metric,
                "value": regex_best.value,
                "unit": regex_best.unit,
                "confidence": regex_best.confidence,
                "extraction_method": attribution or regex_best.extraction_method,
                "source_snippet": regex_best.source_snippet,
            })
            summary.data_points_extracted += 1

        elif decision.decision == 'LLM_ONLY' and llm_result is not None:
            # LLM found a value but regex found nothing.
            # High confidence → auto-accept directly to data_points.
            # Low confidence → review queue for human check.
            if llm_result.confidence >= confidence_threshold:
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
                })
                summary.data_points_extracted += 1
            else:
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
                    "agreement_status": "LLM_ONLY",
                })
                summary.review_flagged += 1

        elif decision.decision in ('REVIEW_QUEUE', 'REGEX_ONLY'):
            raw_val = str(decision.regex_value or decision.llm_value or "")
            conf = (
                regex_best.confidence if regex_best else
                (llm_result.confidence if llm_result else 0.5)
            )
            snippet = (
                regex_best.source_snippet if regex_best else
                (llm_result.source_snippet if llm_result else "")
            )
            # Use OUTLIER_FLAGGED status when the outlier check fired
            agreement_status = (
                'OUTLIER_FLAGGED' if getattr(decision, 'outlier_flag', False)
                else decision.decision
            )
            db.insert_review_item({
                "report_id": report_id,
                "data_point_id": None,
                "ticker": ticker,
                "period": period_str,
                "metric": metric,
                "raw_value": raw_val,
                "confidence": conf,
                "source_snippet": snippet,
                "status": "PENDING",
                "llm_value": decision.llm_value,
                "regex_value": decision.regex_value,
                "agreement_status": agreement_status,
            })
            summary.review_flagged += 1
        # NO_EXTRACTION → period gap, skip

    else:
        # LLM not available — legacy confidence-threshold routing
        if regex_best is None:
            return
        dp = {
            "report_id": report_id,
            "ticker": ticker,
            "period": period_str,
            "metric": metric,
            "value": regex_best.value,
            "unit": regex_best.unit,
            "confidence": regex_best.confidence,
            "extraction_method": attribution or regex_best.extraction_method,
            "source_snippet": regex_best.source_snippet,
        }
        if regex_best.confidence >= confidence_threshold:
            db.insert_data_point(dp)
            summary.data_points_extracted += 1
        else:
            db.insert_review_item({
                "report_id": report_id,
                "data_point_id": None,
                "ticker": ticker,
                "period": period_str,
                "metric": metric,
                "raw_value": str(regex_best.value),
                "confidence": regex_best.confidence,
                "source_snippet": regex_best.source_snippet,
                "status": "PENDING",
                "llm_value": None,
                "regex_value": regex_best.value,
                "agreement_status": "REGEX_ONLY",
            })
            summary.review_flagged += 1


def _interpret_quarterly_report(
    report: dict,
    db,
    registry,
    summary,
    attribution: Optional[str] = None,
) -> 'ExtractionSummary':
    """Quarterly/annual extraction path: LLM only, wider text window, provenance fields.

    Called automatically by extract_report() when source_type is 'edgar_10q' or 'edgar_10k'.
    Regex extraction is not run for these source types — the LLM receives a wider text window
    and uses quarterly/annual-specific prompts.
    """
    from config import CONFIDENCE_REVIEW_THRESHOLD

    source_type = report.get('source_type', '')
    period_type = 'annual' if source_type in _ANNUAL_SOURCES else 'quarterly'
    ticker = report.get('ticker', '')
    report_id = report.get('id')
    covering_period = report.get('covering_period')

    if not covering_period:
        log.warning(
            "Quarterly report id=%s ticker=%s has no covering_period — skipping",
            report_id, ticker,
        )
        db.mark_report_extracted(report_id)
        summary.errors += 1
        return summary

    _q_selector = ContextWindowSelector(doc_type=report.get('source_type', ''))
    text = (report.get('raw_text') or '')[:_q_selector.char_budget]

    llm_interpreter = _get_llm_interpreter(db)
    llm_available = _check_llm_available(llm_interpreter)

    if not llm_available:
        log.warning(
            "LLM not available for quarterly report id=%s ticker=%s %s — will retry when LLM is up",
            report_id, ticker, covering_period,
        )
        # Transient failure: reset to 'pending' so this process can retry without waiting for next boot.
        db.reset_report_to_pending(report_id)
        return summary

    all_metrics = _active_metric_keys(db, registry) if registry.metrics else []
    if not all_metrics:
        log.warning("No metrics in registry for quarterly extraction %s %s", ticker, covering_period)
        db.mark_report_extraction_failed(report_id, 'no_metrics_in_registry')
        return summary

    try:
        llm_results = llm_interpreter.extract_quarterly_batch(
            text, all_metrics, ticker=ticker, period_type=period_type
        )
    except Exception as e:
        log.error(
            "Quarterly LLM extraction failed for %s %s: %s", ticker, covering_period, e, exc_info=True
        )
        db.mark_report_extraction_failed(report_id, str(e)[:500])
        summary.errors += 1
        return summary

    for metric, result in llm_results.items():
        if result is None:
            continue

        # Check if a quarterly data_point already exists for this covering_period
        existing_q = db.get_quarterly_data_point(ticker, covering_period, metric)
        if existing_q is not None and existing_q.get('confidence', 0) >= result.confidence:
            log.debug(
                "Skipping quarterly %s %s %s — existing DP has equal or higher confidence",
                ticker, covering_period, metric,
            )
            continue

        # Route based on confidence threshold
        if result.confidence >= CONFIDENCE_REVIEW_THRESHOLD:
            dp = {
                'report_id':          report_id,
                'ticker':             ticker,
                'period':             covering_period,  # quarterly row: period = covering_period
                'metric':             metric,
                'value':              result.value,
                'unit':               result.unit,
                'confidence':         result.confidence,
                'extraction_method':  attribution or result.extraction_method,
                'source_snippet':     result.source_snippet,
                'source_period_type': period_type,
                'covering_report_id': report_id,
                'covering_period':    covering_period,
            }
            db.upsert_data_point_quarterly(dp)
            summary.data_points_extracted += 1
            log.debug(
                "Stored %s data_point: %s %s %s = %.4f (conf=%.2f)",
                period_type, ticker, covering_period, metric, result.value, result.confidence,
            )
        else:
            db.insert_review_item({
                'data_point_id':  None,
                'ticker':         ticker,
                'period':         covering_period,
                'metric':         metric,
                'raw_value':      str(result.value),
                'confidence':     result.confidence,
                'source_snippet': result.source_snippet,
                'status':         'PENDING',
                'llm_value':      result.value,
                'regex_value':    None,
                'agreement_status': 'LLM_ONLY',
                'report_id':      report_id,
            })
            summary.review_flagged += 1

    summary.reports_processed += 1
    db.mark_report_extracted(report_id)
    return summary


def extract_report(report: dict, db, registry, attribution: Optional[str] = None) -> 'ExtractionSummary':
    """
    Run LLM+regex+agreement on one stored report.

    Skips analyst-protected (ticker, period, metric) triples. Calls
    db.mark_report_extracted(report['id']) after processing (even on error)
    to prevent infinite re-processing loops.

    Returns ExtractionSummary with counts.
    """
    from miner_types import ExtractionSummary
    from config import CONFIDENCE_REVIEW_THRESHOLD

    summary = ExtractionSummary()

    # Mark in-flight immediately so concurrent workers skip this report.
    # Cleared to 'pending' on startup if the process crashes mid-extraction.
    db.mark_report_extraction_running(report['id'])

    text = report.get('raw_text') or ''

    if not text.strip():
        log.warning(
            "Empty raw_text for report id=%s ticker=%s period=%s",
            report.get('id'), report.get('ticker'), report.get('report_date'),
        )
        summary.errors += 1
        db.mark_report_extracted(report['id'])
        return summary

    try:
        log.info(
            "Extracting report id=%s ticker=%s period=%s source=%s",
            report.get('id'), report.get('ticker'), report.get('report_date'),
            report.get('source_type'),
        )

        source_type = report.get('source_type', '')

        # Route quarterly and annual SEC filings to the dedicated extraction path.
        # These do not use regex extraction — LLM only with wider text window.
        if source_type in _QUARTERLY_SOURCES or source_type in _ANNUAL_SOURCES:
            return _interpret_quarterly_report(report, db, registry, summary, attribution)

        report_date = report.get('report_date')

        # Load per-metric rules before regex so valid_range overrides flow in
        metric_rules_by_name = {}
        try:
            for row in db.get_metric_rules():
                metric_rules_by_name[row['metric']] = row
        except Exception as _mre:
            log.debug("Could not load metric_rules (non-fatal): %s", _mre)

        regex_by_metric = _build_regex_by_metric(
            text, registry, report_date=report_date,
            metric_rules_by_name=metric_rules_by_name,
        )

        llm_interpreter = _get_llm_interpreter(db)
        llm_available = _check_llm_available(llm_interpreter)

        if not llm_available:
            log.debug(
                "Ollama not reachable — using legacy confidence routing for %s %s",
                report.get('ticker'), report_date,
            )

        # Strip boilerplate (FORWARD-LOOKING STATEMENTS, SAFE HARBOR, About [Company],
        # etc.) from the back of the document before sending to LLM. Regex extraction
        # still runs on the full raw_text. Use ContextWindowSelector to budget the window.
        _clean_text = _clean_for_llm(text)
        _ctx_selector = ContextWindowSelector(doc_type=report.get('source_type', ''))
        _ctx_windows = _ctx_selector.select_windows(report['id'], _clean_text, 'production_btc', db)
        llm_text = _ctx_windows[0]['text'] if _ctx_windows else _clean_text[:_LLM_TEXT_MAX_CHARS]

        all_metrics = _active_metric_keys(db, registry)
        ticker = report.get('ticker')

        # Primary batch LLM call — pays prefill once for all metrics on window 0.
        # Falls back to per-metric extract() if batch returns empty.
        llm_by_metric = {}
        if llm_available:
            llm_by_metric, _batch_meta = _run_llm_batch(
                llm_interpreter, llm_text, all_metrics, ticker=ticker
            )
            _batch_summary = getattr(llm_interpreter, '_last_batch_summary', '')
            if _batch_summary:
                try:
                    db.update_report_summary(report['id'], _batch_summary)
                except Exception as _sum_err:
                    log.debug("Summary write failed (non-fatal): %s", _sum_err)
            try:
                from interpreters.llm_interpreter import _active_model
                hits = lambda t: sum(1 for r in llm_by_metric.values() if r.confidence >= t)
                db.insert_benchmark_run({
                    'model': _active_model(db),
                    'call_type': 'batch',
                    'ticker': ticker,
                    'period': report.get('report_date'),
                    'report_id': report.get('id'),
                    'prompt_chars': len(llm_text),
                    'response_chars': _batch_meta.get('response_chars', 0),
                    'prompt_tokens': _batch_meta.get('prompt_tokens', 0),
                    'response_tokens': _batch_meta.get('response_tokens', 0),
                    'total_duration_ms': _batch_meta.get('total_duration_ms', 0),
                    'eval_duration_ms': _batch_meta.get('eval_duration_ms', 0),
                    'metrics_requested': len(all_metrics),
                    'metrics_extracted': len(llm_by_metric),
                    'hits_90': hits(0.90),
                    'hits_80': hits(0.80),
                    'hits_75': hits(0.75),
                })
            except Exception as _bench_err:
                log.debug("Benchmark write failed (non-fatal): %s", _bench_err)

            summary.prompt_tokens += _batch_meta.get('prompt_tokens', 0)
            summary.response_tokens += _batch_meta.get('response_tokens', 0)

            # Per-metric fallback: each metric that still needs higher confidence gets
            # its own select_windows call so the context window is metric-appropriate.
            _needs_fallback = [
                m for m in all_metrics
                if _ctx_selector.needs_fallback(llm_by_metric.get(m), db)
            ]
            for _fb_metric in _needs_fallback:
                _fb_windows = _ctx_selector.select_windows(
                    report['id'], _clean_text, _fb_metric, db
                )
                for _fb_window in _fb_windows[1:]:
                    log.debug(
                        "Fallback window %d: retrying %s for %s %s",
                        _fb_window['window_index'], _fb_metric,
                        ticker, report.get('report_date'),
                    )
                    _fb_results, _ = _run_llm_batch(
                        llm_interpreter, _fb_window['text'], [_fb_metric], ticker=ticker
                    )
                    if _fb_metric in _fb_results and not _ctx_selector.needs_fallback(
                        _fb_results[_fb_metric], db
                    ):
                        llm_by_metric[_fb_metric] = _fb_results[_fb_metric]
                        break

        for metric in all_metrics:
            regex_best = regex_by_metric.get(metric)
            # Skip metrics with no data from either source when LLM not available
            if not llm_available and regex_best is None:
                continue

            _apply_agreement(
                metric=metric,
                regex_best=regex_best,
                llm_result=llm_by_metric.get(metric),
                db=db,
                report=report,
                llm_available=llm_available,
                confidence_threshold=CONFIDENCE_REVIEW_THRESHOLD,
                summary=summary,
                attribution=attribution,
                llm_interpreter=llm_interpreter,
                metric_rule=metric_rules_by_name.get(metric),
            )

        # Second-pass: try to fill prior-period gaps if LLM found figures for last month
        if llm_available:
            _try_gap_fill(
                report, db, llm_interpreter, llm_text, all_metrics,
                CONFIDENCE_REVIEW_THRESHOLD, summary,
            )

        summary.reports_processed += 1

    except Exception as e:
        log.error(
            "Extraction failed for report id=%s ticker=%s: %s",
            report.get('id'), report.get('ticker'), e, exc_info=True,
        )
        summary.errors += 1
        db.mark_report_extraction_failed(report['id'], str(e)[:500])
        return summary

    db.mark_report_extracted(report['id'])

    return summary
