"""
Extraction pipeline: LLM extraction with regex used only as a gate.

Public API:
  extract_report(report, db, registry) -> ExtractionSummary

This module owns report extraction logic. Every ingestion path
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
from config import MONTHLY_EXTRACTION_SOURCE_TYPES

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

# Source types that follow the quarterly/annual extraction path
_QUARTERLY_SOURCES = frozenset({'edgar_10q', 'edgar_6k'})
_ANNUAL_SOURCES    = frozenset({'edgar_10k', 'edgar_20f', 'edgar_40f'})
_MONTHLY_SOURCE_TYPES = frozenset(MONTHLY_EXTRACTION_SOURCE_TYPES)


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
    re.compile(r'^Recent Announcements\s*$', re.MULTILINE),
    re.compile(r'^Investor Notice\s*$', re.MULTILINE),
    re.compile(  # canonical-sources: noqa — regex pattern matching company names, not a ticker list
        r'\bABOUT\s+(?:MARATHON|MARA|RIOT|CLEANSPARK|CIPHER|CORE\s+SCIENTIFIC|'
        r'BIT\s+DIGITAL|HIVE|HUT\s+8?|ARGO|STRONGHOLD|TERAWULF|IRIS(?:\s+ENERGY)?|IREN|'
        r'BITFARMS|BITDEER|BIT\s*FUFU|CANGO|APPLIED\s+DIGITAL|AMERICAN\s+BITCOIN|'
        r'STRONGHOLD\s+DIGITAL)\b',
        re.IGNORECASE,
    ),
]


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


def _prior_periods(period_str: str, n: int) -> list:
    """Return up to n YYYY-MM-01 strings going backwards from period_str."""
    result = []
    current = period_str
    for _ in range(n):
        prev = _prior_period(current)
        if prev is None:
            break
        result.append(prev)
        current = prev
    return result


# How many prior months to check for historical data embedded in each press release.
_HISTORICAL_LOOKBACK = 3


def _is_quarterly_doc(report: dict) -> bool:
    """Return True if report.source_type is a quarterly filing (10-Q)."""
    return report.get('source_type') in _QUARTERLY_SOURCES


def _is_annual_doc(report: dict) -> bool:
    """Return True if report.source_type is an annual filing (10-K)."""
    return report.get('source_type') in _ANNUAL_SOURCES


def _is_monthly_source_type(source_type: str) -> bool:
    """Return True for broad miner monthly document sources."""
    return source_type in _MONTHLY_SOURCE_TYPES


_QUARTERLY_8K_URL_RE = re.compile(r'/q([1-4])(\d{2})shareholderletter', re.IGNORECASE)
_QUARTERLY_8K_TEXT_RE = re.compile(
    r'\bshareholder\s+letter\s+q([1-4])\s+(20\d{2})\b', re.IGNORECASE
)


def _infer_quarterly_covering_period(report: dict) -> Optional[str]:
    """Infer covering_period for quarter-style 8-K shareholder letters.

    Some issuers publish quarterly shareholder letters as 8-K exhibits. Those
    documents should flow through the quarterly path even though their
    ``source_type`` remains ``edgar_8k``.
    """
    if report.get('source_type') not in {'edgar_8k', 'edgar_8ka'}:
        return report.get('covering_period')

    covering_period = report.get('covering_period')
    if covering_period:
        return covering_period

    source_url = report.get('source_url') or ''
    m = _QUARTERLY_8K_URL_RE.search(source_url)
    if m:
        quarter = int(m.group(1))
        year = 2000 + int(m.group(2))
        return f"{year:04d}-Q{quarter}"

    raw_text = (report.get('raw_text') or '')[:2000]
    m = _QUARTERLY_8K_TEXT_RE.search(raw_text)
    if m:
        quarter = int(m.group(1))
        year = int(m.group(2))
        return f"{year:04d}-Q{quarter}"

    return None


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
    """Try to fill prior-period gaps via a multi-period second-pass LLM call.

    Looks back up to _HISTORICAL_LOOKBACK months from the report period.
    Press releases typically include a trailing table with the last 3 months
    of production data — this pass captures those historical figures.

    Only fires when:
    (a) at least one prior period has a missing metric in data_points
    (b) LLM returns results with confidence >= threshold

    Never overwrites existing data.
    """
    period_str = report.get('report_date')
    ticker = report.get('ticker')

    target_periods = _prior_periods(period_str, _HISTORICAL_LOOKBACK)
    if not target_periods:
        return

    # Only include periods that have at least one missing metric
    periods_with_gaps = [
        p for p in target_periods
        if _get_missing_metrics(db, ticker, p, all_metrics)
    ]
    if not periods_with_gaps:
        log.debug("Gap fill: all metrics filled for %s %s, skipping", ticker, target_periods)
        return

    log.info(
        "Gap fill: checking %d prior periods for %s (periods: %s)",
        len(periods_with_gaps), ticker, periods_with_gaps,
    )
    try:
        all_results = llm_interpreter.extract_historical_periods(
            llm_text, all_metrics, period_str, periods_with_gaps,
        )
    except Exception as e:
        log.error("Gap fill failed for %s %s: %s", ticker, periods_with_gaps, e, exc_info=True)
        return

    try:
        _gf_meta = dict(llm_interpreter._last_call_meta)
        from interpreters.llm_interpreter import _active_model
        total_results = sum(len(v) for v in all_results.values())
        gf_hits = lambda t: sum(
            1 for period_res in all_results.values()
            for r in period_res.values()
            if r is not None and r.confidence >= t
        )
        db.insert_benchmark_run({
            'model': _active_model(db),
            'call_type': 'gap_fill_multi',
            'ticker': ticker,
            'period': periods_with_gaps[0],
            'report_id': report.get('id'),
            'prompt_chars': len(llm_text),
            'response_chars': _gf_meta.get('response_chars', 0),
            'prompt_tokens': _gf_meta.get('prompt_tokens', 0),
            'response_tokens': _gf_meta.get('response_tokens', 0),
            'total_duration_ms': _gf_meta.get('total_duration_ms', 0),
            'eval_duration_ms': _gf_meta.get('eval_duration_ms', 0),
            'metrics_requested': len(all_metrics) * len(periods_with_gaps),
            'metrics_extracted': total_results,
            'hits_90': gf_hits(0.90),
            'hits_80': gf_hits(0.80),
            'hits_75': gf_hits(0.75),
        })
    except Exception as _bench_err:
        log.debug("Gap fill benchmark write failed (non-fatal): %s", _bench_err)

    for period, period_results in all_results.items():
        for metric, result in period_results.items():
            if result is None or result.confidence < confidence_threshold:
                continue
            # Only store if slot is still empty — another extraction may have filled it
            if db.data_point_exists(ticker, period, metric):
                continue
            db.insert_data_point({
                'report_id': report.get('id'),
                'ticker': ticker,
                'period': period,
                'metric': metric,
                'value': result.value,
                'unit': result.unit,
                'confidence': result.confidence,
                'extraction_method': result.extraction_method,
                'source_snippet': result.source_snippet,
            })
            summary.data_points_extracted += 1
            log.info(
                "Gap fill: stored %s %s %s = %.4f", ticker, period, metric, result.value
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


def _run_llm_batch(
    llm_interpreter,
    text: str,
    metrics: list,
    ticker: str = None,
    expected_granularity: str = 'monthly',
    config=None,
    regex_hits: dict = None,
) -> tuple:
    """
    Try batch extraction first (1 Ollama call for all metrics).
    Falls back to per-metric extract_batch([metric]) if batch returns empty.

    Returns (results: dict, meta: dict) where meta contains timing info from
    llm_interpreter._last_call_meta (populated by _call_ollama).

    config: Optional ExtractionRunConfig. When supplied, it takes precedence
        over expected_granularity and is forwarded to extract_batch so the
        temporal anchor is included in the prompt.
    expected_granularity: legacy param used when config is None.
    regex_hits: dict of regex results keyed by metric (from _build_regex_by_metric).
        When supplied and empty, the per-metric fallback loop is skipped — if both
        the batch call and regex found nothing, individual per-metric calls will
        almost certainly also find nothing and would only waste LLM cycles.
    """
    # Resolve config — create one if not supplied so callers get consistent behaviour
    if config is None:
        from miner_types import ExtractionRunConfig
        config = ExtractionRunConfig(
            expected_granularity=expected_granularity,
            ticker=ticker or '',
        )
    result = llm_interpreter.extract_batch(text, metrics, ticker=ticker, config=config)
    meta = dict(llm_interpreter._last_call_meta)
    if result:
        log.info("  LLM batch returned %d/%d metrics", len(result), len(metrics))
        return result, meta
    log.warning("  LLM batch empty — falling back to per-metric (%d calls)", len(metrics))
    # Skip per-metric fallback when regex also found nothing. Both LLM and regex
    # failing means the document almost certainly has no extractable values —
    # individual metric calls will match this outcome and only waste LLM cycles.
    if regex_hits is not None and not regex_hits:
        log.info(
            "  Skipping per-metric fallback: regex also found 0 hits — document likely non-extractable"
        )
        return {}, meta
    fallback = {}
    for metric in metrics:
        single = llm_interpreter.extract_batch(text, [metric], ticker=ticker, config=config)
        r = single.get(metric)
        if r is not None:
            fallback[metric] = r
        # Update meta with the last per-metric call (best-effort; final call wins)
        meta = dict(llm_interpreter._last_call_meta)
    return fallback, meta


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
    from interpreters.agreement import detect_outlier

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
    raw_html = report.get('raw_html')
    if raw_html:
        from infra.text_utils import edgar_to_plain
        full_text = edgar_to_plain(raw_html)
    else:
        full_text = report.get('raw_text') or ''
    from infra.text_utils import strip_edgar_boilerplate
    full_text = strip_edgar_boilerplate(full_text)
    text = full_text[:_q_selector.char_budget]

    # Keyword gate: skip LLM if no active BTC mining keywords appear in the text.
    # Prevents wasting LLM compute on pre-mining-era quarterly filings
    # (e.g. CLSK 2019-2020 before their Bitcoin pivot, RIOT pre-mining 10-Ks, etc.).
    # The gate is bypassed when no keywords are configured (fresh DB).
    try:
        from infra.keyword_service import get_mining_detection_phrases as _get_det_phrases_q
        kw_phrases = [p.lower() for p in _get_det_phrases_q(db) if p.strip()]
    except Exception as _kw_err:
        log.warning("Could not load mining detection phrases for quarterly gate (non-fatal): %s", _kw_err)
        from infra.keyword_service import _PRODUCTION_GATE_PHRASES
        kw_phrases = [p.lower() for p in _PRODUCTION_GATE_PHRASES]
    if kw_phrases:
        text_lower = text.lower()
        log.debug(
            "event=quarterly_keyword_gate_check ticker=%s period=%s phrases_checked=%d",
            ticker, covering_period, len(kw_phrases),
        )
        if not any(phrase in text_lower for phrase in kw_phrases):
            log.info(
                "event=quarterly_keyword_gate_skip ticker=%s period=%s source=%s "
                "— no BTC mining keywords found, skipping LLM",
                ticker, covering_period, source_type,
            )
            db.mark_report_extracted(report_id)
            summary.reports_processed += 1
            summary.keyword_gated += 1
            return summary

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

    # Per-metric fallback: mirrors _run_llm_batch(). When batch returns {} (e.g.
    # LLM responded with markdown prose instead of JSON), retry each metric
    # individually — single-metric prompts are simpler and more reliably parsed.
    if not llm_results:
        log.warning(
            "Quarterly LLM batch empty for %s %s — falling back to per-metric (%d calls)",
            ticker, covering_period, len(all_metrics),
        )
        fallback: dict = {}
        for _m in all_metrics:
            try:
                single = llm_interpreter.extract_quarterly_batch(
                    text, [_m], ticker=ticker, period_type=period_type
                )
                if _m in single and single[_m] is not None:
                    fallback[_m] = single[_m]
            except Exception as _fb_err:
                log.warning(
                    "Quarterly per-metric fallback failed for %s %s %s: %s",
                    ticker, covering_period, _m, _fb_err,
                )
        llm_results = fallback

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


def extract_report(report: dict, db, registry, attribution: Optional[str] = None, config=None) -> 'ExtractionSummary':
    """
    Run extraction on one stored report.

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

    # Prefer fresh table-aware extraction from raw_html when available.
    # raw_text was stored at ingest time via plain get_text() (no table conversion);
    # re-deriving from raw_html applies convert_tables_to_pipe_text so label-value
    # associations in HTML tables survive flattening.
    raw_html = report.get('raw_html')
    if raw_html:
        from infra.text_utils import html_to_plain
        text = html_to_plain(raw_html)
    else:
        text = report.get('raw_text') or ''

    # Strip navigation headers and boilerplate footer sections
    # (investor notices, forward-looking disclaimers, about/contact blocks,
    # SEC signature blocks, exhibit indexes) before regex gating and LLM extraction.
    _src = report.get('source_type', '')
    if _is_monthly_source_type(_src):
        from infra.text_utils import strip_press_release_boilerplate
        text = strip_press_release_boilerplate(text)
    elif _src.startswith('edgar_'):
        from infra.text_utils import strip_edgar_boilerplate
        text = strip_edgar_boilerplate(text)

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
            "event=interpret_start report_id=%s ticker=%s period=%s source=%s",
            report.get('id'), report.get('ticker'), report.get('report_date'),
            report.get('source_type'),
        )

        source_type = report.get('source_type', '')

        inferred_covering_period = _infer_quarterly_covering_period(report)
        treat_as_quarterly_8k = bool(
            source_type in {'edgar_8k', 'edgar_8ka'} and inferred_covering_period
        )
        effective_report = (
            {**report, 'covering_period': inferred_covering_period}
            if treat_as_quarterly_8k else report
        )

        # Build ExtractionRunConfig if not supplied by caller.
        # Annual SEC sources → annual; quarterly SEC sources and quarter-style
        # 8-K shareholder letters → quarterly; all else → monthly.
        if config is None:
            from miner_types import ExtractionRunConfig
            if source_type in _ANNUAL_SOURCES:
                _eg = 'annual'
            elif source_type in _QUARTERLY_SOURCES or treat_as_quarterly_8k:
                _eg = 'quarterly'
            else:
                _eg = 'monthly'
            config = ExtractionRunConfig(
                expected_granularity=_eg,
                ticker=report.get('ticker', ''),
            )
        _run_config = config

        # Route quarterly and annual SEC filings to the dedicated extraction path.
        # These do not use regex extraction — LLM only with wider text window.
        if source_type in _QUARTERLY_SOURCES or source_type in _ANNUAL_SOURCES or treat_as_quarterly_8k:
            return _interpret_quarterly_report(effective_report, db, registry, summary, attribution)

        # Keyword gate for ALL non-quarterly/annual sources: skip LLM if no BTC
        # mining signals appear in the document.
        # 8-K ingest is already gated by btc_first_filing_date upstream, but
        # archived and IR docs from before a company's mining pivot (e.g. CLSK 2019)
        # have no ingest-level date gate and must be screened here.
        #
        # Uses broad mining detection phrases (%btc%, %bitcoin%, %hash rate%, etc.)
        # rather than the narrower anchor phrases — archive/IR text says "produced
        # 742 BTC" not "bitcoin production", so exact anchor matching is too strict.
        # Hardcoded fallback so the gate ALWAYS fires even when DB/keyword service fails.
        try:
            from infra.keyword_service import get_mining_detection_phrases as _get_det_phrases, _PRODUCTION_GATE_PHRASES
            _det_phrases = [p.lower() for p in _get_det_phrases(db)] or [p.lower() for p in _PRODUCTION_GATE_PHRASES]
        except Exception as _kw_err:
            log.warning("event=kw_gate_load_error error=%s — using hardcoded fallback", _kw_err)
            from infra.keyword_service import _PRODUCTION_GATE_PHRASES
            _det_phrases = [p.lower() for p in _PRODUCTION_GATE_PHRASES]
        if _det_phrases:
            _text_lower = text.lower()
            if not any(phrase in _text_lower for phrase in _det_phrases):
                log.info(
                    "event=keyword_gate_skip ticker=%s period=%s source=%s "
                    "— no BTC mining signals found, skipping",
                    report.get('ticker'), report.get('report_date'), source_type,
                )
                db.mark_report_extracted(report['id'])
                summary.reports_processed += 1
                summary.keyword_gated += 1
                return summary

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

        if _is_monthly_source_type(source_type) and not regex_by_metric:
            log.info(
                "event=regex_gate_skip ticker=%s period=%s source=%s "
                "— no regex metric candidates found, skipping LLM",
                report.get('ticker'), report.get('report_date'), source_type,
            )
            db.mark_report_extracted(report['id'])
            summary.reports_processed += 1
            summary.regex_gated += 1
            return summary

        llm_interpreter = _get_llm_interpreter(db)
        llm_available = _check_llm_available(llm_interpreter)

        if not llm_available:
            log.warning(
                "LLM not available for monthly report id=%s ticker=%s %s — will retry when LLM is up",
                report.get('id'), report.get('ticker'), report_date,
            )
            db.reset_report_to_pending(report['id'])
            return summary

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
        # Falls back to per-metric extract_batch([metric]) if batch returns empty,
        # unless regex also found nothing (saves N wasted LLM calls on empty docs).
        llm_by_metric = {}
        if llm_available:
            llm_by_metric, _batch_meta = _run_llm_batch(
                llm_interpreter, llm_text, all_metrics, ticker=ticker,
                config=_run_config, regex_hits=regex_by_metric,
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
                for _fb_window in _fb_windows[1:2]:
                    log.debug(
                        "Fallback window %d: retrying %s for %s %s",
                        _fb_window['window_index'], _fb_metric,
                        ticker, report.get('report_date'),
                    )
                    _fb_results = llm_interpreter.extract_batch(
                        _fb_window['text'], [_fb_metric], ticker=ticker, config=_run_config
                    )
                    if _fb_metric in _fb_results and not _ctx_selector.needs_fallback(
                        _fb_results[_fb_metric], db
                    ):
                        llm_by_metric[_fb_metric] = _fb_results[_fb_metric]
                        break

        for metric in all_metrics:
            _apply_llm_result(
                metric=metric,
                llm_result=llm_by_metric.get(metric),
                db=db,
                report=report,
                confidence_threshold=CONFIDENCE_REVIEW_THRESHOLD,
                summary=summary,
                attribution=attribution,
                llm_interpreter=llm_interpreter,
                metric_rule=metric_rules_by_name.get(metric),
                run_config=_run_config,
            )

        # Second-pass: try to fill prior-period gaps if LLM found figures for last month.
        # Skip entirely when the main pass stored 0 data points — a document that
        # yielded no current-period figures cannot contain historical figures for
        # prior periods either (covers pre-pivot corporate 8-Ks and zero-yield docs).
        if llm_available and summary.data_points_extracted > 0:
            _try_gap_fill(
                report, db, llm_interpreter, llm_text, all_metrics,
                CONFIDENCE_REVIEW_THRESHOLD, summary,
            )

        if _is_monthly_source_type(source_type):
            try:
                db.refresh_review_precedence_for_month(
                    report.get('ticker'),
                    report.get('report_date'),
                )
            except Exception as _refresh_err:
                log.debug(
                    "Review precedence refresh failed for %s %s: %s",
                    report.get('ticker'),
                    report.get('report_date'),
                    _refresh_err,
                )

        summary.reports_processed += 1

    except Exception as e:
        log.error(
            "event=interpret_error report_id=%s ticker=%s error=%s",
            report.get('id'), report.get('ticker'), e, exc_info=True,
        )
        summary.errors += 1
        db.mark_report_extraction_failed(report['id'], str(e)[:500])
        return summary

    log.info(
        "event=interpret_complete report_id=%s ticker=%s data_points=%s queued=%s errors=%s",
        report.get('id'), report.get('ticker'),
        summary.data_points_extracted, summary.review_flagged, summary.errors,
    )
    db.mark_report_extracted(report['id'])

    return summary
