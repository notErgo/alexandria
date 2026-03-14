"""
Extraction pipeline: LLM-only extraction with keyword gate.

Public API:
  extract_report(report, db) -> ExtractionSummary

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
from interpreters.report_text import _is_monthly_source_type, _clean_for_llm, prepare_report_text
from interpreters.result_router import (  # re-exported for backward-compat (tests import from here)
    _apply_llm_result,
    validate_period_granularity,
    _LLM_TEXT_MAX_CHARS,
)

log = logging.getLogger('miners.interpreters.interpret_pipeline')

# Regex patterns for MD&A section headers in EDGAR filings.
# 10-K uses Item 7; 10-Q uses Item 2.  Both are followed by "MD&A" prose.
_MDA_PATTERNS = [
    re.compile(r'\bitem\s+7[\.\s]', re.IGNORECASE),   # 10-K MD&A
    re.compile(r'\bitem\s+2[\.\s]', re.IGNORECASE),   # 10-Q MD&A
    re.compile(r"management['\u2019]?s\s+discussion", re.IGNORECASE),
]

# Mining-specific phrases used to locate the first numeric data cluster when no
# MD&A header is found.  These are intentionally more specific than the keyword
# gate phrases to avoid matching TOC section titles.
_MINING_DATA_PHRASES = (
    'bitcoin mined', 'btc mined', 'bitcoin produced', 'btc produced',
    'bitcoin we mined', 'bitcoin production', 'hash rate', 'hashrate',
    'exahash', 'bitcoin holdings', 'btc holdings', 'total bitcoin',
)


def _find_quarterly_text_window(full_text: str, budget: int) -> tuple[str, str]:
    """Return the best budget-char window of full_text for quarterly LLM extraction.

    Returns (window_text, strategy) where strategy is one of:
      'mda_header'    — anchored on the MD&A section header
      'keyword_seek'  — anchored on the first mining data phrase
      'start'         — fallback: first budget chars (original behaviour)

    Strategy (first match wins):
      1. Find Item 7 / Item 2 / "Management's Discussion" header with at least
         _MIN_MDA_SECTION_CHARS before the next Item marker (skips TOC entries
         which match the same patterns but have only a single-line body).
         Start 200 chars before the matched header to retain section title context.
      2. Find the earliest occurrence of a mining-specific phrase; start 500 chars
         before it to retain surrounding paragraph context.
      3. Return full_text[:budget] unchanged.
    """
    # Minimum characters between the matched header and the next Item marker before
    # we accept the candidate as a real section body rather than a TOC stub.
    # TOC entries span ~60-80 chars between consecutive items; real sections span
    # thousands. 300 reliably distinguishes them while keeping unit tests green.
    _MIN_MDA_SECTION_CHARS = 300

    text_lower = full_text.lower()

    for pat in _MDA_PATTERNS:
        pos = 0
        while True:
            m = pat.search(full_text, pos)
            if not m:
                break
            # Estimate section length: distance to the next "Item N" marker.
            # Skip only 10 chars (avoids self-match) so TOC entries whose next
            # item appears within 40-80 chars are correctly treated as short stubs.
            tail = full_text[m.start() + 10:]
            next_item = re.search(r'\bItem\s+\d', tail, re.IGNORECASE)
            section_len = next_item.start() if next_item else len(tail)
            if section_len >= _MIN_MDA_SECTION_CHARS:
                start = max(0, m.start() - 200)
                return full_text[start:start + budget], 'mda_header'
            pos = m.end()

    earliest = len(full_text)
    for phrase in _MINING_DATA_PHRASES:
        p = text_lower.find(phrase)
        if 0 <= p < earliest:
            earliest = p
    if earliest < len(full_text):
        start = max(0, earliest - 500)
        return full_text[start:start + budget], 'keyword_seek'

    return full_text[:budget], 'start'

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

# Source types that follow the quarterly/annual extraction path
_QUARTERLY_SOURCES = frozenset({'edgar_10q', 'edgar_6k'})
_ANNUAL_SOURCES    = frozenset({'edgar_10k', 'edgar_20f', 'edgar_40f'})


def _active_metric_keys(db) -> list:
    """Return the list of metric keys to send to the LLM.

    Queries metric_schema for active=1 rows (sector='BTC-miners').
    Logs an error and returns an empty list if the schema table is empty or unavailable.
    """
    try:
        rows = db.get_metric_schema('BTC-miners', active_only=True)
        keys = [r['key'] for r in rows] if rows else []
        if keys:
            return keys
    except Exception as e:
        log.error("Could not load active metric schema: %s", e)
        return []
    log.error("No active metrics found in metric_schema — extraction will be skipped")
    return []



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


_QUARTERLY_8K_URL_RE = re.compile(r'/q([1-4])(\d{2})shareholderletter', re.IGNORECASE)
_QUARTERLY_8K_TEXT_RE = re.compile(
    r'\bshareholder\s+letter\s+q([1-4])\s+(20\d{2})\b', re.IGNORECASE
)
_QUARTERLY_RESULTS_8K_TEXT_RE = re.compile(
    r'\b(?:(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter|q([1-4]))'
    r'(?:\s+and\s+fiscal\s+year)?\s+(?:fy|fiscal(?:\s+year)?\s+)?(20\d{2})(?:\s+\w+){0,3}\s+results\b',
    re.IGNORECASE,
)
# Source types that may embed quarterly earnings text (press releases and 8-K exhibits).
# These are inspected for quarterly-results patterns and rerouted to the quarterly path
# when a match is found.  10-Q/10-K/20-F/40-F are handled via _QUARTERLY_SOURCES already.
_QUARTERLY_EARNINGS_PR_SOURCES = frozenset({'edgar_8k', 'edgar_8ka', 'ir_press_release'})


def _quarter_token_to_int(token: str) -> Optional[int]:
    normalized = (token or '').strip().lower()
    if normalized in {'1', '1st', 'first'}:
        return 1
    if normalized in {'2', '2nd', 'second'}:
        return 2
    if normalized in {'3', '3rd', 'third'}:
        return 3
    if normalized in {'4', '4th', 'fourth'}:
        return 4
    return None


def _infer_quarterly_covering_period(report: dict) -> Optional[str]:
    """Infer covering_period for quarterly earnings press releases and 8-K exhibits.

    Handles EDGAR 8-K earnings exhibits (e.g. "Third Quarter 2025 Financial
    Results") and IR press releases (e.g. "First Quarter FY2023 Financial
    Results" from CLSK).  Documents matching a quarterly pattern are rerouted
    to the quarterly extraction path regardless of their source_type.

    Returns a covering_period string like '2025-Q3' when matched, the report's
    existing covering_period when already set, or None when no quarterly
    pattern is detected.
    """
    source_type = report.get('source_type')
    if source_type not in _QUARTERLY_EARNINGS_PR_SOURCES:
        return report.get('covering_period')

    covering_period = report.get('covering_period')
    if covering_period:
        return covering_period

    # URL-based detection only applies to EDGAR 8-K shareholder letters.
    if source_type in {'edgar_8k', 'edgar_8ka'}:
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

    m = _QUARTERLY_RESULTS_8K_TEXT_RE.search(raw_text)
    if m:
        quarter = _quarter_token_to_int(m.group(1) or m.group(2))
        year = int(m.group(3))
        if quarter is not None:
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


def _invalidate_llm_availability_cache() -> None:
    global _llm_available_cache, _llm_available_cache_time, _llm_available_cache_extractor_id
    with _llm_cache_lock:
        _llm_available_cache = False
        _llm_available_cache_time = 0.0
        _llm_available_cache_extractor_id = None



def _run_llm_batch(
    llm_interpreter,
    text: str,
    metrics: list,
    ticker: str = None,
    expected_granularity: str = 'monthly',
    config=None,
) -> tuple:
    """Run one batch LLM extraction call for all metrics.

    Returns (results: dict, meta: dict) where meta contains timing info from
    llm_interpreter._last_call_meta (populated by _call_ollama).

    config: Optional ExtractionRunConfig. When supplied, it takes precedence
        over expected_granularity and is forwarded to extract_batch so the
        temporal anchor is included in the prompt.
    expected_granularity: legacy param used when config is None.
    """
    if config is None:
        from miner_types import ExtractionRunConfig
        config = ExtractionRunConfig(
            expected_granularity=expected_granularity,
            ticker=ticker or '',
        )
    result = llm_interpreter.extract_batch(text, metrics, ticker=ticker, config=config)
    meta = dict(llm_interpreter._last_call_meta)
    meta['transport_error'] = getattr(llm_interpreter, '_last_transport_error', False) is True
    if result:
        log.info("  LLM batch returned %d/%d metrics", len(result), len(metrics))
    return result, meta


def _insert_zero_extract_review_items(db, report: dict, metrics: list, summary) -> None:
    """Insert one review_queue entry per metric with agreement_status='LLM_EMPTY'.

    Called when a report passes the keyword gate but the LLM returns no values
    for any metric and no review items were created via normal routing.  This
    surfaces the miss so analysts can investigate rather than silently losing
    the document.
    """
    ticker = report.get('ticker', '')
    period = report.get('report_date') or report.get('covering_period') or ''
    report_id = report.get('id')

    # Exclude metrics that already have any verdict (analyst has reviewed this doc+metric)
    acked = db.get_report_metric_verdicts(report_id) if report_id is not None else {}
    active_metrics = [m for m in metrics if m not in acked]
    if not active_metrics:
        log.debug(
            "event=zero_extract_all_acked ticker=%s period=%s report_id=%s"
            " — all metrics acked, skipping LLM_EMPTY inserts",
            ticker, period, report_id,
        )
        return

    for metric in active_metrics:
        try:
            db.insert_review_item({
                'data_point_id':    None,
                'ticker':           ticker,
                'period':           period,
                'metric':           metric,
                'raw_value':        '',
                'confidence':       0.0,
                'source_snippet':   None,
                'status':           'PENDING',
                'llm_value':        None,
                'regex_value':      None,
                'agreement_status': 'LLM_EMPTY',
                'report_id':        report_id,
            })
        except Exception as _rq_err:
            log.debug(
                "event=zero_extract_review_insert_fail ticker=%s period=%s metric=%s err=%s",
                ticker, period, metric, _rq_err,
            )

    summary.zero_extract_misses += 1
    log.warning(
        "event=zero_extract_miss ticker=%s period=%s report_id=%s "
        "— LLM passed keyword gate but returned zero values; inserted LLM_EMPTY review items",
        ticker, period, report_id,
    )


def _interpret_quarterly_report(
    report: dict,
    db,
    summary,
    attribution: Optional[str] = None,
    config=None,
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
            "event=quarterly_no_covering_period report_id=%s ticker=%s source_type=%s — skipping",
            report_id, ticker, source_type,
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
    text, _window_strategy = _find_quarterly_text_window(full_text, _q_selector.char_budget)
    log.debug(
        "event=quarterly_window_select ticker=%s period=%s strategy=%s "
        "full_len=%d window_len=%d",
        ticker, covering_period, _window_strategy, len(full_text), len(text),
    )

    from infra.keyword_service import get_mining_detection_phrases as _get_det_phrases_q
    kw_phrases = [p.lower() for p in _get_det_phrases_q(db) if p.strip()]
    if not kw_phrases:
        log.error(
            "event=quarterly_keyword_gate_missing ticker=%s period=%s source=%s "
            "— no active metric_schema keywords configured",
            ticker, covering_period, source_type,
        )
        db.mark_report_extraction_failed(report_id, 'no_active_metric_keywords')
        summary.errors += 1
        return summary
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
            "event=llm_unavailable_quarterly report_id=%s ticker=%s period=%s — will retry when LLM is up",
            report_id, ticker, covering_period,
        )
        # Transient failure: reset to 'pending' so this process can retry without waiting for next boot.
        db.reset_report_to_pending(report_id)
        log.info(
            "event=interpret_complete report_id=%s ticker=%s data_points=%s queued=%s errors=%s",
            report_id, ticker, 0, 0, 0,
        )
        return summary

    all_metrics = _active_metric_keys(db)
    if config is not None and config.target_metrics:
        all_metrics = [m for m in all_metrics if m in config.target_metrics]
    if not all_metrics:
        log.warning(
            "event=no_active_metrics_quarterly ticker=%s period=%s source_type=%s",
            ticker, covering_period, source_type,
        )
        db.mark_report_extraction_failed(report_id, 'no_metrics_in_schema')
        return summary

    try:
        llm_results = llm_interpreter.extract_quarterly_batch(
            text, all_metrics, ticker=ticker, period_type=period_type, config=config
        )
    except Exception as e:
        log.error(
            "event=quarterly_llm_extract_failed ticker=%s period=%s report_id=%s error=%s",
            ticker, covering_period, report_id, e, exc_info=True,
        )
        db.mark_report_extraction_failed(report_id, str(e)[:500])
        summary.errors += 1
        return summary

    if getattr(llm_interpreter, '_last_transport_error', False) is True:
        log.warning(
            "event=quarterly_llm_transport_error ticker=%s period=%s report_id=%s — resetting to pending",
            ticker, covering_period, report_id,
        )
        _invalidate_llm_availability_cache()
        db.reset_report_to_pending(report_id)
        return summary

    # Per-metric fallback: mirrors _run_llm_batch(). When batch returns {} (e.g.
    # LLM responded with markdown prose instead of JSON), retry each metric
    # individually — single-metric prompts are simpler and more reliably parsed.
    if not llm_results:
        log.warning(
            "event=quarterly_llm_batch_empty ticker=%s period=%s report_id=%s"
            " — falling back to per-metric (%d calls)",
            ticker, covering_period, report_id, len(all_metrics),
        )
        fallback: dict = {}
        for _m in all_metrics:
            try:
                single = llm_interpreter.extract_quarterly_batch(
                    text, [_m], ticker=ticker, period_type=period_type, config=config
                )
                if _m in single and single[_m] is not None:
                    fallback[_m] = single[_m]
            except Exception as _fb_err:
                log.warning(
                    "event=quarterly_per_metric_fallback_failed ticker=%s period=%s metric=%s error=%s",
                    ticker, covering_period, _m, _fb_err,
                )
        if getattr(llm_interpreter, '_last_transport_error', False) is True:
            log.warning(
                "event=quarterly_llm_transport_error_fallback ticker=%s period=%s report_id=%s"
                " — resetting to pending",
                ticker, covering_period, report_id,
            )
            _invalidate_llm_availability_cache()
            db.reset_report_to_pending(report_id)
            return summary
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

    # Safety net: if the keyword gate passed but no values were extracted and no review
    # items created, surface the miss as LLM_EMPTY entries so it is not silently dropped.
    if (
        summary.data_points_extracted == 0
        and summary.review_flagged == 0
        and summary.temporal_rejects == 0
        and all_metrics
    ):
        _insert_zero_extract_review_items(db, report, all_metrics, summary)

    summary.reports_processed += 1
    db.mark_report_extracted(report_id)
    return summary


def extract_report(report: dict, db, attribution: Optional[str] = None, config=None) -> 'ExtractionSummary':
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

    # Extract and strip boilerplate. Prefers raw_html (table-aware) over raw_text.
    # Applies source-type-specific boilerplate stripping (press release vs EDGAR).
    text = prepare_report_text(report)

    if not text.strip():
        log.warning(
            "event=empty_raw_text report_id=%s ticker=%s period=%s source_type=%s",
            report.get('id'), report.get('ticker'), report.get('report_date'),
            report.get('source_type'),
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
        treat_as_quarterly_earnings = bool(
            source_type in _QUARTERLY_EARNINGS_PR_SOURCES and inferred_covering_period
        )
        effective_report = (
            {**report, 'covering_period': inferred_covering_period}
            if treat_as_quarterly_earnings else report
        )

        # Build ExtractionRunConfig if not supplied by caller.
        # Annual SEC sources → annual; quarterly SEC sources and quarter-style
        # earnings press releases (8-K or IR) → quarterly; all else → monthly.
        if config is None:
            from miner_types import ExtractionRunConfig
            if source_type in _ANNUAL_SOURCES:
                _eg = 'annual'
            elif source_type in _QUARTERLY_SOURCES or treat_as_quarterly_earnings:
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
        if source_type in _QUARTERLY_SOURCES or source_type in _ANNUAL_SOURCES or treat_as_quarterly_earnings:
            return _interpret_quarterly_report(effective_report, db, summary, attribution, config=_run_config)

        from infra.keyword_service import get_mining_detection_phrases as _get_det_phrases
        _det_phrases = [p.lower() for p in _get_det_phrases(db) if p.strip()]
        if not _det_phrases:
            log.error(
                "event=keyword_gate_missing ticker=%s period=%s source=%s "
                "— no active metric_schema keywords configured",
                report.get('ticker'), report.get('report_date'), source_type,
            )
            db.mark_report_extraction_failed(report['id'], 'no_active_metric_keywords')
            summary.errors += 1
            return summary
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

        llm_interpreter = _get_llm_interpreter(db)
        llm_available = _check_llm_available(llm_interpreter)

        if not llm_available:
            log.warning(
                "event=llm_unavailable_monthly report_id=%s ticker=%s period=%s source_type=%s"
                " — will retry when LLM is up",
                report.get('id'), report.get('ticker'), report_date, source_type,
            )
            db.reset_report_to_pending(report['id'])
            log.info(
                "event=interpret_complete report_id=%s ticker=%s data_points=%s queued=%s errors=%s",
                report.get('id'), report.get('ticker'), 0, 0, 0,
            )
            return summary

        # Strip boilerplate (FORWARD-LOOKING STATEMENTS, SAFE HARBOR, About [Company],
        # etc.) from the back of the document before sending to LLM.
        _clean_text = _clean_for_llm(text)
        _ctx_selector = ContextWindowSelector(doc_type=report.get('source_type', ''))
        _ctx_windows = _ctx_selector.select_windows(report['id'], _clean_text, 'production_btc', db)
        llm_text = _ctx_windows[0]['text'] if _ctx_windows else _clean_text[:_LLM_TEXT_MAX_CHARS]

        all_metrics = _active_metric_keys(db)
        if _run_config is not None and _run_config.target_metrics:
            all_metrics = [m for m in all_metrics if m in _run_config.target_metrics]
        ticker = report.get('ticker')

        llm_by_metric = {}
        if llm_available:
            llm_by_metric, _batch_meta = _run_llm_batch(
                llm_interpreter, llm_text, all_metrics, ticker=ticker,
                config=_run_config,
            )
            if _batch_meta.get('transport_error'):
                log.warning(
                    "event=monthly_llm_transport_error report_id=%s ticker=%s period=%s source_type=%s"
                    " — resetting to pending",
                    report.get('id'), report.get('ticker'), report_date, source_type,
                )
                _invalidate_llm_availability_cache()
                db.reset_report_to_pending(report['id'])
                return summary
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

        # Safety net: if the keyword gate passed but no values were extracted and no review
        # items created, surface the miss as LLM_EMPTY entries so it is not silently dropped.
        if (
            llm_available
            and summary.data_points_extracted == 0
            and summary.review_flagged == 0
            and summary.temporal_rejects == 0
            and all_metrics
        ):
            _insert_zero_extract_review_items(db, report, all_metrics, summary)

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
