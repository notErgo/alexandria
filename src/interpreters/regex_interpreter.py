"""
Multi-stage regex extraction pipeline.

For each metric, applies patterns in priority order, scores each match,
resolves conflicts (same numeric value matched by multiple patterns),
and returns results sorted by confidence descending.
"""
import re
import logging
from typing import Optional

from interpreters.unit_normalizer import normalize_value
from interpreters.confidence import score_extraction
from miner_types import ExtractionResult
from config import EXTRACTION_CONTEXT_WINDOW, MAX_SOURCE_SNIPPET_LEN

log = logging.getLogger('miners.interpreters.regex_interpreter')

# Phrases in the pre-context window that signal a global/network entity
# rather than the company being reported on.
_ENTITY_DISQUALIFIERS = (
    'global hashrate',
    'network hashrate',
    'global bitcoin',
    'total network',
)


def _extract_context_window(text: str, match_start: int, match_end: int) -> str:
    """Return a substring centered on the match, capped at MAX_SOURCE_SNIPPET_LEN chars."""
    window = EXTRACTION_CONTEXT_WINDOW
    start = max(0, match_start - window)
    end = min(len(text), match_end + window)
    snippet = text[start:end]
    return snippet[:MAX_SOURCE_SNIPPET_LEN]


def _score_match_context(text: str, match_start: int, match_end: int) -> float:
    """
    Return a context multiplier in [0.0, 1.0] for a regex match position.

    0.0  — hard disqualify: number immediately followed by '%' (rate, not BTC/EH)
    0.5  — soft penalty: preceded by global/network entity signal (wrong company)
    1.0  — no disqualifying signals found
    """
    pre = text[max(0, match_start - 150): match_start].lower()
    post = text[match_end: min(len(text), match_end + 10)].lower()

    if re.match(r'\s*%', post):
        return 0.0

    if any(kw in pre for kw in _ENTITY_DISQUALIFIERS):
        return 0.5

    return 1.0


def _pattern_in_scope(pattern_dict: dict, report_date: Optional[str]) -> bool:
    """
    Return True if pattern applies to the given report_date.

    Patterns without valid_from/valid_to always apply.
    Comparison uses YYYY-MM prefix so both 'YYYY-MM' and 'YYYY-MM-DD' dates work.
    When report_date is None, all patterns apply (no filtering).
    """
    if not report_date:
        return True
    report_month = report_date[:7]  # YYYY-MM
    valid_from = pattern_dict.get('valid_from')
    valid_to = pattern_dict.get('valid_to')
    if valid_from and report_month < valid_from:
        return False
    if valid_to and report_month > valid_to:
        return False
    return True


def _apply_pattern(text: str, pattern_dict: dict, metric: str,
                   valid_range=None) -> Optional[ExtractionResult]:
    """
    Apply a single pattern dict to text using finditer.

    Iterates all non-overlapping matches, scores each with _score_match_context,
    skips percent false-positives (context_score == 0.0), and returns the result
    with the highest context score. On ties, the first match found wins.
    Returns None if no valid match is found.
    """
    try:
        compiled = re.compile(pattern_dict['regex'])
    except re.error as e:
        log.error("Invalid regex for pattern %s: %s", pattern_dict.get('id'), e)
        return None

    best_result = None
    best_context_score = -1.0

    for m in compiled.finditer(text):
        context_score = _score_match_context(text, m.start(), m.end())
        if context_score == 0.0:
            continue  # percent false positive — skip

        # For hashrate and realization_rate, pass the full match so the normalizer
        # can see the unit suffix (e.g. "3400 PH/s", "95.0%").
        # For BTC-type metrics, use the captured group (group 1) — it contains
        # just the numeric value, avoiding false first-number matches when a PDF
        # footnote digit appears between the keyword and the value
        # (e.g. "BTC Produced 2 750" — group 1 is "750", not "2").
        if metric in {'production_btc', 'holdings_btc', 'sales_btc'} and m.lastindex and m.lastindex >= 1:
            raw_match = m.group(1)
        else:
            raw_match = m.group(0)
        normalized = normalize_value(raw_match, metric)
        if normalized is None:
            continue

        value, unit = normalized
        confidence = score_extraction(
            pattern_weight=pattern_dict['confidence_weight'] * context_score,
            context_distance=0,
            value=value,
            metric=metric,
            valid_range=valid_range,
        )
        snippet = _extract_context_window(text, m.start(), m.end())
        result = ExtractionResult(
            metric=metric,
            value=value,
            unit=unit,
            confidence=confidence,
            extraction_method=pattern_dict['id'],
            source_snippet=snippet,
            pattern_id=pattern_dict['id'],
        )
        if context_score > best_context_score:
            best_context_score = context_score
            best_result = result

    return best_result


def _resolve_conflicts(results: list) -> list:
    """
    Deduplicate results that matched the same numeric value.
    For each group of results with the same rounded integer value,
    keep only the one with the highest confidence score.
    """
    best: dict = {}
    for r in results:
        key = round(r.value)
        if key not in best or r.confidence > best[key].confidence:
            best[key] = r
    return list(best.values())


def extract_all(text: str, patterns: list, metric: str,
                report_date: Optional[str] = None,
                valid_range=None) -> list:
    """
    Run all patterns against text, resolve conflicts, and return sorted results.

    Args:
        text: Raw text content to search.
        patterns: List of pattern dicts (each with 'regex', 'confidence_weight',
                  'priority', 'id') sorted by priority ascending.
        metric: Metric name for unit normalization and confidence scoring.
        report_date: Optional ISO date string (YYYY-MM-DD or YYYY-MM).
                     Patterns with valid_from/valid_to outside this date are skipped.
                     When None, all patterns apply.

    Returns:
        List of ExtractionResult sorted by confidence descending.
    """
    raw_results = []
    for pattern_dict in patterns:
        if not _pattern_in_scope(pattern_dict, report_date):
            continue
        result = _apply_pattern(text, pattern_dict, metric, valid_range=valid_range)
        if result is not None:
            raw_results.append(result)

    deduped = _resolve_conflicts(raw_results)
    return sorted(deduped, key=lambda r: r.confidence, reverse=True)
