"""
Pattern Suggestion Engine.

Analyses extraction results (found patterns from data_points.source_snippet and
missed patterns from review_queue LLM_EMPTY items) and returns ranked, clustered
suggestions the analyst can review and one-click-apply to llm_ticker_hints or
llm_prompts.

No DB writes — purely read/analyse. Write happens in the route layer on explicit
analyst action.
"""
import re
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger('miners.pattern_suggester')

# Matches comma-formatted numbers like 1,284 or 1,284.5
_RE_COMMA_NUM = re.compile(r'\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b')
# Matches plain numbers including decimals
_RE_PLAIN_NUM = re.compile(r'\b\d+(?:\.\d+)?\b')
# Matches 3+ consecutive spaces (table alignment)
_RE_MULTI_SPACE = re.compile(r' {3,}')
# Matches header-value format after normalization: "Word: [N]"
_RE_HEADER_VALUE = re.compile(r'^\w[\w\s]{2,30}:\s+\[N\]')

# Window around a keyword match (chars)
_KEYWORD_WINDOW_BEFORE = 150
_KEYWORD_WINDOW_AFTER = 200

# Max suggestions returned per metric
_MAX_PER_METRIC = 5


def _normalize_to_pattern(text: str) -> str:
    """Replace numbers with [N] placeholder and normalize whitespace."""
    t = _RE_COMMA_NUM.sub('[N]', text)
    t = _RE_PLAIN_NUM.sub('[N]', t)
    t = re.sub(r' +', ' ', t)
    return t.strip()


def _detect_pattern_type(text: str) -> str:
    """Classify a text snippet as table_row, header_value, or prose."""
    if '|' in text or _RE_MULTI_SPACE.search(text):
        return 'table_row'
    normalized = _normalize_to_pattern(text)
    if _RE_HEADER_VALUE.match(normalized):
        return 'header_value'
    return 'prose'


def _pattern_id(metric: str, normalized_pattern: str) -> str:
    """Stable 8-char hex ID derived from metric + normalized pattern."""
    return hashlib.sha256(f"{metric}:{normalized_pattern}".encode()).hexdigest()[:8]


def _extract_found_patterns(data_point_rows: list) -> list:
    """
    Extract raw pattern hits from data_points rows.

    Each row must have: metric, source_snippet, report_id.
    Returns list of dicts with keys:
      metric, signal, text_window, normalized_pattern, pattern_type, report_id
    """
    hits = []
    for row in data_point_rows:
        snippet = (row.get('source_snippet') or '').strip()
        if not snippet:
            continue
        metric = row.get('metric', '')
        normalized = _normalize_to_pattern(snippet)
        pattern_type = _detect_pattern_type(snippet)
        hits.append({
            'metric': metric,
            'signal': 'found',
            'text_window': snippet,
            'normalized_pattern': normalized,
            'pattern_type': pattern_type,
            'report_id': row.get('report_id'),
        })
    return hits


def _extract_missed_patterns(db, ticker: str, rq_rows: list) -> list:
    """
    Extract raw pattern hits from LLM_EMPTY review_queue items.

    For each row, fetches raw_text, finds keyword occurrences, and extracts
    context windows. Returns list of pattern hit dicts.
    """
    # Build keyword lookup keyed by metric_key
    all_keywords = db.get_all_metric_keywords()
    kw_by_metric: dict = {}
    for kw in all_keywords:
        m = kw.get('metric_key', '')
        kw_by_metric.setdefault(m, []).append(kw.get('phrase', ''))

    hits = []
    for row in rq_rows:
        metric = row.get('metric', '')
        report_id = row.get('report_id')
        if report_id is None:
            continue

        raw_text = db.get_report_raw_text(report_id)
        if not raw_text:
            continue

        phrases = kw_by_metric.get(metric, [])
        if not phrases:
            continue

        for phrase in phrases:
            if not phrase:
                continue
            for m in re.finditer(re.escape(phrase), raw_text, re.IGNORECASE):
                pos = m.start()
                window_start = max(0, pos - _KEYWORD_WINDOW_BEFORE)
                window_end = min(len(raw_text), pos + _KEYWORD_WINDOW_AFTER)
                window = raw_text[window_start:window_end]

                # Scan lines in window for numeric content
                for line in window.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # Only lines that contain a number
                    if not re.search(r'\d', line):
                        continue
                    normalized = _normalize_to_pattern(line)
                    pattern_type = _detect_pattern_type(line)
                    hits.append({
                        'metric': metric,
                        'signal': 'missed',
                        'text_window': line,
                        'normalized_pattern': normalized,
                        'pattern_type': pattern_type,
                        'report_id': report_id,
                    })

    return hits


def _cluster_patterns(hits: list) -> list:
    """
    Cluster raw hits by (metric, normalized_pattern).

    Returns list of suggestion dicts sorted by (found first, then frequency desc),
    capped at _MAX_PER_METRIC per metric.
    """
    groups: dict = {}
    for hit in hits:
        key = (hit['metric'], hit['normalized_pattern'])
        if key not in groups:
            groups[key] = {
                'metric': hit['metric'],
                'signal': hit['signal'],
                'normalized_pattern': hit['normalized_pattern'],
                'pattern_type': hit['pattern_type'],
                'text_windows': [],
                'report_ids': set(),
            }
        groups[key]['text_windows'].append(hit['text_window'])
        if hit.get('report_id') is not None:
            groups[key]['report_ids'].add(hit['report_id'])
        # Prefer 'found' signal over 'missed' if any hit is found
        if hit['signal'] == 'found':
            groups[key]['signal'] = 'found'

    suggestions = []
    for (metric, norm_pattern), grp in groups.items():
        text_windows_deduped = list(dict.fromkeys(grp['text_windows']))
        text_window = max(text_windows_deduped, key=len) if text_windows_deduped else ''
        examples = text_windows_deduped[:5]
        frequency = len(grp['text_windows'])
        report_count = len(grp['report_ids'])
        sid = _pattern_id(metric, norm_pattern)
        example_lines = '\n'.join(f'  - "{w[:120]}"' for w in examples[:3])
        suggestions.append({
            'id': sid,
            'metric': metric,
            'signal': grp['signal'],
            'pattern_type': grp['pattern_type'],
            'text_window': text_window,
            'normalized_pattern': norm_pattern,
            'examples': examples,
            'frequency': frequency,
            'report_count': report_count,
            'suggested_hint_addition': (
                f'Pattern observed for {metric}: "{text_window[:80]}"'
            ),
            'suggested_prompt_addition': (
                f'Look for {metric} values near phrases like:\n{example_lines}'
            ),
        })

    # Sort: found before missed, then by frequency desc
    suggestions.sort(key=lambda s: (0 if s['signal'] == 'found' else 1, -s['frequency']))

    # Cap per metric
    by_metric: dict = {}
    capped = []
    for s in suggestions:
        m = s['metric']
        by_metric[m] = by_metric.get(m, 0) + 1
        if by_metric[m] <= _MAX_PER_METRIC:
            capped.append(s)

    return capped


def generate_suggestions(
    db,
    ticker: str,
    run_id: Optional[int] = None,
    limit_reports: int = 30,
) -> dict:
    """
    Analyse extraction results for a ticker and return ranked pattern suggestions.

    Returns:
      {
        ticker, generated_at, run_id,
        suggestions: [{
          id, metric, signal, pattern_type, text_window,
          normalized_pattern, frequency, report_count,
          suggested_hint_addition, suggested_prompt_addition
        }]
      }
    """
    # Fetch found patterns from data_points
    dp_rows = db.query_data_points(ticker=ticker, limit=200)
    found_hits = _extract_found_patterns(dp_rows)

    # Fetch missed patterns from LLM_EMPTY review queue items
    rq_rows = db.get_review_items(
        ticker=ticker,
        status='PENDING',
        limit=limit_reports,
        include_inactive=True,
    )
    llm_empty_rows = [r for r in rq_rows if r.get('agreement_status') == 'LLM_EMPTY']
    missed_hits = _extract_missed_patterns(db, ticker, llm_empty_rows)

    all_hits = found_hits + missed_hits
    suggestions = _cluster_patterns(all_hits)

    return {
        'ticker': ticker,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'run_id': run_id,
        'suggestions': suggestions,
    }
