"""
Structure-first extraction from HTML tables.

For each <table> element in a BeautifulSoup document, scans rows for labels
that fuzzy-match a known metric alias. When a match is found, extracts the
numeric value from the leftmost data column (index 1) and returns an
ExtractionResult with confidence = 0.90 × match_score.

Table results have extraction_method = 'table_<metric>' so callers can
distinguish them from prose-regex results and give them priority.
"""
import re
import logging
from typing import Optional

from bs4 import BeautifulSoup

from miner_types import ExtractionResult
from interpreters.unit_normalizer import normalize_value
from interpreters.confidence import score_extraction

log = logging.getLogger('miners.interpreters.table_interpreter')

# Known label aliases for each metric.
# Labels are normalised before matching (lowercase, punctuation → space,
# collapse whitespace), so aliases should be lowercase with no punctuation.
_ALIASES: dict[str, list[str]] = {
    'production_btc': [
        'total btc earned',
        'btc earned',
        'btc mined',
        'bitcoin produced',
        'bitcoin mined',
        'bitcoin production',
        'total bitcoin produced',
        'btc produced',
        'self mined bitcoin',
    ],
    'hashrate_eh': [
        'operational hashrate',
        'month end operating eh/s',
        'energized hashrate',
        'average operating hashrate',
        'average hashrate',
        'installed hashrate',
        'total hash rate capacity',
    ],
    'hodl_btc': [
        'bitcoin held',
        'btc held',
        'bitcoin holdings',
        'btc holdings',
        'total bitcoin holdings',
        'treasury bitcoin',
        'bitcoin treasury',
        'end of month btc',
        'bitcoin balance',
    ],
    'sold_btc': [
        'btc sold',
        'bitcoin sold',
        'total btc sold',
        'total bitcoin sold',
        'bitcoin liquidated',
    ],
    'realization_rate': [
        'realization rate',
        'realized hashrate',
        'operating efficiency',
    ],
}

# Pre-normalise all alias strings once at import time.
_NORMALISE_RE = re.compile(r'[^\w\s]')
_SPACE_RE = re.compile(r'\s+')


def _normalize_label(text: str) -> str:
    """Lowercase, replace punctuation with space, collapse whitespace."""
    text = text.lower()
    text = _NORMALISE_RE.sub(' ', text)
    text = _SPACE_RE.sub(' ', text).strip()
    return text


def _label_match_score(cell_text: str, alias: str) -> float:
    """
    Score how well cell_text matches alias after normalisation.
    Returns a float in [0.0, 1.0]:
      1.0  — exact match
      0.85 — one contains the other
      ratio — token intersection ratio if >= 0.6
      0.0  — below threshold or empty alias
    """
    c = _normalize_label(cell_text)
    a = _normalize_label(alias)
    if not a:
        return 0.0
    if c == a:
        return 1.0
    if a in c or c in a:
        return 0.85
    c_tokens = set(c.split())
    a_tokens = set(a.split())
    if not a_tokens:
        return 0.0
    ratio = len(c_tokens & a_tokens) / len(a_tokens)
    return ratio if ratio >= 0.6 else 0.0


def _best_metric_match(label_text: str) -> Optional[tuple[str, float]]:
    """
    Return (metric, score) for the best alias match across all metrics,
    or None if no alias scores >= 0.6.
    """
    best_metric: Optional[str] = None
    best_score = 0.0
    for metric, aliases in _ALIASES.items():
        for alias in aliases:
            score = _label_match_score(label_text, alias)
            if score > best_score:
                best_score = score
                best_metric = metric
    if best_score >= 0.6 and best_metric is not None:
        return (best_metric, best_score)
    return None


def _extract_cell_value(cell_text: str, metric: str) -> Optional[tuple[float, str]]:
    """
    Extract a numeric (value, unit) pair from a table cell for the given metric.

    For hashrate cells that lack a unit string, appends 'EH/s' and retries —
    row labels like 'Month End Operating EH/s' imply the unit even when the
    data cell contains only a bare number such as '7.0'.
    """
    result = normalize_value(cell_text, metric)
    if result is not None:
        return result
    # For hashrate: bare numbers may lack unit context — infer EH/s
    if metric == 'hashrate_eh':
        return normalize_value(cell_text + ' EH/s', metric)
    return None


def interpret_from_tables(
    soup: BeautifulSoup,
    period_hint: Optional[str] = None,
) -> list:
    """
    Scan all <table> elements in soup and emit ExtractionResult objects for
    any row whose left-column label fuzzy-matches a known metric alias.

    Args:
        soup: Parsed HTML document.
        period_hint: Optional ISO date string (YYYY-MM-DD) for future date-column
            selection. Currently unused — always picks column index 1.

    Returns:
        List of ExtractionResult (may be empty). extraction_method is
        'table_<metric>' to distinguish from prose-regex results.
    """
    results = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if not rows:
            continue

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                # Single-cell rows (e.g. colspan section headers) are skipped.
                continue

            # Find the label in the first non-empty cell, then find the data
            # in the first non-empty cell after the label.
            # Use separator=" " so that cells with inline HTML elements
            # (e.g. <span>Bitcoin</span><span>Produced</span>) get a space
            # between sub-elements, enabling alias matching.
            # Some tables (e.g. RIOT format 3) have an empty leading column 0,
            # with the row label in column 1 and a spacer before the data column.
            label_col_idx = None
            for i, cell in enumerate(cells):
                if cell.get_text(separator=" ", strip=True):
                    label_col_idx = i
                    break
            if label_col_idx is None:
                continue

            label_text = cells[label_col_idx].get_text(separator=" ", strip=True)

            match = _best_metric_match(label_text)
            if match is None:
                continue

            metric, match_score = match

            # Find the first non-empty data column after the label column.
            value_col_idx = None
            for col_idx in range(label_col_idx + 1, len(cells)):
                if cells[col_idx].get_text(separator=" ", strip=True):
                    value_col_idx = col_idx
                    break
            if value_col_idx is None:
                continue

            value_text = cells[value_col_idx].get_text(separator=" ", strip=True)

            extracted = _extract_cell_value(value_text, metric)
            if extracted is None:
                log.debug(
                    "Could not extract value from cell %r for metric %s",
                    value_text,
                    metric,
                )
                continue

            value, unit = extracted
            confidence = score_extraction(
                pattern_weight=0.90 * match_score,
                context_distance=0,
                value=value,
                metric=metric,
            )

            if confidence == 0.0:
                log.debug(
                    "Value %.4g out of range for metric %s — skipping",
                    value,
                    metric,
                )
                continue

            snippet = f"{label_text} | {value_text}"
            results.append(ExtractionResult(
                metric=metric,
                value=value,
                unit=unit,
                confidence=confidence,
                extraction_method=f"table_{metric}",
                source_snippet=snippet,
                pattern_id=f"table_{metric}",
            ))

    return results
