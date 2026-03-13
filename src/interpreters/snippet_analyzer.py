"""Snippet analyzer for metric_examples feature.

Pure stdlib only (re, collections.Counter). No project imports.

analyze_snippets(snippets) scans a list of source_snippet strings extracted from
historical data_points rows and returns frequency-ranked patterns split into:
  - table_rows: lines that look like pipe-separated tables or columnar data
  - prose_ngrams: 3-gram and 4-gram phrases from non-table lines

Numbers are normalized to 'X' before counting so that e.g. "713" and "850" in
the same structural template collapse to a single high-frequency pattern.
"""
import re
from collections import Counter

# Maximum items returned per category
_MAX_TABLE_ROWS = 10
_MAX_PROSE_NGRAMS = 15
# Minimum frequency to include in output
_MIN_FREQUENCY = 2


def _is_table_line(text: str) -> bool:
    """Return True if the line looks like a table row."""
    if text.count('|') >= 2:
        return True
    # Columnar: word chars followed by lots of whitespace then digits
    if re.search(r'[\w\s]{3,}\s{3,}[\d,]{1,10}', text):
        return True
    return False


def _normalize(text: str) -> str:
    """Strip outer whitespace/pipes, replace bare numbers with X, collapse spaces."""
    text = text.strip().strip('|').strip()
    # Replace standalone numbers (with optional commas) with X
    text = re.sub(r'\b[\d,]+\b', 'X', text)
    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


def _tokenize(text: str) -> list:
    """Lower-case word tokens with digits replaced by X."""
    text = re.sub(r'\b[\d,]+\b', 'X', text.lower())
    return re.findall(r"[a-z'X]+", text)


def _ngrams(tokens: list, n: int) -> list:
    """Return list of space-joined n-grams."""
    return [' '.join(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]


def analyze_snippets(snippets: list) -> dict:
    """Analyze a list of source_snippet strings.

    Returns::

        {
            'table_rows':    [{'template': str, 'frequency': int}, ...],
            'prose_ngrams':  [{'template': str, 'frequency': int}, ...],
            'total_snippets': int,
            'unique_snippets': int,
        }

    Results are sorted by frequency descending and capped at _MAX_TABLE_ROWS
    / _MAX_PROSE_NGRAMS. Items with frequency < _MIN_FREQUENCY are excluded.
    """
    if not snippets:
        return {'table_rows': [], 'prose_ngrams': [], 'total_snippets': 0, 'unique_snippets': 0}

    unique = list(dict.fromkeys(s for s in snippets if s))

    table_counter: Counter = Counter()
    prose_counter: Counter = Counter()

    for snip in unique:
        for line in snip.splitlines():
            line = line.strip()
            if not line:
                continue
            if _is_table_line(line):
                table_counter[_normalize(line)] += 1
            else:
                tokens = _tokenize(line)
                for n in (3, 4):
                    for gram in _ngrams(tokens, n):
                        prose_counter[gram] += 1

    # Also count across all (non-unique) snippets for table rows to capture true frequency
    full_table_counter: Counter = Counter()
    for snip in snippets:
        if not snip:
            continue
        for line in snip.splitlines():
            line = line.strip()
            if line and _is_table_line(line):
                full_table_counter[_normalize(line)] += 1

    full_prose_counter: Counter = Counter()
    for snip in snippets:
        if not snip:
            continue
        for line in snip.splitlines():
            line = line.strip()
            if line and not _is_table_line(line):
                tokens = _tokenize(line)
                for n in (3, 4):
                    for gram in _ngrams(tokens, n):
                        full_prose_counter[gram] += 1

    table_rows = [
        {'template': tmpl, 'frequency': freq}
        for tmpl, freq in full_table_counter.most_common()
        if freq >= _MIN_FREQUENCY
    ][:_MAX_TABLE_ROWS]

    prose_ngrams = [
        {'template': tmpl, 'frequency': freq}
        for tmpl, freq in full_prose_counter.most_common()
        if freq >= _MIN_FREQUENCY
    ][:_MAX_PROSE_NGRAMS]

    return {
        'table_rows': table_rows,
        'prose_ngrams': prose_ngrams,
        'total_snippets': len(snippets),
        'unique_snippets': len(unique),
    }
