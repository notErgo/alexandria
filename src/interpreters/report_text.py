"""
Report text preparation utilities.

Extracts raw document text from report dicts, strips boilerplate sections,
and provides source-type classification helpers.

Public API:
  prepare_report_text(report: dict) -> str
  _is_monthly_source_type(source_type: str) -> bool
  _clean_for_llm(text: str) -> str
"""
import re

from config import MONTHLY_EXTRACTION_SOURCE_TYPES

_MONTHLY_SOURCE_TYPES = frozenset(MONTHLY_EXTRACTION_SOURCE_TYPES)

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


def _is_monthly_source_type(source_type: str) -> bool:
    """Return True for broad miner monthly document sources."""
    return source_type in _MONTHLY_SOURCE_TYPES


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


def prepare_report_text(report: dict) -> str:
    """Extract and strip boilerplate from report text for LLM extraction.

    Prefers raw_html (re-derives text with table-aware extraction) over
    raw_text. Applies source-type-specific boilerplate stripping.

    Returns cleaned text (may be empty string if the report has no content).
    """
    raw_html = report.get('raw_html')
    if raw_html:
        from infra.text_utils import html_to_plain
        text = html_to_plain(raw_html)
    else:
        text = report.get('raw_text') or ''

    _src = report.get('source_type', '')
    if _is_monthly_source_type(_src):
        from infra.text_utils import strip_press_release_boilerplate
        text = strip_press_release_boilerplate(text)
    elif _src.startswith('edgar_'):
        from infra.text_utils import strip_edgar_boilerplate
        text = strip_edgar_boilerplate(text)

    return text
