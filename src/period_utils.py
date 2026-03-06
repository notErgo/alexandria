"""
Period normalization utilities for the Bitcoin Miner Data Platform.

All functions are pure (no DB access) and produce YYYY-MM calendar strings
from the variety of period representations found in SEC filings and IR docs.
"""
import re
from typing import Optional

# Month name -> calendar month number
_MONTH_NAMES = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}

# Quarter -> start month
_QUARTER_START = {'Q1': 1, 'Q2': 4, 'Q3': 7, 'Q4': 10}

# Patterns applied in order; first match wins
_PATTERNS = [
    # YYYY-MM (canonical — pass through)
    (re.compile(r'^(\d{4})-(\d{2})$'), 'yyyy_mm'),
    # YYYY-MM-DD  (SEC period_of_report format)
    (re.compile(r'^(\d{4})-(\d{2})-\d{2}$'), 'yyyy_mm_dd'),
    # YYYY-Qn  or  YYYY-Q1
    (re.compile(r'^(\d{4})[- ]?[Qq]([1-4])$'), 'yyyy_qn'),
    # Qn YYYY  (e.g. "Q1 2024")
    (re.compile(r'^[Qq]([1-4])\s+(\d{4})$'), 'qn_yyyy'),
    # FY2023 or 2023-FY
    (re.compile(r'^(?:FY)?(\d{4})(?:-FY)?$', re.IGNORECASE), 'fy_year'),
    # "January 2024" / "Jan 2024" / "January, 2024"
    (re.compile(r'^([A-Za-z]+)[,\s]+(\d{4})$'), 'month_name_year'),
    # "2024 January"
    (re.compile(r'^(\d{4})\s+([A-Za-z]+)$'), 'year_month_name'),
]


def normalize_period(raw: str, fiscal_year_end: int = 12) -> Optional[str]:
    """Return YYYY-MM (calendar month) for any input period string.

    Args:
        raw: Raw period string from filing or IR source.
        fiscal_year_end: Month number (1-12) for fiscal year end. Default 12
            (December). Used when raw is "FY2023" — returns the fiscal year-end
            month. When fiscal_year_end=3 (March) and raw="FY2023", returns
            "2023-03".

    Returns:
        "YYYY-MM" string or None if the input cannot be parsed.

    Examples:
        "Q1 2024"   -> "2024-01"  (start month of Q1)
        "2024-Q1"   -> "2024-01"
        "January 2024" -> "2024-01"
        "FY2023"    -> "2023-12"  (with default fiscal_year_end=12)
        "2023-03-31" -> "2023-03"
    """
    if not raw or not isinstance(raw, str):
        return None

    raw = raw.strip()
    if not raw:
        return None

    for pattern, kind in _PATTERNS:
        m = pattern.match(raw)
        if m is None:
            continue

        if kind == 'yyyy_mm':
            year, month = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12:
                return f"{year:04d}-{month:02d}"

        elif kind == 'yyyy_mm_dd':
            year, month = int(m.group(1)), int(m.group(2))
            if 1 <= month <= 12:
                return f"{year:04d}-{month:02d}"

        elif kind == 'yyyy_qn':
            year, q = int(m.group(1)), int(m.group(2))
            start_month = _QUARTER_START[f'Q{q}']
            return f"{year:04d}-{start_month:02d}"

        elif kind == 'qn_yyyy':
            q, year = int(m.group(1)), int(m.group(2))
            start_month = _QUARTER_START[f'Q{q}']
            return f"{year:04d}-{start_month:02d}"

        elif kind == 'fy_year':
            year = int(m.group(1))
            fy_month = max(1, min(12, fiscal_year_end))
            return f"{year:04d}-{fy_month:02d}"

        elif kind == 'month_name_year':
            name = m.group(1).lower().rstrip(',')
            year = int(m.group(2))
            month = _MONTH_NAMES.get(name)
            if month:
                return f"{year:04d}-{month:02d}"

        elif kind == 'year_month_name':
            year = int(m.group(1))
            name = m.group(2).lower()
            month = _MONTH_NAMES.get(name)
            if month:
                return f"{year:04d}-{month:02d}"

    return None


def quarter_to_month_range(period_str: str) -> Optional[list]:
    """Return [YYYY-MM, ...] list (3 months) for a quarterly period string.

    Args:
        period_str: "2024-Q1", "Q1 2024", "2024-03-31", etc.

    Returns:
        List of 3 YYYY-MM strings for the quarter, or None if not parseable.
    """
    start = normalize_period(period_str)
    if not start:
        return None

    year, month = int(start[:4]), int(start[5:7])
    # Snap to quarter start
    q_start = ((month - 1) // 3) * 3 + 1
    return [
        f"{year:04d}-{q_start:02d}",
        f"{year:04d}-{q_start + 1:02d}",
        f"{year:04d}-{q_start + 2:02d}",
    ]
