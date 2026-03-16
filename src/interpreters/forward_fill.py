"""forward_fill.py — Forward-fill projection utilities.

Rules:
- Only confirmed (non-projected) data points contribute to the moving average.
- Quarterly periods (YYYY-Qn) are expanded to 3 monthly periods, each
  receiving the full quarterly value (not divided by 3).
- Annual / FY periods are skipped.
- Fill window: (last_confirmed_period + 1 month) through current_period inclusive.
- No DB writes; returns synthetic rows with is_projected=True.
"""
from __future__ import annotations

import re
from typing import Optional

_QUARTERLY_RE = re.compile(r'^(\d{4})-Q([1-4])$')
_MONTHLY_RE   = re.compile(r'^(\d{4})-(\d{2})')


# ── Period helpers ────────────────────────────────────────────────────────────

def _to_monthly(period: str) -> Optional[str]:
    """Return 'YYYY-MM' for monthly/daily periods; None for quarterly/annual."""
    m = _MONTHLY_RE.match(period)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def _quarter_to_months(period: str) -> list[str]:
    """YYYY-Qn  →  ['YYYY-MM', 'YYYY-MM', 'YYYY-MM']"""
    m = _QUARTERLY_RE.match(period)
    if not m:
        return []
    year, q = int(m.group(1)), int(m.group(2))
    start = (q - 1) * 3 + 1
    return [f"{year}-{start + i:02d}" for i in range(3)]


def _add_months(period: str, n: int) -> str:
    """Add n months to a 'YYYY-MM' period string."""
    year, month = int(period[:4]), int(period[5:7])
    month += n
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return f"{year}-{month:02d}"


def _period_to_month_ordinal(period: str) -> int:
    """Convert 'YYYY-MM' to an integer ordinal (year*12 + month) for comparison."""
    return int(period[:4]) * 12 + int(period[5:7])


# ── Core computation ──────────────────────────────────────────────────────────

def compute_forward_fill(
    rows: list[dict],
    window: int,
    current_period: str,
) -> list[dict]:
    """Compute forward-fill projections for each (ticker, metric) pair.

    Args:
        rows:           List of final_data_points dicts with at minimum:
                        ticker, period, metric, value, unit.
        window:         Moving-average window in months (>= 1).
        current_period: Target end period ('YYYY-MM') inclusive.

    Returns:
        List of projected rows (is_projected=True) covering the gap from each
        pair's last confirmed period up to current_period.  The caller should
        merge these with the original rows.
    """
    if window < 1:
        window = 1

    # ── Build (ticker, metric) → sorted list of (monthly_period, value, unit)
    groups: dict[tuple[str, str], dict[str, tuple[float, str]]] = {}

    for row in rows:
        ticker = row.get('ticker') or ''
        metric = row.get('metric') or ''
        value  = row.get('value')
        unit   = row.get('unit') or ''
        period = row.get('period') or ''

        if value is None or ticker == '' or metric == '':
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue

        key = (ticker, metric)
        if key not in groups:
            groups[key] = {}

        qm = _QUARTERLY_RE.match(period)
        if qm:
            for mp in _quarter_to_months(period):
                if mp not in groups[key]:
                    groups[key][mp] = (v, unit)
        else:
            mp = _to_monthly(period)
            if mp and mp not in groups[key]:
                groups[key][mp] = (v, unit)
        # annual / FY: skip

    projected: list[dict] = []
    current_ord = _period_to_month_ordinal(current_period)

    for (ticker, metric), period_map in groups.items():
        if not period_map:
            continue

        sorted_periods = sorted(period_map.keys())
        last_period = sorted_periods[-1]

        if _period_to_month_ordinal(last_period) >= current_ord:
            continue  # already at or past current period

        # Moving-average base: last `window` confirmed values
        base = sorted_periods[-window:]
        ma_val = sum(period_map[p][0] for p in base) / len(base)
        unit   = period_map[sorted_periods[-1]][1]

        fill_period = _add_months(last_period, 1)
        while _period_to_month_ordinal(fill_period) <= current_ord:
            projected.append({
                'ticker':       ticker,
                'period':       fill_period + '-01',
                'metric':       metric,
                'value':        round(ma_val, 6),
                'unit':         unit,
                'is_projected': True,
                'ma_window':    window,
                'time_grain':   'monthly',
            })
            fill_period = _add_months(fill_period, 1)

    return projected
