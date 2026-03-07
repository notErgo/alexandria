"""Crawl context builder — derives date bounds and coverage gaps from existing data.

Before launching an LLM crawl, this module:
  1. Finds the earliest filing that mentions bitcoin mining (lower bound).
  2. Identifies months in the known range that have no data_points (gaps).
  3. Formats a context block prepended to the crawl prompt so the model
     focuses on uncovered periods and does not search before mining began.

This keeps the non-deterministic LLM crawl tightly bounded by deterministic
structured data (EDGAR filings and existing data_points).
"""
from datetime import date
from typing import Optional


def find_bitcoin_lower_bound(ticker: str, db) -> Optional[str]:
    """Return the earliest covering_period (YYYY-MM-DD) for any report that
    mentions bitcoin mining activity for this ticker.

    Returns None if no such report exists in the DB yet.
    """
    return db.get_earliest_bitcoin_report_period(ticker)


def build_crawl_context(ticker: str, db) -> dict:
    """Return a context dict describing known coverage for ticker.

    Keys:
      lower_bound  -- earliest YYYY-MM-DD with a bitcoin-mention report, or None
      gaps         -- list of YYYY-MM-DD periods between lower_bound and today
                      that have no data_points
      covered      -- list of YYYY-MM-DD periods that already have data_points
    """
    lower_bound = find_bitcoin_lower_bound(ticker, db)

    if not lower_bound:
        return {'lower_bound': None, 'gaps': [], 'covered': []}

    covered = db.get_covered_periods(ticker)
    gaps = db.get_missing_periods(ticker)

    # get_missing_periods only covers the internal spine (min→max of existing
    # data_points). Extend to today so the model knows what's genuinely absent.
    gaps = _extend_gaps_to_today(lower_bound, covered, gaps)

    return {
        'lower_bound': lower_bound,
        'gaps': sorted(set(gaps)),
        'covered': sorted(covered),
    }


def _extend_gaps_to_today(lower_bound: str, covered: list, existing_gaps: list) -> list:
    """Return gaps from lower_bound all the way to today, not just within the
    existing data_points spine."""
    covered_set = set(covered)
    gap_set = set(existing_gaps)

    lb_y, lb_m = int(lower_bound[:4]), int(lower_bound[5:7])
    today = date.today()
    y, m = lb_y, lb_m
    while (y, m) <= (today.year, today.month):
        period = f"{y:04d}-{m:02d}-01"
        if period not in covered_set:
            gap_set.add(period)
        m += 1
        if m > 12:
            m = 1
            y += 1

    return list(gap_set)


def format_context_block(ticker: str, ctx: dict) -> str:
    """Build the text block prepended to the crawl prompt.

    Returns an empty string when no lower bound is known (no constraints injected).
    """
    if not ctx['lower_bound']:
        return ''

    lb = ctx['lower_bound'][:7]  # YYYY-MM
    lines = [
        '## Auto-detected Coverage Context',
        '',
        f'Bitcoin mining activity first detected in DB: {lb}',
        f'Do not search for content dated before {lb}.',
        '',
    ]

    gaps = ctx['gaps']
    if gaps:
        gap_labels = [p[:7] for p in sorted(gaps)[:36]]  # max 3 years listed
        lines.append(f'Missing periods — prioritise these ({len(gaps)} total):')
        lines.append(', '.join(gap_labels))
        lines.append('')

    covered = ctx['covered']
    if covered:
        recent = [p[:7] for p in sorted(covered)[-12:]]  # show most recent 12
        lines.append('Already have data for (lower priority):')
        lines.append(', '.join(recent))
        lines.append('')

    lines.append('---')
    lines.append('')
    return '\n'.join(lines)
