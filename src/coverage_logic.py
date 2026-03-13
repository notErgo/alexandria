"""
Pure functions for coverage grid computation. No DB dependencies.
"""
from datetime import date, timedelta
from typing import List, Optional


def generate_month_range(months: int) -> list:
    """Return list of YYYY-MM-01 strings for last `months` calendar months, ascending."""
    result = []
    today = date.today()
    d = today.replace(day=1)
    for _ in range(months):
        result.append(d.strftime('%Y-%m-01'))
        # go back one month
        d = (d - timedelta(days=1)).replace(day=1)
    return list(reversed(result))


def compute_cell_state(manifest_entries: list, reports: list, has_dp: bool, has_rq: bool) -> str:
    """Determine the coverage state for a (ticker, period) cell.

    Priority (highest first):
      accepted > extracted_in_review > ingested_pending_extraction > pending_ingest > legacy_undated > no_source

    Args:
        manifest_entries: list of asset_manifest dicts for this cell
        reports: list of report dicts for this cell
        has_dp: True if a data_point exists for this ticker+period
        has_rq: True if a PENDING review_queue item exists for this ticker+period
    """
    if has_dp:
        return 'accepted'
    if has_rq:
        return 'extracted_in_review'
    if reports:
        # Has a report — check if extracted
        report = reports[0]
        if report.get('extracted_at'):
            # Extracted but no data_point or review — treat as ingested_pending_extraction
            return 'ingested_pending_extraction'
        return 'ingested_pending_extraction'
    # No report — check manifest
    if manifest_entries:
        states = {m.get('ingest_state') for m in manifest_entries}
        if 'pending' in states or 'failed' in states:
            return 'pending_ingest'
        if 'legacy_undated' in states:
            return 'legacy_undated'
        if 'ingested' in states:
            return 'ingested_pending_extraction'
        return 'pending_ingest'
    return 'no_source'


def compute_cell_state_v2(
    is_analyst_gap: bool,
    has_data_point: bool,
    has_review_pending: bool,
    has_manifest: bool,
    has_parse_error: bool,
    has_extract_error: bool,
    has_scraper_error: bool,
    data_is_quarterly: bool = False,
    has_llm_empty_rq: bool = False,
) -> str:
    """Return one of 9 CellState values for a (ticker, period, metric) cell.

    Priority (highest first):
      1. analyst_gap  — analyst explicitly marked this period as intentionally empty
      2. data         — a monthly data_point exists
      3. data_quarterly — data from a 10-Q/10-K carry or inferred value
      4. review_pending — a PENDING review_queue item exists
      5. llm_empty — LLM ran but returned no values; no other pending review
      6. parse_failed — manifest entry exists; parse_quality = 'parse_failed'
      7. extract_failed — manifest entry exists; extraction ran; no value found
      8. scraper_error — scrape was attempted; HTTP/parse error logged
      9. no_document  — no manifest entry; no scrape attempted
    """
    if is_analyst_gap:
        return 'analyst_gap'
    if has_data_point:
        if data_is_quarterly:
            return 'data_quarterly'
        return 'data'
    if has_review_pending:
        return 'review_pending'
    if has_llm_empty_rq and not has_data_point and not has_review_pending:
        return 'llm_empty'
    if has_parse_error:
        return 'parse_failed'
    if has_extract_error:
        return 'extract_failed'
    if has_scraper_error:
        return 'scraper_error'
    return 'no_document'


def compute_expected_periods(windows: list, as_of_date) -> list:
    """Compute all expected reporting periods from a list of regime windows.

    Args:
        windows: list of dicts (or RegimeWindow objects) with cadence, start_date, end_date.
                 cadence: 'monthly' | 'quarterly'
                 start_date: YYYY-MM-DD string
                 end_date: YYYY-MM-DD string or None (= current regime, bounded by as_of_date)
        as_of_date: datetime.date — ceiling; periods after this are excluded.

    Returns: sorted deduplicated list of YYYY-MM-01 strings.
    """
    if not windows:
        return []

    periods = set()
    for window in windows:
        # Support both dict and dataclass-like objects
        cadence = window['cadence'] if isinstance(window, dict) else window.cadence
        start_str = window['start_date'] if isinstance(window, dict) else window.start_date
        end_str = window.get('end_date') if isinstance(window, dict) else window.end_date

        current = date.fromisoformat(start_str).replace(day=1)
        end_ceiling = date.fromisoformat(end_str) if end_str else as_of_date

        step_months = 1 if cadence == 'monthly' else 3

        while current <= min(end_ceiling, as_of_date):
            periods.add(current.strftime('%Y-%m-01'))
            # Advance by step_months using timedelta through end-of-month
            for _ in range(step_months):
                current = (current + timedelta(days=32)).replace(day=1)

    return sorted(periods)


_ANALYST_METHODS = frozenset({'analyst', 'analyst_approved', 'review_approved', 'review_edited'})


def rank_extractions(candidates: list) -> list:
    """Sort extraction candidates by mutation governance hierarchy.

    Each candidate is a dict with: value, confidence, extraction_method, created_at.

    Ranking:
      1. Analyst-protected (extraction_method in _ANALYST_METHODS) — always rank 1
      2. Among pipeline candidates: highest confidence first
      3. Equal confidence: most recent created_at first (lexicographic ISO sort works)

    Returns: sorted list (best first). Does NOT filter.
    """
    def sort_key(c):
        is_analyst = 1 if c.get('extraction_method') in _ANALYST_METHODS else 0
        confidence = c.get('confidence', 0.0)
        # Negate string to get descending order: '2024-06-01' > '2024-01-01' so
        # negating the comparison by using reverse sort on the tuple is cleaner.
        created_at = c.get('created_at', '')
        return (-is_analyst, -confidence, created_at)

    # Sort ascending on key, but created_at is a string so we need descending there.
    # Use a two-pass: first sort by created_at desc, then stable-sort by the rest.
    by_date = sorted(candidates, key=lambda c: c.get('created_at', ''), reverse=True)

    def final_key(c):
        is_analyst = 1 if c.get('extraction_method') in _ANALYST_METHODS else 0
        confidence = c.get('confidence', 0.0)
        return (-is_analyst, -confidence)

    return sorted(by_date, key=final_key)


def summarize_grid(grid: dict) -> dict:
    """Count cells per state. Input: {ticker: {period: {state: str, ...}}}. Excludes 'summary' key."""
    counts: dict = {}
    for ticker, periods in grid.items():
        if ticker == 'summary':
            continue
        if not isinstance(periods, dict):
            continue
        for period, cell in periods.items():
            state = cell.get('state', 'no_source')
            counts[state] = counts.get(state, 0) + 1
    return counts
