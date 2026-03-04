"""
Pure functions for coverage grid computation. No DB dependencies.
"""
from datetime import date, timedelta


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
