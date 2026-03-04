"""
Tests for coverage_logic.py pure functions — TDD.

Tests should FAIL before coverage_logic.py is implemented.
"""
import pytest
import re


# ── generate_month_range ─────────────────────────────────────────────────────

def test_generate_month_range_count():
    """generate_month_range returns exactly `months` entries."""
    from coverage_logic import generate_month_range
    result = generate_month_range(6)
    assert len(result) == 6


def test_generate_month_range_format():
    """All entries match YYYY-MM-01 format."""
    from coverage_logic import generate_month_range
    result = generate_month_range(12)
    pattern = re.compile(r'^\d{4}-\d{2}-01$')
    for entry in result:
        assert pattern.match(entry), f"Bad format: {entry}"


def test_generate_month_range_ascending():
    """Entries are sorted in ascending order."""
    from coverage_logic import generate_month_range
    result = generate_month_range(12)
    assert result == sorted(result)


def test_generate_month_range_single():
    """generate_month_range(1) returns a single entry."""
    from coverage_logic import generate_month_range
    result = generate_month_range(1)
    assert len(result) == 1


def test_generate_month_range_ends_current_month():
    """Last entry should be the current month (first day)."""
    from coverage_logic import generate_month_range
    from datetime import date
    result = generate_month_range(3)
    today = date.today()
    expected_last = today.strftime('%Y-%m-01')
    assert result[-1] == expected_last


# ── compute_cell_state ───────────────────────────────────────────────────────

def test_compute_cell_state_accepted_wins():
    """has_dp=True → 'accepted' regardless of other signals."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[{'ingest_state': 'ingested'}],
        reports=[{'extracted_at': '2024-01-15', 'id': 1}],
        has_dp=True,
        has_rq=True,
    )
    assert state == 'accepted'


def test_compute_cell_state_in_review():
    """has_rq=True and has_dp=False → 'extracted_in_review'."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[],
        reports=[{'extracted_at': '2024-01-15', 'id': 1}],
        has_dp=False,
        has_rq=True,
    )
    assert state == 'extracted_in_review'


def test_compute_cell_state_ingested_pending_extraction():
    """Report exists but no dp and no rq → 'ingested_pending_extraction'."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[],
        reports=[{'extracted_at': None, 'id': 1}],
        has_dp=False,
        has_rq=False,
    )
    assert state == 'ingested_pending_extraction'


def test_compute_cell_state_pending_ingest():
    """Manifest entry with state='pending', no report → 'pending_ingest'."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[{'ingest_state': 'pending'}],
        reports=[],
        has_dp=False,
        has_rq=False,
    )
    assert state == 'pending_ingest'


def test_compute_cell_state_legacy_undated():
    """Manifest entry with state='legacy_undated', no report → 'legacy_undated'."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[{'ingest_state': 'legacy_undated'}],
        reports=[],
        has_dp=False,
        has_rq=False,
    )
    assert state == 'legacy_undated'


def test_compute_cell_state_no_source():
    """No manifest, no report, no dp, no rq → 'no_source'."""
    from coverage_logic import compute_cell_state
    state = compute_cell_state(
        manifest_entries=[],
        reports=[],
        has_dp=False,
        has_rq=False,
    )
    assert state == 'no_source'


# ── summarize_grid ───────────────────────────────────────────────────────────

def test_summarize_grid_counts():
    """summarize_grid counts cells per state correctly."""
    from coverage_logic import summarize_grid
    grid = {
        'MARA': {
            '2024-01-01': {'state': 'accepted'},
            '2024-02-01': {'state': 'no_source'},
            '2024-03-01': {'state': 'accepted'},
        },
        'RIOT': {
            '2024-01-01': {'state': 'no_source'},
            '2024-02-01': {'state': 'pending_ingest'},
        },
    }
    summary = summarize_grid(grid)
    assert summary.get('accepted') == 2
    assert summary.get('no_source') == 2
    assert summary.get('pending_ingest') == 1


def test_summarize_grid_excludes_summary_key():
    """summarize_grid ignores the 'summary' key in the grid."""
    from coverage_logic import summarize_grid
    grid = {
        'MARA': {'2024-01-01': {'state': 'accepted'}},
        'summary': {'accepted': 1},  # This should be ignored
    }
    summary = summarize_grid(grid)
    # Should count only MARA's cell
    assert summary.get('accepted') == 1


# ── compute_cell_state_v2 ─────────────────────────────────────────────────────

class TestComputeCellStateV2:

    def _call(self, **kwargs):
        from coverage_logic import compute_cell_state_v2
        defaults = dict(
            is_analyst_gap=False,
            has_data_point=False,
            has_review_pending=False,
            has_manifest=False,
            has_parse_error=False,
            has_extract_error=False,
            has_scraper_error=False,
        )
        defaults.update(kwargs)
        return compute_cell_state_v2(**defaults)

    def test_v2_data_state(self):
        assert self._call(has_data_point=True, has_manifest=True) == 'data'

    def test_v2_review_pending_state(self):
        assert self._call(has_review_pending=True) == 'review_pending'

    def test_v2_parse_failed_state(self):
        assert self._call(has_manifest=True, has_parse_error=True) == 'parse_failed'

    def test_v2_extract_failed_state(self):
        assert self._call(has_manifest=True, has_extract_error=True) == 'extract_failed'

    def test_v2_no_document_state(self):
        assert self._call() == 'no_document'

    def test_v2_scraper_error_state(self):
        assert self._call(has_scraper_error=True) == 'scraper_error'

    def test_v2_analyst_gap_state(self):
        assert self._call(is_analyst_gap=True) == 'analyst_gap'

    def test_v2_analyst_gap_wins_over_data(self):
        assert self._call(is_analyst_gap=True, has_data_point=True) == 'analyst_gap'


# ── compute_expected_periods ──────────────────────────────────────────────────

class TestComputeExpectedPeriods:
    from datetime import date as _date

    def _call(self, windows, as_of_date):
        from coverage_logic import compute_expected_periods
        return compute_expected_periods(windows, as_of_date)

    def test_monthly_single_window(self):
        from datetime import date
        windows = [{'cadence': 'monthly', 'start_date': '2024-01-01', 'end_date': None}]
        result = self._call(windows, date(2024, 3, 31))
        assert result == ['2024-01-01', '2024-02-01', '2024-03-01']

    def test_quarterly_single_window(self):
        from datetime import date
        windows = [{'cadence': 'quarterly', 'start_date': '2024-01-01', 'end_date': None}]
        result = self._call(windows, date(2024, 9, 30))
        assert result == ['2024-01-01', '2024-04-01', '2024-07-01']

    def test_two_windows_regime_change(self):
        from datetime import date
        windows = [
            {'cadence': 'monthly', 'start_date': '2024-01-01', 'end_date': '2024-06-30'},
            {'cadence': 'quarterly', 'start_date': '2024-07-01', 'end_date': None},
        ]
        result = self._call(windows, date(2024, 12, 31))
        assert len(result) == 8  # 6 monthly + 2 quarterly

    def test_empty_windows_returns_empty_list(self):
        from datetime import date
        assert self._call([], date(2024, 12, 31)) == []


# ── rank_extractions ──────────────────────────────────────────────────────────

class TestRankExtractions:

    def _call(self, candidates):
        from coverage_logic import rank_extractions
        return rank_extractions(candidates)

    def test_analyst_protected_wins_over_pipeline(self):
        candidates = [
            {'value': 100, 'confidence': 0.5, 'extraction_method': 'analyst', 'created_at': '2024-01-01'},
            {'value': 200, 'confidence': 0.99, 'extraction_method': 'regex', 'created_at': '2024-01-01'},
        ]
        result = self._call(candidates)
        assert result[0]['extraction_method'] == 'analyst'

    def test_highest_confidence_wins_among_pipeline(self):
        candidates = [
            {'value': 100, 'confidence': 0.7, 'extraction_method': 'regex', 'created_at': '2024-01-01'},
            {'value': 200, 'confidence': 0.9, 'extraction_method': 'regex', 'created_at': '2024-01-01'},
        ]
        result = self._call(candidates)
        assert result[0]['confidence'] == 0.9

    def test_equal_confidence_latest_timestamp_wins(self):
        candidates = [
            {'value': 100, 'confidence': 0.8, 'extraction_method': 'regex', 'created_at': '2024-01-01'},
            {'value': 200, 'confidence': 0.8, 'extraction_method': 'regex', 'created_at': '2024-06-01'},
        ]
        result = self._call(candidates)
        assert result[0]['created_at'] == '2024-06-01'
