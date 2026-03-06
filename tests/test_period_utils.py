"""Tests for src/period_utils.py — written first (TDD)."""
import pytest
from period_utils import normalize_period, quarter_to_month_range


class TestNormalizePeriod:

    # ── canonical pass-through ────────────────────────────────────────────────

    def test_canonical_yyyy_mm_passthrough(self):
        assert normalize_period("2024-03") == "2024-03"

    def test_canonical_yyyy_mm_dd_strips_day(self):
        assert normalize_period("2024-03-31") == "2024-03"

    # ── quarterly forms ───────────────────────────────────────────────────────

    def test_normalize_q1_2024_returns_start_month(self):
        """Q1 2024 maps to January (start of Q1)."""
        assert normalize_period("Q1 2024") == "2024-01"

    def test_normalize_q2_2024(self):
        assert normalize_period("Q2 2024") == "2024-04"

    def test_normalize_q3_2024(self):
        assert normalize_period("Q3 2024") == "2024-07"

    def test_normalize_q4_2024(self):
        assert normalize_period("Q4 2024") == "2024-10"

    def test_normalize_yyyy_qn_format(self):
        """2024-Q1 dash-separated format."""
        assert normalize_period("2024-Q1") == "2024-01"

    def test_normalize_yyyy_qn_no_dash(self):
        """2024Q2 no-dash format."""
        assert normalize_period("2024Q2") == "2024-04"

    # ── fiscal year ───────────────────────────────────────────────────────────

    def test_normalize_fiscal_year_default_december(self):
        """FY2023 with default fiscal_year_end=12 returns December."""
        assert normalize_period("FY2023") == "2023-12"

    def test_normalize_fiscal_year_with_non_december_end(self):
        """FY2023 with fiscal_year_end=3 (March) returns 2023-03."""
        assert normalize_period("FY2023", fiscal_year_end=3) == "2023-03"

    def test_normalize_yyyy_fy_format(self):
        """2023-FY format."""
        assert normalize_period("2023-FY") == "2023-12"

    # ── month name forms ──────────────────────────────────────────────────────

    def test_normalize_full_month_name(self):
        assert normalize_period("January 2024") == "2024-01"

    def test_normalize_abbreviated_month_name(self):
        assert normalize_period("Mar 2024") == "2024-03"

    def test_normalize_month_with_comma(self):
        assert normalize_period("December, 2023") == "2023-12"

    # ── invalid / garbage ────────────────────────────────────────────────────

    def test_normalize_returns_none_on_garbage_input(self):
        assert normalize_period("not a period") is None

    def test_normalize_returns_none_on_empty_string(self):
        assert normalize_period("") is None

    def test_normalize_returns_none_on_none(self):
        assert normalize_period(None) is None

    def test_normalize_invalid_month_number(self):
        """Month 13 should not parse."""
        assert normalize_period("2024-13") is None


class TestQuarterToMonthRange:

    def test_q1_range(self):
        assert quarter_to_month_range("Q1 2024") == ["2024-01", "2024-02", "2024-03"]

    def test_q2_range(self):
        assert quarter_to_month_range("Q2 2024") == ["2024-04", "2024-05", "2024-06"]

    def test_q3_range(self):
        assert quarter_to_month_range("Q3 2024") == ["2024-07", "2024-08", "2024-09"]

    def test_q4_range(self):
        assert quarter_to_month_range("Q4 2024") == ["2024-10", "2024-11", "2024-12"]

    def test_quarterly_window_maps_to_month_range(self):
        """10-Q period "2025-03-31" (Q1) maps to the 3 months of Q1."""
        result = quarter_to_month_range("2025-03-31")
        assert result == ["2025-01", "2025-02", "2025-03"]

    def test_garbage_returns_none(self):
        assert quarter_to_month_range("garbage") is None
