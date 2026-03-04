"""Tests for statistical outlier detection in the agreement engine.

Written before implementation (test-first). All tests should fail until
detect_outlier() is added to extractors/agreement.py.
"""
import pytest


class TestDetectOutlier:
    def test_no_outlier_within_threshold(self):
        """Value close to trailing average is not flagged."""
        from extractors.agreement import detect_outlier
        trailing = [700.0, 720.0, 710.0]
        candidate = 730.0  # ~2.8% above avg of ~710 — well within 40%
        is_outlier, avg = detect_outlier(candidate, trailing, threshold_pct=0.40)
        assert is_outlier is False
        assert avg == pytest.approx((700 + 720 + 710) / 3, rel=1e-3)

    def test_outlier_above_threshold(self):
        """Value 50% above 3-month avg is flagged with threshold=0.40."""
        from extractors.agreement import detect_outlier
        trailing = [700.0, 720.0, 710.0]
        # avg = 710; 710 * 1.50 = 1065
        candidate = 1065.0
        is_outlier, avg = detect_outlier(candidate, trailing, threshold_pct=0.40)
        assert is_outlier is True
        assert avg == pytest.approx(710.0, rel=1e-3)

    def test_outlier_below_threshold(self):
        """Value 60% below avg is flagged."""
        from extractors.agreement import detect_outlier
        trailing = [700.0, 720.0, 710.0]
        # avg ~ 710; 710 * 0.40 = 284
        candidate = 284.0
        is_outlier, avg = detect_outlier(candidate, trailing, threshold_pct=0.40)
        assert is_outlier is True

    def test_insufficient_history_returns_false(self):
        """Fewer than OUTLIER_MIN_HISTORY trailing values → no outlier flagged."""
        from extractors.agreement import detect_outlier
        from config import OUTLIER_MIN_HISTORY
        trailing = [700.0] * (OUTLIER_MIN_HISTORY - 1)  # one short
        is_outlier, avg = detect_outlier(99999.0, trailing, threshold_pct=0.40)
        assert is_outlier is False
        assert avg is None

    def test_empty_history_returns_false(self):
        """Empty trailing values → no outlier flagged."""
        from extractors.agreement import detect_outlier
        is_outlier, avg = detect_outlier(99999.0, [], threshold_pct=0.40)
        assert is_outlier is False
        assert avg is None

    def test_zero_avg_handled(self):
        """All trailing values = 0, candidate = 100 → outlier flagged."""
        from extractors.agreement import detect_outlier
        trailing = [0.0, 0.0, 0.0]
        is_outlier, avg = detect_outlier(100.0, trailing, threshold_pct=0.40)
        # avg=0, denominator=max(0,1e-9)=1e-9, ratio = 100/1e-9 >> 0.40
        assert is_outlier is True

    def test_exactly_at_boundary_not_outlier(self):
        """Value exactly at threshold percentage is NOT flagged (boundary is exclusive)."""
        from extractors.agreement import detect_outlier
        trailing = [100.0, 100.0, 100.0]
        # avg=100, threshold=0.40, boundary = 100 * 1.40 = 140
        candidate = 140.0  # exactly at boundary
        is_outlier, avg = detect_outlier(candidate, trailing, threshold_pct=0.40)
        assert is_outlier is False

    def test_just_above_boundary_is_outlier(self):
        """Value just above boundary IS flagged."""
        from extractors.agreement import detect_outlier
        trailing = [100.0, 100.0, 100.0]
        candidate = 140.1  # just above 40% boundary
        is_outlier, avg = detect_outlier(candidate, trailing, threshold_pct=0.40)
        assert is_outlier is True

    def test_returns_trailing_avg(self):
        """The returned avg is the mean of the trailing values."""
        from extractors.agreement import detect_outlier
        trailing = [100.0, 200.0, 300.0]
        _, avg = detect_outlier(150.0, trailing, threshold_pct=0.40)
        assert avg == pytest.approx(200.0)
