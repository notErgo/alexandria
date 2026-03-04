"""Tests for confidence scoring."""
from extractors.confidence import score_extraction


def test_high_weight_close_context_scores_high():
    score = score_extraction(pattern_weight=0.95, context_distance=0,
                             value=450.0, metric="production_btc")
    assert score >= 0.90


def test_large_distance_degrades_score():
    close = score_extraction(0.95, 0, 450.0, "production_btc")
    far = score_extraction(0.95, 200, 450.0, "production_btc")
    assert far < close


def test_out_of_range_value_degrades_score():
    # 100000 BTC/month is well above the valid range (0, 5000)
    score = score_extraction(0.95, 0, 100000.0, "production_btc")
    assert score < 0.50


def test_score_clamped_between_zero_and_one():
    score = score_extraction(1.0, 0, 100.0, "production_btc")
    assert 0.0 <= score <= 1.0


def test_unknown_metric_still_returns_score():
    # Unknown metric has no range — should not crash; range_factor defaults 1.0
    score = score_extraction(0.8, 0, 42.0, "unknown_metric")
    assert 0.0 <= score <= 1.0


def test_realization_rate_in_range():
    # 0.95 ratio is within [0, 1]
    score = score_extraction(0.92, 0, 0.95, "realization_rate")
    assert score >= 0.85
