"""Tests for per-metric agreement thresholds in evaluate_agreement().

These tests verify that the agreement engine uses per-metric tolerances
from METRIC_AGREEMENT_THRESHOLDS, not the single global LLM_AGREEMENT_THRESHOLD.

Written before implementation (test-first). All tests should fail until
evaluate_agreement() accepts the metric= keyword argument.
"""
import pytest
from miner_types import ExtractionResult


def _make_result(value: float, metric: str = 'production_btc') -> ExtractionResult:
    return ExtractionResult(
        value=value,
        unit='BTC',
        confidence=0.9,
        extraction_method='test',
        source_snippet='test snippet',
        metric=metric,
        pattern_id='test_pattern',
    )


class TestHashrateThreshold:
    """hashrate_eh uses 10% tolerance (values round to 1 decimal EH/s)."""

    def test_within_10pct_auto_accepts(self):
        from extractors.agreement import evaluate_agreement
        # 10 vs 10.9 = 9% diff — within 10% tolerance
        regex = _make_result(10.0, 'hashrate_eh')
        llm = _make_result(10.9, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'

    def test_outside_10pct_routes_to_review(self):
        from extractors.agreement import evaluate_agreement
        # 10 vs 11.2 = 12% diff — outside 10% tolerance
        regex = _make_result(10.0, 'hashrate_eh')
        llm = _make_result(11.2, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'REVIEW_QUEUE'

    def test_5pct_diff_auto_accepts_hashrate(self):
        from extractors.agreement import evaluate_agreement
        # 20 vs 21.0 = 5% diff — within 10% tolerance
        regex = _make_result(20.0, 'hashrate_eh')
        llm = _make_result(21.0, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'


class TestBtcThreshold:
    """production_btc uses 1% tolerance (integer BTC counts)."""

    def test_within_1pct_auto_accepts(self):
        from extractors.agreement import evaluate_agreement
        # 1000 vs 1009 = 0.9% diff — within 1% tolerance
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1009.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'AUTO_ACCEPT'

    def test_outside_1pct_routes_to_review(self):
        from extractors.agreement import evaluate_agreement
        # 1000 vs 1012 = 1.2% diff — outside 1% tolerance
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1012.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'REVIEW_QUEUE'

    def test_exact_match_auto_accepts(self):
        from extractors.agreement import evaluate_agreement
        regex = _make_result(750.0, 'production_btc')
        llm = _make_result(750.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'AUTO_ACCEPT'


class TestDefaultThreshold:
    """Unknown metrics use the default 2% threshold."""

    def test_unknown_metric_within_2pct_auto_accepts(self):
        from extractors.agreement import evaluate_agreement
        # 100 vs 101.5 = 1.5% diff — within default 2% threshold
        regex = _make_result(100.0, 'unknown_metric_xyz')
        llm = _make_result(101.5, 'unknown_metric_xyz')
        d = evaluate_agreement(regex, llm, metric='unknown_metric_xyz')
        assert d.decision == 'AUTO_ACCEPT'

    def test_unknown_metric_outside_2pct_routes_to_review(self):
        from extractors.agreement import evaluate_agreement
        # 100 vs 103.0 = 3% diff — outside default 2% threshold
        regex = _make_result(100.0, 'unknown_metric_xyz')
        llm = _make_result(103.0, 'unknown_metric_xyz')
        d = evaluate_agreement(regex, llm, metric='unknown_metric_xyz')
        assert d.decision == 'REVIEW_QUEUE'

    def test_no_metric_arg_uses_default(self):
        """Calling without metric= falls back to default threshold (backward compat)."""
        from extractors.agreement import evaluate_agreement
        # 100 vs 101.5 = 1.5% diff — within default 2%
        regex = _make_result(100.0, 'production_btc')
        llm = _make_result(101.5, 'production_btc')
        d = evaluate_agreement(regex, llm)
        assert d.decision == 'AUTO_ACCEPT'

    def test_hashrate_previously_rejected_now_accepted(self):
        """Regression: 5% hashrate diff was REVIEW_QUEUE with old 2% global, now AUTO_ACCEPT."""
        from extractors.agreement import evaluate_agreement
        # 30.0 vs 31.5 = 5% diff
        # Old behavior (2% threshold): REVIEW_QUEUE
        # New behavior (10% threshold): AUTO_ACCEPT
        regex = _make_result(30.0, 'hashrate_eh')
        llm = _make_result(31.5, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'

    def test_btc_previously_accepted_now_rejected(self):
        """Regression: 1.5% BTC diff was AUTO_ACCEPT with old 2% global, now REVIEW_QUEUE."""
        from extractors.agreement import evaluate_agreement
        # 1000 vs 1016 = 1.6% diff
        # Old behavior (2% threshold): AUTO_ACCEPT
        # New behavior (1% threshold): REVIEW_QUEUE
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1016.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'REVIEW_QUEUE'
