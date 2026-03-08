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
        from interpreters.agreement import evaluate_agreement
        # 10 vs 10.9 = 9% diff — within 10% tolerance
        regex = _make_result(10.0, 'hashrate_eh')
        llm = _make_result(10.9, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'

    def test_outside_10pct_routes_to_review(self):
        from interpreters.agreement import evaluate_agreement
        # 10 vs 11.2 = 12% diff — outside 10% tolerance
        regex = _make_result(10.0, 'hashrate_eh')
        llm = _make_result(11.2, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'REVIEW_QUEUE'

    def test_5pct_diff_auto_accepts_hashrate(self):
        from interpreters.agreement import evaluate_agreement
        # 20 vs 21.0 = 5% diff — within 10% tolerance
        regex = _make_result(20.0, 'hashrate_eh')
        llm = _make_result(21.0, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'


class TestBtcThreshold:
    """production_btc uses 1% tolerance (integer BTC counts)."""

    def test_within_1pct_auto_accepts(self):
        from interpreters.agreement import evaluate_agreement
        # 1000 vs 1009 = 0.9% diff — within 1% tolerance
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1009.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'AUTO_ACCEPT'

    def test_outside_1pct_routes_to_review(self):
        from interpreters.agreement import evaluate_agreement
        # 1000 vs 1012 = 1.2% diff — outside 1% tolerance
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1012.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'REVIEW_QUEUE'

    def test_exact_match_auto_accepts(self):
        from interpreters.agreement import evaluate_agreement
        regex = _make_result(750.0, 'production_btc')
        llm = _make_result(750.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'AUTO_ACCEPT'


class TestDefaultThreshold:
    """Unknown metrics use the default 2% threshold."""

    def test_unknown_metric_within_2pct_auto_accepts(self):
        from interpreters.agreement import evaluate_agreement
        # 100 vs 101.5 = 1.5% diff — within default 2% threshold
        regex = _make_result(100.0, 'unknown_metric_xyz')
        llm = _make_result(101.5, 'unknown_metric_xyz')
        d = evaluate_agreement(regex, llm, metric='unknown_metric_xyz')
        assert d.decision == 'AUTO_ACCEPT'

    def test_unknown_metric_outside_2pct_routes_to_review(self):
        from interpreters.agreement import evaluate_agreement
        # 100 vs 103.0 = 3% diff — outside default 2% threshold
        regex = _make_result(100.0, 'unknown_metric_xyz')
        llm = _make_result(103.0, 'unknown_metric_xyz')
        d = evaluate_agreement(regex, llm, metric='unknown_metric_xyz')
        assert d.decision == 'REVIEW_QUEUE'

    def test_no_metric_arg_uses_default(self):
        """Calling without metric= falls back to default threshold (backward compat)."""
        from interpreters.agreement import evaluate_agreement
        # 100 vs 101.5 = 1.5% diff — within default 2%
        regex = _make_result(100.0, 'production_btc')
        llm = _make_result(101.5, 'production_btc')
        d = evaluate_agreement(regex, llm)
        assert d.decision == 'AUTO_ACCEPT'

    def test_hashrate_previously_rejected_now_accepted(self):
        """Regression: 5% hashrate diff was REVIEW_QUEUE with old 2% global, now AUTO_ACCEPT."""
        from interpreters.agreement import evaluate_agreement
        # 30.0 vs 31.5 = 5% diff
        # Old behavior (2% threshold): REVIEW_QUEUE
        # New behavior (10% threshold): AUTO_ACCEPT
        regex = _make_result(30.0, 'hashrate_eh')
        llm = _make_result(31.5, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh')
        assert d.decision == 'AUTO_ACCEPT'

    def test_btc_previously_accepted_now_rejected(self):
        """Regression: 1.5% BTC diff was AUTO_ACCEPT with old 2% global, now REVIEW_QUEUE."""
        from interpreters.agreement import evaluate_agreement
        # 1000 vs 1016 = 1.6% diff
        # Old behavior (2% threshold): AUTO_ACCEPT
        # New behavior (1% threshold): REVIEW_QUEUE
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1016.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc')
        assert d.decision == 'REVIEW_QUEUE'


class TestExplicitThresholdParam:
    """Explicit threshold= param overrides config lookup (DB-sourced values)."""

    def test_explicit_threshold_overrides_config(self):
        """Passing threshold=0.50 accepts a 40% diff even though config has 1%."""
        from interpreters.agreement import evaluate_agreement
        # production_btc config threshold is 1%; 40% diff would normally REVIEW_QUEUE
        regex = _make_result(100.0, 'production_btc')
        llm = _make_result(140.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc', threshold=0.50)
        assert d.decision == 'AUTO_ACCEPT'

    def test_explicit_threshold_tighter_than_config(self):
        """Passing threshold=0.001 rejects a 5% diff even though config allows 10%."""
        from interpreters.agreement import evaluate_agreement
        # hashrate_eh config threshold is 10%; 5% would normally AUTO_ACCEPT
        regex = _make_result(10.0, 'hashrate_eh')
        llm = _make_result(10.5, 'hashrate_eh')
        d = evaluate_agreement(regex, llm, metric='hashrate_eh', threshold=0.001)
        assert d.decision == 'REVIEW_QUEUE'

    def test_explicit_threshold_none_falls_back_to_config(self):
        """threshold=None falls back to per-metric config lookup (no regression)."""
        from interpreters.agreement import evaluate_agreement
        regex = _make_result(1000.0, 'production_btc')
        llm = _make_result(1005.0, 'production_btc')
        d = evaluate_agreement(regex, llm, metric='production_btc', threshold=None)
        assert d.decision == 'AUTO_ACCEPT'


class TestOutlierMinHistoryFromRule:
    """Fix 7: outlier_min_history must be read from metric_rule when present."""

    def test_outlier_min_history_from_rule_overrides_default(self):
        """get_trailing_data_points must be called with limit from metric_rule, not config."""
        from interpreters.interpret_pipeline import _apply_agreement
        from miner_types import ExtractionResult, ExtractionSummary
        from unittest.mock import MagicMock

        report = {
            'id': 1, 'ticker': 'MARA', 'report_date': '2024-01-31',
            'source_type': 'ir_press_release',
        }
        db = MagicMock()
        db.data_point_exists.return_value = False
        db.get_trailing_data_points.return_value = []

        # Regex and LLM agree exactly → AUTO_ACCEPT → outlier check runs
        regex_result = ExtractionResult(
            value=750.0, unit='BTC', confidence=0.9,
            extraction_method='regex', source_snippet='produced 750 BTC',
            metric='production_btc', pattern_id='btc_1',
        )
        llm_result = ExtractionResult(
            value=750.0, unit='BTC', confidence=0.9,
            extraction_method='llm', source_snippet='produced 750 BTC',
            metric='production_btc', pattern_id=None,
        )

        rule = {
            'metric': 'production_btc',
            'agreement_threshold': 0.01,
            'outlier_threshold': 0.5,
            'outlier_min_history': 7,  # non-default — must override config
            'enabled': 1,
        }

        _apply_agreement(
            metric='production_btc',
            regex_best=regex_result,
            llm_result=llm_result,
            db=db,
            report=report,
            llm_available=True,
            confidence_threshold=0.75,
            summary=ExtractionSummary(),
            metric_rule=rule,
        )

        db.get_trailing_data_points.assert_called()
        call_kwargs = db.get_trailing_data_points.call_args
        actual_limit = (call_kwargs.args[3] if len(call_kwargs.args) > 3
                        else call_kwargs.kwargs.get('limit'))
        assert actual_limit == 7, f"Expected limit=7 from rule, got {actual_limit}"

    def test_outlier_min_history_uses_default_when_no_rule(self):
        """get_trailing_data_points uses OUTLIER_MIN_HISTORY constant when no metric_rule."""
        from interpreters.interpret_pipeline import _apply_agreement
        from miner_types import ExtractionResult, ExtractionSummary
        from config import OUTLIER_MIN_HISTORY
        from unittest.mock import MagicMock

        report = {
            'id': 2, 'ticker': 'MARA', 'report_date': '2024-02-29',
            'source_type': 'ir_press_release',
        }
        db = MagicMock()
        db.data_point_exists.return_value = False
        db.get_trailing_data_points.return_value = []

        regex_result = ExtractionResult(
            value=800.0, unit='BTC', confidence=0.9,
            extraction_method='regex', source_snippet='produced 800 BTC',
            metric='production_btc', pattern_id='btc_1',
        )
        llm_result = ExtractionResult(
            value=800.0, unit='BTC', confidence=0.9,
            extraction_method='llm', source_snippet='produced 800 BTC',
            metric='production_btc', pattern_id=None,
        )

        _apply_agreement(
            metric='production_btc',
            regex_best=regex_result,
            llm_result=llm_result,
            db=db,
            report=report,
            llm_available=True,
            confidence_threshold=0.75,
            summary=ExtractionSummary(),
            metric_rule=None,
        )

        db.get_trailing_data_points.assert_called()
        call_kwargs = db.get_trailing_data_points.call_args
        actual_limit = (call_kwargs.args[3] if len(call_kwargs.args) > 3
                        else call_kwargs.kwargs.get('limit'))
        assert actual_limit == OUTLIER_MIN_HISTORY


class TestMetricRuleEnabledFlag:
    """Issue #4: disabled metric_rule (enabled=0) must fall back to config defaults."""

    def test_disabled_rule_falls_back_to_config_threshold(self):
        """A metric_rule with enabled=0 must be treated as no rule (config threshold used)."""
        from interpreters.interpret_pipeline import _apply_agreement
        from unittest.mock import MagicMock, patch
        from miner_types import ExtractionResult

        regex_result = ExtractionResult(
            value=1000.0, unit='BTC', confidence=0.9,
            extraction_method='regex', source_snippet='produced 1000 BTC',
            metric='production_btc', pattern_id='btc_1',
        )
        llm_result = ExtractionResult(
            value=1015.0, unit='BTC', confidence=0.85,
            extraction_method='llm', source_snippet='produced 1015 BTC',
            metric='production_btc', pattern_id=None,
        )
        report = {
            'id': 1, 'ticker': 'MARA', 'report_date': '2024-01-31',
            'source_type': 'ir_press_release',
        }

        # Rule with enabled=0 and a very loose threshold of 50%
        # If the rule were applied, 1.5% diff would AUTO_ACCEPT (50% > 1.5%)
        # If disabled (falls back to config 1%), 1.5% diff routes to REVIEW_QUEUE
        disabled_rule = {
            'metric': 'production_btc', 'enabled': 0,
            'agreement_threshold': 0.50, 'outlier_threshold': 2.0,
            'outlier_min_history': 3,
        }

        db = MagicMock()
        db.data_point_exists.return_value = False
        db.get_trailing_values.return_value = []

        from miner_types import ExtractionSummary
        summary = ExtractionSummary()

        with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}):
            _apply_agreement(
                metric='production_btc',
                regex_best=regex_result,
                llm_result=llm_result,
                llm_available=True,
                db=db,
                report=report,
                confidence_threshold=0.75,
                summary=summary,
                metric_rule=disabled_rule,
            )

        # With enabled=0, falls back to config 1% threshold → 1.5% diff → REVIEW_QUEUE → insert_review_item
        db.insert_review_item.assert_called_once()
        db.insert_data_point.assert_not_called()
