"""Tests for metric_rules loading and passing to _apply_agreement (Fix 1).

TDD: these tests fail until extraction_pipeline.py loads metric_rules
from db and passes metric_rule= to _apply_agreement().
"""
import pytest
from unittest.mock import MagicMock, patch


def _make_report(**kwargs):
    base = {
        'id': 1,
        'ticker': 'MARA',
        'report_date': '2024-01-31',
        'source_type': 'ir_press_release',
        'raw_text': 'MARA btc produced 750 BTC in January 2024. Hash rate 20 EH/s.',
    }
    base.update(kwargs)
    return base


def _fake_regex_results(metrics=('production_btc',)):
    """Return a fake regex_by_metric dict so the LLM-unavailable skip guard doesn't fire."""
    from miner_types import ExtractionResult
    result = {}
    for m in metrics:
        result[m] = ExtractionResult(
            value=750.0, unit='BTC', confidence=0.9,
            extraction_method='regex', source_snippet='produced 750 BTC',
            metric=m, pattern_id='test_pat',
        )
    return result


class TestMetricRulesLoadedInPipeline:
    """extract_report must call db.get_metric_rules() and pass each rule to _apply_agreement."""

    def test_get_metric_rules_called_once_per_extract_report(self):
        """db.get_metric_rules() must be called during extract_report."""
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        db.get_metric_rules.return_value = []
        db.mark_report_extraction_running.return_value = None

        registry = MagicMock()
        registry.metrics = {'production_btc': []}

        with patch('interpreters.interpret_pipeline._build_regex_by_metric',
                   return_value=_fake_regex_results()), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=False), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=None), \
             patch('interpreters.interpret_pipeline._apply_agreement'):
            extract_report(_make_report(), db, registry)

        db.get_metric_rules.assert_called()

    def test_metric_rule_passed_to_apply_agreement(self):
        """_apply_agreement must be called with metric_rule= set to the matching rule."""
        from interpreters.interpret_pipeline import extract_report

        rule = {
            'metric': 'production_btc',
            'agreement_threshold': 0.50,
            'outlier_threshold': 2.0,
            'outlier_min_history': 3,
            'enabled': 1,
        }
        db = MagicMock()
        db.get_metric_rules.return_value = [rule]
        db.mark_report_extraction_running.return_value = None

        registry = MagicMock()
        registry.metrics = {'production_btc': []}

        with patch('interpreters.interpret_pipeline._build_regex_by_metric',
                   return_value=_fake_regex_results()), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=False), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=None), \
             patch('interpreters.interpret_pipeline._apply_agreement') as mock_apply:
            extract_report(_make_report(), db, registry)

        assert mock_apply.called
        _, call_kwargs = mock_apply.call_args
        assert call_kwargs.get('metric_rule') == rule

    def test_no_rule_passes_none(self):
        """When no rule for a metric, _apply_agreement receives metric_rule=None."""
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        db.get_metric_rules.return_value = []
        db.mark_report_extraction_running.return_value = None

        registry = MagicMock()
        registry.metrics = {'production_btc': []}

        with patch('interpreters.interpret_pipeline._build_regex_by_metric',
                   return_value=_fake_regex_results()), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=False), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=None), \
             patch('interpreters.interpret_pipeline._apply_agreement') as mock_apply:
            extract_report(_make_report(), db, registry)

        assert mock_apply.called
        _, call_kwargs = mock_apply.call_args
        assert call_kwargs.get('metric_rule') is None

    def test_multiple_metrics_each_get_correct_rule(self):
        """Each metric gets its own rule (keyed by metric name)."""
        from interpreters.interpret_pipeline import extract_report

        rules = [
            {'metric': 'production_btc', 'agreement_threshold': 0.01, 'enabled': 1},
            {'metric': 'hashrate_eh', 'agreement_threshold': 0.10, 'enabled': 1},
        ]
        db = MagicMock()
        db.get_metric_rules.return_value = rules
        db.mark_report_extraction_running.return_value = None

        registry = MagicMock()
        registry.metrics = {'production_btc': [], 'hashrate_eh': []}

        calls_by_metric = {}
        with patch('interpreters.interpret_pipeline._build_regex_by_metric',
                   return_value=_fake_regex_results(('production_btc', 'hashrate_eh'))), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=False), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=None), \
             patch('interpreters.interpret_pipeline._apply_agreement') as mock_apply:
            extract_report(_make_report(), db, registry)

        for c in mock_apply.call_args_list:
            _, kw = c
            m = kw.get('metric')
            calls_by_metric[m] = kw.get('metric_rule')

        assert calls_by_metric.get('production_btc') == rules[0]
        assert calls_by_metric.get('hashrate_eh') == rules[1]
