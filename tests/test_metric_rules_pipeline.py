"""Tests for metric_rules loading in extract_report.

Verifies that db.get_metric_rules() is called during extraction and that the
matching rule is forwarded to _apply_llm_result(metric_rule=...).
"""
import pytest
from unittest.mock import MagicMock, patch, call


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


class TestMetricRulesLoadedInPipeline:
    """extract_report must call db.get_metric_rules() and pass each rule to _apply_llm_result."""

    def _make_mock_db(self, rules=None):
        db = MagicMock()
        db.get_metric_rules.return_value = rules or []
        db.mark_report_extraction_running.return_value = None
        db.data_point_exists.return_value = False
        db.get_metric_schema.return_value = [
            {'key': 'production_btc', 'active': 1},
            {'key': 'hashrate_eh', 'active': 1},
        ]
        db.get_all_metric_keywords.return_value = [
            {'phrase': 'btc produced', 'metric_key': 'production_btc'},
        ]
        return db

    def test_get_metric_rules_called_once_per_extract_report(self):
        """db.get_metric_rules() must be called during extract_report."""
        from interpreters.interpret_pipeline import extract_report

        db = self._make_mock_db()
        with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=MagicMock(
                 extract_batch=lambda *a, **kw: {},
                 _last_call_meta={},
                 _last_transport_error=False,
                 _last_batch_summary=None,
             )), \
             patch('interpreters.interpret_pipeline._run_llm_batch', return_value=({}, {})), \
             patch('infra.keyword_service.get_mining_detection_phrases', return_value=['bitcoin', 'btc produced']):
            extract_report(_make_report(), db)

        db.get_metric_rules.assert_called()

    def test_metric_rule_passed_to_apply_llm_result(self):
        """_apply_llm_result must be called with metric_rule= set to the matching rule."""
        from interpreters.interpret_pipeline import extract_report

        rule = {
            'metric': 'production_btc',
            'agreement_threshold': 0.50,
            'outlier_threshold': 2.0,
            'outlier_min_history': 3,
            'enabled': 1,
        }
        db = self._make_mock_db(rules=[rule])

        with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=MagicMock(
                 _last_call_meta={}, _last_transport_error=False, _last_batch_summary=None,
             )), \
             patch('interpreters.interpret_pipeline._run_llm_batch', return_value=({}, {})), \
             patch('infra.keyword_service.get_mining_detection_phrases', return_value=['bitcoin', 'btc produced']), \
             patch('interpreters.interpret_pipeline._apply_llm_result') as mock_apply:
            extract_report(_make_report(), db)

        assert mock_apply.called
        # Check the call for production_btc specifically (not just the last call)
        prod_calls = [
            kw for _, kw in mock_apply.call_args_list
            if kw.get('metric') == 'production_btc'
        ]
        assert prod_calls, "Expected _apply_llm_result called for production_btc"
        assert prod_calls[0].get('metric_rule') == rule

    def test_no_rule_passes_none(self):
        """When no rule for a metric, _apply_llm_result receives metric_rule=None."""
        from interpreters.interpret_pipeline import extract_report

        db = self._make_mock_db(rules=[])

        with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=MagicMock(
                 _last_call_meta={}, _last_transport_error=False, _last_batch_summary=None,
             )), \
             patch('interpreters.interpret_pipeline._run_llm_batch', return_value=({}, {})), \
             patch('infra.keyword_service.get_mining_detection_phrases', return_value=['bitcoin', 'btc produced']), \
             patch('interpreters.interpret_pipeline._apply_llm_result') as mock_apply:
            extract_report(_make_report(), db)

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
        db = self._make_mock_db(rules=rules)

        with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=MagicMock(
                 _last_call_meta={}, _last_transport_error=False, _last_batch_summary=None,
             )), \
             patch('interpreters.interpret_pipeline._run_llm_batch', return_value=({}, {})), \
             patch('infra.keyword_service.get_mining_detection_phrases', return_value=['bitcoin', 'btc produced']), \
             patch('interpreters.interpret_pipeline._apply_llm_result') as mock_apply:
            extract_report(_make_report(), db)

        calls_by_metric = {}
        for c in mock_apply.call_args_list:
            _, kw = c
            m = kw.get('metric')
            calls_by_metric[m] = kw.get('metric_rule')

        assert calls_by_metric.get('production_btc') == rules[0]
        assert calls_by_metric.get('hashrate_eh') == rules[1]
