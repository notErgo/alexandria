"""Tests for extraction status transition fixes (schema v16, issues #3 and #4)."""
import unittest
from unittest.mock import MagicMock, patch, call


def _make_report(report_id=1, ticker='MARA', source_type='ir_press_release',
                 raw_text='MARA bitcoin mined 500 BTC in January 2024.'):
    return {
        'id': report_id,
        'ticker': ticker,
        'report_date': '2024-01-31',
        'source_type': source_type,
        'raw_text': raw_text,
        'covering_period': '2024-Q1' if 'edgar_10q' in source_type else None,
    }


def _make_quarterly_report(report_id=10, ticker='MARA',
                            source_type='edgar_10q',
                            raw_text='Bitcoin mined 1500 BTC in Q1 2024.'):
    return {
        'id': report_id,
        'ticker': ticker,
        'report_date': '2024-03-31',
        'source_type': source_type,
        'raw_text': raw_text,
        'covering_period': '2024-Q1',
    }


class TestRunningStatusSetBeforeExtraction(unittest.TestCase):
    """Issue #4: extraction_status must be set to 'running' before LLM call."""

    def test_running_status_set_before_extraction(self):
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}

        report = _make_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}):
                    mock_llm_fn.return_value = None
                    extract_report(report, db, registry)

        db.mark_report_extraction_running.assert_called_once_with(report['id'])

    def test_running_called_before_mark_extracted(self):
        """mark_report_extraction_running must be called before mark_report_extracted."""
        from interpreters.interpret_pipeline import extract_report

        call_order = []
        db = MagicMock()
        db.mark_report_extraction_running.side_effect = lambda rid: call_order.append('running')
        db.mark_report_extracted.side_effect = lambda rid: call_order.append('extracted')

        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}

        report = _make_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}):
                    mock_llm_fn.return_value = None
                    extract_report(report, db, registry)

        self.assertIn('running', call_order)
        if 'extracted' in call_order:
            self.assertLess(call_order.index('running'), call_order.index('extracted'))


class TestLLMUnavailableLeavesStatusPending(unittest.TestCase):
    """Issue #3: Transient LLM failures must not call mark_report_extracted (status stays pending)."""

    def test_quarterly_llm_unavailable_does_not_mark_extracted(self):
        """When LLM is unavailable for quarterly report, mark_report_extracted must NOT be called."""
        from interpreters.interpret_pipeline import _interpret_quarterly_report
        from miner_types import ExtractionSummary

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}
        summary = ExtractionSummary()

        report = _make_quarterly_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                mock_llm_fn.return_value = None
                _interpret_quarterly_report(report, db, registry, summary)

        db.mark_report_extracted.assert_not_called()

    def test_quarterly_llm_unavailable_does_not_mark_failed(self):
        """LLM unavailability is transient — must not mark the report as permanently failed."""
        from interpreters.interpret_pipeline import _interpret_quarterly_report
        from miner_types import ExtractionSummary

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}
        summary = ExtractionSummary()

        report = _make_quarterly_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                mock_llm_fn.return_value = None
                _interpret_quarterly_report(report, db, registry, summary)

        db.mark_report_extraction_failed.assert_not_called()

    def test_quarterly_llm_unavailable_resets_to_pending(self):
        """Transient LLM failure must reset status from running to pending so same process can retry."""
        from interpreters.interpret_pipeline import _interpret_quarterly_report
        from miner_types import ExtractionSummary

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}
        summary = ExtractionSummary()

        report = _make_quarterly_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                mock_llm_fn.return_value = None
                _interpret_quarterly_report(report, db, registry, summary)

        db.reset_report_to_pending.assert_called_once_with(report['id'])


class TestExtractionExceptionMarksFailed(unittest.TestCase):
    """Issue #3: Exception during extraction must call mark_report_extraction_failed."""

    def test_extraction_exception_marks_failed(self):
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}

        report = _make_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True):
                with patch('interpreters.interpret_pipeline._build_regex_by_metric',
                           side_effect=RuntimeError('simulated error')):
                    mock_llm_fn.return_value = MagicMock()
                    extract_report(report, db, registry)

        db.mark_report_extraction_failed.assert_called_once()
        args = db.mark_report_extraction_failed.call_args[0]
        self.assertEqual(args[0], report['id'])

    def test_quarterly_exception_marks_failed(self):
        from interpreters.interpret_pipeline import _interpret_quarterly_report
        from miner_types import ExtractionSummary

        db = MagicMock()
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}
        summary = ExtractionSummary()

        report = _make_quarterly_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=True):
                mock_llm_fn.return_value = MagicMock()
                mock_llm_fn.return_value.extract_quarterly_batch.side_effect = RuntimeError('llm crashed')
                _interpret_quarterly_report(report, db, registry, summary)

        db.mark_report_extraction_failed.assert_called_once()


class TestSuccessfulExtractionMarksDone(unittest.TestCase):
    """Happy path: successful extraction must still call mark_report_extracted."""

    def test_successful_extraction_marks_done(self):
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        db.data_point_exists.return_value = False
        db.get_trailing_values.return_value = []
        registry = MagicMock()
        registry.metrics = {'production_btc': MagicMock()}

        report = _make_report()

        with patch('interpreters.interpret_pipeline._get_llm_interpreter') as mock_llm_fn:
            with patch('interpreters.interpret_pipeline._check_llm_available', return_value=False):
                with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}):
                    with patch('interpreters.interpret_pipeline._try_gap_fill'):
                        mock_llm_fn.return_value = None
                        extract_report(report, db, registry)

        db.mark_report_extracted.assert_called_once_with(report['id'])


if __name__ == '__main__':
    unittest.main()
