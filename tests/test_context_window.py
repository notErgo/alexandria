"""
Tests for interpreters.context_window.ContextWindowSelector.

Written test-first (TDD). These tests FAIL before context_window.py is created,
and PASS after implementation.
"""
import unittest
from unittest.mock import MagicMock

import pytest


class TestContextWindowSelector:
    """Tests for ContextWindowSelector chunk path, sliding path, and needs_fallback."""

    def _make_chunk(self, chunk_index, text, section='body'):
        return {
            'id': chunk_index,
            'chunk_index': chunk_index,
            'section': section,
            'text': text,
            'char_start': 0,
            'char_end': len(text),
            'token_count': len(text) // 4,
        }

    def test_chunk_path_scores_by_metric_keywords(self):
        """Primary window is the chunk with most keyword hits for target metric."""
        from interpreters.context_window import ContextWindowSelector

        chunk0 = self._make_chunk(0, 'Company had strong revenue this quarter.')
        chunk1 = self._make_chunk(1, 'BTC produced: 750 bitcoin mined. Total mined BTC was 750.')
        chunk2 = self._make_chunk(2, 'Legal disclaimer and forward looking statements.')

        db = MagicMock()
        db.get_chunks_for_report.return_value = [chunk0, chunk1, chunk2]

        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text='dummy', metric='production_btc', db=db)

        assert len(windows) >= 1
        # The first window should include chunk1 content (highest keyword density)
        assert 'bitcoin mined' in windows[0]['text'] or 'BTC produced' in windows[0]['text']

    def test_chunk_path_respects_budget(self):
        """Combined chunks never exceed CONTEXT_CHAR_BUDGET chars."""
        from interpreters.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET

        # Create chunks summing to ~30k chars total
        chunks = [self._make_chunk(i, 'btc mined ' + 'x' * 5000) for i in range(6)]

        db = MagicMock()
        db.get_chunks_for_report.return_value = chunks

        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text='dummy', metric='production_btc', db=db)

        for w in windows:
            assert len(w['text']) <= CONTEXT_CHAR_BUDGET, (
                "Window exceeded budget: {} > {}".format(len(w['text']), CONTEXT_CHAR_BUDGET)
            )

    def test_sliding_path_when_no_chunks(self):
        """When get_chunks_for_report returns [], falls back to first sliding window."""
        from interpreters.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET

        db = MagicMock()
        db.get_chunks_for_report.return_value = []

        raw_text = 'bitcoin mined 500 ' + 'a' * 10000
        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text=raw_text, metric='production_btc', db=db)

        assert len(windows) >= 1
        assert windows[0]['source'] == 'sliding'
        assert windows[0]['text'] == raw_text[:CONTEXT_CHAR_BUDGET]

    def test_sliding_window_overlap(self):
        """Second fallback window starts at budget - budget//4 chars (25% overlap)."""
        from interpreters.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET

        db = MagicMock()
        db.get_chunks_for_report.return_value = []

        # Long enough text to have at least 2 sliding windows
        raw_text = 'x' * (CONTEXT_CHAR_BUDGET * 3)
        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text=raw_text, metric='production_btc', db=db)

        assert len(windows) >= 2
        overlap_start = CONTEXT_CHAR_BUDGET - CONTEXT_CHAR_BUDGET // 4
        expected_window1_text = raw_text[overlap_start: overlap_start + CONTEXT_CHAR_BUDGET]
        assert windows[1]['text'] == expected_window1_text

    def test_fallback_windows_count(self):
        """select_windows() returns at most 3 windows total."""
        from interpreters.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET

        db = MagicMock()
        db.get_chunks_for_report.return_value = []

        # Very long text -- enough for many sliding windows
        raw_text = 'btc mined ' * 10000
        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text=raw_text, metric='production_btc', db=db)

        assert len(windows) <= 3

    def test_needs_fallback_true_on_none(self):
        """needs_fallback(None) returns True."""
        from interpreters.context_window import ContextWindowSelector
        selector = ContextWindowSelector()
        assert selector.needs_fallback(None) is True

    def test_needs_fallback_true_on_low_confidence(self):
        """needs_fallback(result) where result.confidence=0.4 returns True."""
        from interpreters.context_window import ContextWindowSelector
        from miner_types import ExtractionResult

        selector = ContextWindowSelector()
        result = ExtractionResult(
            metric='production_btc', value=500.0, unit='BTC', confidence=0.4,
            extraction_method='llm', source_snippet='mined 500', pattern_id='llm',
        )
        assert selector.needs_fallback(result) is True

    def test_needs_fallback_false_on_good_result(self):
        """needs_fallback returns False when confidence >= 0.5 and value is not None."""
        from interpreters.context_window import ContextWindowSelector
        from miner_types import ExtractionResult

        selector = ContextWindowSelector()
        result = ExtractionResult(
            metric='production_btc', value=500.0, unit='BTC', confidence=0.85,
            extraction_method='llm', source_snippet='mined 500', pattern_id='llm',
        )
        assert selector.needs_fallback(result) is False

    def test_window_never_exceeds_budget(self):
        """Even if a single chunk is larger than budget, returned text is truncated."""
        from interpreters.context_window import ContextWindowSelector
        from config import CONTEXT_CHAR_BUDGET

        oversized_chunk = self._make_chunk(0, 'btc mined 750 ' + 'z' * (CONTEXT_CHAR_BUDGET * 2))
        db = MagicMock()
        db.get_chunks_for_report.return_value = [oversized_chunk]

        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text='dummy', metric='production_btc', db=db)

        assert len(windows) >= 1
        assert len(windows[0]['text']) <= CONTEXT_CHAR_BUDGET

    def test_quarterly_budget_larger_than_monthly(self):
        """edgar_10q doc_type uses a larger budget than ir_press_release."""
        from interpreters.context_window import ContextWindowSelector

        monthly_selector = ContextWindowSelector(doc_type='ir_press_release')
        quarterly_selector = ContextWindowSelector(doc_type='edgar_10q')

        assert quarterly_selector.char_budget > monthly_selector.char_budget

    def test_chunk_path_window_index_set(self):
        """Each returned window dict has window_index key set correctly."""
        from interpreters.context_window import ContextWindowSelector

        chunks = [
            self._make_chunk(0, 'btc mined 500 bitcoin'),
            self._make_chunk(1, 'hashrate deployed energized'),
        ]
        db = MagicMock()
        db.get_chunks_for_report.return_value = chunks

        selector = ContextWindowSelector(doc_type='ir_press_release')
        windows = selector.select_windows(report_id=1, raw_text='dummy', metric='production_btc', db=db)

        for i, w in enumerate(windows):
            assert w['window_index'] == i


class TestPerMetricFallbackWindows:
    """Fix 5: fallback retry loop must call select_windows per-metric, not reuse production_btc windows."""

    def test_fallback_calls_select_windows_with_each_metric(self):
        """select_windows must be called with hashrate_eh when that metric needs fallback."""
        from unittest.mock import patch, MagicMock
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        db.get_metric_rules.return_value = []
        db.mark_report_extraction_running.return_value = None
        db.get_chunks_for_report.return_value = []

        report = {
            'id': 1, 'ticker': 'MARA', 'report_date': '2024-01-31',
            'source_type': 'ir_press_release',
            'raw_text': 'produced 750 BTC, hashrate 35.0 EH/s',
        }
        registry = MagicMock()
        registry.metrics = {'production_btc': [], 'hashrate_eh': []}

        # LLM available, batch returns empty (triggers per-metric fallback)
        mock_llm = MagicMock()
        mock_llm.extract_batch.return_value = {}
        mock_llm.extract.return_value = None
        mock_llm._last_call_meta = {}

        two_windows = [
            {'text': 'window0 produced 750', 'source': 'sliding', 'window_index': 0},
            {'text': 'window1 hashrate 35', 'source': 'sliding', 'window_index': 1},
        ]

        with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=mock_llm), \
             patch('interpreters.interpret_pipeline._apply_agreement'), \
             patch('interpreters.interpret_pipeline._try_gap_fill'), \
             patch('interpreters.context_window.ContextWindowSelector.select_windows',
                   return_value=two_windows) as mock_select:
            extract_report(report, db, registry)

        # After fix, select_windows must be called with 'hashrate_eh' (not just 'production_btc')
        call_metrics = []
        for c in mock_select.call_args_list:
            # select_windows(self, report_id, raw_text, metric, db)
            args = c.args
            metric_arg = args[2] if len(args) > 2 else c.kwargs.get('metric')
            call_metrics.append(metric_arg)

        assert 'hashrate_eh' in call_metrics, (
            f"Expected 'hashrate_eh' in select_windows calls, got: {call_metrics}"
        )

    def test_fallback_does_not_use_production_btc_windows_for_hashrate(self):
        """Before fix, fallback reused production_btc windows; after fix, per-metric windows used."""
        from unittest.mock import patch, MagicMock, call
        from interpreters.interpret_pipeline import extract_report

        db = MagicMock()
        db.get_metric_rules.return_value = []
        db.mark_report_extraction_running.return_value = None
        db.get_chunks_for_report.return_value = []

        report = {
            'id': 2, 'ticker': 'MARA', 'report_date': '2024-02-29',
            'source_type': 'ir_press_release',
            'raw_text': 'produced 800 BTC, hashrate 40.0 EH/s',
        }
        registry = MagicMock()
        registry.metrics = {'hashrate_eh': []}

        mock_llm = MagicMock()
        mock_llm.extract_batch.return_value = {}
        mock_llm.extract.return_value = None
        mock_llm._last_call_meta = {}

        two_windows = [
            {'text': 'window0', 'source': 'sliding', 'window_index': 0},
            {'text': 'window1', 'source': 'sliding', 'window_index': 1},
        ]

        with patch('interpreters.interpret_pipeline._build_regex_by_metric', return_value={}), \
             patch('interpreters.interpret_pipeline._check_llm_available', return_value=True), \
             patch('interpreters.interpret_pipeline._get_llm_interpreter', return_value=mock_llm), \
             patch('interpreters.interpret_pipeline._apply_agreement'), \
             patch('interpreters.interpret_pipeline._try_gap_fill'), \
             patch('interpreters.context_window.ContextWindowSelector.select_windows',
                   return_value=two_windows) as mock_select:
            extract_report(report, db, registry)

        # For a hashrate_eh-only registry, the per-metric fallback should call select_windows
        # with 'hashrate_eh', not just the initial 'production_btc' proxy call.
        call_metrics = []
        for c in mock_select.call_args_list:
            args = c.args
            metric_arg = args[2] if len(args) > 2 else c.kwargs.get('metric')
            call_metrics.append(metric_arg)

        assert 'hashrate_eh' in call_metrics, (
            f"select_windows not called with 'hashrate_eh'. Calls: {call_metrics}"
        )
