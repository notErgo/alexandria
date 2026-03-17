"""Tests for interpreters.result_router._apply_llm_result.

Covers:
- temporal_reject creates a review item (Bug A fix)
- temporal_reject does not silently drop data
"""
import pytest
from unittest.mock import MagicMock, call


def _make_report(**kwargs):
    base = {
        'id': 1,
        'ticker': 'MARA',
        'report_date': '2023-04-01',
        'source_type': 'ir_press_release',
        'raw_text': 'MARA mined 379 BTC in April 2023.',
    }
    base.update(kwargs)
    return base


def _make_llm_result(value=379.0, confidence=0.92, period_granularity='quarterly'):
    from miner_types import ExtractionResult
    return ExtractionResult(
        metric='production_btc',
        value=value,
        unit='BTC',
        confidence=confidence,
        extraction_method='llm_test',
        source_snippet='mined 379 BTC',
        pattern_id='llm_test',
        period_granularity=period_granularity,
    )


def _make_run_config(expected_granularity='monthly', force_review=False):
    from miner_types import ExtractionRunConfig
    return ExtractionRunConfig(expected_granularity=expected_granularity, force_review=force_review)


def _make_summary():
    from miner_types import ExtractionSummary
    return ExtractionSummary()


def _make_mock_db():
    db = MagicMock()
    db.data_point_exists.return_value = False
    db.get_report_metric_verdict.return_value = None
    db.get_trailing_data_points.return_value = []
    db._derive_time_grain.return_value = 'monthly'
    db.insert_review_item.return_value = 1
    db.insert_data_point.return_value = 1
    return db


class TestTemporalRejectCreatesReviewItem:
    """Fix A: temporal rejects must land in review_queue, not be silently dropped."""

    def test_temporal_reject_creates_review_item(self):
        """When LLM labels period_granularity='quarterly' but expected='monthly',
        a review item with agreement_status='TEMPORAL_REJECT' must be inserted."""
        from interpreters.result_router import _apply_llm_result

        db = _make_mock_db()
        summary = _make_summary()
        report = _make_report()
        llm_result = _make_llm_result(period_granularity='quarterly')
        run_config = _make_run_config(expected_granularity='monthly')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=run_config,
        )

        db.insert_review_item.assert_called_once()
        call_kwargs = db.insert_review_item.call_args[0][0]
        assert call_kwargs['agreement_status'] == 'TEMPORAL_REJECT'
        assert call_kwargs['ticker'] == 'MARA'
        assert call_kwargs['metric'] == 'production_btc'
        assert call_kwargs['status'] == 'PENDING'

    def test_temporal_reject_increments_counters(self):
        """temporal_rejects and review_flagged must both increment on a temporal reject."""
        from interpreters.result_router import _apply_llm_result

        db = _make_mock_db()
        summary = _make_summary()
        report = _make_report()
        llm_result = _make_llm_result(period_granularity='quarterly')
        run_config = _make_run_config(expected_granularity='monthly')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=run_config,
        )

        assert summary.temporal_rejects == 1
        assert summary.review_flagged == 1

    def test_temporal_reject_not_silently_dropped(self):
        """temporal_reject must not silently drop data: data_points_extracted==0
        but review_flagged > 0 so the safety net does not fire spuriously."""
        from interpreters.result_router import _apply_llm_result

        db = _make_mock_db()
        summary = _make_summary()
        report = _make_report()
        llm_result = _make_llm_result(period_granularity='quarterly')
        run_config = _make_run_config(expected_granularity='monthly')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=run_config,
        )

        assert summary.data_points_extracted == 0
        assert summary.review_flagged > 0
        db.insert_data_point.assert_not_called()

    def test_no_temporal_reject_when_granularity_matches(self):
        """When period_granularity matches expected, no temporal reject fires."""
        from interpreters.result_router import _apply_llm_result

        db = _make_mock_db()
        summary = _make_summary()
        report = _make_report()
        llm_result = _make_llm_result(period_granularity='monthly')
        run_config = _make_run_config(expected_granularity='monthly')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=run_config,
        )

        assert summary.temporal_rejects == 0

    def test_temporal_reject_review_item_carries_value(self):
        """The review item must carry the raw LLM value so an analyst can approve it."""
        from interpreters.result_router import _apply_llm_result

        db = _make_mock_db()
        summary = _make_summary()
        report = _make_report()
        llm_result = _make_llm_result(value=379.0, period_granularity='quarterly')
        run_config = _make_run_config(expected_granularity='monthly')

        _apply_llm_result(
            metric='production_btc',
            llm_result=llm_result,
            db=db,
            report=report,
            confidence_threshold=0.75,
            summary=summary,
            run_config=run_config,
        )

        item = db.insert_review_item.call_args[0][0]
        assert item['llm_value'] == 379.0
        assert item['raw_value'] == '379.0'
