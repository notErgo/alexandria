"""Tests for coverage matrix helpers used by 'cli.py diagnose'."""
import pytest
from analysis.coverage import build_coverage_row, GapReason


class TestBuildCoverageRow:
    def test_no_file(self):
        row = build_coverage_row(
            ticker="RIOT",
            period="2021-02-01",
            data_points=[],
            reports=[],
            review_queue=[],
        )
        assert row.reason == GapReason.NO_FILE

    def test_file_no_extraction(self):
        row = build_coverage_row(
            ticker="RIOT",
            period="2021-06-01",
            data_points=[],
            reports=[{"period": "2021-06-01", "source_type": "archive_html"}],
            review_queue=[],
        )
        assert row.reason == GapReason.NO_EXTRACTION

    def test_low_confidence(self):
        row = build_coverage_row(
            ticker="MARA",
            period="2022-07-01",
            data_points=[],
            reports=[{"period": "2022-07-01"}],
            review_queue=[{"confidence": 0.42}],
        )
        assert row.reason == GapReason.LOW_CONFIDENCE
        assert row.max_confidence == pytest.approx(0.42)

    def test_ok(self):
        row = build_coverage_row(
            ticker="MARA",
            period="2021-05-01",
            data_points=[{"metric": "production_btc", "value": 226.6}],
            reports=[{"period": "2021-05-01"}],
            review_queue=[],
        )
        assert row.reason == GapReason.OK

    def test_ok_carries_metric_values(self):
        row = build_coverage_row(
            ticker="MARA",
            period="2021-05-01",
            data_points=[
                {"metric": "production_btc", "value": 226.6},
                {"metric": "hodl_btc", "value": 5518.0},
            ],
            reports=[{"period": "2021-05-01"}],
            review_queue=[],
        )
        assert row.reason == GapReason.OK
        assert row.values.get("production_btc") == pytest.approx(226.6)
        assert row.values.get("hodl_btc") == pytest.approx(5518.0)

    def test_low_confidence_uses_max_from_multiple_review_items(self):
        row = build_coverage_row(
            ticker="MARA",
            period="2022-07-01",
            data_points=[],
            reports=[{"period": "2022-07-01"}],
            review_queue=[{"confidence": 0.42}, {"confidence": 0.65}],
        )
        assert row.reason == GapReason.LOW_CONFIDENCE
        assert row.max_confidence == pytest.approx(0.65)

    def test_ok_overrides_review_items(self):
        """If production_btc is present in data_points, row is OK even if review items exist."""
        row = build_coverage_row(
            ticker="MARA",
            period="2021-05-01",
            data_points=[{"metric": "production_btc", "value": 226.6}],
            reports=[{"period": "2021-05-01"}],
            review_queue=[{"confidence": 0.50}],
        )
        assert row.reason == GapReason.OK

    def test_period_and_ticker_stored_on_row(self):
        row = build_coverage_row(
            ticker="RIOT",
            period="2021-04-01",
            data_points=[],
            reports=[],
            review_queue=[],
        )
        assert row.ticker == "RIOT"
        assert row.period == "2021-04-01"
