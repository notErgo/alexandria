"""
Tests for coverage_bridge.py gap-fill logic.
Written before implementation — these tests define the expected contract.
"""
import pytest
from unittest.mock import MagicMock, call


class TestMonthToQuarter:
    def test_month_to_quarter_jan(self):
        from coverage_bridge import month_to_quarter
        assert month_to_quarter("2025-01-01") == "2025-Q1"

    def test_month_to_quarter_mar(self):
        from coverage_bridge import month_to_quarter
        assert month_to_quarter("2025-03-01") == "2025-Q1"

    def test_month_to_quarter_apr(self):
        from coverage_bridge import month_to_quarter
        assert month_to_quarter("2025-04-01") == "2025-Q2"

    def test_month_to_quarter_dec(self):
        from coverage_bridge import month_to_quarter
        assert month_to_quarter("2025-12-01") == "2025-Q4"


class TestQuarterMonths:
    def test_quarter_months_q1(self):
        from coverage_bridge import quarter_months
        months = quarter_months("2025-Q1")
        assert months == ["2025-01-01", "2025-02-01", "2025-03-01"]

    def test_quarter_months_q4(self):
        from coverage_bridge import quarter_months
        months = quarter_months("2025-Q4")
        assert months == ["2025-10-01", "2025-11-01", "2025-12-01"]

    def test_annual_months(self):
        from coverage_bridge import annual_months
        months = annual_months("2024-FY")
        assert len(months) == 12
        assert months[0] == "2024-01-01"
        assert months[-1] == "2024-12-01"


class TestFlowMetricBridging:
    def _make_db(self, known_dps, quarterly_dp):
        """Build a mock DB with configurable data_points state."""
        mock_db = MagicMock()

        def data_point_exists(ticker, period, metric):
            return (ticker, period, metric) in known_dps

        def get_quarterly_data_point(ticker, covering_period, metric):
            key = (ticker, covering_period, metric)
            return quarterly_dp.get(key)

        def get_data_point_value(ticker, period, metric):
            return known_dps.get((ticker, period, metric))

        mock_db.data_point_exists.side_effect = data_point_exists
        mock_db.get_quarterly_data_point.side_effect = get_quarterly_data_point
        # get_regime_cadence returns a simple value
        mock_db.get_regime_cadence_for_period = MagicMock(return_value='monthly')
        return mock_db

    def test_flow_infer_1_missing_month(self):
        """Q=900, Jan=300, Mar=250 known -> Feb=350 inferred, method=quarterly_inferred, conf=0.65."""
        from coverage_bridge import bridge_gaps
        from miner_types import EXTRACTION_METHOD_QUARTERLY_INFERRED

        inserted = []
        mock_db = MagicMock()
        mock_db.data_point_exists.side_effect = lambda t, p, m: p in ("2025-01-01", "2025-03-01")
        mock_db.get_data_point_value.side_effect = lambda t, p, m: (
            300.0 if p == "2025-01-01" else 250.0 if p == "2025-03-01" else None
        )
        mock_db.get_quarterly_data_point.return_value = {
            'value': 900.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'monthly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        summary = bridge_gaps(
            db=mock_db,
            ticker="MARA",
            covering_period="2025-Q1",
            metric="production_btc",
        )

        # Feb should be inferred
        feb_dps = [dp for dp in inserted if dp.get('period') == '2025-02-01']
        assert len(feb_dps) == 1
        assert feb_dps[0]['value'] == pytest.approx(350.0)
        assert feb_dps[0]['extraction_method'] == EXTRACTION_METHOD_QUARTERLY_INFERRED
        assert feb_dps[0]['confidence'] == pytest.approx(0.65)

    def test_flow_carry_all_3_missing_quarterly_regime(self):
        """Q=900, quarterly regime -> Jan=Feb=Mar=300, method=quarterly_carry, conf=0.80."""
        from coverage_bridge import bridge_gaps
        from miner_types import EXTRACTION_METHOD_QUARTERLY_CARRY

        inserted = []
        mock_db = MagicMock()
        mock_db.data_point_exists.return_value = False
        mock_db.get_quarterly_data_point.return_value = {
            'value': 900.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'quarterly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        summary = bridge_gaps(
            db=mock_db,
            ticker="WULF",
            covering_period="2025-Q1",
            metric="production_btc",
        )

        assert len(inserted) == 3
        for dp in inserted:
            assert dp['value'] == pytest.approx(300.0)
            assert dp['extraction_method'] == EXTRACTION_METHOD_QUARTERLY_CARRY
            assert dp['confidence'] == pytest.approx(0.80)

    def test_flow_carry_2_missing_monthly_regime(self):
        """Q=900, Jan=300 known, monthly regime -> Feb+Mar=300 each (carry since only 1 known)."""
        from coverage_bridge import bridge_gaps
        from miner_types import EXTRACTION_METHOD_QUARTERLY_CARRY

        inserted = []
        mock_db = MagicMock()
        mock_db.data_point_exists.side_effect = lambda t, p, m: p == "2025-01-01"
        mock_db.get_data_point_value.side_effect = lambda t, p, m: 300.0 if p == "2025-01-01" else None
        mock_db.get_quarterly_data_point.return_value = {
            'value': 900.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'monthly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        bridge_gaps(db=mock_db, ticker="MARA", covering_period="2025-Q1", metric="production_btc")

        # 2 months inserted (Feb + Mar), each = Q/3 = 300
        assert len(inserted) == 2
        for dp in inserted:
            assert dp['value'] == pytest.approx(300.0)
            assert dp['extraction_method'] == EXTRACTION_METHOD_QUARTERLY_CARRY


class TestSnapshotMetricBridging:
    def test_snapshot_quarterly_regime_last_month_only(self):
        """hashrate Q=50 EH/s, quarterly regime -> Mar=50, Jan/Feb untouched."""
        from coverage_bridge import bridge_gaps
        from miner_types import EXTRACTION_METHOD_QUARTERLY_CARRY

        inserted = []
        mock_db = MagicMock()
        mock_db.data_point_exists.return_value = False
        mock_db.get_quarterly_data_point.return_value = {
            'value': 50.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'quarterly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        bridge_gaps(db=mock_db, ticker="WULF", covering_period="2025-Q1", metric="hashrate_eh")

        # Only the last month of Q1 (March = 2025-03-01) gets a value
        assert len(inserted) == 1
        assert inserted[0]['period'] == '2025-03-01'
        assert inserted[0]['value'] == pytest.approx(50.0)
        assert inserted[0]['extraction_method'] == EXTRACTION_METHOD_QUARTERLY_CARRY

    def test_snapshot_monthly_regime_routes_to_review(self):
        """Missing Feb hodl_btc, monthly regime -> review_queue with needs_disaggregation."""
        from coverage_bridge import bridge_gaps

        review_items = []
        mock_db = MagicMock()
        mock_db.data_point_exists.side_effect = lambda t, p, m: p in ("2025-01-01", "2025-03-01")
        mock_db.get_quarterly_data_point.return_value = {
            'value': 13000.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'monthly'
        mock_db.insert_review_item.side_effect = lambda item: review_items.append(item)

        bridge_gaps(db=mock_db, ticker="MARA", covering_period="2025-Q1", metric="hodl_btc")

        assert len(review_items) >= 1
        assert any(item.get('agreement_status') == 'needs_disaggregation' for item in review_items)


class TestAnnualBridging:
    def test_annual_flow_carry_12_months(self):
        """FY production=3600 -> each of 12 months=300, method=annual_carry."""
        from coverage_bridge import bridge_gaps
        from miner_types import EXTRACTION_METHOD_ANNUAL_CARRY

        inserted = []
        mock_db = MagicMock()
        mock_db.data_point_exists.return_value = False
        mock_db.get_quarterly_data_point.return_value = {
            'value': 3600.0, 'covering_period': '2024-FY', 'covering_report_id': 20
        }
        mock_db.get_regime_cadence_for_period.return_value = 'quarterly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        bridge_gaps(db=mock_db, ticker="WULF", covering_period="2024-FY", metric="production_btc")

        assert len(inserted) == 12
        for dp in inserted:
            assert dp['value'] == pytest.approx(300.0)
            assert dp['extraction_method'] == EXTRACTION_METHOD_ANNUAL_CARRY


class TestBridgeAnalystProtection:
    def test_bridge_skips_analyst_protected_cells(self):
        """cell with extraction_method='analyst' must not be overwritten."""
        from coverage_bridge import bridge_gaps

        inserted = []
        mock_db = MagicMock()

        def dp_exists(ticker, period, metric):
            # Jan is protected (analyst), Feb and Mar are missing
            return period == "2025-01-01"

        def get_dp_method(ticker, period, metric):
            if period == "2025-01-01":
                return {'extraction_method': 'analyst', 'value': 300.0}
            return None

        mock_db.data_point_exists.side_effect = dp_exists
        mock_db.get_data_point_by_key = MagicMock(side_effect=get_dp_method)
        mock_db.get_quarterly_data_point.return_value = {
            'value': 900.0, 'covering_period': '2025-Q1', 'covering_report_id': 10
        }
        mock_db.get_regime_cadence_for_period.return_value = 'quarterly'
        mock_db.insert_data_point.side_effect = lambda dp: inserted.append(dp)

        bridge_gaps(db=mock_db, ticker="MARA", covering_period="2025-Q1", metric="production_btc")

        # Should NOT insert for Jan (analyst-protected)
        jan_dps = [dp for dp in inserted if dp.get('period') == '2025-01-01']
        assert len(jan_dps) == 0
