"""
Unit tests for gap_fill.py — quarterly-to-monthly inference engine.

TDD: these tests were written before the implementation.
"""
import json
import pytest


def _insert_quarterly(db, ticker, covering_period, metric, value):
    """Insert a quarterly data_point via upsert_data_point_quarterly."""
    db.upsert_data_point_quarterly({
        'report_id':          None,
        'ticker':             ticker,
        'period':             covering_period,
        'metric':             metric,
        'value':              value,
        'unit':               'BTC',
        'confidence':         0.9,
        'extraction_method':  'llm',
        'source_period_type': 'quarterly',
        'covering_period':    covering_period,
        'covering_report_id': None,
    })


def _insert_monthly(db, ticker, period, metric, value, method='llm'):
    """Insert a monthly data_point via insert_data_point."""
    db.insert_data_point({
        'report_id':         None,
        'ticker':            ticker,
        'period':            period + '-01' if len(period) == 7 else period,
        'metric':            metric,
        'value':             value,
        'unit':              'BTC',
        'confidence':        0.9,
        'extraction_method': method,
    })


# ── Test: flow metric delta inference ─────────────────────────────────────────

class TestFlowMetricDeltaInference:
    """production_btc: 2 known months + quarterly total -> infer 3rd month."""

    def test_infers_missing_month(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 900)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 1, f"Expected 1 filled row, got {result['rows']}"
        row = filled[0]
        assert row['period'] == '2023-03'
        assert row['metric'] == 'production_btc'
        assert abs(row['inferred_value'] - 300) < 0.01
        assert row['extraction_method'] == 'inferred_delta'

    def test_inference_writes_to_db(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 900)

        from interpreters.gap_fill import fill_quarterly_gaps
        fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        val = db.get_data_point_value(ticker, '2023-03-01', 'production_btc')
        assert val is not None
        assert abs(val - 300) < 0.01

    def test_inference_stores_inference_notes(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 900)

        from interpreters.gap_fill import fill_quarterly_gaps
        fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        row = db.get_data_point_by_key(ticker, '2023-03-01', 'production_btc')
        assert row is not None
        assert row.get('inference_notes') is not None
        notes = json.loads(row['inference_notes'])
        assert notes['method'] == 'quarterly_delta'
        assert notes['quarterly_value'] == 2200
        assert abs(notes['computed_value'] - 300) < 0.01

    def test_dry_run_does_not_write(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 900)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'], dry_run=True)

        assert result['filled'] == 0
        val = db.get_data_point_value(ticker, '2023-03-01', 'production_btc')
        assert val is None


# ── Test: snapshot metric endpoint propagation ────────────────────────────────

class TestSnapshotMetricEndpoint:
    """holdings_btc: missing last month of quarter -> propagate quarter-end value."""

    def test_propagates_quarter_end_to_last_month(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 13000)
        # First two months present, last missing
        _insert_monthly(db, ticker, '2023-04', 'holdings_btc', 12500)
        _insert_monthly(db, ticker, '2023-05', 'holdings_btc', 12800)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['holdings_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 1
        assert filled[0]['period'] == '2023-06'
        assert filled[0]['inferred_value'] == 13000
        assert filled[0]['extraction_method'] == 'inferred_snapshot'

    def test_snapshot_skipped_when_last_month_present(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 13000)
        _insert_monthly(db, ticker, '2023-04', 'holdings_btc', 12500)
        _insert_monthly(db, ticker, '2023-05', 'holdings_btc', 12800)
        _insert_monthly(db, ticker, '2023-06', 'holdings_btc', 13000)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['holdings_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 0


# ── Test: no-fill conditions ──────────────────────────────────────────────────

class TestNoFillConditions:
    def test_no_fill_when_all_months_present(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 700)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 800)
        _insert_monthly(db, ticker, '2023-03', 'production_btc', 700)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 0

    def test_no_fill_when_negative_delta(self, db):
        """quarterly_total < sum(known_months) -> data inconsistency -> skip."""
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 600)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 700)
        # Sum = 1300 > 1000: inferred = -300, should be skipped.

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 0
        skipped = [r for r in result['rows'] if r.get('reason') == 'negative_delta']
        assert len(skipped) == 1

    def test_no_fill_for_protected_method(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2200)
        _insert_monthly(db, ticker, '2023-01', 'production_btc', 1000)
        _insert_monthly(db, ticker, '2023-02', 'production_btc', 900)
        # Insert analyst-protected row for the missing month
        _insert_monthly(db, ticker, '2023-03', 'production_btc', 250, 'analyst')

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        # Analyst row should not be overwritten; no new filled rows
        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 0
        val = db.get_data_point_value(ticker, '2023-03-01', 'production_btc')
        assert abs(val - 250) < 0.01  # original analyst value intact


# ── Test: all months missing -> prorate ───────────────────────────────────────

class TestProrate:
    def test_all_missing_prorate(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'production_btc', 2100)
        # No monthly rows at all

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker=ticker, db=db, metrics=['production_btc'])

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 3
        for row in filled:
            assert abs(row['inferred_value'] - 700) < 0.01
            assert row['extraction_method'] == 'inferred_prorated'


# ── Test: stepwise fill mode ─────────────────────────────────────────────────

class TestStepwiseFillMode:
    """fill_mode='stepwise': all missing months in a quarter get the quarter-end value."""

    def test_fills_all_three_missing_months(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 9600)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='stepwise')

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        assert len(filled) == 3
        periods = {r['period'] for r in filled}
        assert periods == {'2023-04', '2023-05', '2023-06'}
        for row in filled:
            assert abs(row['inferred_value'] - 9600) < 0.01
            assert row['extraction_method'] == 'inferred_stepwise'

    def test_fills_only_missing_months(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 9600)
        _insert_monthly(db, ticker, '2023-04', 'holdings_btc', 9200)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='stepwise')

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        periods = {r['period'] for r in filled}
        assert '2023-04' not in periods
        assert {'2023-05', '2023-06'} == periods

    def test_stepwise_does_not_overwrite_protected(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 9600)
        _insert_monthly(db, ticker, '2023-05', 'holdings_btc', 9100, method='analyst')

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='stepwise')

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        periods = {r['period'] for r in filled}
        assert '2023-05' not in periods
        val = db.get_data_point_value(ticker, '2023-05-01', 'holdings_btc')
        assert abs(val - 9100) < 0.01

    def test_stepwise_stores_inference_notes_method(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'holdings_btc', 8000)

        from interpreters.gap_fill import fill_quarterly_gaps
        fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='stepwise')

        row = db.get_data_point_by_key(ticker, '2023-01-01', 'holdings_btc')
        notes = json.loads(row['inference_notes'])
        assert notes['method'] == 'quarterly_stepwise'


# ── Test: linear fill mode ────────────────────────────────────────────────────

class TestLinearFillMode:
    """fill_mode='linear': interpolate between previous quarter end and current quarter end."""

    def test_interpolates_across_two_quarters(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'holdings_btc', 8000)
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 9600)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='linear')

        filled = {r['period']: r for r in result['rows'] if r.get('status') == 'filled'}
        # Q1 months: no prev quarter -> stepwise fallback (all = 8000)
        assert abs(filled['2023-01']['inferred_value'] - 8000) < 0.5
        # Q2 months: interpolate from 8000 (Q1 end) to 9600 (Q2 end)
        # April (1/3): 8000 + 1600*1/3 = 8533.33
        assert abs(filled['2023-04']['inferred_value'] - 8533.33) < 1.0
        # May (2/3): 8000 + 1600*2/3 = 9066.67
        assert abs(filled['2023-05']['inferred_value'] - 9066.67) < 1.0
        # June (3/3): 9600
        assert abs(filled['2023-06']['inferred_value'] - 9600) < 0.01

    def test_linear_uses_stepwise_fallback_when_no_prev_quarter(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'holdings_btc', 8000)

        from interpreters.gap_fill import fill_quarterly_gaps
        result = fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='linear')

        filled = [r for r in result['rows'] if r.get('status') == 'filled']
        # No prev quarter -> all months get Q1 value (8000)
        for row in filled:
            assert abs(row['inferred_value'] - 8000) < 0.01

    def test_linear_method_tag_in_notes(self, db):
        ticker = 'MARA'
        _insert_quarterly(db, ticker, '2023-Q1', 'holdings_btc', 8000)
        _insert_quarterly(db, ticker, '2023-Q2', 'holdings_btc', 9600)

        from interpreters.gap_fill import fill_quarterly_gaps
        fill_quarterly_gaps(ticker, db, metrics=['holdings_btc'], fill_mode='linear')

        row = db.get_data_point_by_key(ticker, '2023-04-01', 'holdings_btc')
        notes = json.loads(row['inference_notes'])
        assert notes['method'] == 'quarterly_linear'
        assert 'prev_quarter_value' in notes


# ── Test: derive_net_balance_change ──────────────────────────────────────────

class TestDeriveNetBalanceChange:
    """Derive net_btc_balance_change from consecutive holdings_btc final values."""

    def _insert_final(self, db, ticker, period, metric, value):
        db.upsert_final_data_point(ticker, period, metric, value, unit='BTC', confidence=1.0)

    def test_derives_consecutive_months(self, db):
        ticker = 'MARA'
        self._insert_final(db, ticker, '2023-01-01', 'holdings_btc', 8000)
        self._insert_final(db, ticker, '2023-02-01', 'holdings_btc', 8500)
        self._insert_final(db, ticker, '2023-03-01', 'holdings_btc', 8200)

        from interpreters.gap_fill import derive_net_balance_change
        result = derive_net_balance_change(ticker, db)

        assert result['derived'] == 2
        rows = {r['period']: r for r in result['rows'] if r.get('status') == 'derived'}
        assert abs(rows['2023-02-01']['value'] - 500) < 0.01
        assert abs(rows['2023-03-01']['value'] - (-300)) < 0.01

    def test_derive_skips_non_consecutive_months(self, db):
        ticker = 'MARA'
        self._insert_final(db, ticker, '2023-01-01', 'holdings_btc', 8000)
        # Skip Feb — gap of 2 months
        self._insert_final(db, ticker, '2023-03-01', 'holdings_btc', 8600)

        from interpreters.gap_fill import derive_net_balance_change
        result = derive_net_balance_change(ticker, db)

        # Gap between Jan and Mar = 2 months apart -> skip
        assert result['derived'] == 0
        skipped = [r for r in result['rows'] if r.get('status') == 'skipped']
        assert len(skipped) == 1

    def test_derive_writes_to_final_data_points(self, db):
        ticker = 'MARA'
        self._insert_final(db, ticker, '2023-01-01', 'holdings_btc', 8000)
        self._insert_final(db, ticker, '2023-02-01', 'holdings_btc', 8750)

        from interpreters.gap_fill import derive_net_balance_change
        derive_net_balance_change(ticker, db)

        finals = db.get_final_data_points(ticker)
        nbc = [f for f in finals if f['metric'] == 'net_btc_balance_change' and f['period'] == '2023-02-01']
        assert len(nbc) == 1
        assert abs(nbc[0]['value'] - 750) < 0.01

    def test_derive_dry_run_does_not_write(self, db):
        ticker = 'MARA'
        self._insert_final(db, ticker, '2023-01-01', 'holdings_btc', 8000)
        self._insert_final(db, ticker, '2023-02-01', 'holdings_btc', 8750)

        from interpreters.gap_fill import derive_net_balance_change
        result = derive_net_balance_change(ticker, db, dry_run=True)

        assert result['derived'] == 0
        finals = db.get_final_data_points(ticker)
        nbc = [f for f in finals if f['metric'] == 'net_btc_balance_change']
        assert len(nbc) == 0

    def test_derive_does_not_overwrite_existing_final(self, db):
        ticker = 'MARA'
        self._insert_final(db, ticker, '2023-01-01', 'holdings_btc', 8000)
        self._insert_final(db, ticker, '2023-02-01', 'holdings_btc', 8500)
        # Analyst already put a value here
        self._insert_final(db, ticker, '2023-02-01', 'net_btc_balance_change', 999)

        from interpreters.gap_fill import derive_net_balance_change
        result = derive_net_balance_change(ticker, db, overwrite=False)

        finals = db.get_final_data_points(ticker)
        nbc = [f for f in finals if f['metric'] == 'net_btc_balance_change' and f['period'] == '2023-02-01']
        assert abs(nbc[0]['value'] - 999) < 0.01
        assert result['derived'] == 0


# ── Test: derive_sales_btc ───────────────────────────────────────────────────

class TestDeriveSalesBtc:
    """derive_sales_btc: prev_holdings + production - curr_holdings."""

    def _final(self, db, ticker, period, metric, value):
        db.upsert_final_data_point(ticker, period, metric, value, unit='BTC', confidence=1.0)

    def test_derives_basic_case(self, db):
        # prev_holdings=10000, production=750, curr_holdings=10500
        # expected sales = 10000 + 750 - 10500 = 250
        self._final(db, 'HIVE', '2024-01-01', 'holdings_btc', 10000)
        self._final(db, 'HIVE', '2024-02-01', 'holdings_btc', 10500)
        self._final(db, 'HIVE', '2024-02-01', 'production_btc', 750)
        from interpreters.gap_fill import derive_sales_btc
        result = derive_sales_btc('HIVE', db)
        assert result['derived'] == 1
        rows = {r['period']: r for r in result['rows'] if r.get('status') == 'derived'}
        assert abs(rows['2024-02-01']['value'] - 250) < 0.01

    def test_skips_missing_prev_holdings(self, db):
        # No holdings for Jan — cannot derive Feb
        self._final(db, 'HIVE', '2024-02-01', 'holdings_btc', 10500)
        self._final(db, 'HIVE', '2024-02-01', 'production_btc', 750)
        from interpreters.gap_fill import derive_sales_btc
        result = derive_sales_btc('HIVE', db)
        assert result['derived'] == 0

    def test_skips_negative_result(self, db):
        # production=100, holdings went UP by 500 -> sales would be -400 -> skip
        self._final(db, 'HIVE', '2024-01-01', 'holdings_btc', 10000)
        self._final(db, 'HIVE', '2024-02-01', 'holdings_btc', 10500)
        self._final(db, 'HIVE', '2024-02-01', 'production_btc', 100)
        from interpreters.gap_fill import derive_sales_btc
        result = derive_sales_btc('HIVE', db)
        skipped = [r for r in result['rows'] if r.get('reason') == 'negative_value']
        assert len(skipped) == 1
        assert result['derived'] == 0

    def test_dry_run_does_not_write(self, db):
        self._final(db, 'HIVE', '2024-01-01', 'holdings_btc', 10000)
        self._final(db, 'HIVE', '2024-02-01', 'holdings_btc', 10500)
        self._final(db, 'HIVE', '2024-02-01', 'production_btc', 750)
        from interpreters.gap_fill import derive_sales_btc
        result = derive_sales_btc('HIVE', db, dry_run=True)
        assert result['derived'] == 0
        finals = db.get_final_data_points('HIVE')
        assert not any(f['metric'] == 'sales_btc' for f in finals)

    def test_does_not_overwrite_existing(self, db):
        self._final(db, 'HIVE', '2024-01-01', 'holdings_btc', 10000)
        self._final(db, 'HIVE', '2024-02-01', 'holdings_btc', 10500)
        self._final(db, 'HIVE', '2024-02-01', 'production_btc', 750)
        self._final(db, 'HIVE', '2024-02-01', 'sales_btc', 999)  # existing
        from interpreters.gap_fill import derive_sales_btc
        result = derive_sales_btc('HIVE', db, overwrite=False)
        finals = db.get_final_data_points('HIVE')
        sales = [f for f in finals if f['metric'] == 'sales_btc' and f['period'] == '2024-02-01']
        assert abs(sales[0]['value'] - 999) < 0.01
        assert result['derived'] == 0
