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
