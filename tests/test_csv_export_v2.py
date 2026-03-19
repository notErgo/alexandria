"""
Tests for CSV export wide/pivot format.

The export reads final_data_points (same source as /api/timeseries):
  - One row per monthly period (YYYY-MM)
  - One column per ticker
  - Quarterly and annual periods are excluded
"""
import csv
import io
import pytest
from infra.db import MinerDB


def _seed_final(db, ticker='MARA', period='2023-01-01', metric='production_btc', value=692.0):
    """Insert a row into final_data_points."""
    db.upsert_final_data_point(
        ticker=ticker,
        period=period,
        metric=metric,
        value=value,
        unit='BTC',
        confidence=0.95,
    )


@pytest.fixture
def app_with_data(db_with_company, tmp_path):
    """Flask test app with MARA company seeded (no data points yet)."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    app_globals._db = db_with_company

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app, db_with_company


class TestCsvExportV2:
    def test_csv_wide_format_header(self, app_with_data):
        """Header must be: period, then one column per ticker."""
        flask_app, db = app_with_data
        _seed_final(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        assert resp.status_code == 200
        assert 'text/csv' in resp.content_type

        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        assert reader.fieldnames[0] == 'period', f"First column must be 'period'; got {reader.fieldnames}"
        assert 'MARA' in reader.fieldnames

    def test_csv_no_long_format_columns(self, app_with_data):
        """Wide format must not include long-format columns."""
        flask_app, db = app_with_data
        _seed_final(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        long_cols = {'ticker', 'metric', 'source_url', 'llm_value', 'regex_value',
                     'agreement_status', 'extraction_method', 'confidence'}
        overlap = long_cols & set(reader.fieldnames)
        assert not overlap, f"Long-format columns found: {overlap}"

    def test_csv_value_in_ticker_column(self, app_with_data):
        """Value appears in the MARA column for the correct period."""
        flask_app, db = app_with_data
        _seed_final(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]['period'] == '2023-01'
        assert float(rows[0]['MARA']) == 692.0

    def test_csv_quarterly_periods_excluded(self, app_with_data):
        """Quarterly periods (YYYY-Qn) must not appear in the output."""
        flask_app, db = app_with_data
        _seed_final(db, period='2023-01-01', value=692.0)
        _seed_final(db, period='2023-Q1', metric='production_btc', value=999.0)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        periods = [r['period'] for r in reader]
        assert all('-Q' not in p for p in periods), f"Quarterly period found: {periods}"
        assert len(periods) == 1

    def test_csv_period_is_yyyy_mm(self, app_with_data):
        """Period column must be YYYY-MM (not YYYY-MM-DD)."""
        flask_app, db = app_with_data
        _seed_final(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        for row in reader:
            import re
            assert re.match(r'^\d{4}-\d{2}$', row['period']), \
                f"Period not YYYY-MM: {row['period']!r}"

    def test_csv_filter_by_ticker(self, app_with_data):
        """?ticker=MARA yields only a MARA column."""
        flask_app, db = app_with_data
        _seed_final(db, ticker='MARA')

        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://www.riotplatforms.com/news',
            'pr_base_url': 'https://www.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        _seed_final(db, ticker='RIOT')

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv?ticker=MARA')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        assert 'MARA' in reader.fieldnames
        assert 'RIOT' not in reader.fieldnames

    def test_csv_multi_ticker_multi_column(self, app_with_data):
        """Two tickers → two columns, one row for the shared period."""
        flask_app, db = app_with_data
        _seed_final(db, ticker='MARA', value=692.0)

        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://www.riotplatforms.com/news',
            'pr_base_url': 'https://www.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        _seed_final(db, ticker='RIOT', value=555.0)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        rows = list(reader)
        assert len(rows) == 1
        assert float(rows[0]['MARA']) == 692.0
        assert float(rows[0]['RIOT']) == 555.0

    def test_csv_period_sort_order(self, app_with_data):
        """Periods are sorted chronologically."""
        flask_app, db = app_with_data
        for period in ('2023-03-01', '2023-01-01', '2023-02-01'):
            _seed_final(db, period=period, value=100.0)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        periods = [r['period'] for r in reader]
        assert periods == sorted(periods), f"Periods not sorted: {periods}"

    def test_csv_missing_cell_is_empty(self, app_with_data):
        """Ticker with no value for a period gets an empty cell."""
        flask_app, db = app_with_data
        _seed_final(db, ticker='MARA', period='2023-01-01', value=692.0)

        db.insert_company({
            'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
            'ir_url': 'https://www.riotplatforms.com/news',
            'pr_base_url': 'https://www.riotplatforms.com',
            'cik': '0001167419', 'active': 1,
        })
        _seed_final(db, ticker='RIOT', period='2023-02-01', value=555.0)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv')
        reader = csv.DictReader(io.StringIO(resp.data.decode('utf-8')))
        rows = {r['period']: r for r in reader}
        assert rows['2023-01']['RIOT'] == ''
        assert rows['2023-02']['MARA'] == ''

    def test_csv_filename_includes_metric(self, app_with_data):
        """Filename includes the metric name when filtered."""
        flask_app, db = app_with_data
        _seed_final(db)

        with flask_app.test_client() as client:
            resp = client.get('/api/export.csv?metric=production_btc')
        assert 'production_btc' in resp.headers['Content-Disposition']
