"""
Tests for stacked bar chart data transformation and API endpoint.

GET /api/dashboard/stacked?metric=production_btc&months=24
  Returns one series per company with values sorted by largest current-month total.

TDD: tests written before implementation.
"""
import pytest
from infra.db import MinerDB


# ── Pure function tests ───────────────────────────────────────────────────────

class TestSortSeriesByCurrentMonth:
    """sort_series_by_current_month: sort companies largest-first by most recent value."""

    def test_sorts_descending_by_last_non_null(self):
        from routes.dashboard import sort_series_by_current_month
        series = [
            {'ticker': 'MARA', 'values': [100.0, 200.0, 150.0]},
            {'ticker': 'RIOT', 'values': [300.0, 400.0, None]},  # last non-null = 400
            {'ticker': 'CLSK', 'values': [50.0, 60.0, 70.0]},
        ]
        result = sort_series_by_current_month(series)
        tickers = [s['ticker'] for s in result]
        assert tickers == ['RIOT', 'MARA', 'CLSK']

    def test_null_at_end_treated_as_zero(self):
        from routes.dashboard import sort_series_by_current_month
        series = [
            {'ticker': 'MARA', 'values': [100.0, None, None]},
            {'ticker': 'RIOT', 'values': [None, None, None]},
        ]
        result = sort_series_by_current_month(series)
        tickers = [s['ticker'] for s in result]
        assert tickers[0] == 'MARA'  # 100.0 > 0 (RIOT all-null)

    def test_empty_series_returns_empty(self):
        from routes.dashboard import sort_series_by_current_month
        assert sort_series_by_current_month([]) == []

    def test_single_series_unchanged(self):
        from routes.dashboard import sort_series_by_current_month
        series = [{'ticker': 'MARA', 'values': [100.0]}]
        result = sort_series_by_current_month(series)
        assert len(result) == 1
        assert result[0]['ticker'] == 'MARA'


# ── API endpoint tests ────────────────────────────────────────────────────────

@pytest.fixture
def app_with_data(db_with_company, tmp_path):
    """Flask test app with MARA + RIOT companies seeded with accepted final_data_points.

    The dashboard only reads final_data_points (analyst-accepted values).
    Raw data_points are intentionally not seeded here.
    """
    # Add RIOT company
    db_with_company.insert_company({
        'ticker': 'RIOT', 'name': 'Riot Platforms', 'tier': 1,
        'ir_url': 'https://www.riotplatforms.com/news',
        'pr_base_url': 'https://www.riotplatforms.com',
        'cik': '0001167419', 'active': 1,
    })
    # Seed MARA finalized data (3 months)
    for period, value in [('2024-10-01', 900.0), ('2024-11-01', 950.0), ('2024-12-01', 1000.0)]:
        db_with_company.upsert_final_data_point(
            ticker='MARA', period=period, metric='production_btc',
            value=value, unit='BTC', confidence=0.95, analyst_note='review_approved',
        )
    # Seed RIOT finalized data (2 months, smaller values)
    for period, value in [('2024-11-01', 500.0), ('2024-12-01', 550.0)]:
        db_with_company.upsert_final_data_point(
            ticker='RIOT', period=period, metric='production_btc',
            value=value, unit='BTC', confidence=0.92, analyst_note='review_approved',
        )

    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    app_globals._db = db_with_company

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app, db_with_company


class TestStackedBarRoute:
    def test_endpoint_exists(self, app_with_data):
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked?metric=production_btc')
        assert resp.status_code == 200

    def test_returns_success_shape(self, app_with_data):
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked?metric=production_btc')
        data = resp.get_json()
        assert data['success'] is True
        assert 'time_spine' in data['data']
        assert 'series' in data['data']
        assert 'metric' in data['data']

    def test_series_sorted_by_largest_current(self, app_with_data):
        """MARA (1000 BTC in Dec) should appear before RIOT (550 BTC in Dec)."""
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked?metric=production_btc')
        series = resp.get_json()['data']['series']
        tickers = [s['ticker'] for s in series]
        assert tickers.index('MARA') < tickers.index('RIOT')

    def test_time_spine_limited_to_months_param(self, app_with_data):
        """months=3 should return at most 3 periods."""
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked?metric=production_btc&months=3')
        spine = resp.get_json()['data']['time_spine']
        assert len(spine) <= 3

    def test_missing_metric_param_returns_400(self, app_with_data):
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked')
        assert resp.status_code == 400

    def test_unknown_metric_returns_400(self, app_with_data):
        flask_app, _ = app_with_data
        with flask_app.test_client() as client:
            resp = client.get('/api/dashboard/stacked?metric=fake_metric')
        assert resp.status_code == 400
