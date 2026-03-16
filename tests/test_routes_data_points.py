"""Tests for GET /api/data/management-inventory endpoint."""
import pytest
from helpers import make_report, make_review_item, make_data_point


@pytest.fixture
def app_management(db_with_company, monkeypatch):
    """Flask test app with seeded DB for management-inventory tests."""
    db = db_with_company
    r_id = db.insert_report(make_report())
    db.insert_review_item(make_review_item(
        ticker='MARA', period='2024-01-01', metric='production_btc',
        raw_value='700.0', status='PENDING',
    ))

    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db)

    import importlib
    import run_web
    importlib.reload(run_web)
    app = run_web.create_app()
    app.config['TESTING'] = True
    return app


class TestManagementInventoryEndpoint:
    def test_management_inventory_returns_list(self, app_management):
        """GET /api/data/management-inventory returns a list."""
        with app_management.test_client() as c:
            resp = c.get('/api/data/management-inventory')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert isinstance(data['data'], list)

    def test_management_inventory_per_ticker_counts(self, app_management):
        """Each entry has ticker, reports, data_points, review_pending, review_all, final_values."""
        with app_management.test_client() as c:
            resp = c.get('/api/data/management-inventory')
            data = resp.get_json()
            assert len(data['data']) >= 1
            entry = data['data'][0]
            for key in ('ticker', 'reports', 'data_points', 'review_pending', 'review_all', 'final_values'):
                assert key in entry, f"Missing key: {key}"

    def test_management_inventory_ticker_filter(self, app_management):
        """GET /api/data/management-inventory?ticker=MARA returns only MARA."""
        with app_management.test_client() as c:
            resp = c.get('/api/data/management-inventory?ticker=MARA')
            data = resp.get_json()
            assert data['success'] is True
            assert len(data['data']) == 1
            assert data['data'][0]['ticker'] == 'MARA'

    def test_management_inventory_mara_has_one_report(self, app_management):
        """MARA entry shows reports=1 and review_pending=1 from fixture."""
        with app_management.test_client() as c:
            resp = c.get('/api/data/management-inventory?ticker=MARA')
            data = resp.get_json()
            entry = data['data'][0]
            assert entry['reports'] == 1
            assert entry['review_pending'] == 1
