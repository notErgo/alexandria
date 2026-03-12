"""
Tests for reviewed_periods API endpoints — TDD.

Tests FAIL before the endpoints are implemented in routes/interpret.py.
"""
import pytest
import json


@pytest.fixture
def app_with_company(db_with_company, monkeypatch):
    """Flask test app with a single MARA company pre-loaded."""
    import sys
    import os
    import app_globals
    monkeypatch.setattr(app_globals, 'get_db', lambda: db_with_company)

    import importlib
    import run_web
    importlib.reload(run_web)
    app = run_web.create_app()
    app.config['TESTING'] = True
    return app


class TestMarkReviewed:
    def test_post_reviewed_returns_201(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.post(
                '/api/interpret/MARA/reviewed',
                json={'periods': ['2024-01-01']},
            )
            assert resp.status_code == 201
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['count'] == 1

    def test_post_reviewed_bad_ticker_404(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.post(
                '/api/interpret/UNKNOWN/reviewed',
                json={'periods': ['2024-01-01']},
            )
            assert resp.status_code == 404

    def test_post_reviewed_missing_periods_400(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.post('/api/interpret/MARA/reviewed', json={})
            assert resp.status_code == 400

    def test_post_reviewed_empty_list_400(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.post('/api/interpret/MARA/reviewed', json={'periods': []})
            assert resp.status_code == 400

    def test_post_reviewed_idempotent(self, app_with_company):
        """Second POST with same period returns count=0 (INSERT OR IGNORE)."""
        with app_with_company.test_client() as c:
            c.post('/api/interpret/MARA/reviewed', json={'periods': ['2024-01-01']})
            resp = c.post('/api/interpret/MARA/reviewed', json={'periods': ['2024-01-01']})
            data = resp.get_json()
            assert data['data']['count'] == 0


class TestUnmarkReviewed:
    def test_delete_reviewed_period_returns_200(self, app_with_company, db_with_company):
        db_with_company.set_reviewed_periods('MARA', ['2024-01-01'])
        with app_with_company.test_client() as c:
            resp = c.delete(
                '/api/interpret/MARA/reviewed',
                json={'period': '2024-01-01'},
            )
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['success'] is True
            assert data['data']['deleted'] == 1

    def test_delete_reviewed_missing_period_400(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.delete('/api/interpret/MARA/reviewed', json={})
            assert resp.status_code == 400


class TestClearAllReviewed:
    def test_delete_all_reviewed_returns_200(self, app_with_company):
        with app_with_company.test_client() as c:
            # Seed data through the API so both POST and DELETE use the same get_db()
            c.post('/api/interpret/MARA/reviewed', json={'periods': ['2024-01-01', '2024-02-01']})
            resp = c.delete('/api/interpret/MARA/reviewed/all')
            assert resp.status_code == 200
            data = resp.get_json()
            assert data['data']['deleted'] >= 2

    def test_delete_all_reviewed_bad_ticker_404(self, app_with_company):
        with app_with_company.test_client() as c:
            resp = c.delete('/api/interpret/UNKNOWN/reviewed/all')
            assert resp.status_code == 404


class TestFallbackMetricValidation:
    """Tests for _VALID_METRICS_FALLBACK correctness in finalize endpoint.

    The reprompt/rerun-sec endpoints have been removed. LLM extraction goes
    through POST /api/operations/interpret. The _VALID_METRICS_FALLBACK concern
    now applies to the finalize() route.
    """

    @pytest.fixture
    def app_with_company_db_error(self, db_with_company, monkeypatch):
        """Flask app where get_metric_schema raises so the fallback is used."""
        import app_globals
        monkeypatch.setattr(app_globals, 'get_db', lambda: db_with_company)

        def raise_error(*a, **kw):
            raise RuntimeError("DB unavailable")
        monkeypatch.setattr(db_with_company, 'get_metric_schema', raise_error)

        import importlib
        import run_web
        importlib.reload(run_web)
        app = run_web.create_app()
        app.config['TESTING'] = True
        return app

    def test_finalize_accepts_holdings_btc_when_db_unavailable(self, app_with_company_db_error):
        """When DB unavailable, 'holdings_btc' must be in fallback (not rejected)."""
        with app_with_company_db_error.test_client() as c:
            resp = c.post(
                '/api/interpret/MARA/finalize',
                json={'values': [{'metric': 'holdings_btc', 'period': '2024-01-01', 'value': 100.0}]},
            )
            assert resp.status_code != 400 or resp.get_json().get('error', {}).get('code') != 'VALIDATION_ERROR', (
                f"holdings_btc wrongly rejected; response: {resp.get_json()}"
            )

    def test_finalize_rejects_hodl_btc(self, app_with_company_db_error):
        """Deprecated 'hodl_btc' must be rejected by fallback."""
        with app_with_company_db_error.test_client() as c:
            resp = c.post(
                '/api/interpret/MARA/finalize',
                json={'values': [{'metric': 'hodl_btc', 'period': '2024-01-01', 'value': 100.0}]},
            )
            assert resp.status_code == 400
            data = resp.get_json()
            assert data['error']['code'] == 'VALIDATION_ERROR'

    def test_reprompt_endpoint_removed(self, app_with_company):
        """The /reprompt endpoint has been removed — must return 404."""
        with app_with_company.test_client() as c:
            resp = c.post('/api/interpret/MARA/reprompt', json={})
            assert resp.status_code == 404

    def test_rerun_sec_endpoint_removed(self, app_with_company):
        """The /rerun-sec endpoint has been removed — must return 404."""
        with app_with_company.test_client() as c:
            resp = c.post('/api/interpret/MARA/rerun-sec', json={})
            assert resp.status_code == 404
