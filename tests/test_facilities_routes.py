"""
Unit tests for facilities/btc_loans/source_audit API routes.

TDD: tests written before implementation.
"""
import json
import pytest
from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    db = MinerDB(str(tmp_path / 'test.db'))
    app_globals._db = db

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Facilities routes ─────────────────────────────────────────────────────────

class TestFacilitiesRoutes:
    def test_list_facilities_empty(self, client):
        resp = client.get('/api/facilities')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['success'] is True
        assert data['data']['facilities'] == []

    def test_create_facility_success(self, client):
        payload = {
            'ticker': 'MARA',
            'name': 'Garden City',
            'purpose': 'MINING',
            'size_mw': 200.0,
            'city': 'Garden City',
            'state': 'TX',
        }
        resp = client.post('/api/facilities', json=payload)
        assert resp.status_code == 201
        data = resp.get_json()
        assert data['success'] is True
        assert 'id' in data['data']

    def test_list_facilities_after_insert(self, client):
        client.post('/api/facilities', json={'ticker': 'MARA', 'name': 'Site A', 'purpose': 'MINING'})
        client.post('/api/facilities', json={'ticker': 'RIOT', 'name': 'Rockdale', 'purpose': 'MINING'})
        resp = client.get('/api/facilities')
        assert resp.get_json()['data']['facilities'].__len__() == 2

    def test_list_facilities_filtered_by_ticker(self, client):
        client.post('/api/facilities', json={'ticker': 'MARA', 'name': 'Site A', 'purpose': 'MINING'})
        client.post('/api/facilities', json={'ticker': 'RIOT', 'name': 'Rockdale', 'purpose': 'MINING'})
        resp = client.get('/api/facilities?ticker=mara')  # lowercase normalized
        data = resp.get_json()
        assert data['success'] is True
        assert len(data['data']['facilities']) == 1
        assert data['data']['facilities'][0]['ticker'] == 'MARA'

    def test_create_facility_missing_ticker(self, client):
        resp = client.post('/api/facilities', json={'name': 'Site A', 'purpose': 'MINING'})
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_INPUT'

    def test_create_facility_invalid_purpose(self, client):
        resp = client.post('/api/facilities', json={'ticker': 'MARA', 'name': 'Site A', 'purpose': 'DATACENTER'})
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_INPUT'

    def test_create_facility_invalid_size_mw(self, client):
        resp = client.post('/api/facilities', json={'ticker': 'MARA', 'name': 'Site A', 'purpose': 'MINING', 'size_mw': 'big'})
        assert resp.status_code == 400


# ── BTC Loans routes ──────────────────────────────────────────────────────────

class TestBtcLoansRoutes:
    def test_list_loans_requires_ticker(self, client):
        resp = client.get('/api/btc_loans')
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_PARAM'

    def test_create_and_list_loan(self, client):
        payload = {
            'ticker': 'MARA',
            'counterparty': 'Silvergate',
            'total_btc_encumbered': 3000.0,
            'as_of_date': '2023-06-30',
        }
        resp = client.post('/api/btc_loans', json=payload)
        assert resp.status_code == 201
        assert resp.get_json()['success'] is True

        resp2 = client.get('/api/btc_loans?ticker=MARA')
        data = resp2.get_json()
        assert data['success'] is True
        assert len(data['data']['loans']) == 1
        assert data['data']['loans'][0]['counterparty'] == 'Silvergate'

    def test_create_loan_missing_ticker(self, client):
        resp = client.post('/api/btc_loans', json={'total_btc_encumbered': 1000.0})
        assert resp.status_code == 400

    def test_create_loan_missing_amount(self, client):
        resp = client.post('/api/btc_loans', json={'ticker': 'MARA'})
        assert resp.status_code == 400

    def test_create_loan_negative_amount(self, client):
        resp = client.post('/api/btc_loans', json={'ticker': 'MARA', 'total_btc_encumbered': -100.0})
        assert resp.status_code == 400


# ── Source Audit routes ───────────────────────────────────────────────────────

class TestSourceAuditRoutes:
    def test_list_requires_ticker(self, client):
        resp = client.get('/api/source_audit')
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_PARAM'

    def test_upsert_and_get(self, client):
        payload = {
            'ticker': 'CORZ',
            'source_type': 'IR_PRIMARY',
            'http_status': 502,
            'status': 'DEAD',
            'notes': '502 since 2024-11',
        }
        resp = client.post('/api/source_audit', json=payload)
        assert resp.status_code == 200
        assert resp.get_json()['success'] is True

        resp2 = client.get('/api/source_audit?ticker=CORZ')
        data = resp2.get_json()
        assert data['success'] is True
        rows = data['data']['rows']
        assert len(rows) == 1
        assert rows[0]['status'] == 'DEAD'

    def test_upsert_missing_ticker(self, client):
        resp = client.post('/api/source_audit', json={'source_type': 'EDGAR', 'status': 'ACTIVE'})
        assert resp.status_code == 400

    def test_upsert_invalid_source_type(self, client):
        resp = client.post('/api/source_audit', json={
            'ticker': 'MARA', 'source_type': 'UNKNOWN', 'status': 'ACTIVE'
        })
        assert resp.status_code == 400
        assert resp.get_json()['error']['code'] == 'INVALID_INPUT'
