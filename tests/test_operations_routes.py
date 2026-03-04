"""
Operations panel API route tests — TDD.

Tests should FAIL before routes/operations.py is created.
"""
import pytest
import json
from infra.db import MinerDB


@pytest.fixture
def app(tmp_path):
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
    import app_globals
    db = MinerDB(str(tmp_path / 'test.db'))
    db.insert_company({
        'ticker': 'MARA',
        'name': 'MARA Holdings, Inc.',
        'tier': 1,
        'ir_url': 'https://www.marathondh.com/news',
        'pr_base_url': 'https://www.marathondh.com',
        'cik': '0001437491',
        'active': 1,
    })
    app_globals._db = db

    import importlib, run_web
    importlib.reload(run_web)
    flask_app = run_web.create_app()
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Operations queue ──────────────────────────────────────────────────────────

def test_operations_queue_returns_200(client):
    """GET /api/operations/queue returns 200 with expected structure."""
    resp = client.get('/api/operations/queue')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    queue = data['data']
    assert 'pending_extraction' in queue
    assert 'legacy_files' in queue


# ── Operations extract ────────────────────────────────────────────────────────

def test_extract_missing_ticker_returns_400(client):
    """POST /api/operations/extract with no ticker returns 400."""
    resp = client.post('/api/operations/extract', json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False
    assert 'ticker' in data['error']['message']


def test_extract_returns_task_id(client, monkeypatch):
    """POST /api/operations/extract with valid ticker returns task_id."""
    import routes.operations as ops_mod
    # Mock: prevent real extraction thread from starting
    monkeypatch.setattr(ops_mod, '_active_tickers', set())

    resp = client.post('/api/operations/extract', json={'ticker': 'MARA'})
    # Either 200 (task started) or 409 (already running)
    assert resp.status_code in (200, 409)
    if resp.status_code == 200:
        data = resp.get_json()
        assert data['success'] is True
        assert 'task_id' in data['data']


# ── Operations assign_period ──────────────────────────────────────────────────

def test_assign_period_invalid_format_returns_400(client):
    """POST /api/operations/assign_period with bad period format returns 400."""
    resp = client.post('/api/operations/assign_period', json={
        'manifest_id': 1,
        'period': '2024-01',  # wrong format (missing -01)
    })
    assert resp.status_code == 400
    data = resp.get_json()
    assert data['success'] is False


def test_assign_period_missing_manifest_id_returns_400(client):
    """POST /api/operations/assign_period with no manifest_id returns 400."""
    resp = client.post('/api/operations/assign_period', json={
        'period': '2024-01-01',
    })
    assert resp.status_code == 400


def test_assign_period_valid(client, tmp_path):
    """POST /api/operations/assign_period with valid data updates manifest."""
    import app_globals
    db = app_globals.get_db()
    manifest_id = db.upsert_asset_manifest({
        'ticker': 'MARA',
        'period': None,
        'source_type': 'archive_html',
        'file_path': '/tmp/test_undated.html',
        'filename': 'test_undated.html',
        'ingest_state': 'legacy_undated',
    })
    resp = client.post('/api/operations/assign_period', json={
        'manifest_id': manifest_id,
        'period': '2024-06-01',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert data['data']['period'] == '2024-06-01'


# ── Operations progress ───────────────────────────────────────────────────────

def test_progress_unknown_task_returns_404(client):
    """GET /api/operations/extract/<task_id>/progress for unknown task returns 404."""
    resp = client.get('/api/operations/extract/nonexistent-task-id/progress')
    assert resp.status_code == 404
